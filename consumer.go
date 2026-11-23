package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is the message processing interface for Consumer
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Consumer will automatically handle FINishing.
//
// When the returned value is non-nil Consumer will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
//	consumer.AddHandler(nsq.HandlerFunc(func(m *Message) error {
//		// handle the message
//	}))
type HandlerFunc func(message *Message) error

// HandleMessage implements the Handler interface
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// DiscoveryFilter is an interface accepted by `SetBehaviorDelegate()`
// for filtering the nsqds returned from discovery via nsqlookupd
type DiscoveryFilter interface {
	Filter([]string) []string
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Consumer specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

// ConsumerStats represents a snapshot of the state of a Consumer's connections and the messages
// it has seen
type ConsumerStats struct {
	MessagesReceived uint64
	MessagesFinished uint64
	MessagesRequeued uint64
	Connections      int
}

var instCount int64

type backoffSignal int

const (
	backoffFlag backoffSignal = iota
	continueFlag
	resumeFlag
)

// Consumer is a high-level type to consume from NSQ.
//
// A Consumer instance is supplied a Handler that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel. See: Handler/HandlerFunc
// for details on implementing the interface to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Consumer struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	totalRdyCount    int64
	maxInFlight      int32

	mtx sync.RWMutex

	logger   []logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	behaviorDelegate interface{}

	id      int64
	topic   string
	channel string
	config  Config

	rngMtx sync.Mutex
	rng    *rand.Rand

	needRDYRedistributed int32

	// generation is bumped whenever the set of connections is rebuilt
	// (new connection / reconnect / topology change). Backoff timers capture
	// the generation they were scheduled under and no-op when it is stale.
	generation int64

	backoffMtx sync.Mutex

	incomingMessages chan *Message

	// activeMessages tracks messages delivered to a handler but not yet
	// given a terminal response. It is the local occupancy used to size RDY
	// and to cancel in-flight work on Stop.
	activeMessages    map[MessageID]*Message
	activeMessagesMtx sync.Mutex

	pendingConnections map[string]*Conn
	connections        map[string]*Conn

	nsqdTCPAddrs []string

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int
	lookupdHttpClient  *http.Client

	wg              sync.WaitGroup
	runningHandlers int32
	stopFlag        int32
	connectedFlag   int32
	stopHandler     sync.Once
	exitHandler     sync.Once

	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
	exitChan chan int
}

// NewConsumer creates a new instance of Consumer for the specified topic/channel
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewConsumer the values are no longer mutable (they are copied).
func NewConsumer(topic string, channel string, config *Config) (*Consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	r := &Consumer{
		id: atomic.AddInt64(&instCount, 1),

		topic:   topic,
		channel: channel,
		config:  *config,

		logger:      make([]logger, LogLevelMax+1),
		logLvl:      LogLevelInfo,
		maxInFlight: int32(config.MaxInFlight),

		incomingMessages: make(chan *Message),

		activeMessages:     make(map[MessageID]*Message),
		pendingConnections: make(map[string]*Conn),
		connections:        make(map[string]*Conn),

		lookupdRecheckChan: make(chan int, 1),

		rng: rand.New(rand.NewSource(time.Now().UnixNano())),

		StopChan: make(chan int),
		exitChan: make(chan int),
	}

	// Set default logger for all log levels
	l := log.New(os.Stderr, "", log.Flags())
	for index := range r.logger {
		r.logger[index] = l
	}

	r.wg.Add(1)
	go r.rdyLoop()
	return r, nil
}

// Stats retrieves the current connection and message statistics for a Consumer
func (r *Consumer) Stats() *ConsumerStats {
	return &ConsumerStats{
		MessagesReceived: atomic.LoadUint64(&r.messagesReceived),
		MessagesFinished: atomic.LoadUint64(&r.messagesFinished),
		MessagesRequeued: atomic.LoadUint64(&r.messagesRequeued),
		Connections:      len(r.conns()),
	}
}

func (r *Consumer) conns() []*Conn {
	r.mtx.RLock()
	conns := make([]*Conn, 0, len(r.connections))
	for _, c := range r.connections {
		conns = append(conns, c)
	}
	r.mtx.RUnlock()
	return conns
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//	Output(calldepth int, s string) error
func (r *Consumer) SetLogger(l logger, lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	for level := range r.logger {
		r.logger[level] = l
	}
	r.logLvl = lvl
}

// SetLoggerForLevel assigns the same logger for specified `level`.
func (r *Consumer) SetLoggerForLevel(l logger, lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	r.logger[lvl] = l
}

// SetLoggerLevel sets the package logging level.
func (r *Consumer) SetLoggerLevel(lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	r.logLvl = lvl
}

func (r *Consumer) getLogger(lvl LogLevel) (logger, LogLevel) {
	r.logGuard.RLock()
	defer r.logGuard.RUnlock()

	return r.logger[lvl], r.logLvl
}

func (r *Consumer) getLogLevel() LogLevel {
	r.logGuard.RLock()
	defer r.logGuard.RUnlock()

	return r.logLvl
}

// SetBehaviorDelegate takes a type implementing one or more
// of the following interfaces that modify the behavior
// of the `Consumer`:
//
//	DiscoveryFilter
func (r *Consumer) SetBehaviorDelegate(cb interface{}) {
	matched := false

	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	r.behaviorDelegate = cb
}

// perConnMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Consumer
// is responsible for.
func (r *Consumer) perConnMaxInFlight() int64 {
	b := float64(r.getMaxInFlight())
	s := b / float64(len(r.conns()))
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this consumer are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (r *Consumer) IsStarved() bool {
	for _, conn := range r.conns() {
		threshold := int64(float64(conn.RDY()) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && !conn.IsClosing() {
			return true
		}
	}
	return false
}

func (r *Consumer) getMaxInFlight() int32 {
	return atomic.LoadInt32(&r.maxInFlight)
}

// ChangeMaxInFlight sets a new maximum number of messages this comsumer instance
// will allow in-flight, and updates all existing connections as appropriate.
//
// # For example, ChangeMaxInFlight(0) would pause message flow
//
// If already connected, it updates the reader RDY state for each connection.
func (r *Consumer) ChangeMaxInFlight(maxInFlight int) {
	if r.getMaxInFlight() == int32(maxInFlight) {
		return
	}

	atomic.StoreInt32(&r.maxInFlight, int32(maxInFlight))

	for _, c := range r.conns() {
		r.maybeUpdateRDY(c)
	}
}

// set lookupd http client
func (r *Consumer) SetLookupdHttpClient(httpclient *http.Client) {
	r.lookupdHttpClient = httpclient
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Consumer instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupd(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}
	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	parsedAddr, err := buildLookupAddr(addr, r.topic)
	if err != nil {
		return err
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	r.mtx.Lock()
	for _, x := range r.lookupdHTTPAddrs {
		if x == parsedAddr {
			r.mtx.Unlock()
			return nil
		}
	}
	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs, parsedAddr)
	if r.lookupdHttpClient == nil {
		transport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   r.config.LookupdPollTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: r.config.LookupdPollTimeout,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
		}
		r.lookupdHttpClient = &http.Client{
			Transport: transport,
			Timeout:   r.config.LookupdPollTimeout,
		}
	}

	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		r.queryLookupd()
		r.wg.Add(1)
		go r.lookupdLoop()
	}

	return nil
}

// ConnectToNSQLookupds adds multiple nsqlookupd address to the list for this Consumer instance.
//
// If adding the first address it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (r *Consumer) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	r.rngMtx.Lock()
	jitter := time.Duration(int64(r.rng.Float64() *
		r.config.LookupdPollJitter * float64(r.config.LookupdPollInterval)))
	r.rngMtx.Unlock()
	var ticker *time.Ticker

	select {
	case <-time.After(jitter):
	case <-r.exitChan:
		goto exit
	}

	ticker = time.NewTicker(r.config.LookupdPollInterval)

	for {
		select {
		case <-ticker.C:
			r.queryLookupd()
		case <-r.lookupdRecheckChan:
			r.queryLookupd()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	r.log(LogLevelInfo, "exiting lookupdLoop")
	r.wg.Done()
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
func (r *Consumer) nextLookupdEndpoint() string {
	r.mtx.RLock()
	if r.lookupdQueryIndex >= len(r.lookupdHTTPAddrs) {
		r.lookupdQueryIndex = 0
	}
	addr := r.lookupdHTTPAddrs[r.lookupdQueryIndex]
	num := len(r.lookupdHTTPAddrs)
	r.mtx.RUnlock()
	r.lookupdQueryIndex = (r.lookupdQueryIndex + 1) % num

	return addr
}

type lookupResp struct {
	Channels  []string    `json:"channels"`
	Producers []*peerInfo `json:"producers"`
	Timestamp int64       `json:"timestamp"`
}

type peerInfo struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// make an HTTP req to one of the configured nsqlookupd instances to discover
// which nsqd's provide the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (r *Consumer) queryLookupd() {
	retries := 0

retry:
	endpoint := r.nextLookupdEndpoint()

	r.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	var data lookupResp
	headers := make(http.Header)
	if r.config.AuthSecret != "" && r.config.LookupdAuthorization {
		headers.Set("Authorization", fmt.Sprintf("Bearer %s", r.config.AuthSecret))
	}
	err := apiRequestNegotiateV1(r.lookupdHttpClient, "GET", endpoint, headers, &data)
	if err != nil {
		r.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		retries++
		if retries < 3 {
			r.log(LogLevelInfo, "retrying with next nsqlookupd")
			goto retry
		}
		return
	}

	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}
	// apply filter
	if discoveryFilter, ok := r.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}
	for _, addr := range nsqdAddrs {
		err = r.ConnectToNSQD(addr)
		if err != nil && err != ErrAlreadyConnected {
			r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
			continue
		}
	}
}

// ConnectToNSQDs takes multiple nsqd addresses to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to local instance.
func (r *Consumer) ConnectToNSQDs(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQD(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectToNSQD takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (r *Consumer) ConnectToNSQD(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}

	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	// a new connection incarnation starts a fresh generation: any backoff
	// timer scheduled under an older generation must not manipulate the
	// rebuilt RDY state.
	r.mtx.Lock()
	r.generation++
	generation := r.generation
	r.mtx.Unlock()

	conn := NewConn(addr, &r.config, &consumerConnDelegate{r}, generation)
	conn.SetLoggerLevel(r.getLogLevel())
	format := fmt.Sprintf("%3d [%s/%s] (%%s)", r.id, r.topic, r.channel)
	for index := range r.logger {
		conn.SetLoggerForLevel(r.logger[index], LogLevel(index), format)
	}
	r.mtx.Lock()
	_, pendingOk := r.pendingConnections[addr]
	_, ok := r.connections[addr]
	if ok || pendingOk {
		r.mtx.Unlock()
		return ErrAlreadyConnected
	}
	r.pendingConnections[addr] = conn
	if idx := indexOf(addr, r.nsqdTCPAddrs); idx == -1 {
		r.nsqdTCPAddrs = append(r.nsqdTCPAddrs, addr)
	}
	r.mtx.Unlock()

	r.log(LogLevelInfo, "(%s) connecting to nsqd", addr)

	cleanupConnection := func() {
		r.mtx.Lock()
		delete(r.pendingConnections, addr)
		r.mtx.Unlock()
		conn.Close()
	}

	resp, err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}

	if resp != nil {
		if resp.MaxRdyCount < int64(r.getMaxInFlight()) {
			r.log(LogLevelWarning,
				"(%s) max RDY count %d < consumer max in flight %d, truncation possible",
				conn.String(), resp.MaxRdyCount, r.getMaxInFlight())
		}
	}

	cmd := Subscribe(r.topic, r.channel)
	err = conn.WriteCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s",
			conn, r.topic, r.channel, err.Error())
	}

	r.mtx.Lock()
	delete(r.pendingConnections, addr)
	r.connections[addr] = conn
	r.mtx.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	for _, c := range r.conns() {
		if c != conn {
			r.maybeUpdateRDY(c)
		}
	}
	r.maybeUpdateRDY(conn)

	return nil
}

func indexOf(n string, h []string) int {
	for i, a := range h {
		if n == a {
			return i
		}
	}
	return -1
}

// DisconnectFromNSQD closes the connection to and removes the specified
// `nsqd` address from the list
func (r *Consumer) DisconnectFromNSQD(addr string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOf(addr, r.nsqdTCPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	// slice delete
	r.nsqdTCPAddrs = append(r.nsqdTCPAddrs[:idx], r.nsqdTCPAddrs[idx+1:]...)

	pendingConn, pendingOk := r.pendingConnections[addr]
	conn, ok := r.connections[addr]

	if ok {
		conn.Close()
	} else if pendingOk {
		pendingConn.Close()
	}

	return nil
}

// DisconnectFromNSQLookupd removes the specified `nsqlookupd` address
// from the list used for periodic discovery.
func (r *Consumer) DisconnectFromNSQLookupd(addr string) error {
	parsedAddr, err := buildLookupAddr(addr, r.topic)
	if err != nil {
		return err
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOf(parsedAddr, r.lookupdHTTPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	if len(r.lookupdHTTPAddrs) == 1 {
		return fmt.Errorf("cannot disconnect from only remaining nsqlookupd HTTP address %s", addr)
	}

	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs[:idx], r.lookupdHTTPAddrs[idx+1:]...)

	return nil
}

func (r *Consumer) onConnMessage(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesReceived, 1)
	r.registerInFlight(c, msg)
	// while stopping, no new handler should pick this up; the connection's
	// own drain logic requeues/abandons it rather than blocking the read loop
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return
	}
	r.incomingMessages <- msg
}

// registerInFlight accounts for a delivered message against both the server's
// RDY credits and the consumer's local handler occupancy.
func (r *Consumer) registerInFlight(c *Conn, msg *Message) {
	// delivery consumes one of the server's RDY credits; keep the consumer's
	// bookkeeping in sync so RDY + local occupancy stays the capacity source
	// of truth and credits are replenished only as handlers free concurrency
	if c.RDY() > 0 {
		atomic.AddInt64(&r.totalRdyCount, -1)
		c.decrementRDY()
	}
	r.activeMessagesMtx.Lock()
	r.activeMessages[msg.ID] = msg
	r.activeMessagesMtx.Unlock()
}

// messageResponded forgets a message once its single terminal response has
// been queued to the connection. It is the counterpart to the registration
// done in onConnMessage and frees the local occupancy that sizes RDY.
func (r *Consumer) messageResponded(msg *Message) {
	r.activeMessagesMtx.Lock()
	delete(r.activeMessages, msg.ID)
	r.activeMessagesMtx.Unlock()
}

// inFlightCount is the local occupancy: messages delivered to this consumer
// that have not yet received a terminal response. Unlike the server-side RDY
// bookkeeping, this counts messages parked inside slow handlers.
func (r *Consumer) inFlightCount() int64 {
	r.activeMessagesMtx.Lock()
	n := int64(len(r.activeMessages))
	r.activeMessagesMtx.Unlock()
	return n
}

// cancelActiveMessages interrupts every still-unresponded message's context
// so handlers selecting on Message.Context().Done() can unwind during Stop.
func (r *Consumer) cancelActiveMessages() {
	r.activeMessagesMtx.Lock()
	msgs := make([]*Message, 0, len(r.activeMessages))
	for _, m := range r.activeMessages {
		msgs = append(msgs, m)
	}
	r.activeMessagesMtx.Unlock()
	for _, m := range msgs {
		m.cancel()
	}
}

// removeConnMessages forgets and cancels in-flight messages that belonged to
// a closed connection, keeping local occupancy accurate across reconnects.
func (r *Consumer) removeConnMessages(c *Conn) {
	r.activeMessagesMtx.Lock()
	var msgs []*Message
	for id, m := range r.activeMessages {
		if m.NSQDAddress == c.String() {
			delete(r.activeMessages, id)
			msgs = append(msgs, m)
		}
	}
	r.activeMessagesMtx.Unlock()
	for _, m := range msgs {
		m.cancel()
	}
}

func (r *Consumer) onConnMessageFinished(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesFinished, 1)
	r.messageResponded(msg)
}

func (r *Consumer) onConnMessageRequeued(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesRequeued, 1)
	r.messageResponded(msg)
}

func (r *Consumer) onConnResponse(c *Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		// server is ready for us to close (it ack'd our StartClose)
		// we can assume we will not receive any more messages over this channel
		// (but we can still write back responses)
		r.log(LogLevelInfo, "(%s) received CLOSE_WAIT from nsqd", c.String())
		c.Close()
	}
}

func (r *Consumer) onConnError(c *Conn, data []byte) {}

func (r *Consumer) onConnHeartbeat(c *Conn) {}

func (r *Consumer) onConnIOError(c *Conn, err error) {
	c.Close()
}

func (r *Consumer) onConnClose(c *Conn) {
	// a connection going away invalidates its pending backoff timer; make
	// sure no stale timer callback writes RDY to a dead/replaced connection
	c.stopBackoffTimer()

	// remove this connections RDY count from the consumer's total
	rdyCount := c.RDY()
	atomic.AddInt64(&r.totalRdyCount, -rdyCount)

	// any message still registered against this connection can never receive
	// a terminal response over it; drop the occupancy and interrupt handlers
	// that are selecting on their message context
	r.removeConnMessages(c)

	r.mtx.Lock()
	delete(r.connections, c.String())
	left := len(r.connections)
	r.mtx.Unlock()

	r.log(LogLevelWarning, "there are %d connections left alive", left)

	if rdyCount > 0 &&
		(int32(left) == r.getMaxInFlight() || c.InBackoff()) {
		// we're toggling out of (normal) redistribution cases and this conn
		// had a RDY count...
		//
		// trigger RDY redistribution to make sure this RDY is moved
		// to a new connection
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	// we were the last one (and stopping)
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		if left == 0 {
			r.stopHandlers()
		}
		return
	}

	r.mtx.RLock()
	numLookupd := len(r.lookupdHTTPAddrs)
	reconnect := indexOf(c.String(), r.nsqdTCPAddrs) >= 0
	r.mtx.RUnlock()
	if numLookupd > 0 {
		// trigger a poll of the lookupd
		select {
		case r.lookupdRecheckChan <- 1:
		default:
		}
	} else if reconnect {
		// there are no lookupd and we still have this nsqd TCP address in our list...
		// try to reconnect after a bit
		go func(addr string) {
			for {
				r.log(LogLevelInfo, "(%s) re-connecting in %s", addr, r.config.LookupdPollInterval)
				time.Sleep(r.config.LookupdPollInterval)
				if atomic.LoadInt32(&r.stopFlag) == 1 {
					break
				}
				r.mtx.RLock()
				reconnect := indexOf(addr, r.nsqdTCPAddrs) >= 0
				r.mtx.RUnlock()
				if !reconnect {
					r.log(LogLevelWarning, "(%s) skipped reconnect after removal...", addr)
					return
				}
				err := r.ConnectToNSQD(addr)
				if err != nil && err != ErrAlreadyConnected {
					r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
				break
			}
		}(c.String())
	}
}

func (r *Consumer) onConnBackoff(c *Conn) {
	r.startStopContinueBackoff(c, backoffFlag)
}

func (r *Consumer) onConnContinue(c *Conn) {
	r.startStopContinueBackoff(c, continueFlag)
}

func (r *Consumer) onConnResume(c *Conn) {
	r.startStopContinueBackoff(c, resumeFlag)
}

// startStopContinueBackoff drives the per-connection backoff machine.
//
// Backoff is scoped to the connection that produced the failed message:
// RDY on every other connection keeps flowing. Signals arriving while the
// backoff timer is ticking are coalesced (the timer's own resume re-reads the
// counter), preventing a burst of failures from instantly saturating the
// bounded backoff duration.
func (r *Consumer) startStopContinueBackoff(conn *Conn, signal backoffSignal) {
	// serialize backoff transitions across connections, and protect the
	// per-connection timer bookkeeping
	r.backoffMtx.Lock()
	defer r.backoffMtx.Unlock()

	if conn.InBackoffTimeout() {
		// a backoff wait is already in flight; the resume callback will
		// observe the updated counter, so drop this coalesced signal
		return
	}

	counter := atomic.LoadInt32(&conn.backoff.counter)
	wasInBackoff := counter > 0
	switch signal {
	case resumeFlag:
		if counter > 0 {
			counter--
			atomic.StoreInt32(&conn.backoff.counter, counter)
		}
	case backoffFlag:
		nextBackoff := r.config.BackoffStrategy.Calculate(int(counter) + 1)
		if nextBackoff <= r.config.MaxBackoffDuration {
			counter++
			atomic.StoreInt32(&conn.backoff.counter, counter)
		}
	case continueFlag:
		// RequeueWithoutBackoff: neither escalate nor de-escalate the
		// counter/timer. The consumed RDY credit still has to be replenished
		// (one-at-a-time while in backoff) so the probe flow is not parked
		// waiting on a backoff timer that did not restart.
		if wasInBackoff {
			r.log(LogLevelDebug,
				"(%s) backoff unaffected by requeue-without-backoff, replenishing RDY 1",
				conn.String())
			r.updateRDY(conn, 1)
		}
		return
	}

	switch {
	case wasInBackoff && counter == 0:
		// exit backoff and restore the RDY this connection can actually use
		count := r.targetRDY(conn)
		r.log(LogLevelWarning,
			"(%s) exiting backoff, returning to RDY %d", conn.String(), count)
		r.updateRDY(conn, count)
	case counter > 0 && signal != resumeFlag:
		// a new failure starts or deepens backoff on this connection only
		backoffDuration := r.config.BackoffStrategy.Calculate(int(counter))
		if backoffDuration > r.config.MaxBackoffDuration {
			backoffDuration = r.config.MaxBackoffDuration
		}

		r.log(LogLevelWarning,
			"(%s) backing off for %s (backoff level %d), setting RDY 0",
			conn.String(), backoffDuration, counter)

		// stop delivery on this connection only; other ready connections
		// (and idle local handlers) keep processing messages
		r.updateRDY(conn, 0)
		r.scheduleBackoff(conn, backoffDuration)
	case counter > 0:
		// a successful probe during backoff (resumeFlag): hold the reduced
		// level and re-arm the bounded wait before probing again
		backoffDuration := r.config.BackoffStrategy.Calculate(int(counter))
		if backoffDuration > r.config.MaxBackoffDuration {
			backoffDuration = r.config.MaxBackoffDuration
		}
		r.scheduleBackoff(conn, backoffDuration)
	}
}

// scheduleBackoff arms the bounded, generation-aware backoff timer for a
// connection. Any previously pending timer is replaced.
func (r *Consumer) scheduleBackoff(conn *Conn, d time.Duration) {
	conn.backoff.timerMtx.Lock()
	defer conn.backoff.timerMtx.Unlock()
	if conn.backoff.timer != nil {
		conn.backoff.timer.Stop()
	}
	generation := conn.Generation()
	atomic.StoreInt64(&conn.backoff.duration, d.Nanoseconds())
	conn.backoff.timer = time.AfterFunc(d, func() {
		r.resumeConn(conn, generation)
	})
}

// resumeConn is the backoff timer callback. It no-ops when the connection
// incarnation it belongs to has been superseded or closed.
func (r *Consumer) resumeConn(conn *Conn, generation int64) {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		atomic.StoreInt64(&conn.backoff.duration, 0)
		return
	}
	if conn.IsClosing() || conn.Generation() != generation {
		// topology/reconnect generation change: this timer is stale
		atomic.StoreInt64(&conn.backoff.duration, 0)
		return
	}

	r.backoffMtx.Lock()
	defer r.backoffMtx.Unlock()

	atomic.StoreInt64(&conn.backoff.duration, 0)

	if !conn.InBackoff() {
		// counter was drained by successful messages while the timer raced;
		// just restore normal RDY
		r.updateRDY(conn, r.targetRDY(conn))
		return
	}

	r.log(LogLevelWarning,
		"(%s) backoff timeout expired, sending RDY 1", conn.String())

	// while in backoff only ever let 1 message at a time through
	if err := r.updateRDY(conn, 1); err != nil {
		r.log(LogLevelWarning, "(%s) error resuming RDY 1 - %s", conn.String(), err)
		r.scheduleBackoff(conn, time.Second)
	}
}

func (r *Consumer) maybeUpdateRDY(conn *Conn) {
	if conn.InBackoff() || conn.InBackoffTimeout() {
		r.log(LogLevelDebug, "(%s) skip sending RDY inBackoff:%v || inBackoffTimeout:%v",
			conn, conn.InBackoff(), conn.InBackoffTimeout())
		return
	}

	count := r.targetRDY(conn)
	r.log(LogLevelDebug, "(%s) sending RDY %d", conn, count)
	if err := r.updateRDY(conn, count); err != nil {
		r.log(LogLevelWarning, "(%s) error sending RDY %d: %v", conn, count, err)
	}
}

// targetRDY computes the RDY level a connection may hold given the actual
// local occupancy. In-flight, un-finished handlers consume capacity just
// like outstanding RDY credits, so RDY only ever replenishes genuinely idle
// concurrency and a slow handler cannot cause the server to over-deliver.
func (r *Consumer) targetRDY(c *Conn) int64 {
	maxInFlight := int64(r.getMaxInFlight())
	// never request more than this connection's fair share, and never more
	// than the idle concurrency the whole consumer currently has
	perConn := r.perConnMaxInFlight()
	idle := maxInFlight - r.inFlightCount()
	if idle < 0 {
		idle = 0
	}
	count := perConn
	if idle < count {
		count = idle
	}
	return count
}

func (r *Consumer) rdyLoop() {
	redistributeTicker := time.NewTicker(r.config.RDYRedistributeInterval)

	for {
		select {
		case <-redistributeTicker.C:
			r.redistributeRDY()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	redistributeTicker.Stop()
	r.log(LogLevelInfo, "rdyLoop exiting")
	r.wg.Done()
}

func (r *Consumer) updateRDY(c *Conn, count int64) error {
	if c.IsClosing() {
		return ErrClosing
	}

	// never exceed the nsqd's configured max RDY count
	if count > c.MaxRDY() {
		count = c.MaxRDY()
	}

	// capacity is shared by outstanding RDY credits and messages already
	// being processed locally; count the local occupancy so replenishing a
	// credit consumed by an in-flight (not yet responded) message is allowed
	rdyCount := c.RDY()
	maxPossibleRdy := int64(r.getMaxInFlight()) -
		atomic.LoadInt64(&r.totalRdyCount) - r.inFlightCount() + rdyCount
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	if maxPossibleRdy <= 0 && count > rdyCount {
		// no global headroom remains; the next terminal response (which frees
		// local occupancy) re-arms RDY, so there is nothing to retry here
		return ErrOverMaxInFlight
	}

	return r.sendRDY(c, count)
}

func (r *Consumer) sendRDY(c *Conn, count int64) error {
	if count == 0 && c.LastRDY() == 0 {
		// no need to send. It's already that RDY count
		return nil
	}

	atomic.AddInt64(&r.totalRdyCount, count-c.RDY())

	lastRDY := c.LastRDY()
	c.SetRDY(count)
	if count == lastRDY {
		return nil
	}

	err := c.WriteCommand(Ready(int(count)))
	if err != nil {
		r.log(LogLevelError, "(%s) error sending RDY %d - %s", c.String(), count, err)
		return err
	}
	return nil
}

// redistributeRDY periodically rebalances RDY across connections while
// respecting both the global MaxInFlight budget and each connection's
// backoff state. Connections parked in backoff never receive redistributed
// credits, so a failing connection cannot starve the healthy ones.
func (r *Consumer) redistributeRDY() {
	conns := r.conns()
	if len(conns) == 0 {
		return
	}

	maxInFlight := r.getMaxInFlight()
	if len(conns) > int(maxInFlight) {
		r.log(LogLevelDebug, "redistributing RDY state (%d conns > %d max_in_flight)",
			len(conns), maxInFlight)
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	for _, c := range conns {
		if c.InBackoff() && len(conns) > 1 {
			r.log(LogLevelDebug, "redistributing RDY state (%s in backoff)", c.String())
			atomic.StoreInt32(&r.needRDYRedistributed, 1)
			break
		}
	}

	if !atomic.CompareAndSwapInt32(&r.needRDYRedistributed, 1, 0) {
		return
	}

	possibleConns := make([]*Conn, 0, len(conns))
	for _, c := range conns {
		lastMsgDuration := time.Since(c.LastMessageTime())
		lastRdyDuration := time.Since(c.LastRdyTime())
		rdyCount := c.RDY()
		r.log(LogLevelDebug, "(%s) rdy: %d (last message received %s)",
			c.String(), rdyCount, lastMsgDuration)
		if rdyCount > 0 {
			if lastMsgDuration > r.config.LowRdyIdleTimeout {
				r.log(LogLevelDebug, "(%s) idle connection, giving up RDY", c.String())
				r.updateRDY(c, 0)
			} else if lastRdyDuration > r.config.LowRdyTimeout {
				r.log(LogLevelDebug, "(%s) RDY timeout, giving up RDY", c.String())
				r.updateRDY(c, 0)
			}
		}
		// backoff connections are not candidates for donated credits
		if !c.InBackoff() && !c.InBackoffTimeout() {
			possibleConns = append(possibleConns, c)
		}
	}

	// headroom is measured against local occupancy plus outstanding RDY,
	// so redistribution can never promise more than the idle concurrency
	availableMaxInFlight := int64(maxInFlight) - r.inFlightCount() - atomic.LoadInt64(&r.totalRdyCount)

	for len(possibleConns) > 0 && availableMaxInFlight > 0 {
		availableMaxInFlight--
		r.rngMtx.Lock()
		i := r.rng.Int() % len(possibleConns)
		r.rngMtx.Unlock()
		c := possibleConns[i]
		// delete
		possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
		r.log(LogLevelDebug, "(%s) redistributing RDY", c.String())
		r.updateRDY(c, c.RDY()+1)
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (r *Consumer) Stop() {
	if !atomic.CompareAndSwapInt32(&r.stopFlag, 0, 1) {
		return
	}

	r.log(LogLevelInfo, "stopping...")

	conns := r.conns()

	// revoke every unused RDY credit and silence backoff timers so no new
	// message can be delivered or re-armed while in-flight work drains
	for _, c := range conns {
		c.stopBackoffTimer()
		if err := r.updateRDY(c, 0); err != nil {
			r.log(LogLevelWarning, "(%s) error revoking RDY - %s", c.String(), err)
		}
	}

	// interrupt handlers parked on long-running work; handlers that ignore
	// the context are still bounded by the forced-exit timer below
	r.cancelActiveMessages()

	if len(conns) == 0 {
		r.stopHandlers()
	} else {
		for _, c := range conns {
			err := c.WriteCommand(StartClose())
			if err != nil {
				r.log(LogLevelError, "(%s) error sending CLS - %s", c.String(), err)
			}
		}

		time.AfterFunc(time.Second*30, func() {
			// if we've waited this long handlers are blocked on processing messages
			// so we can't just stopHandlers (if any adtl. messages were pending processing
			// we would cause a panic on channel close)
			//
			// instead, we just bypass handler closing and skip to the final exit
			r.exit()
		})
	}
}

func (r *Consumer) stopHandlers() {
	r.stopHandler.Do(func() {
		r.log(LogLevelInfo, "stopping handlers")
		close(r.incomingMessages)
	})
}

// AddHandler sets the Handler for messages received by this Consumer. This can be called
// multiple times to add additional handlers. Handler will have a 1:1 ratio to message handling goroutines.
//
// # This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (r *Consumer) AddHandler(handler Handler) {
	r.AddConcurrentHandlers(handler, 1)
}

// AddConcurrentHandlers sets the Handler for messages received by this Consumer.  It
// takes a second argument which indicates the number of goroutines to spawn for
// message handling.
//
// # This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (r *Consumer) AddConcurrentHandlers(handler Handler, concurrency int) {
	if atomic.LoadInt32(&r.connectedFlag) == 1 {
		panic("already connected")
	}

	atomic.AddInt32(&r.runningHandlers, int32(concurrency))
	for i := 0; i < concurrency; i++ {
		go r.handlerLoop(handler)
	}
}

func (r *Consumer) handlerLoop(handler Handler) {
	r.log(LogLevelDebug, "starting Handler")

	for {
		message, ok := <-r.incomingMessages
		if !ok {
			goto exit
		}

		if r.shouldFailMessage(message, handler) {
			message.Finish()
			continue
		}

		err := handler.HandleMessage(message)
		if err != nil {
			r.log(LogLevelError, "Handler returned error (%s) for msg %s", err, message.ID)
			if !message.IsAutoResponseDisabled() {
				message.Requeue(-1)
			}
			continue
		}

		if !message.IsAutoResponseDisabled() {
			message.Finish()
		}
	}

exit:
	r.log(LogLevelDebug, "stopping Handler")
	if atomic.AddInt32(&r.runningHandlers, -1) == 0 {
		r.exit()
	}
}

func (r *Consumer) shouldFailMessage(message *Message, handler interface{}) bool {
	// message passed the max number of attempts
	if r.config.MaxAttempts > 0 && message.Attempts > r.config.MaxAttempts {
		r.log(LogLevelWarning, "msg %s attempted %d times, giving up",
			message.ID, message.Attempts)

		logger, ok := handler.(FailedMessageLogger)
		if ok {
			logger.LogFailedMessage(message)
		}

		return true
	}
	return false
}

func (r *Consumer) exit() {
	r.exitHandler.Do(func() {
		close(r.exitChan)
		r.wg.Wait()
		close(r.StopChan)
	})
}

func (r *Consumer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := r.getLogger(lvl)

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d [%s/%s] %s",
		lvl, r.id, r.topic, r.channel,
		fmt.Sprintf(line, args...)))
}

func buildLookupAddr(addr, topic string) (string, error) {
	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return "", err
	}

	if u.Port() == "" {
		return "", errors.New("missing port")
	}

	if u.Path == "/" || u.Path == "" {
		u.Path = "/lookup"
	}

	v, _ := url.ParseQuery(u.RawQuery)
	v.Add("topic", topic)
	u.RawQuery = v.Encode()
	return u.String(), nil
}
