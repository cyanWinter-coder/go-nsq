package nsq

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
)

// IdentifyResponse represents the metadata
// returned from an IDENTIFY command to nsqd
type IdentifyResponse struct {
	MaxRdyCount  int64 `json:"max_rdy_count"`
	TLSv1        bool  `json:"tls_v1"`
	Deflate      bool  `json:"deflate"`
	Snappy       bool  `json:"snappy"`
	AuthRequired bool  `json:"auth_required"`
	// MsgTimeout is the effective server-side message timeout (milliseconds)
	// that nsqd will apply to messages delivered on this connection.
	MsgTimeout int64 `json:"msg_timeout"`
}

// AuthResponse represents the metadata
// returned from an AUTH command to nsqd
type AuthResponse struct {
	Identity        string `json:"identity"`
	IdentityUrl     string `json:"identity_url"`
	PermissionCount int64  `json:"permission_count"`
}

type msgResponse struct {
	msg     *Message
	cmd     *Command
	success bool
	backoff bool
	// touch marks a TOUCH command. It is serialized with FIN/REQ but does not
	// retire the message or affect the in-flight count.
	touch bool
	// drop marks a response that was resolved locally (expired message or a
	// stale connection generation/attempt) and must not be written to nsqd.
	// It only releases the message's in-flight accounting.
	drop bool
}

// inFlightMessage records which delivered message instance currently owns a
// message ID, along with the generation and attempt count it was delivered
// with. This is what makes responses for stale deliveries detectable.
type inFlightMessage struct {
	// msg is only used to mark a previous highest-attempt delivery as
	// superseded. It is cleared when that delivery unregisters so the map
	// never retains a settled Message after its ref count reaches zero.
	msg        *Message
	generation uint64
	attempts   uint16
	refs       int
}

// connGeneration is incremented for every Conn instance, giving each
// connection (and therefore every message it delivers) a unique generation.
var connGeneration uint64

// Conn represents a connection to nsqd
//
// Conn exposes a set of callbacks for the
// various events that occur on a connection
type Conn struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesInFlight int64
	maxRdyCount      int64
	rdyCount         int64
	lastRdyTimestamp int64
	lastMsgTimestamp int64

	// generation uniquely identifies the delivery epoch of this connection.
	// A message records the generation it was delivered under, so responses
	// arriving after a reconnect can be identified and rejected locally.
	generation uint64

	// msgTimeout is the effective server-side message timeout for deliveries
	// on this connection (0 == the server reported none, so expiry is not
	// enforced locally).
	msgTimeout time.Duration

	// disconnected is set when readLoop exits because the connection failed
	// (as opposed to a client-initiated graceful close). Once set, late
	// message responses are resolved locally instead of being written to a
	// stream the server no longer owns.
	disconnected int32

	mtx sync.Mutex

	// inFlight tracks the message instance currently delivered for each ID.
	// The server may redeliver the same ID with a higher attempt count (for
	// example after a message timeout, or after a reconnect); a response is
	// only valid while it refers to the tracked instance.
	inFlightMtx sync.Mutex
	inFlight    map[MessageID]*inFlightMessage

	// responseDone is closed once the connection has fully drained and no
	// longer consumes msgResponseChan. Message responses select on it so a
	// late response racing with connection close always returns instead of
	// blocking forever.
	responseDone chan struct{}

	config *Config

	conn    *net.TCPConn
	tlsConn *tls.Conn
	addr    string

	delegate ConnDelegate

	logger   []logger
	logLvl   LogLevel
	logFmt   []string
	logGuard sync.RWMutex

	r io.Reader
	w io.Writer

	cmdChan         chan *Command
	msgResponseChan chan *msgResponse
	exitChan        chan int
	drainReady      chan int

	closeFlag int32
	stopper   sync.Once
	wg        sync.WaitGroup

	readLoopRunning int32
}

// NewConn returns a new Conn instance
func NewConn(addr string, config *Config, delegate ConnDelegate) *Conn {
	if !config.initialized {
		panic("Config must be created with NewConfig()")
	}
	return &Conn{
		addr: addr,

		config:   config,
		delegate: delegate,

		generation: nextConnGeneration(),

		maxRdyCount:      2500,
		lastMsgTimestamp: time.Now().UnixNano(),

		cmdChan:         make(chan *Command),
		msgResponseChan: make(chan *msgResponse),
		exitChan:        make(chan int),
		drainReady:      make(chan int),
		responseDone:    make(chan struct{}),
		inFlight:        make(map[MessageID]*inFlightMessage),

		logger: make([]logger, LogLevelMax+1),
		logFmt: make([]string, LogLevelMax+1),
	}
}

// nextConnGeneration returns a process-wide unique connection generation.
func nextConnGeneration() uint64 {
	return atomic.AddUint64(&connGeneration, 1)
}

// SetLogger assigns the logger to use as well as a level.
//
// The format parameter is expected to be a printf compatible string with
// a single %s argument.  This is useful if you want to provide additional
// context to the log messages that the connection will print, the default
// is '(%s)'.
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//	Output(calldepth int, s string)
func (c *Conn) SetLogger(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	if format == "" {
		format = "(%s)"
	}
	for level := range c.logger {
		c.logger[level] = l
		c.logFmt[level] = format
	}
	c.logLvl = lvl
}

func (c *Conn) SetLoggerForLevel(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	if format == "" {
		format = "(%s)"
	}
	c.logger[lvl] = l
	c.logFmt[lvl] = format
}

// SetLoggerLevel sets the package logging level.
func (c *Conn) SetLoggerLevel(lvl LogLevel) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	c.logLvl = lvl
}

func (c *Conn) getLogger(lvl LogLevel) (logger, LogLevel, string) {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logger[lvl], c.logLvl, c.logFmt[lvl]
}

func (c *Conn) getLogLevel() LogLevel {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logLvl
}

// Connect dials and bootstraps the nsqd connection
// (including IDENTIFY) and returns the IdentifyResponse
func (c *Conn) Connect() (*IdentifyResponse, error) {
	// Every dial starts a new delivery epoch. Even if this Conn instance is
	// reused, messages from the previous transport can no longer be
	// acknowledged over the new one.
	atomic.StoreUint64(&c.generation, nextConnGeneration())
	atomic.StoreInt32(&c.disconnected, 0)

	dialer := &net.Dialer{
		LocalAddr: c.config.LocalAddr,
		Timeout:   c.config.DialTimeout,
	}

	conn, err := dialer.Dial("tcp", c.addr)
	if err != nil {
		return nil, err
	}
	c.conn = conn.(*net.TCPConn)
	c.r = conn
	c.w = conn

	_, err = c.Write(MagicV2)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
	}

	resp, err := c.identify()
	if err != nil {
		return nil, err
	}

	if resp != nil && resp.AuthRequired {
		if c.config.AuthSecret == "" {
			c.log(LogLevelError, "Auth Required")
			return nil, errors.New("Auth Required")
		}
		err := c.auth(c.config.AuthSecret)
		if err != nil {
			c.log(LogLevelError, "Auth Failed %s", err)
			return nil, err
		}
	}

	c.wg.Add(2)
	atomic.StoreInt32(&c.readLoopRunning, 1)
	go c.readLoop()
	go c.writeLoop()
	return resp, nil
}

// Close idempotently initiates connection close
func (c *Conn) Close() error {
	atomic.StoreInt32(&c.closeFlag, 1)
	if c.conn != nil && atomic.LoadInt64(&c.messagesInFlight) == 0 {
		return c.conn.CloseRead()
	}
	return nil
}

// IsClosing indicates whether or not the
// connection is currently in the processing of
// gracefully closing
func (c *Conn) IsClosing() bool {
	return atomic.LoadInt32(&c.closeFlag) == 1
}

// Generation returns the process-wide unique generation assigned to this
// connection instance. Messages record the generation that delivered them so
// late responses can be rejected after a reconnect or generation change.
func (c *Conn) Generation() uint64 {
	return atomic.LoadUint64(&c.generation)
}

// SetGeneration overrides the generation. It is intended for tests and
// controlled reconnect bookkeeping; production code assigns generations in
// NewConn and Connect.
func (c *Conn) SetGeneration(generation uint64) {
	atomic.StoreUint64(&c.generation, generation)
}

// setGeneration is the unexported alias used by in-package tests and consumers.
func (c *Conn) setGeneration(generation uint64) {
	atomic.StoreUint64(&c.generation, generation)
}

// RDY returns the current RDY count
func (c *Conn) RDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

// LastRDY returns the previously set RDY count
func (c *Conn) LastRDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

// SetRDY stores the specified RDY count
func (c *Conn) SetRDY(rdy int64) {
	atomic.StoreInt64(&c.rdyCount, rdy)
	if rdy > 0 {
		atomic.StoreInt64(&c.lastRdyTimestamp, time.Now().UnixNano())
	}
}

// MaxRDY returns the nsqd negotiated maximum
// RDY count that it will accept for this connection
func (c *Conn) MaxRDY() int64 {
	return c.maxRdyCount
}

// LastRdyTime returns the time of the last non-zero RDY
// update for this connection
func (c *Conn) LastRdyTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastRdyTimestamp))
}

// LastMessageTime returns a time.Time representing
// the time at which the last message was received
func (c *Conn) LastMessageTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastMsgTimestamp))
}

// RemoteAddr returns the configured destination nsqd address
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// String returns the fully-qualified address
func (c *Conn) String() string {
	return c.addr
}

// Read performs a deadlined read on the underlying TCP connection
func (c *Conn) Read(p []byte) (int, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	return c.r.Read(p)
}

// Write performs a deadlined write on the underlying TCP connection
func (c *Conn) Write(p []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	return c.w.Write(p)
}

// WriteCommand is a goroutine safe method to write a Command
// to this connection, and flush.
func (c *Conn) WriteCommand(cmd *Command) error {
	c.mtx.Lock()

	_, err := cmd.WriteTo(c)
	if err != nil {
		goto exit
	}
	err = c.Flush()

exit:
	c.mtx.Unlock()
	if err != nil {
		c.log(LogLevelError, "IO error - %s", err)
		c.delegate.OnIOError(c, err)
	}
	return err
}

type flusher interface {
	Flush() error
}

// Flush writes all buffered data to the underlying TCP connection
func (c *Conn) Flush() error {
	if f, ok := c.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

func (c *Conn) identify() (*IdentifyResponse, error) {
	ci := make(map[string]interface{})
	ci["client_id"] = c.config.ClientID
	ci["hostname"] = c.config.Hostname
	ci["user_agent"] = c.config.UserAgent
	ci["short_id"] = c.config.ClientID // deprecated
	ci["long_id"] = c.config.Hostname  // deprecated
	ci["tls_v1"] = c.config.TlsV1
	ci["deflate"] = c.config.Deflate
	ci["deflate_level"] = c.config.DeflateLevel
	ci["snappy"] = c.config.Snappy
	ci["feature_negotiation"] = true
	if c.config.HeartbeatInterval == -1 {
		ci["heartbeat_interval"] = -1
	} else {
		ci["heartbeat_interval"] = int64(c.config.HeartbeatInterval / time.Millisecond)
	}
	ci["sample_rate"] = c.config.SampleRate
	ci["output_buffer_size"] = c.config.OutputBufferSize
	if c.config.OutputBufferTimeout == -1 {
		ci["output_buffer_timeout"] = -1
	} else {
		ci["output_buffer_timeout"] = int64(c.config.OutputBufferTimeout / time.Millisecond)
	}
	ci["msg_timeout"] = int64(c.config.MsgTimeout / time.Millisecond)
	ci["topology_region"] = c.config.TopologyRegion
	ci["topology_zone"] = c.config.TopologyZone
	cmd, err := Identify(ci)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	err = c.WriteCommand(cmd)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	frameType, data, err := ReadUnpackedResponse(c, c.config.MaxMsgSize)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	if frameType == FrameTypeError {
		return nil, ErrIdentify{string(data)}
	}

	// The client-requested timeout remains useful even when the server replies
	// with plain OK and no JSON capabilities. A positive server-advertised
	// value below overrides it.
	if c.config.MsgTimeout > 0 {
		c.msgTimeout = c.config.MsgTimeout
	}

	// check to see if the server was able to respond w/ capabilities
	// i.e. it was a JSON response
	if data[0] != '{' {
		return nil, nil
	}

	resp := &IdentifyResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	c.log(LogLevelDebug, "IDENTIFY response: %+v", resp)

	c.maxRdyCount = resp.MaxRdyCount
	if resp.MsgTimeout > 0 {
		c.msgTimeout = time.Duration(resp.MsgTimeout) * time.Millisecond
	}

	if resp.TLSv1 {
		c.log(LogLevelInfo, "upgrading to TLS")
		err := c.upgradeTLS(c.config.TlsConfig)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Deflate {
		c.log(LogLevelInfo, "upgrading to Deflate")
		err := c.upgradeDeflate(c.config.DeflateLevel)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Snappy {
		c.log(LogLevelInfo, "upgrading to Snappy")
		err := c.upgradeSnappy()
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	// now that connection is bootstrapped, enable read buffering
	// (and write buffering if it's not already capable of Flush())
	c.r = bufio.NewReader(c.r)
	if _, ok := c.w.(flusher); !ok {
		c.w = bufio.NewWriter(c.w)
	}

	return resp, nil
}

func (c *Conn) upgradeTLS(tlsConf *tls.Config) error {
	host, _, err := net.SplitHostPort(c.addr)
	if err != nil {
		return err
	}

	// create a local copy of the config to set ServerName for this connection
	conf := &tls.Config{}
	if tlsConf != nil {
		conf = tlsConf.Clone()
	}
	conf.ServerName = host

	c.tlsConn = tls.Client(c.conn, conf)
	err = c.tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.r = c.tlsConn
	c.w = c.tlsConn
	frameType, data, err := ReadUnpackedResponse(c, c.config.MaxMsgSize)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from TLS upgrade")
	}
	return nil
}

func (c *Conn) upgradeDeflate(level int) error {
	conn := net.Conn(c.conn)
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	fw, _ := flate.NewWriter(conn, level)
	c.r = flate.NewReader(conn)
	c.w = fw
	frameType, data, err := ReadUnpackedResponse(c, c.config.MaxMsgSize)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Deflate upgrade")
	}
	return nil
}

func (c *Conn) upgradeSnappy() error {
	conn := net.Conn(c.conn)
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	c.r = snappy.NewReader(conn)
	c.w = snappy.NewBufferedWriter(conn)
	frameType, data, err := ReadUnpackedResponse(c, c.config.MaxMsgSize)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Snappy upgrade")
	}
	return nil
}

func (c *Conn) auth(secret string) error {
	cmd, err := Auth(secret)
	if err != nil {
		return err
	}

	err = c.WriteCommand(cmd)
	if err != nil {
		return err
	}

	frameType, data, err := ReadUnpackedResponse(c, c.config.MaxMsgSize)
	if err != nil {
		return err
	}

	if frameType == FrameTypeError {
		return errors.New("Error authenticating " + string(data))
	}

	resp := &AuthResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return err
	}

	c.log(LogLevelInfo, "Auth accepted. Identity: %q %s Permissions: %d",
		resp.Identity, resp.IdentityUrl, resp.PermissionCount)

	return nil
}

func (c *Conn) readLoop() {
	delegate := &connMessageDelegate{c}
	// transportFailed distinguishes a connection that broke (where late
	// responses must be resolved locally) from a clean, client-initiated
	// close (where nsqd still accepts FIN/REQ while draining).
	transportFailed := false
	for {
		if atomic.LoadInt32(&c.closeFlag) == 1 {
			goto exit
		}

		frameType, data, err := ReadUnpackedResponse(c, c.config.MaxMsgSize)
		if err != nil {
			if err == io.EOF && atomic.LoadInt32(&c.closeFlag) == 1 {
				goto exit
			}
			if !strings.Contains(err.Error(), "use of closed network connection") {
				transportFailed = true
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
			}
			goto exit
		}

		if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
			c.log(LogLevelDebug, "heartbeat received")
			c.delegate.OnHeartbeat(c)
			err := c.WriteCommand(Nop())
			if err != nil {
				transportFailed = true
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			continue
		}

		switch frameType {
		case FrameTypeResponse:
			c.delegate.OnResponse(c, data)
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				transportFailed = true
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			msg.Delegate = delegate
			msg.NSQDAddress = c.String()
			msg.bindDelivery(c, time.Now())

			// Account for the delivery before registering it so a response can
			// never release a slot that has not been counted yet.
			atomic.AddInt64(&c.messagesInFlight, 1)
			atomic.StoreInt32(&msg.accounted, 1)
			c.trackDelivery(msg)
			atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

			c.delegate.OnMessage(c, msg)
		case FrameTypeError:
			c.log(LogLevelError, "protocol error - %s", data)
			c.delegate.OnError(c, data)
		default:
			transportFailed = true
			c.log(LogLevelError, "IO error - %s", err)
			c.delegate.OnIOError(c, fmt.Errorf("unknown frame type %d", frameType))
		}
	}

exit:
	atomic.StoreInt32(&c.readLoopRunning, 0)
	if transportFailed {
		// The transport is gone. Any message response from here on must be
		// resolved locally rather than written to a dead stream.
		atomic.StoreInt32(&c.disconnected, 1)
	}
	// start the connection close
	messagesInFlight := atomic.LoadInt64(&c.messagesInFlight)
	if messagesInFlight == 0 {
		// if we exited readLoop with no messages in flight
		// we need to explicitly trigger the close because
		// writeLoop won't
		c.close()
	} else {
		c.log(LogLevelWarning, "delaying close, %d outstanding messages", messagesInFlight)
	}
	c.wg.Done()
	c.log(LogLevelInfo, "readLoop exiting")
}

func (c *Conn) writeLoop() {
	for {
		select {
		case <-c.exitChan:
			c.log(LogLevelInfo, "breaking out of writeLoop")
			// Indicate drainReady because we will not pull any more off msgResponseChan
			close(c.drainReady)
			goto exit
		case cmd := <-c.cmdChan:
			err := c.WriteCommand(cmd)
			if err != nil {
				c.log(LogLevelError, "error sending command %s - %s", cmd, err)
				c.close()
				continue
			}
		case resp := <-c.msgResponseChan:
			if resp.touch {
				// TOUCH is serialized with FIN/REQ on this channel so it can
				// never be written after a terminal response. It does not
				// retire the message or release its in-flight slot.
				if err := c.responseError(resp.msg, time.Now()); err != nil {
					c.log(LogLevelDebug, "dropping stale TOUCH for %s: %s", resp.msg.ID, err)
					continue
				}
				err := c.WriteCommand(resp.cmd)
				if err != nil {
					c.log(LogLevelError, "error sending command %s - %s", resp.cmd, err)
					c.close()
				}
				continue
			}

			// Release the message's in-flight slot exactly once, here, even if
			// the command can't be written to nsqd.
			c.releaseMessage(resp.msg)
			msgsInFlight := atomic.LoadInt64(&c.messagesInFlight)

			writeErr := c.responseError(resp.msg, time.Now())
			if resp.drop {
				writeErr = ErrStaleDelivery
			}
			if writeErr != nil {
				// The response was rejected locally (or invalidated between
				// accept time and write time); nothing is written.
				c.log(LogLevelDebug, "dropped response for %s: %s", resp.msg.ID, writeErr)
				c.untrackDelivery(resp.msg)
				if msgsInFlight == 0 && atomic.LoadInt32(&c.closeFlag) == 1 {
					c.close()
				}
				continue
			}

			if resp.success {
				c.log(LogLevelDebug, "FIN %s", resp.msg.ID)
				c.delegate.OnMessageFinished(c, resp.msg)
				c.delegate.OnResume(c)
			} else {
				c.log(LogLevelDebug, "REQ %s", resp.msg.ID)
				c.delegate.OnMessageRequeued(c, resp.msg)
				if resp.backoff {
					c.delegate.OnBackoff(c)
				} else {
					c.delegate.OnContinue(c)
				}
			}

			err := c.WriteCommand(resp.cmd)
			// The delivery is retired once its terminal command has been
			// written (or attempted); only then is the ID free for redelivery.
			c.untrackDelivery(resp.msg)
			if err != nil {
				c.log(LogLevelError, "error sending command %s - %s", resp.cmd, err)
				c.close()
				continue
			}

			if msgsInFlight == 0 &&
				atomic.LoadInt32(&c.closeFlag) == 1 {
				c.close()
				continue
			}
		}
	}

exit:
	// Once writeLoop has stopped consuming msgResponseChan, no new response
	// can ever be written. Wake any response that is waiting on the channel so
	// it can resolve locally instead of blocking forever.
	close(c.responseDone)
	c.wg.Done()
	c.log(LogLevelInfo, "writeLoop exiting")
}

func (c *Conn) close() {
	// a "clean" connection close is orchestrated as follows:
	//
	//     1. CLOSE cmd sent to nsqd
	//     2. CLOSE_WAIT response received from nsqd
	//     3. set c.closeFlag
	//     4. readLoop() exits
	//         a. if messages-in-flight > 0 delay close()
	//             i. writeLoop() continues receiving on c.msgResponseChan chan
	//                 x. when messages-in-flight == 0 call close()
	//         b. else call close() immediately
	//     5. c.exitChan close
	//         a. writeLoop() exits
	//             i. c.drainReady close
	//     6a. launch cleanup() goroutine (we're racing with intraprocess
	//        routed messages, see comments below)
	//         a. wait on c.drainReady
	//         b. loop and receive on c.msgResponseChan chan
	//            until messages-in-flight == 0
	//            i. ensure that readLoop has exited
	//     6b. launch waitForCleanup() goroutine
	//         b. wait on waitgroup (covers readLoop() and writeLoop()
	//            and cleanup goroutine)
	//         c. underlying TCP connection close
	//         d. trigger Delegate OnClose()
	//
	c.stopper.Do(func() {
		c.log(LogLevelInfo, "beginning close")
		close(c.exitChan)
		c.conn.CloseRead()

		c.wg.Add(1)
		go c.cleanup()

		go c.waitForCleanup()
	})
}

func (c *Conn) cleanup() {
	<-c.drainReady
	ticker := time.NewTicker(100 * time.Millisecond)
	lastWarning := time.Now()
	// writeLoop has exited, drain any remaining in flight messages.
	//
	// In-flight accounting is released by whoever consumes the response
	// (writeLoop normally, this loop for anything that raced with its exit),
	// so this loop only releases slots it receives and otherwise just waits
	// for outstanding messages to resolve and for readLoop to exit.
	for {
		// we're racing with readLoop which potentially has a message
		// for handling so infinitely loop until messagesInFlight == 0
		// and readLoop has exited
		select {
		case resp := <-c.msgResponseChan:
			// A response raced with writeLoop exiting. Release its in-flight
			// slot (TOUCH never held one); the command itself can no longer be
			// written.
			if !resp.touch {
				c.untrackDelivery(resp.msg)
				c.releaseMessage(resp.msg)
			}
		case <-ticker.C:
		}
		msgsInFlight := atomic.LoadInt64(&c.messagesInFlight)
		if msgsInFlight > 0 {
			if time.Since(lastWarning) > time.Second {
				c.log(LogLevelWarning, "draining... waiting for %d messages in flight", msgsInFlight)
				lastWarning = time.Now()
			}
			continue
		}
		// until the readLoop has exited we cannot be sure that there
		// still won't be a race
		if atomic.LoadInt32(&c.readLoopRunning) == 1 {
			if time.Since(lastWarning) > time.Second {
				c.log(LogLevelWarning, "draining... readLoop still running")
				lastWarning = time.Now()
			}
			continue
		}
		goto exit
	}

exit:
	ticker.Stop()
	// no further response can be delivered on this connection; drop any
	// remaining message references so they don't outlive it
	c.clearInFlight()
	c.wg.Done()
	c.log(LogLevelInfo, "finished draining, cleanup exiting")
}

func (c *Conn) waitForCleanup() {
	// this blocks until readLoop and writeLoop
	// (and cleanup goroutine above) have exited
	c.wg.Wait()
	c.conn.CloseWrite()
	c.log(LogLevelInfo, "clean close complete")
	c.delegate.OnClose(c)
}

func (c *Conn) onMessageFinish(m *Message) error {
	if err := c.responseError(m, time.Now()); err != nil {
		return err
	}
	if c.enqueueResponse(&msgResponse{msg: m, cmd: Finish(m.ID), success: true}) {
		return nil
	}
	return ErrDeliveryClosed
}

func (c *Conn) onMessageRequeue(m *Message, delay time.Duration, backoff bool) error {
	if err := c.responseError(m, time.Now()); err != nil {
		return err
	}
	if delay == -1 {
		// linear delay, based on the attempt count the message was
		// delivered with (not a value a handler may have mutated)
		delay = c.config.DefaultRequeueDelay * time.Duration(m.responseAttempts())
		// bound the requeueDelay to configured max
		if delay > c.config.MaxRequeueDelay {
			delay = c.config.MaxRequeueDelay
		}
	}
	if c.enqueueResponse(&msgResponse{msg: m, cmd: Requeue(m.ID, delay), success: false, backoff: backoff}) {
		return nil
	}
	return ErrDeliveryClosed
}

func (c *Conn) onMessageTouch(m *Message) error {
	if err := c.responseError(m, time.Now()); err != nil {
		return err
	}
	if c.enqueueResponse(&msgResponse{msg: m, cmd: Touch(m.ID), touch: true}) {
		return nil
	}
	return ErrDeliveryClosed
}

// enqueueResponse hands a message response to the connection's writer (or the
// cleanup drain once the writer is gone).
//
// It always returns: if no consumer remains, the response is resolved locally
// and the message's in-flight slot is released here. This makes a response
// racing with connection close deterministic instead of blocking forever.
func (c *Conn) enqueueResponse(resp *msgResponse) bool {
	select {
	case c.msgResponseChan <- resp:
		return true
	case <-c.responseDone:
		if !resp.touch {
			c.untrackDelivery(resp.msg)
			c.releaseMessage(resp.msg)
		}
		return false
	}
}

// releaseMessage returns m's in-flight slot to the connection exactly once.
//
// It is safe to call from a message response and from the connection drain
// racing with it; only the first caller wins.
func (c *Conn) releaseMessage(m *Message) bool {
	if !atomic.CompareAndSwapInt32(&m.releaseOnce, 0, 1) {
		return false
	}
	if atomic.LoadInt32(&m.accounted) == 1 {
		atomic.AddInt64(&c.messagesInFlight, -1)
	}
	return true
}

// trackDelivery registers a live delivery for a message ID.
//
// NSQ may reuse an ID for an increased attempt after a timeout. Only that
// higher attempt supersedes older deliveries; distinct frames carrying the
// same attempt remain independently acknowledgeable (including historical
// clients/tests that reuse an ID without incrementing Attempts).
func (c *Conn) trackDelivery(m *Message) {
	c.inFlightMtx.Lock()
	defer c.inFlightMtx.Unlock()

	entry, ok := c.inFlight[m.ID]
	if !ok {
		c.inFlight[m.ID] = &inFlightMessage{
			msg:        m,
			generation: m.deliveryGeneration,
			attempts:   m.deliveryAttempts,
			refs:       1,
		}
		return
	}

	entry.refs++
	if m.deliveryAttempts > entry.attempts {
		if entry.msg != nil {
			atomic.StoreInt32(&entry.msg.superseded, 1)
		}
		entry.msg = m
		entry.attempts = m.deliveryAttempts
		entry.generation = m.deliveryGeneration
	}
}

// untrackDelivery releases one delivery reference. The map entry is removed
// only after every delivery instance currently registered for the ID has
// settled, allowing equal-attempt duplicate frames to finish independently.
func (c *Conn) untrackDelivery(m *Message) {
	if !atomic.CompareAndSwapInt32(&m.untrackOnce, 0, 1) {
		return
	}

	c.inFlightMtx.Lock()
	defer c.inFlightMtx.Unlock()

	entry, ok := c.inFlight[m.ID]
	if !ok {
		return
	}
	entry.refs--
	if entry.msg == m {
		entry.msg = nil
	}
	if entry.refs <= 0 {
		delete(c.inFlight, m.ID)
	}
}

// clearInFlight drops all message references once the connection can no longer
// produce responses. Outstanding messages have already been accounted for by
// the cleanup drain.
func (c *Conn) clearInFlight() {
	c.inFlightMtx.Lock()
	c.inFlight = make(map[MessageID]*inFlightMessage)
	c.inFlightMtx.Unlock()
}

// responseError reports whether a response for m may still be written to nsqd
// on this connection and, when not, classifies the local failure.
//
// It rejects responses after the transport failed, for a different connection
// generation, for a message the server has already redelivered (superseded,
// different attempt count or instance), and for a message whose local delivery
// deadline has passed.
//
// Note that a graceful close does not disable responses: nsqd keeps accepting
// FIN/REQ for in-flight messages while the client drains.
func (c *Conn) responseError(m *Message, now time.Time) error {
	// Messages constructed directly and never bound by readLoop retain the
	// historical delegate behavior instead of being rejected by bookkeeping.
	if m.deliveryGeneration == 0 && atomic.LoadInt32(&m.accounted) == 0 {
		return nil
	}
	if atomic.LoadInt32(&c.disconnected) == 1 {
		return fmt.Errorf("%w: transport closed", ErrStaleDelivery)
	}
	currentGeneration := c.Generation()
	if m.deliveryGeneration != currentGeneration {
		return fmt.Errorf("%w: generation %d does not match current %d",
			ErrStaleGeneration, m.deliveryGeneration, currentGeneration)
	}
	if atomic.LoadInt32(&m.superseded) == 1 {
		return fmt.Errorf("%w: delivery attempt %d superseded",
			ErrAttemptMismatch, m.deliveryAttempts)
	}
	if deadline := atomic.LoadInt64(&m.deliveryDeadline); deadline > 0 && now.UnixNano() > deadline {
		return fmt.Errorf("%w: delivery deadline %s passed",
			ErrMessageExpired, time.Unix(0, deadline).Format(time.RFC3339Nano))
	}

	c.inFlightMtx.Lock()
	entry, ok := c.inFlight[m.ID]
	currentAttempt := uint16(0)
	if ok {
		currentAttempt = entry.attempts
	}
	c.inFlightMtx.Unlock()
	if !ok {
		return fmt.Errorf("%w: no current in-flight delivery for message", ErrStaleDelivery)
	}
	if m.deliveryAttempts < currentAttempt {
		return fmt.Errorf("%w: attempt %d is older than current %d",
			ErrAttemptMismatch, m.deliveryAttempts, currentAttempt)
	}
	return nil
}

// acceptsResponse is the boolean form of responseError.
func (c *Conn) acceptsResponse(m *Message, now time.Time) bool {
	return c.responseError(m, now) == nil
}

// canWriteResponse is the final check performed by writeLoop just before a
// FIN/REQ/TOUCH command is written.
func (c *Conn) canWriteResponse(m *Message) bool {
	return c.responseError(m, time.Now()) == nil
}

func (c *Conn) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl, logFmt := c.getLogger(lvl)

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %s %s", lvl,
		fmt.Sprintf(logFmt, c.String()),
		fmt.Sprintf(line, args...)))
}
