package nsq

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Types and helpers in this file use the uid430 prefix to avoid collisions
// with helpers that external/hidden test files may add to package nsq.

type uid430CountDelegate struct {
	mu       sync.Mutex
	finishes int
	requeues int
	touches  int
}

func (d *uid430CountDelegate) OnFinish(*Message) {
	d.mu.Lock()
	d.finishes++
	d.mu.Unlock()
}

func (d *uid430CountDelegate) OnRequeue(*Message, time.Duration, bool) {
	d.mu.Lock()
	d.requeues++
	d.mu.Unlock()
}

func (d *uid430CountDelegate) OnTouch(*Message) {
	d.mu.Lock()
	d.touches++
	d.mu.Unlock()
}

func (d *uid430CountDelegate) snapshot() (int, int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.finishes, d.requeues, d.touches
}

func TestUID430ConcurrentTerminalCallsAreIdempotent(t *testing.T) {
	const goroutines = 24
	for iter := 0; iter < 30; iter++ {
		msg := NewMessage(MessageID{'l', 'a', 't', 'e', byte(iter)}, []byte("x"))
		delegate := &uid430CountDelegate{}
		msg.Delegate = delegate

		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < goroutines; i++ {
			wg.Add(3)
			go func() {
				defer wg.Done()
				<-start
				msg.Finish()
			}()
			go func() {
				defer wg.Done()
				<-start
				msg.Requeue(time.Second)
			}()
			go func() {
				defer wg.Done()
				<-start
				msg.Touch()
			}()
		}
		close(start)
		wg.Wait()

		finishes, requeues, _ := delegate.snapshot()
		if finishes+requeues != 1 {
			t.Fatalf("iter %d: expected exactly one terminal call, got FIN=%d REQ=%d",
				iter, finishes, requeues)
		}
		if !msg.HasResponded() {
			t.Fatalf("iter %d: message was not marked responded", iter)
		}

		beforeFin, beforeReq, beforeTouch := delegate.snapshot()
		// Repeating either operation is side-effect free. Repeating the exact
		// winner returns nil; a conflicting terminal call may report the
		// settled state.
		msg.Finish()
		msg.Requeue(time.Second)
		msg.Touch()
		afterFin, afterReq, afterTouch := delegate.snapshot()
		if beforeFin != afterFin || beforeReq != afterReq || beforeTouch != afterTouch {
			t.Fatalf("post-terminal calls issued commands: (%d,%d,%d) -> (%d,%d,%d)",
				beforeFin, beforeReq, beforeTouch, afterFin, afterReq, afterTouch)
		}
	}
}

func TestUID430DuplicateFinishReturnsSameLocalResult(t *testing.T) {
	msg := NewMessage(MessageID{'d', 'u', 'p'}, []byte("x"))
	delegate := &uid430CountDelegate{}
	msg.Delegate = delegate

	if err := uid430Finish(msg); err != nil {
		t.Fatalf("first Finish: %v", err)
	}
	if err := uid430Finish(msg); err != nil {
		t.Fatalf("idempotent Finish: %v", err)
	}
	if err := uid430Requeue(msg, -1); !errors.Is(err, ErrAlreadyResponded) {
		t.Fatalf("conflicting Requeue should return ErrAlreadyResponded, got %v", err)
	}
	if f, r, _ := delegate.snapshot(); f != 1 || r != 0 {
		t.Fatalf("expected one OnFinish and no OnRequeue, got FIN=%d REQ=%d", f, r)
	}
}

type uid430LineServer struct {
	listener net.Listener
	lines    chan string

	mu    sync.Mutex
	conns []net.Conn
}

func newUID430LineServer(t *testing.T) *uid430LineServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &uid430LineServer{
		listener: listener,
		lines:    make(chan string, 1024),
	}
	go s.acceptLoop()
	t.Cleanup(s.close)
	return s
}

func (s *uid430LineServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.readLoop(conn)
	}
}

func (s *uid430LineServer) readLoop(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		select {
		case s.lines <- strings.TrimRight(line, "\r\n"):
		default:
		}
	}
}

func (s *uid430LineServer) close() {
	_ = s.listener.Close()
	s.mu.Lock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
}

func (s *uid430LineServer) waitLine(prefix string, timeout time.Duration) (string, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var seen []string
	for {
		select {
		case line := <-s.lines:
			seen = append(seen, line)
			if strings.HasPrefix(line, prefix) {
				return line, true
			}
		case <-deadline.C:
			return strings.Join(seen, " | "), false
		}
	}
}

func (s *uid430LineServer) assertNoLine(t *testing.T, prefix string, duration time.Duration) {
	t.Helper()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	for {
		select {
		case line := <-s.lines:
			if strings.HasPrefix(line, prefix) {
				t.Fatalf("unexpected command %q", line)
			}
		case <-deadline.C:
			return
		}
	}
}

type uid430ConnDelegate struct {
	closed     chan struct{}
	closeOnce  sync.Once
	finished   int32
	requeued   int32
	ioErrors   int32
	lastIOErr  error
	ioErrMutex sync.Mutex
}

func (d *uid430ConnDelegate) OnResponse(*Conn, []byte)          {}
func (d *uid430ConnDelegate) OnError(*Conn, []byte)             {}
func (d *uid430ConnDelegate) OnMessage(*Conn, *Message)         {}
func (d *uid430ConnDelegate) OnMessageFinished(*Conn, *Message) { atomic.AddInt32(&d.finished, 1) }
func (d *uid430ConnDelegate) OnMessageRequeued(*Conn, *Message) { atomic.AddInt32(&d.requeued, 1) }
func (d *uid430ConnDelegate) OnBackoff(*Conn)                   {}
func (d *uid430ConnDelegate) OnContinue(*Conn)                  {}
func (d *uid430ConnDelegate) OnResume(*Conn)                    {}
func (d *uid430ConnDelegate) OnHeartbeat(*Conn)                 {}
func (d *uid430ConnDelegate) OnIOError(_ *Conn, err error) {
	d.ioErrMutex.Lock()
	d.lastIOErr = err
	d.ioErrMutex.Unlock()
	atomic.AddInt32(&d.ioErrors, 1)
}
func (d *uid430ConnDelegate) OnClose(*Conn) {
	d.closeOnce.Do(func() { close(d.closed) })
}

func newUID430WiredConn(t *testing.T, server *uid430LineServer) (*Conn, *uid430ConnDelegate) {
	t.Helper()
	raw, err := net.Dial("tcp", server.listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	tcpConn := raw.(*net.TCPConn)

	delegate := &uid430ConnDelegate{closed: make(chan struct{})}
	c := NewConn(server.listener.Addr().String(), NewConfig(), delegate)
	c.conn = tcpConn
	c.r = tcpConn
	c.w = tcpConn

	c.wg.Add(1)
	go c.writeLoop()

	t.Cleanup(func() {
		c.close()
		select {
		case <-delegate.closed:
		case <-time.After(2 * time.Second):
			t.Errorf("connection close did not complete")
		}
	})
	return c, delegate
}

func trackUID430Message(c *Conn, id byte, attempts uint16) *Message {
	msg := NewMessage(MessageID{id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id}, []byte("x"))
	msg.Attempts = attempts
	msg.Delegate = &connMessageDelegate{c}
	msg.bindDelivery(c, time.Now())
	atomic.StoreInt32(&msg.accounted, 1)
	atomic.AddInt64(&c.messagesInFlight, 1)
	c.trackDelivery(msg)
	return msg
}

func waitUID430Condition(t *testing.T, timeout time.Duration, fn func() bool, format string, args ...interface{}) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf(format, args...)
}

// The public response methods preserve their historical void signatures.
// These helpers exercise the internal deterministic result path in tests.
func uid430Finish(m *Message) error {
	return m.respond(responseFinish, 0, false)
}

func uid430Requeue(m *Message, delay time.Duration) error {
	return m.respond(responseRequeue, delay, true)
}

func uid430Touch(m *Message) error {
	return m.respond(responseTouch, 0, false)
}

func TestUID430StaleGenerationFailsLocallyWithoutCommand(t *testing.T) {
	server := newUID430LineServer(t)
	c, _ := newUID430WiredConn(t, server)
	msg := trackUID430Message(c, 'g', 1)

	c.setGeneration(c.Generation() + 1)
	err := uid430Finish(msg)
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected ErrStaleGeneration, got %v", err)
	}
	if !msg.HasResponded() {
		t.Fatal("stale terminal response should settle message state")
	}
	if got := atomic.LoadInt64(&c.messagesInFlight); got != 0 {
		t.Fatalf("in-flight references not released: %d", got)
	}
	server.assertNoLine(t, "FIN", 75*time.Millisecond)

	// A duplicate remains the same local failure and still emits no command.
	if err := uid430Finish(msg); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("duplicate stale Finish changed result: %v", err)
	}
	server.assertNoLine(t, "FIN", 25*time.Millisecond)
}

func TestUID430CommandRoutedToReconnectedGenerationIsRejected(t *testing.T) {
	server := newUID430LineServer(t)
	oldConn, _ := newUID430WiredConn(t, server)
	newConn, _ := newUID430WiredConn(t, server)
	msg := trackUID430Message(oldConn, 'r', 1)

	// Simulate a late caller or address-based routing handing the old
	// delivery to the replacement connection's delegate.
	msg.Delegate = &connMessageDelegate{newConn}
	err := uid430Finish(msg)
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected routed command to fail generation match, got %v", err)
	}
	if got := atomic.LoadInt64(&oldConn.messagesInFlight); got != 0 {
		t.Fatalf("owning connection did not release reference: %d", got)
	}
	server.assertNoLine(t, "FIN", 75*time.Millisecond)
}

func TestUID430ExpiredFailsLocally(t *testing.T) {
	server := newUID430LineServer(t)
	c, _ := newUID430WiredConn(t, server)
	c.msgTimeout = time.Minute
	msg := trackUID430Message(c, 'e', 1)
	atomic.StoreInt64(&msg.deliveryDeadline, time.Now().Add(-time.Second).UnixNano())

	if err := uid430Touch(msg); !errors.Is(err, ErrMessageExpired) {
		t.Fatalf("expected expired Touch error, got %v", err)
	}
	if msg.HasResponded() {
		t.Fatal("Touch must not become the terminal response")
	}
	if err := uid430Requeue(msg, time.Second); !errors.Is(err, ErrMessageExpired) {
		t.Fatalf("expected expired Requeue error, got %v", err)
	}
	if got := atomic.LoadInt64(&c.messagesInFlight); got != 0 {
		t.Fatalf("expired terminal did not release in-flight slot: %d", got)
	}
	server.assertNoLine(t, "REQ", 75*time.Millisecond)
	server.assertNoLine(t, "TOUCH", 25*time.Millisecond)
}

func TestUID430NewAttemptSupersedesOldDelivery(t *testing.T) {
	server := newUID430LineServer(t)
	c, delegate := newUID430WiredConn(t, server)

	oldMsg := trackUID430Message(c, 's', 1)
	newMsg := trackUID430Message(c, 's', 2)
	if atomic.LoadInt32(&oldMsg.superseded) != 1 {
		t.Fatal("redelivery did not supersede old message")
	}

	if err := uid430Finish(oldMsg); !errors.Is(err, ErrAttemptMismatch) {
		t.Fatalf("old attempt should fail with attempt mismatch, got %v", err)
	}
	if _, ok := server.waitLine("FIN", 75*time.Millisecond); ok {
		t.Fatal("superseded attempt wrote FIN")
	}

	if err := uid430Finish(newMsg); err != nil {
		t.Fatalf("current attempt Finish: %v", err)
	}
	line, ok := server.waitLine("FIN", time.Second)
	if !ok {
		t.Fatalf("current attempt did not write FIN; lines: %s", line)
	}
	if !strings.Contains(line, string(newMsg.ID[:])) {
		t.Fatalf("FIN %q does not contain message ID", line)
	}
	waitUID430Condition(t, time.Second, func() bool {
		return atomic.LoadInt32(&delegate.finished) == 1
	}, "expected one finished callback")
	waitUID430Condition(t, time.Second, func() bool {
		return atomic.LoadInt64(&c.messagesInFlight) == 0
	}, "in-flight count did not reach zero")
}

func TestUID430CloseRaceReturnsAndReleasesReferences(t *testing.T) {
	server := newUID430LineServer(t)
	c, delegate := newUID430WiredConn(t, server)

	const count = 40
	msgs := make([]*Message, count)
	for i := range msgs {
		msgs[i] = trackUID430Message(c, byte(i+1), 1)
	}

	start := make(chan struct{})
	var calls sync.WaitGroup
	for _, msg := range msgs {
		for _, fn := range []func(*Message) error{
			uid430Finish,
			func(m *Message) error { return uid430Requeue(m, time.Millisecond) },
			uid430Touch,
		} {
			calls.Add(1)
			go func(m *Message, call func(*Message) error) {
				defer calls.Done()
				<-start
				_ = call(m)
			}(msg, fn)
		}
	}

	close(start)
	go func() {
		time.Sleep(time.Millisecond)
		c.close()
	}()

	done := make(chan struct{})
	go func() {
		calls.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message response call blocked during connection close race")
	}

	select {
	case <-delegate.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection OnClose did not complete")
	}
	waitUID430Condition(t, time.Second, func() bool {
		return atomic.LoadInt64(&c.messagesInFlight) == 0
	}, "in-flight references leaked: %d", atomic.LoadInt64(&c.messagesInFlight))
}

type uid430NopLogger struct{}

func (uid430NopLogger) Output(int, string) error { return nil }

type uid430MockCommand struct {
	conn int
	line string
}

type uid430MockConn struct {
	id     int
	raw    net.Conn
	writeM sync.Mutex
}

type uid430ReconnectMock struct {
	t        *testing.T
	listener net.Listener
	commands chan uid430MockCommand

	connM sync.Mutex
	conns map[int]*uid430MockConn

	recordM  sync.Mutex
	recorded []uid430MockCommand

	nextConnID uint32
}

func newUID430ReconnectMock(t *testing.T) *uid430ReconnectMock {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	m := &uid430ReconnectMock{
		t:        t,
		listener: listener,
		commands: make(chan uid430MockCommand, 1024),
		conns:    make(map[int]*uid430MockConn),
	}
	go m.acceptLoop()
	t.Cleanup(m.close)
	return m
}

func (m *uid430ReconnectMock) addr() string { return m.listener.Addr().String() }

func (m *uid430ReconnectMock) acceptLoop() {
	for {
		raw, err := m.listener.Accept()
		if err != nil {
			return
		}
		id := int(atomic.AddUint32(&m.nextConnID, 1))
		conn := &uid430MockConn{id: id, raw: raw}
		m.connM.Lock()
		m.conns[id] = conn
		m.connM.Unlock()
		go m.serve(conn)
	}
}

func (m *uid430ReconnectMock) serve(conn *uid430MockConn) {
	reader := bufio.NewReader(conn.raw)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(reader, magic); err != nil {
		return
	}
	if _, err := reader.ReadBytes('\n'); err != nil { // IDENTIFY line
		return
	}
	var sizeBytes [4]byte
	if _, err := io.ReadFull(reader, sizeBytes[:]); err != nil {
		return
	}
	size := binary.BigEndian.Uint32(sizeBytes[:])
	if _, err := io.ReadFull(reader, make([]byte, size)); err != nil {
		return
	}
	if err := conn.write(framedResponse(FrameTypeResponse, []byte("OK"))); err != nil {
		return
	}

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		command := uid430MockCommand{conn: conn.id, line: strings.TrimRight(string(line), "\r\n")}
		m.recordM.Lock()
		m.recorded = append(m.recorded, command)
		m.recordM.Unlock()
		select {
		case m.commands <- command:
		default:
		}
		if strings.HasPrefix(command.line, "CLS") {
			_ = conn.write(framedResponse(FrameTypeResponse, []byte("CLOSE_WAIT")))
		}
	}
}

func (c *uid430MockConn) write(frame []byte) error {
	c.writeM.Lock()
	defer c.writeM.Unlock()
	_, err := c.raw.Write(frame)
	return err
}

func (m *uid430ReconnectMock) writeFrame(connID int, frame []byte) {
	m.connM.Lock()
	conn := m.conns[connID]
	m.connM.Unlock()
	if conn == nil {
		m.t.Fatalf("mock connection %d does not exist", connID)
	}
	if err := conn.write(frame); err != nil {
		m.t.Fatalf("write frame to conn %d: %v", connID, err)
	}
}

func (m *uid430ReconnectMock) closeConn(connID int) {
	m.connM.Lock()
	conn := m.conns[connID]
	m.connM.Unlock()
	if conn != nil {
		_ = conn.raw.Close()
	}
}

func (m *uid430ReconnectMock) close() {
	_ = m.listener.Close()
	m.connM.Lock()
	for _, conn := range m.conns {
		_ = conn.raw.Close()
	}
	m.connM.Unlock()
}

func (m *uid430ReconnectMock) waitCommand(timeout time.Duration, match func(uid430MockCommand) bool) (uid430MockCommand, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case command := <-m.commands:
			if match(command) {
				return command, true
			}
		case <-deadline.C:
			return uid430MockCommand{}, false
		}
	}
}

func (m *uid430ReconnectMock) snapshotCommands() []uid430MockCommand {
	m.recordM.Lock()
	defer m.recordM.Unlock()
	out := make([]uid430MockCommand, len(m.recorded))
	copy(out, m.recorded)
	return out
}

type uid430ReconnectHandler struct {
	first      chan *Message
	release    chan struct{}
	secondDone chan struct{}
	secondErr  chan error
	once       sync.Once
}

func (h *uid430ReconnectHandler) HandleMessage(msg *Message) error {
	msg.DisableAutoResponse()
	if msg.Attempts == 1 {
		h.first <- msg
		<-h.release
		return nil
	}
	err := uid430Finish(msg)
	h.once.Do(func() {
		h.secondErr <- err
		close(h.secondDone)
	})
	return nil
}

func TestUID430ReconnectDoesNotWriteOldGenerationCommand(t *testing.T) {
	server := newUID430ReconnectMock(t)

	config := NewConfig()
	config.MaxInFlight = 2
	config.LookupdPollInterval = 10 * time.Millisecond
	config.HeartbeatInterval = -1
	config.MsgTimeout = time.Minute

	consumer, err := NewConsumer("late_ack_reconnect"+strconv.FormatInt(time.Now().UnixNano(), 10), "ch", config)
	if err != nil {
		t.Fatal(err)
	}
	consumer.SetLogger(uid430NopLogger{}, LogLevelError)

	handler := &uid430ReconnectHandler{
		first:      make(chan *Message, 1),
		release:    make(chan struct{}),
		secondDone: make(chan struct{}),
		secondErr:  make(chan error, 1),
	}
	consumer.AddConcurrentHandlers(handler, 2)
	if err := consumer.ConnectToNSQD(server.addr()); err != nil {
		t.Fatal(err)
	}

	if _, ok := server.waitCommand(2*time.Second, func(cmd uid430MockCommand) bool {
		return cmd.conn == 1 && strings.HasPrefix(cmd.line, "RDY ")
	}); !ok {
		t.Fatal("first connection did not send RDY")
	}
	waitUID430Condition(t, time.Second, func() bool { return len(consumer.conns()) == 1 },
		"consumer did not register first connection")
	oldConn := consumer.conns()[0]

	id := MessageID{'r', 'e', 'c', 'o', 'n', 'n', 'e', 'c', 't', 'i', 'o', 'n', 't', 'e', 's', 't'}
	first := NewMessage(id, []byte("payload"))
	first.Attempts = 1
	server.writeFrame(1, framedResponse(FrameTypeMessage, frameMessage(first)))

	var oldMsg *Message
	select {
	case oldMsg = <-handler.first:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not receive first delivery")
	}
	if oldMsg.Generation() != oldConn.Generation() {
		t.Fatalf("message generation %d != connection generation %d",
			oldMsg.Generation(), oldConn.Generation())
	}

	server.closeConn(1)
	waitUID430Condition(t, 2*time.Second, func() bool {
		return atomic.LoadInt32(&oldConn.disconnected) == 1
	}, "old connection was not marked disconnected")

	finishResult := make(chan error, 1)
	go func() { finishResult <- uid430Finish(oldMsg) }()
	select {
	case err := <-finishResult:
		if !errors.Is(err, ErrStaleDelivery) {
			t.Fatalf("late Finish after transport loss: expected ErrStaleDelivery, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late Finish blocked after connection close")
	}
	if got := atomic.LoadInt64(&oldConn.messagesInFlight); got != 0 {
		t.Fatalf("old connection leaked in-flight reference: %d", got)
	}

	// Reconnect is driven by the old connection's terminal bookkeeping.
	if _, ok := server.waitCommand(3*time.Second, func(cmd uid430MockCommand) bool {
		return cmd.conn == 2 && strings.HasPrefix(cmd.line, "RDY ")
	}); !ok {
		t.Fatal("consumer did not reconnect and send RDY on generation 2")
	}
	waitUID430Condition(t, 2*time.Second, func() bool {
		conns := consumer.conns()
		return len(conns) == 1 && conns[0] != oldConn
	}, "consumer did not replace old connection")
	newConn := consumer.conns()[0]
	if newConn.Generation() == oldConn.Generation() {
		t.Fatal("reconnected connection did not get a new generation")
	}

	redelivery := NewMessage(id, []byte("payload"))
	redelivery.Attempts = 2
	server.writeFrame(2, framedResponse(FrameTypeMessage, frameMessage(redelivery)))

	select {
	case <-handler.secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not finish redelivery")
	}
	if err := <-handler.secondErr; err != nil {
		t.Fatalf("current generation Finish failed: %v", err)
	}
	if _, ok := server.waitCommand(time.Second, func(cmd uid430MockCommand) bool {
		return cmd.conn == 2 && strings.HasPrefix(cmd.line, "FIN ")
	}); !ok {
		t.Fatal("reconnected generation did not write FIN")
	}

	// A duplicate old-generation call keeps its local failure and cannot be
	// routed to the new connection.
	if err := uid430Finish(oldMsg); !errors.Is(err, ErrStaleDelivery) {
		t.Fatalf("duplicate old-generation Finish returned %v", err)
	}

	close(handler.release)
	consumer.Stop()
	select {
	case <-consumer.StopChan:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}

	var oldFin, oldReq, newFin int
	for _, cmd := range server.snapshotCommands() {
		switch {
		case cmd.conn == 1 && strings.HasPrefix(cmd.line, "FIN "):
			oldFin++
		case cmd.conn == 1 && strings.HasPrefix(cmd.line, "REQ "):
			oldReq++
		case cmd.conn == 2 && strings.HasPrefix(cmd.line, "FIN "):
			newFin++
		}
	}
	if oldFin != 0 || oldReq != 0 {
		t.Fatalf("old connection received late terminal commands FIN=%d REQ=%d", oldFin, oldReq)
	}
	if newFin != 1 {
		t.Fatalf("expected exactly one FIN on reconnected connection, got %d", newFin)
	}
}
