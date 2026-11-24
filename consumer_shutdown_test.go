package nsq

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"
)

// newShutdownMock builds a mockNSQD that delivers one message (if body != nil)
// and stays connected until the consumer initiates a graceful close.
//
// holdOpen makes the mock acknowledge CLOSE_WAIT while keeping its side of the
// socket open (like a real nsqd waiting for in-flight handlers), which is
// required to exercise the ShutdownTimeout path deterministically.
func newShutdownMock(t *testing.T, body []byte, holdOpen bool) (*mockNSQD, *Message) {
	t.Helper()

	script := []instruction{
		// IDENTIFY
		{0, FrameTypeResponse, []byte("OK")},
		// SUB
		{0, FrameTypeResponse, []byte("OK")},
	}
	var msg *Message
	if body != nil {
		msg = NewMessage(MessageID{'s', 'h', 'u', 't', 'd', 'o', 'w', 'n', '0', '0', '0', '0', '0', '0', '0', '1'}, body)
		script = append(script, instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msg)})
	}
	// safety net: the mock normally exits on CLS long before this fires
	script = append(script, instruction{30 * time.Second, -1, []byte("exit")})

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	return newMockNSQDOpts(t, script, addr.String(), holdOpen), msg
}

// recordedCmds returns a snapshot of the commands observed by the mock.
func recordedCmds(n *mockNSQD) [][]byte {
	n.gotMtx.Lock()
	defer n.gotMtx.Unlock()
	out := make([][]byte, len(n.got))
	copy(out, n.got)
	return out
}

func newShutdownConsumer(t *testing.T, timeout time.Duration) (*Consumer, string) {
	t.Helper()

	config := NewConfig()
	config.ShutdownTimeout = timeout
	config.MaxInFlight = 1
	topicName := "test_shutdown" + strconv.Itoa(int(time.Now().UnixNano()))
	q, err := NewConsumer(topicName, "ch", config)
	if err != nil {
		t.Fatalf("NewConsumer: %s", err)
	}
	// discard logs so goroutines that outlive the test cannot panic by logging
	q.SetLogger(nullLogger, LogLevelMax)
	return q, topicName
}

// indexOfCmd returns the first recorded command with the given prefix, or -1.
func indexOfCmd(got [][]byte, prefix string) int {
	for i, line := range got {
		if len(line) >= len(prefix) && string(line[:len(prefix)]) == prefix {
			return i
		}
	}
	return -1
}

func waitForStop(t *testing.T, q *Consumer, maxWait time.Duration) {
	t.Helper()
	select {
	case <-q.StopChan:
	case <-time.After(maxWait):
		t.Fatalf("consumer did not stop within %s", maxWait)
	}
}

// TestConsumerStopRevokesRDY verifies the stop entry point first stops new
// dispatch (RDY 0) before sending the graceful close, and that a consumer whose
// handler finishes promptly shuts down well inside the configured deadline
// without canceling the handler context.
func TestConsumerStopRevokesRDY(t *testing.T) {
	n, msg := newShutdownMock(t, []byte("good"), false)
	q, topicName := newShutdownConsumer(t, 5*time.Second)

	handled := make(chan struct{})
	q.AddHandler(HandlerFunc(func(m *Message) error {
		close(handled)
		return nil
	}))
	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatalf("ConnectToNSQD: %s", err)
	}

	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("message was never handled")
	}

	start := time.Now()
	q.Stop()
	waitForStop(t, q, time.Second)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("graceful stop took %s, expected well under 1s", elapsed)
	}

	select {
	case <-n.exitChan:
	case <-time.After(time.Second):
		t.Fatal("mock nsqd did not observe the close")
	}

	got := recordedCmds(n)
	rdyZero := indexOfCmd(got, "RDY 0")
	cls := indexOfCmd(got, "CLS")
	if rdyZero == -1 {
		t.Fatalf("expected an RDY 0 revoke before close, got %v", got)
	}
	if cls == -1 {
		t.Fatalf("expected a CLS command, got %v", got)
	}
	if rdyZero > cls {
		t.Fatalf("RDY 0 (idx %d) must be sent before CLS (idx %d): %v", rdyZero, cls, got)
	}
	if stats := q.Stats(); stats.Connections != 0 {
		t.Fatalf("expected 0 connections after stop, got %d", stats.Connections)
	}
	if q.stopCtx.Err() != nil {
		t.Fatal("handler context should not be canceled on a clean graceful stop")
	}
	_ = msg
	_ = topicName
}

// TestConsumerStopLongHandlerWithinDeadline ensures a long-running handler
// that finishes inside ShutdownTimeout is not interrupted and the stop still
// completes cleanly.
func TestConsumerStopLongHandlerWithinDeadline(t *testing.T) {
	n, _ := newShutdownMock(t, []byte("slow"), false)
	q, _ := newShutdownConsumer(t, 2*time.Second)

	started := make(chan struct{})
	finished := make(chan struct{})
	q.AddHandler(HandlerFunc(func(m *Message) error {
		close(started)
		select {
		case <-time.After(150 * time.Millisecond):
		case <-m.Context().Done():
			t.Error("context canceled even though handler finished within deadline")
		}
		close(finished)
		return nil
	}))
	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatalf("ConnectToNSQD: %s", err)
	}

	// only begin stopping once the handler is actively processing
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler never started")
	}

	q.Stop()
	waitForStop(t, q, time.Second)

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish")
	}
	if q.stopCtx.Err() != nil {
		t.Fatal("context must remain uncanceled when handler meets the deadline")
	}
}

// TestConsumerStopTimeoutCancelsHandler is the long-handler/timeout case: a
// handler blocked past ShutdownTimeout must be unblocked via context
// cancellation and the whole stop must return in bounded time.
func TestConsumerStopTimeoutCancelsHandler(t *testing.T) {
	n, _ := newShutdownMock(t, []byte("stuck"), true)
	shutdownTimeout := 200 * time.Millisecond
	q, _ := newShutdownConsumer(t, shutdownTimeout)

	started := make(chan struct{})
	canceled := make(chan error, 1)
	q.AddHandler(HandlerFunc(func(m *Message) error {
		close(started)
		<-m.Context().Done()
		canceled <- m.Context().Err()
		return nil
	}))
	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatalf("ConnectToNSQD: %s", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler never started")
	}

	start := time.Now()
	q.Stop()
	waitForStop(t, q, 2*time.Second)
	elapsed := time.Since(start)

	if elapsed < shutdownTimeout {
		t.Fatalf("stop returned in %s, before the %s shutdown deadline", elapsed, shutdownTimeout)
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("bounded stop took too long: %s", elapsed)
	}

	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler never observed context cancellation")
	}

	// the watchdog must have force-closed the held-open connection
	select {
	case <-n.exitChan:
	case <-time.After(time.Second):
		t.Fatal("force-closed connection did not unwind the mock")
	}
}

// TestConsumerStopForceExitsStuckHandler guarantees Stop still returns when a
// handler ignores context cancellation entirely (the forced-exit fallback),
// instead of hanging forever.
func TestConsumerStopForceExitsStuckHandler(t *testing.T) {
	n, _ := newShutdownMock(t, []byte("ignored"), true)
	shutdownTimeout := 100 * time.Millisecond
	q, _ := newShutdownConsumer(t, shutdownTimeout)

	started := make(chan struct{})
	release := make(chan struct{})
	q.AddHandler(HandlerFunc(func(m *Message) error {
		close(started)
		// deliberately ignore m.Context().Done()
		<-release
		return nil
	}))
	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatalf("ConnectToNSQD: %s", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler never started")
	}

	start := time.Now()
	q.Stop()
	waitForStop(t, q, 2*time.Second)
	elapsed := time.Since(start)

	// forced exit happens after roughly two shutdown deadlines
	if elapsed < shutdownTimeout {
		t.Fatalf("stop returned in %s, expected to wait at least %s", elapsed, shutdownTimeout)
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("forced exit was not bounded: %s", elapsed)
	}

	// release the leaked handler goroutine so it cannot touch later state
	close(release)
}

// TestConsumerStopIdempotent checks repeated Stop calls converge on the same
// final state and that a stopped consumer can not be restarted.
func TestConsumerStopIdempotent(t *testing.T) {
	n, _ := newShutdownMock(t, nil, false)
	q, _ := newShutdownConsumer(t, time.Second)
	q.AddHandler(HandlerFunc(func(m *Message) error { return nil }))
	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatalf("ConnectToNSQD: %s", err)
	}

	q.Stop()
	q.Stop()
	q.Stop()
	waitForStop(t, q, time.Second)

	// StopChan is closed exactly once and stays closed
	select {
	case <-q.StopChan:
	default:
		t.Fatal("StopChan should be closed")
	}

	if err := q.ConnectToNSQD(n.tcpAddr.String()); err == nil {
		t.Fatal("ConnectToNSQD after Stop should fail")
	}
	if err := q.ConnectToNSQLookupd("127.0.0.1:4161"); err == nil {
		t.Fatal("ConnectToNSQLookupd after Stop should fail")
	}
}

// TestConsumerStopRequeueDuringShutdown ensures a server-side requeue produced
// while shutting down does not re-arm RDY or otherwise delay the exit.
func TestConsumerStopRequeueDuringShutdown(t *testing.T) {
	n, _ := newShutdownMock(t, []byte("requeue"), false)
	q, _ := newShutdownConsumer(t, time.Second)

	q.AddHandler(HandlerFunc(func(m *Message) error {
		// requeue right as shutdown begins; backoff signals must be ignored
		m.Requeue(-1)
		return nil
	}))
	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatalf("ConnectToNSQD: %s", err)
	}

	q.Stop()
	waitForStop(t, q, time.Second)

	select {
	case <-n.exitChan:
	case <-time.After(time.Second):
		t.Fatal("mock nsqd did not observe the close")
	}

	// once the shutdown RDY 0 has been sent no later RDY command may raise
	// the count again
	got := recordedCmds(n)
	sawRevoke := false
	for _, line := range got {
		cmd := string(line)
		if cmd == "RDY 0" {
			sawRevoke = true
			continue
		}
		if sawRevoke && len(cmd) >= 4 && cmd[:4] == "RDY " && cmd != "RDY 0" {
			t.Fatalf("RDY was re-armed to %q during shutdown: %v", cmd, got)
		}
	}
	if !sawRevoke {
		t.Fatalf("expected an RDY 0 revoke during shutdown, got %v", got)
	}
}

// TestConsumerStopZeroTimeout checks a ShutdownTimeout of zero keeps the stop
// bounded (the watchdog fires immediately rather than blocking).
func TestConsumerStopZeroTimeout(t *testing.T) {
	n, _ := newShutdownMock(t, []byte("good"), false)
	q, _ := newShutdownConsumer(t, 0)

	done := make(chan struct{})
	q.AddHandler(HandlerFunc(func(m *Message) error {
		close(done)
		return nil
	}))
	if err := q.ConnectToNSQD(n.tcpAddr.String()); err != nil {
		t.Fatalf("ConnectToNSQD: %s", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("message was never handled")
	}

	q.Stop()
	waitForStop(t, q, 2*time.Second)
}

// TestConsumerStopNoConnections covers Stop before any connection was made.
func TestConsumerStopNoConnections(t *testing.T) {
	q, _ := newShutdownConsumer(t, time.Second)
	q.AddHandler(HandlerFunc(func(m *Message) error { return nil }))
	q.Stop()
	waitForStop(t, q, time.Second)
}

// TestConsumerStopShutdownTimeoutConfig confirms the new option is wired
// through the reflective config setter with its bounds and default.
func TestConsumerStopShutdownTimeoutConfig(t *testing.T) {
	c := NewConfig()
	if c.ShutdownTimeout != 30*time.Second {
		t.Fatalf("default ShutdownTimeout = %s, want 30s", c.ShutdownTimeout)
	}
	if err := c.Set("shutdown_timeout", "250ms"); err != nil {
		t.Fatalf("Set shutdown_timeout: %s", err)
	}
	if c.ShutdownTimeout != 250*time.Millisecond {
		t.Fatalf("ShutdownTimeout = %s, want 250ms", c.ShutdownTimeout)
	}
	if err := c.Set("shutdown_timeout", 61*time.Minute); err == nil {
		t.Fatal("expected validation error above the 60m max")
	}
}
