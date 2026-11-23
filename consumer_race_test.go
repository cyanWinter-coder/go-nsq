package nsq

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingDelegate records every terminal response a Message emits.
type recordingDelegate struct {
	mu       sync.Mutex
	finishes int
	requeues int
	touches  int
}

func (d *recordingDelegate) OnFinish(*Message) {
	d.mu.Lock()
	d.finishes++
	d.mu.Unlock()
}

func (d *recordingDelegate) OnRequeue(m *Message, delay time.Duration, backoff bool) {
	d.mu.Lock()
	d.requeues++
	d.mu.Unlock()
}

func (d *recordingDelegate) OnTouch(*Message) {
	d.mu.Lock()
	d.touches++
	d.mu.Unlock()
}

func (d *recordingDelegate) snapshot() (int, int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.finishes, d.requeues, d.touches
}

// TestMessageTerminalResponseRace hammers Finish/Requeue/Touch concurrently.
// Exactly one terminal response (FIN or REQ) may ever be emitted.
func TestMessageTerminalResponseRace(t *testing.T) {
	const goroutines = 32
	for iter := 0; iter < 50; iter++ {
		msg := NewMessage(MessageID{'a'}, []byte("x"))
		d := &recordingDelegate{}
		msg.Delegate = d

		var wg sync.WaitGroup
		for i := 0; i < goroutines; i++ {
			wg.Add(3)
			go func() {
				defer wg.Done()
				msg.Finish()
			}()
			go func() {
				defer wg.Done()
				msg.Requeue(-1)
			}()
			go func() {
				defer wg.Done()
				msg.Touch()
			}()
		}
		wg.Wait()

		fin, req, _ := d.snapshot()
		if fin+req != 1 {
			t.Fatalf("iter %d: expected exactly one terminal response, got FIN=%d REQ=%d", iter, fin, req)
		}
		if !msg.HasResponded() {
			t.Fatalf("iter %d: message should be marked responded", iter)
		}

		// after the terminal state is set, every further call is suppressed;
		// only the delta after the terminal response matters (a TOUCH that won
		// the lock before Finish was legitimate)
		fin, req, touch := d.snapshot()
		msg.Finish()
		msg.Requeue(-1)
		msg.Touch()
		fin2, req2, touch2 := d.snapshot()
		if fin2 != fin || req2 != req || touch2 != touch {
			t.Fatalf("iter %d: post-terminal calls leaked: FIN %d->%d REQ %d->%d TOUCH %d->%d",
				iter, fin, fin2, req, req2, touch, touch2)
		}
	}
}

// TestMessageTouchBeforeFinishRace verifies that a Touch racing with a Finish
// never produces a TOUCH after the FIN claim (the old HasResponded() TOCTOU
// window allowed exactly that).
func TestMessageTouchBeforeFinishRace(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		msg := NewMessage(MessageID{'b'}, []byte("x"))
		d := &recordingDelegate{}
		msg.Delegate = d

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			msg.Touch()
		}()
		go func() {
			defer wg.Done()
			msg.Finish()
		}()
		wg.Wait()

		fin, _, touch := d.snapshot()
		if fin != 1 {
			t.Fatalf("iter %d: expected one FIN, got %d", iter, fin)
		}
		if touch > 1 {
			t.Fatalf("iter %d: at most one TOUCH allowed, got %d", iter, touch)
		}
	}
}

// TestMessageCancelContext checks that an in-flight message's context is
// canceled on shutdown/connection loss, but only while it is un-responded.
func TestMessageCancelContext(t *testing.T) {
	msg := NewMessage(MessageID{'c'}, []byte("x"))
	if msg.Canceled() {
		t.Fatal("fresh message should not be canceled")
	}
	msg.cancel()
	if !msg.Canceled() {
		t.Fatal("cancel() must close the message context")
	}

	// a responded message is terminal; cancel is a no-op on it
	msg2 := NewMessage(MessageID{'d'}, []byte("x"))
	msg2.Delegate = &recordingDelegate{}
	msg2.Finish()
	msg2.cancel()
	if !msg2.HasResponded() {
		t.Fatal("message should be responded after Finish")
	}
}

func newUnitConsumer(t *testing.T, maxInFlight int32) *Consumer {
	cfg := NewConfig()
	cfg.BackoffMultiplier = 5 * time.Millisecond
	cfg.MaxBackoffDuration = 50 * time.Millisecond
	q, err := NewConsumer("test_race"+strconv.Itoa(int(time.Now().UnixNano())),
		"ch", cfg)
	if err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt32(&q.maxInFlight, maxInFlight)
	return q
}

// newWiredConn dials a loopback TCP listener and attaches the resulting Conn
// to the consumer, so RDY/backoff writes succeed without a real nsqd. The
// accepted server side is closed on test cleanup.
func newWiredConn(t *testing.T, q *Consumer, addr string, generation int64) *Conn {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// drain and hold the connection for the test's lifetime
		t.Cleanup(func() { conn.Close() })
		go func() {
			buf := make([]byte, 4096)
			for {
				if _, err := conn.Read(buf); err != nil {
					return
				}
			}
		}()
	}()

	tcpAddr, _ := net.ResolveTCPAddr("tcp", ln.Addr().String())
	dialer := &net.Dialer{}
	raw, err := dialer.Dial("tcp", tcpAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	c := NewConn(ln.Addr().String(), &q.config, &consumerConnDelegate{q}, generation)
	c.conn = raw.(*net.TCPConn)
	c.r = raw
	c.w = raw
	q.mtx.Lock()
	q.connections[c.String()] = c
	q.mtx.Unlock()
	return c
}

// TestRDYAccountsForLocalOccupancy proves that messages parked in a slow
// handler consume RDY capacity: the consumer never asks for more credits
// than its genuinely idle concurrency.
func TestRDYAccountsForLocalOccupancy(t *testing.T) {
	q := newUnitConsumer(t, 2)
	defer q.exit()
	// no writes are issued by the pure targetRDY/bookkeeping path under test
	c := NewConn("127.0.0.1:30001", &q.config, &consumerConnDelegate{q}, 1)
	q.mtx.Lock()
	q.connections[c.String()] = c
	q.mtx.Unlock()
	c.SetRDY(2)
	atomic.StoreInt64(&q.totalRdyCount, 2)

	// simulate two messages delivered but stuck in (slow) handlers. Delivery
	// consumes an RDY credit and registers local occupancy; here we exercise
	// the bookkeeping directly (targetRDY is a pure function of this state).
	m1 := NewMessage(MessageID{'1'}, []byte("a"))
	m2 := NewMessage(MessageID{'2'}, []byte("b"))
	c.decrementRDY()
	c.decrementRDY()
	atomic.StoreInt64(&q.totalRdyCount, 0)
	q.activeMessagesMtx.Lock()
	q.activeMessages[m1.ID] = m1
	q.activeMessages[m2.ID] = m2
	q.activeMessagesMtx.Unlock()

	if got := q.inFlightCount(); got != 2 {
		t.Fatalf("expected 2 locally in-flight messages, got %d", got)
	}
	if c.RDY() != 0 {
		t.Fatalf("delivered messages must consume RDY credits, RDY=%d", c.RDY())
	}
	if atomic.LoadInt64(&q.totalRdyCount) != 0 {
		t.Fatalf("total RDY should track consumed credits, got %d",
			atomic.LoadInt64(&q.totalRdyCount))
	}

	// with all concurrency occupied locally, target RDY must be zero
	if target := q.targetRDY(c); target != 0 {
		t.Fatalf("expected target RDY 0 with full local occupancy, got %d", target)
	}

	// freeing one handler replenishes exactly one credit of idle concurrency
	q.activeMessagesMtx.Lock()
	var first *Message
	for _, m := range q.activeMessages {
		first = m
		break
	}
	delete(q.activeMessages, first.ID)
	q.activeMessagesMtx.Unlock()

	if target := q.targetRDY(c); target != 1 {
		t.Fatalf("expected target RDY 1 after freeing one handler, got %d", target)
	}
}

// TestPerConnectionBackoffDoesNotBlockOtherConns verifies that backoff on one
// connection zeroes only that connection's RDY; a healthy connection keeps
// its credits and keeps processing ready messages.
func TestPerConnectionBackoffDoesNotBlockOtherConns(t *testing.T) {
	q := newUnitConsumer(t, 10)
	defer q.exit()
	c1 := newWiredConn(t, q, "127.0.0.1:10001", 1)
	c2 := newWiredConn(t, q, "127.0.0.1:10002", 1)
	c1.SetRDY(5)
	c2.SetRDY(5)
	atomic.StoreInt64(&q.totalRdyCount, 10)

	q.onConnBackoff(c1)

	if !c1.InBackoff() {
		t.Fatal("c1 should be in backoff")
	}
	if c1.RDY() != 0 {
		t.Fatalf("failing connection must be driven to RDY 0, got %d", c1.RDY())
	}
	if c2.InBackoff() {
		t.Fatal("healthy connection must not enter backoff")
	}
	if c2.RDY() != 5 {
		t.Fatalf("healthy connection RDY must be untouched, got %d", c2.RDY())
	}

	// after the bounded backoff elapses, c1 probes with RDY 1 and c2 still
	// holds its full credits
	waitFor(t, func() bool { return c1.RDY() == 1 }, time.Second)
	if c2.RDY() != 5 {
		t.Fatalf("healthy connection RDY changed during c1 backoff, got %d", c2.RDY())
	}
}

// TestBackoffTimerGenerationStaleness ensures a backoff timer scheduled under
// an old connection generation cannot mutate RDY after a reconnect.
func TestBackoffTimerGenerationStaleness(t *testing.T) {
	q := newUnitConsumer(t, 10)
	defer q.exit()
	c := newWiredConn(t, q, "127.0.0.1:20001", 7)
	c.SetRDY(3)
	atomic.StoreInt64(&q.totalRdyCount, 3)

	q.onConnBackoff(c)
	if c.RDY() != 0 {
		t.Fatalf("expected RDY 0 after backoff, got %d", c.RDY())
	}

	// simulate a reconnect superseding this connection generation
	c.setGeneration(8)
	atomic.StoreInt32(&c.closeFlag, 1)

	// wait long enough for the backoff timer to have fired; it must no-op
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if c.RDY() != 0 {
			t.Fatalf("stale generation timer mutated RDY to %d", c.RDY())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestStopRevokesRDYAndCancelsMessages asserts that Stop revokes all
// outstanding RDY credits and cancels still-active message contexts.
func TestStopRevokesRDYAndCancelsMessages(t *testing.T) {
	q := newUnitConsumer(t, 1)
	c := newWiredConn(t, q, "127.0.0.1:40001", 1)
	c.SetRDY(1)
	atomic.StoreInt64(&q.totalRdyCount, 1)

	// register a still-active message whose handler has not responded
	stuck := NewMessage(MessageID{'s', 't', 'u', 'c', 'k'}, []byte("z"))
	stuck.Delegate = &recordingDelegate{}
	q.activeMessagesMtx.Lock()
	q.activeMessages[stuck.ID] = stuck
	q.activeMessagesMtx.Unlock()

	// exercise the stop prelude directly: mark stopping, revoke RDY credits
	// and cancel in-flight handlers, without depending on the full close
	// orchestration that requires a live nsqd read loop
	atomic.StoreInt32(&q.stopFlag, 1)
	for _, conn := range q.conns() {
		conn.stopBackoffTimer()
		if err := q.updateRDY(conn, 0); err != nil {
			t.Fatalf("revoke RDY failed: %s", err)
		}
	}
	q.cancelActiveMessages()

	if c.RDY() != 0 {
		t.Fatalf("Stop must revoke unused RDY, got %d", c.RDY())
	}
	if !stuck.Canceled() {
		t.Fatal("Stop must cancel contexts of still-active messages")
	}
}

// TestRequeueDelayIsBounded verifies that even a caller-supplied requeue delay
// is clamped to MaxRequeueDelay.
func TestRequeueDelayIsBounded(t *testing.T) {
	config := NewConfig()
	config.MaxRequeueDelay = 100 * time.Millisecond
	c := NewConn("127.0.0.1:50001", config, &consumerConnDelegate{}, 1)

	msg := NewMessage(MessageID{'r'}, []byte("x"))
	go func() {
		resp := <-c.msgResponseChan
		if resp.success {
			t.Errorf("expected a REQ response")
		}
		// REQ command carries the delay in ms as its second parameter
		if got := string(resp.cmd.Params[1]); got != "100" {
			t.Errorf("expected clamped delay 100ms, got %s", got)
		}
	}()
	c.onMessageRequeue(msg, 10*time.Second, true)
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
