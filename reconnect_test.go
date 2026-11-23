package nsq

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

func waitForConns(t *testing.T, q *Consumer, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(q.conns()) == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %d conns, have %d", n, len(q.conns()))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForConnGeneration(t *testing.T, q *Consumer, minGen uint64) *Conn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, c := range q.conns() {
			if c.Generation() > minGen {
				return c
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for connection generation > %d", minGen)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitMockExit(t *testing.T, n *mockNSQD) {
	t.Helper()
	select {
	case <-n.exitChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for mock nsqd exit")
	}
}

func checkGotCommands(t *testing.T, n *mockNSQD, expected []string) {
	t.Helper()
	for i, r := range n.got {
		t.Logf("%d: %s", i, r)
	}
	if len(n.got) != len(expected) {
		t.Fatalf("we got %d commands != %d expected", len(n.got), len(expected))
	}
	for i, r := range n.got {
		if string(r) != expected[i] {
			t.Fatalf("cmd %d bad %s != %s", i, r, expected[i])
		}
	}
}

// TestConsumerReconnectGenerations exercises consecutive disconnects: each
// reconnect must be assigned a new, monotonically increasing generation and
// the topic/channel subscription must be restored exactly once per
// generation.
func TestConsumerReconnectGenerations(t *testing.T) {
	msgIDGood := MessageID{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 's', 'd', 'f', 'g', 'h'}
	msgGood := NewMessage(msgIDGood, []byte("good"))

	script := []instruction{
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		instruction{20 * time.Millisecond, FrameTypeMessage, frameMessage(msgGood)},
		// needed to exit
		instruction{50 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	n := newMockNSQD(t, script, addr.String())

	topicName := "test_reconnect_generations" + strconv.Itoa(int(time.Now().Unix()))
	config := NewConfig()
	config.MaxInFlight = 1
	config.LookupdPollInterval = 10 * time.Millisecond
	config.RDYRedistributeInterval = 10 * time.Millisecond
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)
	q.AddHandler(&testHandler{})
	err := q.ConnectToNSQD(n.tcpAddr.String())
	if err != nil {
		t.Fatalf(err.Error())
	}

	expected := []string{
		"IDENTIFY",
		"SUB " + topicName + " ch",
		"RDY 1",
		fmt.Sprintf("FIN %s", msgIDGood),
	}

	var prevGen uint64
	for generation := 1; generation <= 3; generation++ {
		conn := waitForConnGeneration(t, q, prevGen)
		if conn.Generation() <= prevGen {
			t.Fatalf("connection generation did not increase: %d <= %d",
				conn.Generation(), prevGen)
		}
		prevGen = conn.Generation()

		waitMockExit(t, n)

		// exactly one IDENTIFY and one SUB per connection generation
		checkGotCommands(t, n, expected)

		if generation < 3 {
			// consecutive disconnect: bring up the next generation's nsqd
			n = newMockNSQD(t, script, n.tcpAddr.String())
		}
	}

	waitForConns(t, q, 0)
	q.Stop()
	select {
	case <-q.StopChan:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for consumer stop")
	}
}

// TestConsumerStaleGenerationCallbacks verifies that responses arriving from
// an old connection generation (out of order, after its replacement is
// already registered) complete their own waiters only and do not mutate the
// state of the new generation.
func TestConsumerStaleGenerationCallbacks(t *testing.T) {
	config := NewConfig()
	q, _ := NewConsumer("test_stale_generation", "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)

	addr := "127.0.0.1:4150"
	oldConn := NewConn(addr, &q.config, &consumerConnDelegate{q})
	oldConn.generation = 1
	newConn := NewConn(addr, &q.config, &consumerConnDelegate{q})
	newConn.generation = 2

	// the new generation has replaced the old one
	q.mtx.Lock()
	q.connections[addr] = newConn
	q.connGenerations[addr] = 2
	q.mtx.Unlock()

	// a pending RDY retry timer from an old generation is stopped when a
	// new generation registers...
	q.rdyRetryMtx.Lock()
	q.rdyRetryTimers[addr] = &rdyRetryTimer{generation: 1, timer: time.NewTimer(time.Hour)}
	q.rdyRetryMtx.Unlock()
	q.stopStaleRDYRetryTimer(addr, 2)
	q.rdyRetryMtx.Lock()
	_, ok := q.rdyRetryTimers[addr]
	q.rdyRetryMtx.Unlock()
	if ok {
		t.Fatal("stale RDY retry timer was not stopped by new generation")
	}

	// ...but a timer belonging to the current generation is kept
	q.rdyRetryMtx.Lock()
	q.rdyRetryTimers[addr] = &rdyRetryTimer{generation: 2, timer: time.NewTimer(time.Hour)}
	q.rdyRetryMtx.Unlock()
	q.stopStaleRDYRetryTimer(addr, 2)
	q.rdyRetryMtx.Lock()
	_, ok = q.rdyRetryTimers[addr]
	q.rdyRetryMtx.Unlock()
	if !ok {
		t.Fatal("current generation RDY retry timer was wrongly stopped")
	}

	// stale generation responses/signals must not update shared state
	q.onConnResponse(oldConn, []byte("CLOSE_WAIT"))
	q.onConnBackoff(oldConn)
	q.onConnResume(oldConn)
	q.onConnContinue(oldConn)
	if q.inBackoff() {
		t.Fatal("stale generation signal mutated backoff state")
	}

	// a stale generation closing late must not evict its replacement nor
	// stop the replacement's timers
	q.onConnClose(oldConn)
	conns := q.conns()
	if len(conns) != 1 || conns[0] != newConn {
		t.Fatal("stale generation close evicted the current connection")
	}
	q.rdyRetryMtx.Lock()
	_, ok = q.rdyRetryTimers[addr]
	q.rdyRetryMtx.Unlock()
	if !ok {
		t.Fatal("stale generation close stopped current generation's timer")
	}

	// the current generation's signals still take effect
	q.onConnBackoff(newConn)
	if !q.inBackoff() {
		t.Fatal("current generation backoff signal was ignored")
	}

	// the current generation's close removes it and releases its timer
	q.onConnClose(newConn)
	if len(q.conns()) != 0 {
		t.Fatal("current generation close did not remove the connection")
	}
	q.rdyRetryMtx.Lock()
	_, ok = q.rdyRetryTimers[addr]
	q.rdyRetryMtx.Unlock()
	if ok {
		t.Fatal("current generation close did not release its timer")
	}

	q.Stop()
}

// TestConsumerReconnectQueue verifies the bounded command queue used while
// a connection is being re-established: capacity errors are immediate and
// commands that outlive the deadline fail with an explicit error.
func TestConsumerReconnectQueue(t *testing.T) {
	config := NewConfig()
	config.ReconnectQueueSize = 2
	config.ReconnectQueueTimeout = 50 * time.Millisecond
	q, _ := NewConsumer("test_reconnect_queue", "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)

	addr := "127.0.0.1:4150"

	// commands for an address that is not registered fail immediately
	if _, err := q.queueCommandForAddr(addr, Ready(1), true, 1); err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}

	q.mtx.Lock()
	q.nsqdTCPAddrs = append(q.nsqdTCPAddrs, addr)
	q.mtx.Unlock()

	r1, err := q.queueCommandForAddr(addr, Ready(1), true, 1)
	if err != nil {
		t.Fatalf("unexpected queue error - %s", err)
	}
	r2, err := q.queueCommandForAddr(addr, Ready(2), true, 2)
	if err != nil {
		t.Fatalf("unexpected queue error - %s", err)
	}

	// the queue is bounded: the next command fails immediately
	if _, err := q.queueCommandForAddr(addr, Ready(3), true, 3); err != ErrReconnectQueueFull {
		t.Fatalf("expected ErrReconnectQueueFull, got %v", err)
	}

	// commands that outlive the deadline get an explicit error
	for i, rc := range []chan error{r1, r2} {
		select {
		case err := <-rc:
			if err != ErrCommandExpired {
				t.Fatalf("cmd %d: expected ErrCommandExpired, got %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("cmd %d: timeout waiting for command expiry", i)
		}
	}

	q.mtx.Lock()
	remaining := len(q.reconnectQueues)
	q.mtx.Unlock()
	if remaining != 0 {
		t.Fatalf("expected reconnect queues to be drained, %d remaining", remaining)
	}

	// an RDY update targeting a closing connection is carried into the
	// bounded queue for the next generation (and expires there)
	closingConn := NewConn(addr, &q.config, &consumerConnDelegate{q})
	closingConn.closeFlag = 1
	if err := q.updateRDY(closingConn, 1); err != nil {
		t.Fatalf("expected RDY update to be queued, got %v", err)
	}
	q.mtx.Lock()
	queued := len(q.reconnectQueues[addr])
	q.mtx.Unlock()
	if queued != 1 {
		t.Fatalf("expected 1 queued command, got %d", queued)
	}

	// an RDY 0 update is not carried over
	if err := q.updateRDY(closingConn, 0); err != ErrClosing {
		t.Fatalf("expected ErrClosing for RDY 0, got %v", err)
	}
}

// TestConsumerReconnectQueueFlush verifies that commands queued while the
// connection is down are delivered, in order, on the next connection
// generation.
func TestConsumerReconnectQueueFlush(t *testing.T) {
	script := []instruction{
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		// needed to exit
		instruction{100 * time.Millisecond, -1, []byte("exit")},
	}

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	n := newMockNSQD(t, script, addr.String())

	topicName := "test_reconnect_queue_flush" + strconv.Itoa(int(time.Now().Unix()))
	config := NewConfig()
	config.MaxInFlight = 3
	config.LookupdPollInterval = 10 * time.Millisecond
	config.ReconnectQueueSize = 4
	config.ReconnectQueueTimeout = 5 * time.Second
	q, _ := NewConsumer(topicName, "ch", config)
	q.SetLogger(newTestLogger(t), LogLevelDebug)
	q.AddHandler(&testHandler{})
	err := q.ConnectToNSQD(n.tcpAddr.String())
	if err != nil {
		t.Fatalf(err.Error())
	}

	waitMockExit(t, n)
	waitForConns(t, q, 0)

	// queue an RDY update while the connection is down; it must be
	// flushed onto the next connection generation
	result, err := q.queueCommandForAddr(n.tcpAddr.String(), Ready(3), true, 3)
	if err != nil {
		t.Fatalf("unexpected queue error - %s", err)
	}

	script = []instruction{
		// IDENTIFY
		instruction{0, FrameTypeResponse, []byte("OK")},
		// SUB
		instruction{0, FrameTypeResponse, []byte("OK")},
		// needed to exit
		instruction{200 * time.Millisecond, -1, []byte("exit")},
	}
	n = newMockNSQD(t, script, n.tcpAddr.String())

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("queued command failed - %s", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for queued command flush")
	}

	waitMockExit(t, n)

	// the flushed RDY 3 is delivered once; the post-connect maybeUpdateRDY
	// de-duplicates against it (MaxInFlight == 3)
	expected := []string{
		"IDENTIFY",
		"SUB " + topicName + " ch",
		"RDY 3",
	}
	checkGotCommands(t, n, expected)

	waitForConns(t, q, 0)
	q.Stop()
	select {
	case <-q.StopChan:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for consumer stop")
	}
}
