package nsq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// batchTestConn is a producerConn mock that records every written command
// and lets the test script MPUB responses (or connection failure)
type batchTestConn struct {
	delegate ConnDelegate

	mu       sync.Mutex
	commands []*Command
	gate     chan struct{} // when non-nil, MPUB writes block until it is closed
	onMPUB   func(cmd *Command)

	closeCh   chan struct{}
	closeOnce sync.Once
}

func newBatchTestConn() *batchTestConn {
	return &batchTestConn{closeCh: make(chan struct{})}
}

func (m *batchTestConn) String() string { return "127.0.0.1:0" }

func (m *batchTestConn) SetLogger(logger logger, level LogLevel, prefix string) {}

func (m *batchTestConn) SetLoggerLevel(lvl LogLevel) {}

func (m *batchTestConn) SetLoggerForLevel(logger logger, level LogLevel, format string) {}

func (m *batchTestConn) Connect() (*IdentifyResponse, error) { return &IdentifyResponse{}, nil }

func (m *batchTestConn) Close() error {
	m.closeOnce.Do(func() {
		close(m.closeCh)
		// a real conn delivers OnClose from its readLoop goroutine;
		// doing so synchronously would deadlock against Producer.Stop
		go m.delegate.OnClose(nil)
	})
	return nil
}

func (m *batchTestConn) WriteCommand(cmd *Command) error {
	m.mu.Lock()
	m.commands = append(m.commands, cmd)
	gate := m.gate
	onMPUB := m.onMPUB
	m.mu.Unlock()

	if bytes.Equal(cmd.Name, []byte("MPUB")) {
		if gate != nil {
			<-gate
		}
		if onMPUB != nil {
			onMPUB(cmd)
		}
	}
	return nil
}

func (m *batchTestConn) getCommands() []*Command {
	m.mu.Lock()
	defer m.mu.Unlock()
	cmds := make([]*Command, len(m.commands))
	copy(cmds, m.commands)
	return cmds
}

// die simulates the connection being lost without any response
func (m *batchTestConn) die() {
	m.Close()
}

// respondOK delivers an OK response frame for one MPUB
func (m *batchTestConn) respondOK() {
	go m.delegate.OnResponse(nil, []byte("OK"))
}

func (m *batchTestConn) respond(data []byte) {
	go m.delegate.OnResponse(nil, data)
}

func (m *batchTestConn) respondError(err string) {
	go m.delegate.OnError(nil, []byte(err))
}

func newBatchTestProducer(m *batchTestConn) *Producer {
	config := NewConfig()
	p, _ := NewProducer("127.0.0.1:0", config)
	p.SetLogger(nullLogger, LogLevelInfo)
	m.delegate = &producerConnDelegate{p}
	p.conn = m
	atomic.StoreInt32(&p.state, StateConnected)
	p.closeChan = make(chan int)
	p.wg.Add(1)
	go p.router()
	return p
}

func parseMPUBBodies(t *testing.T, cmd *Command) [][]byte {
	t.Helper()
	if !bytes.Equal(cmd.Name, []byte("MPUB")) {
		t.Fatalf("expected MPUB command, got %s", cmd.Name)
	}
	r := bytes.NewReader(cmd.Body)
	var num int32
	if err := binary.Read(r, binary.BigEndian, &num); err != nil {
		t.Fatalf("failed to read MPUB count: %s", err)
	}
	var bodies [][]byte
	for i := 0; i < int(num); i++ {
		var l int32
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			t.Fatalf("failed to read MPUB message size: %s", err)
		}
		b := make([]byte, l)
		if _, err := io.ReadFull(r, b); err != nil {
			t.Fatalf("failed to read MPUB message body: %s", err)
		}
		bodies = append(bodies, b)
	}
	return bodies
}

func waitForBatchState(t *testing.T, b *PublishBatch, want BatchState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.State() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("batch %s did not reach state %s (state=%s)", b.ID, want, b.State())
}

func testBodies(n int) [][]byte {
	var bodies [][]byte
	for i := 0; i < n; i++ {
		bodies = append(bodies, []byte(fmt.Sprintf("msg-%d", i)))
	}
	return bodies
}

// A partial MPUB response must reconcile message-by-message: successes are
// recorded against the original messages and only the transient failures
// are aggregated for retry
func TestProducerBatchPartialResponse(t *testing.T) {
	m := newBatchTestConn()
	var mpubCalls int32
	m.onMPUB = func(cmd *Command) {
		if atomic.AddInt32(&mpubCalls, 1) == 1 {
			m.respond([]byte("PARTIAL\n0 OK\n1 E_FAILED transient\n2 OK\n3 E_FAILED transient\n4 OK"))
		} else {
			m.respondOK()
		}
	}
	p := newBatchTestProducer(m)
	defer p.Stop()

	bodies := testBodies(5)
	doneChan := make(chan *PublishBatch, 1)
	b, err := p.MultiPublishBatchAsync("batch_partial", bodies, doneChan)
	if err != nil {
		t.Fatalf("publish failed: %s", err)
	}
	if b.ID == "" {
		t.Fatal("batch must have a stable correlation ID")
	}

	select {
	case done := <-doneChan:
		if done != b {
			t.Fatal("doneChan should receive the same batch instance")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for batch completion")
	}

	err = b.Wait()
	if err == nil {
		t.Fatal("Wait should report the partial failure")
	}
	berr, ok := err.(*BatchError)
	if !ok {
		t.Fatalf("Wait error should be *BatchError, got %T (%s)", err, err)
	}
	if berr.BatchID != b.ID {
		t.Fatalf("BatchError ID %q != batch ID %q", berr.BatchID, b.ID)
	}
	if len(berr.Failed) != 2 || berr.Failed[0].Index != 1 || berr.Failed[1].Index != 3 {
		t.Fatalf("BatchError should aggregate messages 1 and 3, got %+v", berr.Failed)
	}

	// every response entry maps to the original message
	results := b.Results()
	wantStatus := []BatchMessageStatus{
		BatchMessageSucceeded, BatchMessageFailed, BatchMessageSucceeded,
		BatchMessageFailed, BatchMessageSucceeded,
	}
	for i, r := range results {
		if r.Status != wantStatus[i] {
			t.Fatalf("message %d status = %d, want %d", i, r.Status, wantStatus[i])
		}
		if !bytes.Equal(r.Body, bodies[i]) {
			t.Fatalf("message %d body = %q, want %q", i, r.Body, bodies[i])
		}
		if r.Status == BatchMessageFailed {
			if pe, ok := r.Error.(ErrProtocol); !ok || pe.Reason != "E_FAILED transient" {
				t.Fatalf("message %d error = %v", i, r.Error)
			}
		}
	}
	if b.Pending() != 0 || b.State() != BatchStateDone {
		t.Fatalf("batch should be done with 0 pending, got %d/%s", b.Pending(), b.State())
	}

	// only the transient failures are aggregated for retry
	failed := b.FailedBodies()
	if len(failed) != 2 || !bytes.Equal(failed[0], bodies[1]) || !bytes.Equal(failed[1], bodies[3]) {
		t.Fatalf("FailedBodies = %q, want [msg-1 msg-3]", failed)
	}

	retry, err := b.RetryBatch()
	if err != nil {
		t.Fatalf("RetryBatch failed: %s", err)
	}
	if retry.ID != b.ID+"-r1" {
		t.Fatalf("retry ID %q should derive from %q", retry.ID, b.ID)
	}
	if retry.Size() != 2 {
		t.Fatalf("retry batch should hold 2 messages, got %d", retry.Size())
	}

	retryDone := make(chan *PublishBatch, 1)
	if err := p.PublishBatchAsync(retry, retryDone); err != nil {
		t.Fatalf("retry publish failed: %s", err)
	}
	select {
	case <-retryDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for retry completion")
	}
	if err := retry.Wait(); err != nil {
		t.Fatalf("retry should succeed: %s", err)
	}

	// the retry on the wire must contain only the failed messages —
	// accepted messages are never re-published
	cmds := m.getCommands()
	if len(cmds) != 2 {
		t.Fatalf("expected 2 MPUB commands, got %d", len(cmds))
	}
	sent := parseMPUBBodies(t, cmds[1])
	if len(sent) != 2 || !bytes.Equal(sent[0], bodies[1]) || !bytes.Equal(sent[1], bodies[3]) {
		t.Fatalf("retry MPUB bodies = %q, want [msg-1 msg-3]", sent)
	}
}

// An OK response accepts every message; a later connection failure must
// not mutate the terminal results
func TestProducerBatchOKResponse(t *testing.T) {
	m := newBatchTestConn()
	m.onMPUB = func(cmd *Command) { m.respondOK() }
	p := newBatchTestProducer(m)
	defer p.Stop()

	b, err := p.MultiPublishBatch("batch_ok", testBodies(3))
	if err != nil {
		t.Fatalf("batch should succeed: %s", err)
	}
	if b.State() != BatchStateDone {
		t.Fatalf("state = %s, want done", b.State())
	}
	for _, r := range b.Results() {
		if r.Status != BatchMessageSucceeded {
			t.Fatalf("message %d status = %d, want succeeded", r.Index, r.Status)
		}
	}

	// a connection error after completion must not change terminal results
	m.die()
	time.Sleep(50 * time.Millisecond)
	for _, r := range b.Results() {
		if r.Status != BatchMessageSucceeded {
			t.Fatalf("terminal result mutated: message %d status = %d", r.Index, r.Status)
		}
	}
	if err := b.Wait(); err != nil {
		t.Fatalf("Wait after disconnect should still be nil: %s", err)
	}
}

// Losing the connection before the response must move the batch to a
// deterministic terminal state, and the batch must never be (re)sent on
// another connection
func TestProducerBatchDisconnect(t *testing.T) {
	m := newBatchTestConn()
	m.onMPUB = func(cmd *Command) {
		// connection drops before any response arrives
		m.die()
	}
	p := newBatchTestProducer(m)
	defer p.Stop()

	doneChan := make(chan *PublishBatch, 1)
	b, err := p.MultiPublishBatchAsync("batch_disconnect", testBodies(3), doneChan)
	if err != nil {
		t.Fatalf("publish failed: %s", err)
	}

	select {
	case <-doneChan:
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not reach a terminal state after disconnect")
	}

	err = b.Wait()
	berr, ok := err.(*BatchError)
	if !ok {
		t.Fatalf("Wait error should be *BatchError, got %T (%v)", err, err)
	}
	if len(berr.Failed) != 3 {
		t.Fatalf("all 3 messages should fail, got %d", len(berr.Failed))
	}
	for _, r := range b.Results() {
		if r.Status != BatchMessageFailed || r.Error != ErrNotConnected {
			t.Fatalf("message %d = (%d, %v), want (failed, ErrNotConnected)", r.Index, r.Status, r.Error)
		}
	}
	if b.State() != BatchStateDone {
		t.Fatalf("state = %s, want done", b.State())
	}

	// the batch was written exactly once; nothing re-sends it
	time.Sleep(50 * time.Millisecond)
	if cmds := m.getCommands(); len(cmds) != 1 {
		t.Fatalf("batch sent %d times, want exactly 1", len(cmds))
	}

	// the same batch instance can never be submitted again
	if err := p.PublishBatchAsync(b, nil); err == nil {
		t.Fatal("re-submitting a completed batch should fail")
	}
}

// An error frame fails every pending message with the protocol error, and
// the retry carries the full original payload
func TestProducerBatchErrorResponse(t *testing.T) {
	m := newBatchTestConn()
	m.onMPUB = func(cmd *Command) { m.respondError("E_FAILED topic does not exist") }
	p := newBatchTestProducer(m)
	defer p.Stop()

	bodies := testBodies(2)
	b, err := p.MultiPublishBatchAsync("batch_error", bodies, nil)
	if err != nil {
		t.Fatalf("publish failed: %s", err)
	}
	err = b.Wait()
	berr, ok := err.(*BatchError)
	if !ok || len(berr.Failed) != 2 {
		t.Fatalf("Wait = %v, want *BatchError with 2 failures", err)
	}
	for _, r := range b.Results() {
		if pe, ok := r.Error.(ErrProtocol); !ok || pe.Reason != "E_FAILED topic does not exist" {
			t.Fatalf("message %d error = %v", r.Index, r.Error)
		}
	}

	retry, err := b.RetryBatch()
	if err != nil {
		t.Fatalf("RetryBatch failed: %s", err)
	}
	if got := retry.FailedBodies(); len(got) != 0 {
		t.Fatalf("fresh retry batch should have no failures yet, got %d", len(got))
	}
	if retry.Size() != 2 {
		t.Fatalf("retry should carry both messages, got %d", retry.Size())
	}
}

// Canceling a queued (not yet written) batch must release its buffers and
// waiters, and the router must never write it to the connection
func TestProducerBatchCancelQueued(t *testing.T) {
	m := newBatchTestConn()
	m.gate = make(chan struct{})
	m.onMPUB = func(cmd *Command) { m.respondOK() }
	p := newBatchTestProducer(m)
	defer p.Stop()

	// b1 occupies the router (its write blocks on the gate)
	b1Done := make(chan *PublishBatch, 1)
	b1, err := p.MultiPublishBatchAsync("batch_cancel", testBodies(1), b1Done)
	if err != nil {
		t.Fatalf("publish b1 failed: %s", err)
	}
	waitForBatchState(t, b1, BatchStateInFlight)

	// b2 queues behind b1
	b2Done := make(chan *PublishBatch, 1)
	b2, err := p.NewBatch("batch_cancel", testBodies(2))
	if err != nil {
		t.Fatalf("NewBatch failed: %s", err)
	}
	pubErr := make(chan error, 1)
	go func() { pubErr <- p.PublishBatchAsync(b2, b2Done) }()
	waitForBatchState(t, b2, BatchStateQueued)

	if !b2.Cancel() {
		t.Fatal("cancel of a queued batch should succeed")
	}
	if err := b2.Wait(); err != ErrBatchCanceled {
		t.Fatalf("b2 Wait = %v, want ErrBatchCanceled", err)
	}
	for _, r := range b2.Results() {
		if r.Status != BatchMessageFailed || r.Error != ErrBatchCanceled {
			t.Fatalf("message %d = (%d, %v), want (failed, ErrBatchCanceled)", r.Index, r.Status, r.Error)
		}
		if r.Body != nil {
			t.Fatalf("message %d body should be released on cancel", r.Index)
		}
	}
	if _, err := b2.RetryBatch(); err == nil {
		t.Fatal("a canceled batch must not be retryable")
	}

	// let the router proceed; b2 must be skipped, b1 written once
	close(m.gate)
	if err := <-pubErr; err != nil {
		t.Fatalf("b2 async publish returned: %s", err)
	}
	select {
	case <-b1Done:
	case <-time.After(5 * time.Second):
		t.Fatal("b1 did not complete")
	}
	if err := b1.Wait(); err != nil {
		t.Fatalf("b1 should succeed: %s", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(m.getCommands()) < 1 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // allow a (buggy) second write to appear
	cmds := m.getCommands()
	if len(cmds) != 1 {
		t.Fatalf("canceled batch must not be written: %d MPUB commands on the wire", len(cmds))
	}
	if sent := parseMPUBBodies(t, cmds[0]); len(sent) != 1 {
		t.Fatalf("the single MPUB should be b1's, got %d messages", len(sent))
	}
}

// Canceling a batch that was never submitted resolves it terminally and
// releases its buffers; an in-flight batch cannot be revoked
func TestProducerBatchCancel(t *testing.T) {
	config := NewConfig()
	p, _ := NewProducer("127.0.0.1:0", config)
	p.SetLogger(nullLogger, LogLevelInfo)

	b, err := p.NewBatch("batch_cancel_ready", testBodies(2))
	if err != nil {
		t.Fatalf("NewBatch failed: %s", err)
	}
	if !b.Cancel() {
		t.Fatal("cancel of a ready batch should succeed")
	}
	if b.Cancel() {
		t.Fatal("second cancel should report false")
	}
	if err := b.Wait(); err != ErrBatchCanceled {
		t.Fatalf("Wait = %v, want ErrBatchCanceled", err)
	}
	if b.State() != BatchStateCanceled {
		t.Fatalf("state = %s, want canceled", b.State())
	}
	if err := p.PublishBatchAsync(b, nil); err == nil {
		t.Fatal("publishing a canceled batch should fail")
	}

	// an in-flight batch has already been sent and cannot be revoked
	m := newBatchTestConn()
	m.gate = make(chan struct{})
	m.onMPUB = func(cmd *Command) { m.respondOK() }
	p2 := newBatchTestProducer(m)
	defer p2.Stop()

	b2, err := p2.MultiPublishBatchAsync("batch_cancel_inflight", testBodies(1), nil)
	if err != nil {
		t.Fatalf("publish failed: %s", err)
	}
	waitForBatchState(t, b2, BatchStateInFlight)
	if b2.Cancel() {
		t.Fatal("an in-flight batch must not be cancelable")
	}
	close(m.gate)
	if err := b2.Wait(); err != nil {
		t.Fatalf("in-flight batch should complete normally: %s", err)
	}
}

// A partial response that does not account for every message must still
// terminate the batch deterministically
func TestBatchIncompletePartialResponse(t *testing.T) {
	b := newBatch("batch_incomplete", testBodies(2), "1-1")
	if !b.queue(nil) {
		t.Fatal("queue failed")
	}
	b.applyResponse(FrameTypeResponse, []byte("PARTIAL\n0 OK"))

	if b.State() != BatchStateDone {
		t.Fatalf("state = %s, want done", b.State())
	}
	results := b.Results()
	if results[0].Status != BatchMessageSucceeded {
		t.Fatalf("message 0 status = %d, want succeeded", results[0].Status)
	}
	if results[1].Status != BatchMessageFailed || results[1].Error != errBatchIncompleteResponse {
		t.Fatalf("message 1 = (%d, %v), want (failed, errBatchIncompleteResponse)",
			results[1].Status, results[1].Error)
	}
}

// A terminal batch ignores late or duplicate resolutions
func TestBatchTerminalIdempotent(t *testing.T) {
	b := newBatch("batch_idempotent", testBodies(2), "1-1")
	if !b.queue(nil) {
		t.Fatal("queue failed")
	}
	b.applyResponse(FrameTypeResponse, []byte("OK"))
	if err := b.Wait(); err != nil {
		t.Fatalf("Wait = %v", err)
	}

	// duplicate response and connection error arrive after completion
	b.applyResponse(FrameTypeError, []byte("E_FAILED late error"))
	b.applyConnError(ErrNotConnected)

	for _, r := range b.Results() {
		if r.Status != BatchMessageSucceeded {
			t.Fatalf("terminal result mutated: message %d status = %d", r.Index, r.Status)
		}
	}
	if err := b.Wait(); err != nil {
		t.Fatalf("Wait after duplicate resolutions = %v", err)
	}
}

// An unrecognized MPUB response body fails the whole batch as a protocol error
func TestBatchUnrecognizedResponse(t *testing.T) {
	b := newBatch("batch_unrecognized", testBodies(2), "1-1")
	if !b.queue(nil) {
		t.Fatal("queue failed")
	}
	b.applyResponse(FrameTypeResponse, []byte("SOMETHING-ELSE"))

	err := b.Wait()
	berr, ok := err.(*BatchError)
	if !ok || len(berr.Failed) != 2 {
		t.Fatalf("Wait = %v, want *BatchError with 2 failures", err)
	}
	for _, r := range b.Results() {
		if _, ok := r.Error.(ErrProtocol); !ok {
			t.Fatalf("message %d error = %T, want ErrProtocol", r.Index, r.Error)
		}
	}
}

// The existing ProducerTransaction-based async API is unaffected by batches
func TestProducerAsyncAPIUnchanged(t *testing.T) {
	m := newBatchTestConn()
	m.onMPUB = func(cmd *Command) { m.respondOK() }
	p := newBatchTestProducer(m)
	defer p.Stop()

	doneChan := make(chan *ProducerTransaction, 1)
	if err := p.MultiPublishAsync("batch_legacy", testBodies(3), doneChan, "arg"); err != nil {
		t.Fatalf("MultiPublishAsync failed: %s", err)
	}
	select {
	case trans := <-doneChan:
		if trans.Error != nil {
			t.Fatalf("transaction error: %s", trans.Error)
		}
		if trans.Args[0].(string) != "arg" {
			t.Fatalf("proxied arg = %v", trans.Args[0])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for transaction")
	}
}

// Errors must be comparable by identity for callers reconciling failures
func TestBatchErrorIdentity(t *testing.T) {
	if !errors.Is(ErrBatchCanceled, ErrBatchCanceled) {
		t.Fatal("ErrBatchCanceled should be comparable")
	}
	b := newBatch("batch_err", testBodies(1), "1-1")
	b.Cancel()
	var canceled = b.Wait()
	if !errors.Is(canceled, ErrBatchCanceled) {
		t.Fatalf("Wait = %v, want ErrBatchCanceled", canceled)
	}
}
