package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// ErrBatchCanceled is recorded as the per-message error (and returned by
// PublishBatch.Wait) when a PublishBatch is canceled before it was written
// to a connection
var ErrBatchCanceled = errors.New("batch canceled")

// errBatchIncompleteResponse is recorded for any message that a partial
// MPUB response failed to account for, so the batch still reaches a
// deterministic terminal state
var errBatchIncompleteResponse = errors.New("partial response did not account for every message in the batch")

// BatchState is the lifecycle state of a PublishBatch
type BatchState int32

const (
	// BatchStateReady - created, not yet submitted to a Producer
	BatchStateReady BatchState = iota
	// BatchStateQueued - submitted, waiting to be written to a connection
	BatchStateQueued
	// BatchStateInFlight - written to a connection, awaiting the response
	BatchStateInFlight
	// BatchStateDone - terminal: every message resolved (success or failure)
	BatchStateDone
	// BatchStateCanceled - terminal: canceled before being written
	BatchStateCanceled
)

// String returns a human readable representation of the BatchState
func (s BatchState) String() string {
	switch s {
	case BatchStateReady:
		return "ready"
	case BatchStateQueued:
		return "queued"
	case BatchStateInFlight:
		return "in-flight"
	case BatchStateDone:
		return "done"
	case BatchStateCanceled:
		return "canceled"
	}
	return "unknown"
}

func (s BatchState) terminal() bool {
	return s == BatchStateDone || s == BatchStateCanceled
}

// BatchMessageStatus is the resolution status of a single message in a PublishBatch
type BatchMessageStatus int32

const (
	// BatchMessagePending - no result recorded yet
	BatchMessagePending BatchMessageStatus = iota
	// BatchMessageSucceeded - accepted by nsqd; will never be retried by the batch
	BatchMessageSucceeded
	// BatchMessageFailed - rejected or unresolved; eligible for retry via RetryBatch
	BatchMessageFailed
)

// BatchMessageResult maps one message of a batch to its outcome.
//
// Index and Body identify the original message as passed to NewBatch.
type BatchMessageResult struct {
	Index  int
	Body   []byte
	Status BatchMessageStatus
	Error  error
}

// BatchError is returned by PublishBatch.Wait when a batch completed with
// one or more failed messages. It aggregates the independent per-message
// results so callers can reconcile (or retry) exactly the failed entries.
type BatchError struct {
	BatchID string
	Failed  []BatchMessageResult
}

// Error returns a stringified error
func (e *BatchError) Error() string {
	return fmt.Sprintf("batch %s: %d of its messages failed", e.BatchID, len(e.Failed))
}

// PublishBatch tracks a single MPUB submission as a first-class,
// reconcilable unit of work.
//
// A batch has a stable correlation identity (ID) and one result slot per
// original message. A batch instance is written to at most one connection
// exactly once; retrying failures produces a new derived batch (see
// RetryBatch) containing only the failed messages, so messages already
// accepted by nsqd are never re-published.
//
// A PublishBatch is not safe to construct directly; use Producer.NewBatch
// or Producer.MultiPublishBatchAsync.
type PublishBatch struct {
	// ID is the stable correlation identity of this batch. It is unique
	// per Producer and, for retries, derived from the original batch ID.
	ID string

	topic  string
	bodies [][]byte

	mu       sync.Mutex
	state    BatchState
	results  []BatchMessageResult
	pending  int
	attempt  int
	baseID   string
	conn     producerConn // the connection the batch was written to (nil until sent)
	doneCh   chan struct{}
	doneChan chan *PublishBatch
}

func newBatch(topic string, bodies [][]byte, id string) *PublishBatch {
	b := &PublishBatch{
		ID:     id,
		topic:  topic,
		bodies: bodies,
		baseID: id,
		state:  BatchStateReady,
		doneCh: make(chan struct{}),
	}
	b.results = make([]BatchMessageResult, len(bodies))
	for i, body := range bodies {
		b.results[i] = BatchMessageResult{Index: i, Body: body, Status: BatchMessagePending}
	}
	b.pending = len(bodies)
	return b
}

// Topic returns the topic the batch publishes to
func (b *PublishBatch) Topic() string { return b.topic }

// Size returns the number of messages in the batch
func (b *PublishBatch) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.results)
}

// State returns the current lifecycle state of the batch
func (b *PublishBatch) State() BatchState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Pending returns the number of messages without a recorded result
func (b *PublishBatch) Pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pending
}

// Done returns a channel that is closed when the batch reaches a terminal
// state (BatchStateDone or BatchStateCanceled)
func (b *PublishBatch) Done() <-chan struct{} { return b.doneCh }

// Results returns a snapshot of the per-message results, in the original
// message order. Entries are BatchMessagePending until resolved.
func (b *PublishBatch) Results() []BatchMessageResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	results := make([]BatchMessageResult, len(b.results))
	copy(results, b.results)
	return results
}

// Wait blocks until the batch reaches a terminal state and returns the
// aggregate outcome: nil if every message succeeded, a *BatchError
// aggregating the independent per-message failures, or ErrBatchCanceled
// if the batch was canceled.
func (b *PublishBatch) Wait() error {
	<-b.doneCh
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BatchStateCanceled {
		return ErrBatchCanceled
	}
	var failed []BatchMessageResult
	for _, r := range b.results {
		if r.Status == BatchMessageFailed {
			failed = append(failed, r)
		}
	}
	if len(failed) == 0 {
		return nil
	}
	return &BatchError{BatchID: b.ID, Failed: failed}
}

// Cancel transitions a not-yet-written batch to its terminal canceled
// state: every pending message is failed with ErrBatchCanceled, the batch
// buffers are released, and waiters are unblocked.
//
// Cancel returns false (and has no effect) once the batch has been written
// to a connection — an in-flight batch cannot be revoked, its already-sent
// messages are still resolved by the response or by the connection
// terminating.
func (b *PublishBatch) Cancel() bool {
	b.mu.Lock()
	if b.state != BatchStateReady && b.state != BatchStateQueued {
		b.mu.Unlock()
		return false
	}
	for i := range b.results {
		b.resolveLocked(i, ErrBatchCanceled)
	}
	b.state = BatchStateCanceled
	b.releaseLocked()
	close(b.doneCh)
	dc := b.doneChan
	b.mu.Unlock()

	if dc != nil {
		// best-effort: a caller that cancels may no longer be draining
		// doneChan; Done/Wait remain the guaranteed notification
		select {
		case dc <- b:
		default:
		}
	}
	return true
}

// FailedBodies returns the bodies of the messages that failed, in original
// order. It is only meaningful once the batch is terminal.
func (b *PublishBatch) FailedBodies() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	var failed [][]byte
	for _, r := range b.results {
		if r.Status == BatchMessageFailed {
			failed = append(failed, r.Body)
		}
	}
	return failed
}

// RetryBatch derives a new batch containing only the messages that failed
// in this batch. Messages already accepted by nsqd are excluded, so they
// are never published twice. The derived batch has its own independent
// results and an ID of the form "<baseID>-r<attempt>".
//
// It returns an error if the batch has not completed, has no failures, or
// its buffers have been released.
func (b *PublishBatch) RetryBatch() (*PublishBatch, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BatchStateDone {
		return nil, fmt.Errorf("nsq: cannot retry batch %s in state %s", b.ID, b.state)
	}
	var failed [][]byte
	for _, r := range b.results {
		if r.Status == BatchMessageFailed {
			if r.Body == nil {
				return nil, fmt.Errorf("nsq: batch %s buffers have been released", b.ID)
			}
			failed = append(failed, r.Body)
		}
	}
	if len(failed) == 0 {
		return nil, fmt.Errorf("nsq: batch %s has no failed messages to retry", b.ID)
	}
	attempt := b.attempt + 1
	nb := newBatch(b.topic, failed, fmt.Sprintf("%s-r%d", b.baseID, attempt))
	nb.attempt = attempt
	return nb, nil
}

// Release drops the batch's references to the message bodies. Results keep
// their Status and Error but their Body is set to nil. It has no effect on
// a batch that has not reached a terminal state.
func (b *PublishBatch) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.state.terminal() {
		return
	}
	b.releaseLocked()
}

func (b *PublishBatch) releaseLocked() {
	b.bodies = nil
	for i := range b.results {
		b.results[i].Body = nil
	}
}

// queue transitions Ready -> Queued, recording the completion channel.
// Returns false if the batch was already submitted or canceled.
func (b *PublishBatch) queue(doneChan chan *PublishBatch) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BatchStateReady {
		return false
	}
	b.state = BatchStateQueued
	b.doneChan = doneChan
	return true
}

// command serializes the batch into its MPUB command
func (b *PublishBatch) command() (*Command, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return MultiPublish(b.topic, b.bodies)
}

// markSent transitions Queued -> InFlight, recording the connection the
// batch is being written to. A batch can be sent at most once, on a single
// connection; it returns false if the batch was canceled while queued.
func (b *PublishBatch) markSent(c producerConn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BatchStateQueued {
		return false
	}
	b.state = BatchStateInFlight
	b.conn = c
	return true
}

// resolveLocked records the outcome of message i. Duplicate resolutions are
// ignored so a terminal batch can never be mutated by a late response.
func (b *PublishBatch) resolveLocked(i int, err error) {
	r := &b.results[i]
	if r.Status != BatchMessagePending {
		return
	}
	if err == nil {
		r.Status = BatchMessageSucceeded
	} else {
		r.Status = BatchMessageFailed
		r.Error = err
	}
	b.pending--
}

// finishLocked resolves every remaining pending message with err (nil for
// success) and moves the batch to BatchStateDone. Caller must not hold mu;
// the doneChan delivery happens after the lock is released.
func (b *PublishBatch) finish(err error) {
	b.mu.Lock()
	if b.state.terminal() {
		b.mu.Unlock()
		return
	}
	for i := range b.results {
		b.resolveLocked(i, err)
	}
	b.state = BatchStateDone
	close(b.doneCh)
	dc := b.doneChan
	b.mu.Unlock()

	if dc != nil {
		dc <- b
	}
}

// applyResponse maps an MPUB response frame onto the individual messages
// of the batch, after which the batch is terminal:
//
//   - an error frame fails every pending message with the protocol error
//   - an "OK" response accepts every pending message
//   - a "PARTIAL" response (see parsePartialBatchResponse) resolves each
//     message independently; any message the response does not account for
//     is failed with errBatchIncompleteResponse
func (b *PublishBatch) applyResponse(frameType int32, data []byte) {
	if frameType == FrameTypeError {
		b.finish(ErrProtocol{string(data)})
		return
	}
	if bytes.Equal(data, []byte("OK")) {
		b.finish(nil)
		return
	}
	results, err := parsePartialBatchResponse(data)
	if err != nil {
		b.finish(err)
		return
	}
	b.mu.Lock()
	if b.state.terminal() {
		b.mu.Unlock()
		return
	}
	for idx, rerr := range results {
		if idx < 0 || idx >= len(b.results) {
			continue
		}
		b.resolveLocked(idx, rerr)
	}
	// a partial response is the only response this MPUB will get; fail
	// anything it did not account for so the batch is deterministic
	for i := range b.results {
		b.resolveLocked(i, errBatchIncompleteResponse)
	}
	b.state = BatchStateDone
	close(b.doneCh)
	dc := b.doneChan
	b.mu.Unlock()

	if dc != nil {
		dc <- b
	}
}

// applyConnError terminally fails every pending message when the
// connection the batch was queued on or written to is lost
func (b *PublishBatch) applyConnError(err error) {
	b.finish(err)
}

// parsePartialBatchResponse parses a per-message MPUB response body:
//
//	PARTIAL\n
//	<index> OK\n
//	<index> E_<reason>\n
//
// and returns a map of message index to its individual result (nil error
// for accepted messages).
func parsePartialBatchResponse(data []byte) (map[int]error, error) {
	lines := strings.Split(string(data), "\n")
	if lines[0] != "PARTIAL" {
		reason := string(data)
		if len(reason) > 32 {
			reason = reason[:32]
		}
		return nil, ErrProtocol{fmt.Sprintf("unrecognized MPUB response: %q", reason)}
	}
	results := make(map[int]error)
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		sep := strings.IndexByte(line, ' ')
		if sep <= 0 {
			return nil, ErrProtocol{fmt.Sprintf("malformed partial response line: %q", line)}
		}
		idx, err := strconv.Atoi(line[:sep])
		if err != nil {
			return nil, ErrProtocol{fmt.Sprintf("malformed partial response index: %q", line[:sep])}
		}
		status := line[sep+1:]
		if status == "OK" {
			results[idx] = nil
		} else {
			results[idx] = ErrProtocol{status}
		}
	}
	return results, nil
}
