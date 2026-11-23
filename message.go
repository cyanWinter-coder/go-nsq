package nsq

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// The number of bytes for a Message.ID
const MsgIDLength = 16

// MessageID is the ASCII encoded hexadecimal message ID
type MessageID [MsgIDLength]byte

// Message is the fundamental data type containing
// the id, body, and metadata
type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	NSQDAddress string

	Delegate MessageDelegate

	autoResponseDisabled int32
	responded            int32

	// responseMtx serializes the decision logic of Finish/Requeue/Touch so
	// that a Touch racing with a terminal response can never observe a
	// stale state (the old HasResponded() check was a TOCTOU window).
	responseMtx sync.Mutex

	// ctx is canceled when the Consumer is stopping; handlers that select on
	// it can abandon in-flight work instead of blocking shutdown.
	ctx      context.Context
	cancelFn context.CancelFunc
}

// NewMessage creates a Message, initializes some metadata,
// and returns a pointer
func NewMessage(id MessageID, body []byte) *Message {
	ctx, cancel := context.WithCancel(context.Background())
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
		ctx:       ctx,
		cancelFn:  cancel,
	}
}

// Context returns a context that is canceled when the Consumer shuts down
// while this message is still in flight. Handlers may select on
// Context().Done() to make long-running processing interruptible.
func (m *Message) Context() context.Context {
	return m.ctx
}

// Canceled is a non-blocking check for whether the Consumer has given up
// waiting for this message's handler to complete.
func (m *Message) Canceled() bool {
	select {
	case <-m.ctx.Done():
		return true
	default:
		return false
	}
}

// cancel interrupts a handler blocked on Context().Done(). It is a no-op once
// a terminal response (FIN/REQ) has been sent, because a responded message is
// no longer considered in flight by the Consumer.
func (m *Message) cancel() {
	m.responseMtx.Lock()
	defer m.responseMtx.Unlock()
	if atomic.LoadInt32(&m.responded) == 0 {
		m.cancelFn()
	}
}

// DisableAutoResponse disables the automatic response that
// returns (FIN/REQ based on the error value returned).
//
// This is useful if you want to batch, buffer, or asynchronously
// respond to messages.
func (m *Message) DisableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 1)
}

// IsAutoResponseDisabled indicates whether or not this message
// will be responded to automatically
func (m *Message) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}

// HasResponded indicates whether or not this message has been responded to
func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// Finish sends a FIN command to the nsqd which
// sent this message
func (m *Message) Finish() {
	if !m.beginTerminalResponse() {
		return
	}
	m.Delegate.OnFinish(m)
}

// Touch sends a TOUCH command to the nsqd which
// sent this message
func (m *Message) Touch() {
	m.responseMtx.Lock()
	defer m.responseMtx.Unlock()
	// holding responseMtx guarantees a Finish/Requeue cannot win the
	// terminal-state CAS between this check and the delegate call
	if atomic.LoadInt32(&m.responded) == 1 {
		return
	}
	m.Delegate.OnTouch(m)
}

// Requeue sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// A delay of -1 will automatically calculate
// based on the number of attempts and the
// configured default_requeue_delay
func (m *Message) Requeue(delay time.Duration) {
	m.doRequeue(delay, true)
}

// RequeueWithoutBackoff sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// Notably, using this method to respond does not trigger a backoff
// event on the configured Delegate.
func (m *Message) RequeueWithoutBackoff(delay time.Duration) {
	m.doRequeue(delay, false)
}

func (m *Message) doRequeue(delay time.Duration, backoff bool) {
	if !m.beginTerminalResponse() {
		return
	}
	m.Delegate.OnRequeue(m, delay, backoff)
}

// beginTerminalResponse claims the single terminal-response slot.
// Only the first Finish/Requeue wins; every later terminal call, and any
// Touch after it, is suppressed. The mutex makes the claim mutually
// exclusive with Touch's state check.
func (m *Message) beginTerminalResponse() bool {
	m.responseMtx.Lock()
	defer m.responseMtx.Unlock()
	return atomic.CompareAndSwapInt32(&m.responded, 0, 1)
}

// WriteTo implements the WriterTo interface and serializes
// the message into the supplied producer.
//
// It is suggested that the target Writer is buffered to
// avoid performing many system calls.
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// DecodeMessage deserializes data (as []byte) and creates a new Message
// message format:
//
//	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
//	|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
//	|       8-byte         ||    ||                 16-byte                      || N-byte
//	------------------------------------------------------------------------------------------...
//	  nanosecond timestamp    ^^                   message ID                       message body
//	                       (uint16)
//	                        2-byte
//	                       attempts
func DecodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < 10+MsgIDLength {
		return nil, errors.New("not enough data to decode valid message")
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}
