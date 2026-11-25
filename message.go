package nsq

import (
	"encoding/binary"
	"errors"
	"fmt"
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
	// deliveryDeadline is unix nanoseconds (0 == none), derived from
	// MsgTimeout and extended by Touch. It is accessed atomically and kept
	// first for 32-bit alignment.
	deliveryDeadline int64

	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	NSQDAddress string

	Delegate MessageDelegate

	autoResponseDisabled int32
	responded            int32

	// deliveryConn, deliveryGeneration and deliveryAttempts identify the
	// connection generation that delivered this particular message instance
	// and the attempt count it was delivered with. Finish, Requeue and Touch
	// are only allowed while all of these still match the owning connection.
	//
	// superseded is set when the server redelivers the same message ID before
	// the previous delivery was responded to. The older message must never
	// produce a FIN/REQ afterwards.
	deliveryConn       *Conn
	deliveryGeneration uint64
	deliveryAttempts   uint16
	superseded         int32

	// releaseOnce ensures the message's in-flight accounting is released
	// exactly once, no matter how many response paths race with close.
	releaseOnce int32

	// untrackOnce makes delivery-reference removal idempotent independently
	// from in-flight counter release.
	untrackOnce int32

	// accounted is set after the connection has incremented its in-flight
	// counter for this delivery. Directly constructed unit-test messages can
	// leave it zero; rejection then must not drive the counter negative.
	accounted int32

	// stateMtx serializes the response methods so that a message can only
	// ever have a single terminal response and so that Touch commands cannot
	// be ordered after Finish/Requeue.
	stateMtx sync.Mutex

	// terminalKind and terminalErr record the winning terminal transition and
	// its local result. Repeated terminal calls return the same result, which
	// makes Finish/Requeue idempotent without issuing another command.
	terminalKind responseKind
	terminalErr  error
}

// NewMessage creates a Message, initializes some metadata,
// and returns a pointer
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// DisableAutoResponse disables the automatic response that
// would normally be sent when a handler.HandleMessage
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

// HasResponded indicates whether or not this message has reached a terminal
// response state.
//
// A message reaches this state after a successful Finish or Requeue, and also
// when a response is rejected locally because the message expired or no longer
// belongs to the connection that delivered it.
func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// Finish sends a FIN command to the nsqd which sent this message.
//
// Finish is idempotent: only the first terminal call can produce a command.
// The local outcome is also recorded and available from Err. The signature is
// intentionally unchanged so existing handler code and method values remain
// source compatible.
func (m *Message) Finish() {
	_ = m.respond(responseFinish, 0, false)
}

// Touch sends a TOUCH command to the nsqd which sent this message.
//
// Touch has no effect once the message has reached a terminal state. Repeated
// Touches while the message is active remain valid deadline heartbeats.
func (m *Message) Touch() {
	_ = m.respond(responseTouch, 0, false)
}

// Requeue sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// A delay of -1 will automatically calculate
// based on the number of attempts and the
// configured default_requeue_delay
func (m *Message) Requeue(delay time.Duration) {
	_ = m.respond(responseRequeue, delay, true)
}

// RequeueWithoutBackoff sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// Notably, using this method to respond does not trigger a backoff
// event on the configured Delegate.
func (m *Message) RequeueWithoutBackoff(delay time.Duration) {
	_ = m.respond(responseRequeue, delay, false)
}

type responseKind int

const (
	responseNone responseKind = iota
	responseFinish
	responseRequeue
	responseTouch
)

// resultMessageDelegate is implemented by the concrete connection delegate.
// It lets Message report the deterministic enqueue/close result without
// changing the public MessageDelegate interface.
type resultMessageDelegate interface {
	finishResponse(*Message) error
	requeueResponse(m *Message, delay time.Duration, backoff bool) error
	touchResponse(*Message) error
}

// Generation returns the receiving connection's generation. The zero value is
// returned for messages created locally and not yet delivered by a Conn.
func (m *Message) Generation() uint64 {
	return m.deliveryGeneration
}

// ConnectionGeneration is a descriptive alias for Generation.
func (m *Message) ConnectionGeneration() uint64 {
	return m.deliveryGeneration
}

// ReceivedAttempts returns the immutable attempt count carried by the delivery
// frame. The exported Attempts field remains mutable for handler compatibility;
// response validation and requeue backoff use this received value.
func (m *Message) ReceivedAttempts() uint16 {
	return m.deliveryAttempts
}

// Deadline returns the local processing deadline derived from MsgTimeout.
// It returns the zero time when no local deadline is enforced.
func (m *Message) Deadline() time.Time {
	if d := atomic.LoadInt64(&m.deliveryDeadline); d > 0 {
		return time.Unix(0, d)
	}
	return time.Time{}
}

// IsExpired reports whether the local delivery deadline has passed.
func (m *Message) IsExpired() bool {
	d := atomic.LoadInt64(&m.deliveryDeadline)
	return d > 0 && time.Now().UnixNano() > d
}

// Err returns the local result of the winning terminal response, or nil if no
// terminal call has completed with an error. It does not report asynchronous
// network write failures after a command has been accepted by the Conn.
func (m *Message) Err() error {
	m.stateMtx.Lock()
	defer m.stateMtx.Unlock()
	return m.terminalErr
}

// bindDelivery records the connection generation and attempt count that this
// message was delivered with. These values are verified again before a
// response is sent so that a late response can never be applied to a
// different delivery of the same message.
func (m *Message) bindDelivery(c *Conn, now time.Time) {
	m.deliveryConn = c
	m.deliveryGeneration = c.Generation()
	m.deliveryAttempts = m.Attempts
	if c.msgTimeout > 0 {
		atomic.StoreInt64(&m.deliveryDeadline, now.Add(c.msgTimeout).UnixNano())
	}
}

// responseAttempts returns the attempt count as it was received from nsqd.
// This is what requeue delays must be based on, even if the handler mutated
// the public Attempts field.
func (m *Message) responseAttempts() uint16 {
	if m.deliveryAttempts != 0 || m.deliveryConn != nil {
		return m.deliveryAttempts
	}
	return m.Attempts
}

// respond is the single entry point for Finish, Requeue and Touch.
func (m *Message) respond(kind responseKind, delay time.Duration, backoff bool) error {
	m.stateMtx.Lock()
	defer m.stateMtx.Unlock()

	// A winning terminal state makes every later terminal/Touch call stable
	// and idempotent. Repeating the winning operation returns its original
	// local result; a conflicting terminal operation reports the settled
	// state without issuing another command.
	if m.terminalKind != responseNone {
		if kind == m.terminalKind {
			return m.terminalErr
		}
		if kind == responseTouch {
			return fmt.Errorf("%w: touch after terminal response %d", ErrAlreadyResponded, m.terminalKind)
		}
		return fmt.Errorf("%w: terminal response %d already won", ErrAlreadyResponded, m.terminalKind)
	}

	if m.Delegate == nil {
		if kind != responseTouch {
			m.markTerminal(kind, nil)
		}
		return nil
	}

	if c := m.deliveryConn; c != nil {
		now := time.Now()
		if err := c.responseError(m, now); err != nil {
			if kind == responseTouch {
				return err
			}
			return m.failTerminalResponse(c, kind, err)
		}

		if kind == responseTouch {
			err := m.dispatchLocked(kind, delay, backoff)
			if err == nil && c.msgTimeout > 0 {
				atomic.StoreInt64(&m.deliveryDeadline, now.Add(c.msgTimeout).UnixNano())
			}
			return err
		}

		m.markTerminal(kind, nil)
		err := m.dispatchLocked(kind, delay, backoff)
		if err != nil {
			m.terminalErr = err
			// If the delegate rejected the command before enqueue, this is
			// the only release path. If enqueueResponse hit responseDone, it
			// already released both references; both operations are CAS-safe.
			c.untrackDelivery(m)
			c.releaseMessage(m)
			if atomic.LoadInt64(&c.messagesInFlight) == 0 &&
				atomic.LoadInt32(&c.closeFlag) == 1 {
				c.close()
			}
		}
		m.releaseReferencesLocked()
		return err
	}

	if kind == responseTouch {
		return m.dispatchLocked(kind, delay, backoff)
	}

	m.markTerminal(kind, nil)
	err := m.dispatchLocked(kind, delay, backoff)
	if err != nil {
		m.terminalErr = err
	}
	m.releaseReferencesLocked()
	return err
}

func (m *Message) markTerminal(kind responseKind, err error) {
	m.terminalKind = kind
	m.terminalErr = err
	atomic.StoreInt32(&m.responded, 1)
}

func (m *Message) failTerminalResponse(c *Conn, kind responseKind, err error) error {
	m.markTerminal(kind, err)
	c.log(LogLevelWarning,
		"dropping late response for msg %s (generation %d, attempts %d): %s",
		m.ID, m.deliveryGeneration, m.deliveryAttempts, err)

	// Retire local delivery/reference bookkeeping without writing a misleading
	// FIN/REQ. releaseMessage is idempotent across this path and close drain.
	c.untrackDelivery(m)
	c.releaseMessage(m)
	// A directly-rejected terminal response does not pass through writeLoop,
	// so it must advance the same delayed-close state machine itself.
	if atomic.LoadInt64(&c.messagesInFlight) == 0 && atomic.LoadInt32(&c.closeFlag) == 1 {
		c.close()
	}
	m.releaseReferencesLocked()
	return err
}

func (m *Message) dispatchLocked(kind responseKind, delay time.Duration, backoff bool) error {
	delegate := m.Delegate
	if rd, ok := delegate.(resultMessageDelegate); ok {
		switch kind {
		case responseFinish:
			return rd.finishResponse(m)
		case responseRequeue:
			return rd.requeueResponse(m, delay, backoff)
		case responseTouch:
			return rd.touchResponse(m)
		}
	}

	switch kind {
	case responseFinish:
		delegate.OnFinish(m)
	case responseRequeue:
		delegate.OnRequeue(m, delay, backoff)
	case responseTouch:
		delegate.OnTouch(m)
	}
	return nil
}

// releaseReferencesLocked breaks the Message -> Conn reference after a
// terminal call. The immutable generation/attempt values stay on Message for
// diagnostics and duplicate-call matching.
func (m *Message) releaseReferencesLocked() {
	m.deliveryConn = nil
	m.Delegate = nil
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
