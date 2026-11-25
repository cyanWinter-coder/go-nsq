package nsq

import (
	"errors"
	"fmt"
)

// ErrNotConnected is returned when a publish command is made
// against a Producer that is not connected
var ErrNotConnected = errors.New("not connected")

// ErrStopped is returned when a publish command is
// made against a Producer that has been stopped
var ErrStopped = errors.New("stopped")

// ErrClosing is returned when a connection is closing
var ErrClosing = errors.New("closing")

// ErrDeliveryClosed is returned when a message response cannot be queued
// because its connection has finished draining. It wraps ErrClosing so callers
// can classify either name.
var ErrDeliveryClosed = fmt.Errorf("%w: message delivery connection closed", ErrClosing)

// ErrAlreadyConnected is returned from ConnectToNSQD when already connected
var ErrAlreadyConnected = errors.New("already connected")

// ErrAlreadyResponded is returned when a conflicting terminal response is
// attempted after another Finish or Requeue already won. Repeating the same
// terminal call remains idempotent and returns its original result.
var ErrAlreadyResponded = errors.New("message already has a terminal response")

// ErrMessageExpired is returned when a response is attempted after the local
// delivery deadline derived from NSQ's message timeout.
var ErrMessageExpired = errors.New("message delivery deadline expired")

// ErrStaleDelivery is returned when a response carries an old connection
// generation or attempt and no longer matches the current delivery.
var ErrStaleDelivery = errors.New("stale message delivery generation or attempt")

// ErrStaleGeneration is an alias kept for callers that name the connection
// generation case explicitly.
var ErrStaleGeneration = ErrStaleDelivery

// ErrAttemptMismatch is an alias kept for callers that name the redelivery
// attempt case explicitly.
var ErrAttemptMismatch = ErrStaleDelivery

// ErrOverMaxInFlight is returned from Consumer if over max-in-flight
var ErrOverMaxInFlight = errors.New("over configure max-inflight")

// ErrIdentify is returned from Conn as part of the IDENTIFY handshake
type ErrIdentify struct {
	Reason string
}

// Error returns a stringified error
func (e ErrIdentify) Error() string {
	return fmt.Sprintf("failed to IDENTIFY - %s", e.Reason)
}

// ErrProtocol is returned from Producer when encountering
// an NSQ protocol level error
type ErrProtocol struct {
	Reason string
}

// Error returns a stringified error
func (e ErrProtocol) Error() string {
	return e.Reason
}
