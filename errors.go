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

// ErrAlreadyConnected is returned from ConnectToNSQD when already connected
var ErrAlreadyConnected = errors.New("already connected")

// ErrOverMaxInFlight is returned from Consumer if over max-in-flight
var ErrOverMaxInFlight = errors.New("over configure max-inflight")

// ErrReconnectQueueFull is returned when a command cannot be queued for
// delivery on the next connection generation because the bounded
// reconnect queue for that nsqd address is full
var ErrReconnectQueueFull = errors.New("reconnect queue full")

// ErrCommandExpired is delivered to the waiter of a queued command when
// the command was not flushed to a new connection generation within
// the configured reconnect queue timeout
var ErrCommandExpired = errors.New("command expired waiting for reconnect")

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
