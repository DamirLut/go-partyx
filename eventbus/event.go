package eventbus

import "github.com/damirlut/go-partyx/protocol"

// Event is a published message: a numeric opcode plus an already encoded
// payload, so the payload is marshaled exactly once per publish no matter
// how many subscribers receive it.
type Event struct {
	Op      uint16
	Payload []byte
}

// NewEvent encodes msg eagerly and returns the event. A nil msg yields an
// empty payload.
func NewEvent(op uint16, msg protocol.Marshaler) Event {
	return Event{Op: op, Payload: protocol.Encode(msg)}
}

// NewEventBytes wraps a pre-encoded payload.
func NewEventBytes(op uint16, payload []byte) Event {
	return Event{Op: op, Payload: payload}
}
