package command

import "github.com/damirlut/go-partyx/protocol"

// Handler processes an RPC request. payload is the raw arpack-encoded
// request body from the envelope; the returned Marshaler is encoded once
// and sent as the response payload (nil means an empty payload).
//
// Handlers are invoked from the calling client's read loop: do not block on
// long work here. Room-scoped game logic belongs in room.Module handlers,
// which run inside the room actor.
type Handler func(ctx *Context, payload []byte) (protocol.Marshaler, error)
