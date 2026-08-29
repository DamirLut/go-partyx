package command

import "github.com/damirlut/go-partyx/protocol"

// Handler processes an RPC request. payload is the raw arpack-encoded
// request body; the returned Marshaler is encoded once (nil means an empty
// payload). Handlers run on the calling client's read loop: do not block on
// long work here — room-bound game logic belongs in room.Module handlers,
// which run inside the room actor.
type Handler func(ctx *Context, payload []byte) (protocol.Marshaler, error)
