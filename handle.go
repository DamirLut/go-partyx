package partyx

import (
	"fmt"

	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/protocol"
)

func panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// Handle registers a typed global RPC handler: the request payload is
// decoded into Req (an empty payload yields a zero Req), Validate() is
// called when Req implements it (failures map to a 400 error), and the
// response is encoded by the framework. A nil response means an empty
// payload.
//
//	partyx.Handle(app, uint16(op.GameGuess), func(ctx *partyx.Context, req *GuessRequest) (*GuessResponse, error) {
//		return &GuessResponse{Correct: req.Word == "слово"}, nil
//	})
//
// Game logic that belongs to a room should live in room.Module handlers
// instead (they run inside the room actor). It panics when op is already
// claimed by a registered room type.
func Handle[Req any, Resp any, PReq interface {
	*Req
	protocol.Unmarshaler
}, PResp interface {
	*Resp
	protocol.Marshaler
}](app *App, op uint16, fn func(ctx *Context, req PReq) (PResp, error)) {
	HandleRaw(app, op, func(ctx *command.Context, payload []byte) (protocol.Marshaler, error) {
		req, err := protocol.Decode[Req, PReq](payload)
		if err != nil {
			return nil, protocol.NewError(400, "invalid payload")
		}
		if v, ok := any(PReq(req)).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return nil, protocol.NewError(400, err.Error())
			}
		}
		resp, err := fn(ctx, PReq(req))
		if err != nil {
			return nil, err
		}
		// Compare via the core type: a typed-nil pointer boxed in an
		// interface is not == nil, but the client expects an empty payload.
		if (*Resp)(resp) == nil {
			return nil, nil
		}
		return resp, nil
	})
}

// HandleRaw registers a global RPC handler that works with the raw
// arpack-encoded payload. Most code should use the generic Handle instead.
// It panics on a duplicate opcode or when op is claimed by a room type.
func HandleRaw(app *App, op uint16, fn func(ctx *Context, payload []byte) (protocol.Marshaler, error)) {
	if _, claimed := app.rooms.OpType(op); claimed {
		panicf("partyx: opcode %d is already claimed by a registered room type", op)
	}
	app.commands.Register(op, command.Handler(fn))
}
