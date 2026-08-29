package partyx

import (
	"fmt"

	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/protocol"
)

func panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// Handle registers a typed global RPC handler: the payload is decoded into
// Req (an empty payload yields a zero Req), Validate() is called when Req
// implements it (failures map to a 400 error), and the response is encoded
// by the framework (a nil response means an empty payload). Panics when op
// is already claimed by a registered room type — room-bound game logic
// belongs in room.Module handlers instead.
//
//	app.Handle(uint16(op.GameGuess), func(ctx *partyx.Context, req *GuessRequest) (*GuessResponse, error) {
//		return &GuessResponse{Correct: req.Word == "слово"}, nil
//	})
func (app *App) Handle[Req any, Resp any, PReq interface {
	*Req
	protocol.Unmarshaler
}, PResp interface {
	*Resp
	protocol.Marshaler
}](op uint16, fn func(ctx *Context, req PReq) (PResp, error)) {
	app.HandleRaw(op, func(ctx *command.Context, payload []byte) (protocol.Marshaler, error) {
		return protocol.Call(payload, func(req PReq) (PResp, error) {
			return fn(ctx, req)
		})
	})
}

// HandleRaw registers a global RPC handler working with the raw
// arpack-encoded payload; most code should use Handle instead. Panics on a
// duplicate opcode or when op is claimed by a room type.
func (app *App) HandleRaw(op uint16, fn func(ctx *Context, payload []byte) (protocol.Marshaler, error)) {
	if _, claimed := app.rooms.OpType(op); claimed {
		panicf("partyx: opcode %d is already claimed by a registered room type", op)
	}
	app.commands.Register(op, command.Handler(fn))
}
