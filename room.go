package partyx

import (
	"context"
	"time"

	"github.com/damirlut/go-partyx/protocol"
	"github.com/damirlut/go-partyx/room"
)

// RoomBuilder is the fluent definition of a room type. It reads top to
// bottom — settings, hooks, handlers — and ends with Register:
//
//	partyx.Room[WordState]("wordgame").
//		State(func() *WordState { return &WordState{Round: 1} }).
//		MaxPlayers(2).
//		Singleton(protocol.SingletonReject).
//		OnJoin(func(ctx context.Context, r *room.Room[WordState], p *room.Player) { ... }).
//		Handle(uint16(messages.GameOpGuess), guessHandler).
//		Register(app)
//
// The state type must be named explicitly: Go cannot infer it from the
// chained calls. Under the hood the builder assembles an opaque room.Module.
type RoomBuilder[S any] struct {
	module *room.Module[S]
}

// Room starts the definition of a room type. typ is matched against
// CreateRoomRequest.Type.
func Room[S any](typ string) *RoomBuilder[S] {
	return &RoomBuilder[S]{module: room.NewModule[S](typ)}
}

// State sets the state factory; without it the state is the zero value of S.
func (b *RoomBuilder[S]) State(fn func() *S) *RoomBuilder[S] {
	b.module.State(fn)
	return b
}

// MaxPlayers sets the default player cap (0 = unlimited); the create request
// may override it per room.
func (b *RoomBuilder[S]) MaxPlayers(n uint16) *RoomBuilder[S] {
	b.module.MaxPlayers(n)
	return b
}

// Singleton sets the mode for rooms of this type; the create request cannot
// override it.
func (b *RoomBuilder[S]) Singleton(mode protocol.SingletonMode) *RoomBuilder[S] {
	b.module.Singleton(mode)
	return b
}

// EmptyGrace keeps an emptied room alive for the given duration before it
// is removed, so a disconnected player can reconnect into their seat; a
// join during the grace cancels the removal. The create request cannot
// override it.
func (b *RoomBuilder[S]) EmptyGrace(d time.Duration) *RoomBuilder[S] {
	b.module.EmptyGrace(d)
	return b
}

// OnInit runs synchronously at construction, before the room is shared. ctx
// is the room's lifetime context.
func (b *RoomBuilder[S]) OnInit(fn func(ctx context.Context, r *room.Room[S])) *RoomBuilder[S] {
	b.module.OnInit(fn)
	return b
}

// OnJoin runs in the same serialized step as the player add. ctx is the
// room's lifetime context.
func (b *RoomBuilder[S]) OnJoin(fn func(ctx context.Context, r *room.Room[S], p *room.Player)) *RoomBuilder[S] {
	b.module.OnJoin(fn)
	return b
}

func (b *RoomBuilder[S]) OnLeave(fn func(ctx context.Context, r *room.Room[S], p *room.Player)) *RoomBuilder[S] {
	b.module.OnLeave(fn)
	return b
}

// OnClose runs when the room shuts down; ctx is already canceled.
func (b *RoomBuilder[S]) OnClose(fn func(ctx context.Context, r *room.Room[S])) *RoomBuilder[S] {
	b.module.OnClose(fn)
	return b
}

// Tick enables the game loop: fn runs inside the actor every rate; dt is the
// time since the previous tick. ctx is the room's lifetime context.
func (b *RoomBuilder[S]) Tick(rate time.Duration, fn func(ctx context.Context, r *room.Room[S], dt time.Duration)) *RoomBuilder[S] {
	b.module.Tick(rate, fn)
	return b
}

// HandleRaw registers a raw (bytes-in, Marshaler-out) handler; most code
// should use Handle. Panics on a duplicate opcode.
func (b *RoomBuilder[S]) HandleRaw(op uint16, h room.MessageHandler[S]) *RoomBuilder[S] {
	b.module.HandleRaw(op, h)
	return b
}

// Handle registers a typed message handler: the payload is decoded into
// Req (an empty payload yields a zero Req), Validate() is called when
// Req implements it (failures map to a 400 error), and the response is
// encoded by the framework (a nil response means an empty payload). Panics
// on a duplicate opcode. All type parameters are inferred from fn.
func (b *RoomBuilder[S]) Handle[Req, Resp any, PReq interface {
	*Req
	protocol.Unmarshaler
}, PResp interface {
	*Resp
	protocol.Marshaler
}](op uint16, fn func(ctx context.Context, r *room.Room[S], p *room.Player, req PReq) (PResp, error)) *RoomBuilder[S] {
	b.module.Handle(op, fn)
	return b
}

// Register registers the room type: rooms created with CreateRoomRequest.Type
// == typ get the defined state, hooks, game loop and handlers. Panics on a
// duplicate type or an opcode conflict with a global command.
func (b *RoomBuilder[S]) Register(app *App) {
	RegisterRoomType(app, b.module)
}
