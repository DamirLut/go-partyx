package partyx

import (
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
//		OnJoin(func(r *room.Room[WordState], p *room.Player) { ... }).
//		Handle(uint16(messages.GameOpGuess), room.Typed(guessHandler)).
//		Register(app)
//
// The type parameter must be named explicitly: Go cannot infer it from the
// chained State call. Under the hood the builder assembles an opaque
// room.Module, so its internals can evolve without breaking your code.
type RoomBuilder[S any] struct {
	module *room.Module[S]
}

// Room starts the definition of a room type. typ is matched against
// CreateRoomRequest.Type.
func Room[S any](typ string) *RoomBuilder[S] {
	return &RoomBuilder[S]{module: room.NewModule[S](typ)}
}

// State sets the factory for the initial game state. Without it the state
// is the zero value of S.
func (b *RoomBuilder[S]) State(fn func() *S) *RoomBuilder[S] {
	b.module.State(fn)
	return b
}

// MaxPlayers sets the default player cap (0 = unlimited). The create
// request may override it per room.
func (b *RoomBuilder[S]) MaxPlayers(n uint16) *RoomBuilder[S] {
	b.module.MaxPlayers(n)
	return b
}

// Singleton sets the singleton mode for rooms of this type. It always comes
// from the module; the create request cannot override it.
func (b *RoomBuilder[S]) Singleton(mode protocol.SingletonMode) *RoomBuilder[S] {
	b.module.Singleton(mode)
	return b
}

// OnInit runs once when the room is created, synchronously before the actor
// starts.
func (b *RoomBuilder[S]) OnInit(fn func(r *room.Room[S])) *RoomBuilder[S] {
	b.module.OnInit(fn)
	return b
}

// OnJoin runs after a player is added to the room.
func (b *RoomBuilder[S]) OnJoin(fn func(r *room.Room[S], p *room.Player)) *RoomBuilder[S] {
	b.module.OnJoin(fn)
	return b
}

// OnLeave runs after a player is removed from the room.
func (b *RoomBuilder[S]) OnLeave(fn func(r *room.Room[S], p *room.Player)) *RoomBuilder[S] {
	b.module.OnLeave(fn)
	return b
}

// OnClose runs when the room is shut down (removed).
func (b *RoomBuilder[S]) OnClose(fn func(r *room.Room[S])) *RoomBuilder[S] {
	b.module.OnClose(fn)
	return b
}

// Tick enables the game loop: fn runs inside the actor every rate. dt is
// the elapsed time since the previous tick.
func (b *RoomBuilder[S]) Tick(rate time.Duration, fn func(r *room.Room[S], dt time.Duration)) *RoomBuilder[S] {
	b.module.Tick(rate, fn)
	return b
}

// Handle registers a handler for op and returns the builder for chaining.
// It panics on a duplicate opcode. Wrap a typed handler with room.Typed —
// Go has no generic methods, so a typed Handle cannot be a method:
//
//	Handle(uint16(messages.GameOpGuess), room.Typed(guessHandler))
func (b *RoomBuilder[S]) Handle(op uint16, h room.MessageHandler[S]) *RoomBuilder[S] {
	b.module.Handle(op, h)
	return b
}

// Register registers the room type with the app: rooms created with
// CreateRoomRequest.Type == typ get the defined state, hooks, game loop and
// message handlers. It panics on a duplicate type, or when one of the
// opcodes is already registered globally.
func (b *RoomBuilder[S]) Register(app *App) {
	RegisterRoomType(app, b.module)
}
