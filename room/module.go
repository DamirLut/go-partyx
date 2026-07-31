package room

import (
	"fmt"
	"time"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

// Module describes a room type: how to build its game state, lifecycle
// hooks, a game loop, and the opcodes of its messages. All hooks and
// handlers run inside the room actor, so they may freely read and mutate
// room.State without locks.
//
// A Module is an internal, opaque descriptor: build it with the fluent
// chain — NewModule (or the partyx.Room builder, which additionally
// registers the module with the app):
//
//	room.NewModule[WordState]("wordgame").
//		State(func() *WordState { return &WordState{...} }).
//		MaxPlayers(2).
//		Singleton(protocol.SingletonReject).
//		OnJoin(func(r *room.Room[WordState], p *room.Player) { ... }).
//		Handle(op, room.Typed(guessHandler))
//
// A zero Module is valid: a plain stateless room with no hooks.
type Module[S any] struct {
	// typ is the room type matched against CreateRoomRequest.Type.
	typ string
	// config provides defaults for rooms of this type. SingletonMode always
	// comes from here for module-backed rooms; Name and MaxPlayers can be
	// overridden per room by the create request.
	config RoomConfig
	// newState builds the initial game state; nil means the zero value of S.
	newState func() *S

	onInit  func(r *Room[S])
	onJoin  func(r *Room[S], p *Player)
	onLeave func(r *Room[S], p *Player)
	onClose func(r *Room[S])

	tickRate time.Duration
	onTick   func(r *Room[S], dt time.Duration)

	handlers map[uint16]MessageHandler[S]
}

// MessageHandler processes a room-scoped message inside the room actor.
// payload is the raw arpack-encoded request body; the returned Marshaler is
// encoded once and sent as the response payload (nil = empty payload).
type MessageHandler[S any] func(r *Room[S], p *Player, payload []byte) (protocol.Marshaler, error)

// NewModule starts the definition of a room type. typ is matched against
// CreateRoomRequest.Type.
func NewModule[S any](typ string) *Module[S] {
	return &Module[S]{typ: typ}
}

// State sets the factory for the initial game state. Without it the state
// is the zero value of S.
func (m *Module[S]) State(fn func() *S) *Module[S] {
	m.newState = fn
	return m
}

// MaxPlayers sets the default player cap for rooms of this type
// (0 = unlimited). The create request may override it per room.
func (m *Module[S]) MaxPlayers(n uint16) *Module[S] {
	m.config.MaxPlayers = n
	return m
}

// Singleton sets the singleton mode for rooms of this type. It always comes
// from the module; the create request cannot override it.
func (m *Module[S]) Singleton(mode protocol.SingletonMode) *Module[S] {
	m.config.SingletonMode = mode
	return m
}

// OnInit runs once when the room is created, synchronously before the actor
// starts (deterministic, the room is not shared yet).
func (m *Module[S]) OnInit(fn func(r *Room[S])) *Module[S] {
	m.onInit = fn
	return m
}

// OnJoin runs after a player is added to the room, inside the same
// serialized step.
func (m *Module[S]) OnJoin(fn func(r *Room[S], p *Player)) *Module[S] {
	m.onJoin = fn
	return m
}

// OnLeave runs after a player is removed from the room.
func (m *Module[S]) OnLeave(fn func(r *Room[S], p *Player)) *Module[S] {
	m.onLeave = fn
	return m
}

// OnClose runs when the room is shut down (removed).
func (m *Module[S]) OnClose(fn func(r *Room[S])) *Module[S] {
	m.onClose = fn
	return m
}

// Tick enables the game loop: fn runs inside the actor every rate
// (e.g. 100ms for 10 Hz). dt is the elapsed time since the previous tick.
func (m *Module[S]) Tick(rate time.Duration, fn func(r *Room[S], dt time.Duration)) *Module[S] {
	m.tickRate = rate
	m.onTick = fn
	return m
}

// Handle registers a raw (bytes-in, Marshaler-out) handler for op and
// returns the module for chaining. It panics on a duplicate opcode. Most
// code should wrap a typed handler with Typed instead.
func (m *Module[S]) Handle(op uint16, h MessageHandler[S]) *Module[S] {
	if m.handlers == nil {
		m.handlers = make(map[uint16]MessageHandler[S])
	}
	if _, dup := m.handlers[op]; dup {
		panic(fmt.Sprintf("room: opcode %d already registered in module %q", op, m.typ))
	}
	m.handlers[op] = h
	return m
}

// Typed adapts a typed message handler to a raw MessageHandler: the payload
// is decoded into Req (an empty payload yields a zero Req), Validate() is
// called when Req implements it (failures map to a 400 error), and the
// response is encoded by the framework. A nil response means an empty
// payload.
//
// It exists because Go has no generic methods, so a typed Handle cannot be
// a method on Module:
//
//	Handle(uint16(messages.GameOpGuess), room.Typed(guessHandler))
func Typed[S any, Req any, Resp any, PReq interface {
	*Req
	protocol.Unmarshaler
}, PResp interface {
	*Resp
	protocol.Marshaler
}](fn func(r *Room[S], p *Player, req PReq) (PResp, error)) MessageHandler[S] {
	return func(r *Room[S], p *Player, payload []byte) (protocol.Marshaler, error) {
		req, err := protocol.Decode[Req, PReq](payload)
		if err != nil {
			return nil, protocol.NewError(400, "invalid payload")
		}
		if v, ok := any(PReq(req)).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return nil, protocol.NewError(400, err.Error())
			}
		}
		resp, err := fn(r, p, PReq(req))
		if err != nil {
			return nil, err
		}
		// Compare via the core type: a typed-nil pointer boxed in an
		// interface is not == nil, but the client expects an empty payload.
		if (*Resp)(resp) == nil {
			return nil, nil
		}
		return resp, nil
	}
}

// AnyRoom is the type-erased view of a room used by the Manager.
type AnyRoom interface {
	ID() string
	Config() RoomConfig
	Info() protocol.RoomInfo
	Join(clientID uint64, userID string) error
	JoinReplace(oldClientID, clientID uint64, userID string) error
	Leave(clientID uint64)
	Close()
	Open()
	IsOpen() bool
	Shutdown()
	HandlesOp(op uint16) bool
	HandleMessage(clientID uint64, op uint16, payload []byte) (protocol.Marshaler, error)
	setOnEmpty(func(string))
}

// AnyModule is the type-erased view of a Module used by the Manager.
type AnyModule interface {
	RoomType() string
	BaseConfig() RoomConfig
	Create(config RoomConfig, bus *eventbus.EventBus) AnyRoom
	Ops() []uint16
}

// RoomType implements AnyModule.
func (m *Module[S]) RoomType() string { return m.typ }

// BaseConfig implements AnyModule.
func (m *Module[S]) BaseConfig() RoomConfig { return m.config }

// Create implements AnyModule.
func (m *Module[S]) Create(config RoomConfig, bus *eventbus.EventBus) AnyRoom {
	return newRoom(config, m, bus)
}

// Ops implements AnyModule.
func (m *Module[S]) Ops() []uint16 {
	ops := make([]uint16, 0, len(m.handlers))
	for op := range m.handlers {
		ops = append(ops, op)
	}
	return ops
}

// EmptyState is the state of a plain room (no module registered for its
// type).
type EmptyState struct{}

var plainModule = &Module[EmptyState]{}
