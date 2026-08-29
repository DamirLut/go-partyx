package room

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

// Module describes a room type: how to build its game state, lifecycle
// hooks, a game loop, and the opcodes of its messages. All hooks and
// handlers run inside the room actor, so they may freely read and mutate
// room.State without locks. Build it with the fluent partyx.Room builder or
// NewModule; a zero Module is a plain stateless room with no hooks.
type Module[S any] struct {
	// typ is the room type matched against CreateRoomRequest.Type.
	typ string
	// config provides defaults; SingletonMode always comes from here, Name
	// and MaxPlayers may be overridden per room by the create request.
	config RoomConfig
	// newState builds the initial game state; nil means the zero value of S.
	newState func() *S

	// Hooks receive the room's lifetime context: it is canceled when the
	// room shuts down, so OnClose always observes it in the done state.
	onInit  func(ctx context.Context, r *Room[S])
	onJoin  func(ctx context.Context, r *Room[S], p *Player)
	onLeave func(ctx context.Context, r *Room[S], p *Player)
	onClose func(ctx context.Context, r *Room[S])

	tickRate time.Duration
	onTick   func(ctx context.Context, r *Room[S], dt time.Duration)

	handlers map[uint16]MessageHandler[S]
}

// MessageHandler processes a room-scoped message inside the room actor.
// payload is the raw arpack-encoded request body; the returned Marshaler is
// encoded once and sent as the response payload (nil = empty payload). ctx
// is the request context: canceled when the connection closes or the server
// shuts down.
type MessageHandler[S any] func(ctx context.Context, r *Room[S], p *Player, payload []byte) (protocol.Marshaler, error)

// NewModule starts the definition of a room type. typ is matched against
// CreateRoomRequest.Type.
func NewModule[S any](typ string) *Module[S] {
	return &Module[S]{typ: typ}
}

// State sets the state factory; without it the state is the zero value of S.
func (m *Module[S]) State(fn func() *S) *Module[S] {
	m.newState = fn
	return m
}

// MaxPlayers sets the default player cap (0 = unlimited); the create request
// may override it per room.
func (m *Module[S]) MaxPlayers(n uint16) *Module[S] {
	m.config.MaxPlayers = n
	return m
}

// Singleton sets the mode for rooms of this type; the create request cannot
// override it.
func (m *Module[S]) Singleton(mode protocol.SingletonMode) *Module[S] {
	m.config.SingletonMode = mode
	return m
}

// OnInit runs synchronously at construction, before the room is shared. ctx
// is the room's lifetime context.
func (m *Module[S]) OnInit(fn func(ctx context.Context, r *Room[S])) *Module[S] {
	m.onInit = fn
	return m
}

// OnJoin runs in the same serialized step as the player add. ctx is the
// room's lifetime context.
func (m *Module[S]) OnJoin(fn func(ctx context.Context, r *Room[S], p *Player)) *Module[S] {
	m.onJoin = fn
	return m
}

func (m *Module[S]) OnLeave(fn func(ctx context.Context, r *Room[S], p *Player)) *Module[S] {
	m.onLeave = fn
	return m
}

// OnClose runs when the room shuts down; ctx is already canceled, which is
// the documented way to observe the shutdown inside the hook.
func (m *Module[S]) OnClose(fn func(ctx context.Context, r *Room[S])) *Module[S] {
	m.onClose = fn
	return m
}

// Tick enables the game loop: fn runs inside the actor every rate; dt is the
// time since the previous tick. ctx is the room's lifetime context.
func (m *Module[S]) Tick(rate time.Duration, fn func(ctx context.Context, r *Room[S], dt time.Duration)) *Module[S] {
	m.tickRate = rate
	m.onTick = fn
	return m
}

// HandleRaw registers a raw (bytes-in, Marshaler-out) handler; most code
// should use Handle. Panics on a duplicate opcode.
func (m *Module[S]) HandleRaw(op uint16, h MessageHandler[S]) *Module[S] {
	if m.handlers == nil {
		m.handlers = make(map[uint16]MessageHandler[S])
	}
	if _, dup := m.handlers[op]; dup {
		panic(fmt.Sprintf("room: opcode %d already registered in module %q", op, m.typ))
	}
	m.handlers[op] = h
	return m
}

// Handle registers a typed message handler: the payload is decoded
// into Req (an empty payload yields a zero Req), Validate() is called when
// Req implements it (failures map to a 400 error), and the response is
// encoded by the framework (a nil response means an empty payload). Panics
// on a duplicate opcode. All type parameters are inferred from fn.
func (m *Module[S]) Handle[Req, Resp any, PReq interface {
	*Req
	protocol.Unmarshaler
}, PResp interface {
	*Resp
	protocol.Marshaler
}](op uint16, fn func(ctx context.Context, r *Room[S], p *Player, req PReq) (PResp, error)) *Module[S] {
	return m.HandleRaw(op, func(ctx context.Context, r *Room[S], p *Player, payload []byte) (protocol.Marshaler, error) {
		return protocol.Call(payload, func(req PReq) (PResp, error) {
			return fn(ctx, r, p, req)
		})
	})
}

// AnyRoom is the type-erased view of a room used by the Manager.
type AnyRoom interface {
	ID() string
	Config() RoomConfig
	// Info returns a room snapshot, or ErrRoomClosed if the room is already
	// shut down. It runs on the room actor.
	Info() (protocol.RoomInfo, error)
	Join(clientID uint64, userID string) error
	JoinReplace(oldClientID, clientID uint64, userID string) error
	Leave(clientID uint64)
	Close()
	Open()
	IsOpen() bool
	Shutdown()
	HandlesOp(op uint16) bool
	// HandleMessage runs the matching module handler inside the room actor;
	// ctx is passed through to the handler.
	HandleMessage(ctx context.Context, clientID uint64, op uint16, payload []byte) (protocol.Marshaler, error)
	setOnEmpty(func(string))
}

// AnyModule is the type-erased view of a Module used by the Manager.
type AnyModule interface {
	RoomType() string
	BaseConfig() RoomConfig
	Create(config RoomConfig, bus *eventbus.EventBus, logger *slog.Logger) AnyRoom
	Ops() []uint16
}

func (m *Module[S]) RoomType() string { return m.typ }

func (m *Module[S]) BaseConfig() RoomConfig { return m.config }

func (m *Module[S]) Create(config RoomConfig, bus *eventbus.EventBus, logger *slog.Logger) AnyRoom {
	return newRoom(config, m, bus, logger)
}

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
