# partyx Usage Guide

partyx is a game server framework for fast prototyping. Out of the box: a
binary WebSocket transport ([arpack](https://github.com/edmand46/arpack)),
authentication, sessions, opcode-based RPC, pub/sub events, room actors with
typed state (Colyseus-style), and a lobby. You only write: the message
schema, an authenticator, game commands, and room modules.

## A Server in 5 Lines

A complete working server (`go get github.com/damirlut/go-partyx`):

```go
app := partyx.New(partyx.Config{
    Engine:        gin.New(),
    Addr:          ":8080",
    Authenticator: partyx.DevAuth(), // dev only, see section 2
})

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
defer stop()
log.Fatal(app.Run(ctx)) // graceful shutdown on signal
```

`partyx.New` spins up the event bus, session store, room manager, lobby,
command registry and gateway, and registers the built-in methods
(`room.create`, `room.join`, `room.leave`, `lobby.list`; disable them with
`DisableDefaultHandlers: true`).

Config:

| Field | Default | Purpose |
|-------|---------|---------|
| `Engine` | required | Your gin engine — the framework mounts the WS route on it; add middleware and custom HTTP routes to it before `New` |
| `WSPath` | `"/ws"` | WebSocket endpoint path |
| `Addr` | `":8080"` | HTTP/WebSocket listen address |
| `Authenticator` | `DevAuth()` + a log warning | Token validation |
| `CheckOrigin` | `nil` (same-origin, safe) | WebSocket Origin check |
| `DisableDefaultHandlers` | `false` | Turn off built-in methods |
| `Logger` | `slog.Default()` | Where all library logging goes (hook panics, slow-consumer disconnects) |
| `MaxMessageSize` | 64 KiB | Max size of one WS frame |
| `AuthTimeout` | 10s | Deadline for the first (`auth`) message |
| `PongWait` | 60s | Read deadline refreshed by pong/data frames |
| `WriteWait` | 10s | Per-write deadline |
| `PingPeriod` | 30s | Server ping period (must stay below `PongWait`) |
| `SendBufferSize` | 256 | Per-connection outbound queue; overflow disconnects the slow client |
| `ShutdownTimeout` | 5s | Budget for the graceful teardown started by canceling `Run`'s context |

You own the gin engine, so plain gin idioms work for the HTTP side:

```go
engine := gin.Default()
engine.Use(requestLogger())
engine.Use(rateLimiter()) // wraps the /ws upgrade too

app := partyx.New(partyx.Config{
	Engine:        engine,
	WSPath:        "/api/v1/ws", // optional
	Addr:          ":8080",
	Authenticator: partyx.DevAuth(), // dev only
})

engine.GET("/healthz", func(c *gin.Context) { c.Status(200) })
log.Fatal(app.Run(ctx))
```

Subsystem accessors for advanced wiring: `app.Rooms()`, `app.Bus()`,
`app.Commands()`, `app.Sessions()`, `app.Lobby()`, `app.Engine()` (the
engine from `Config`).

## 1. Custom Messages and Code Generation

The protocol is binary: every WS frame is one arpack message. Describe your
types in a schema (a single Go file) and generate the serializers:

```go
// internal/messages/schema.go
package messages

//go:generate go run github.com/edmand46/arpack/cmd/arpack -in schema.go -out-go . -out-ts ../../web/src/messages

// Method opcodes: >= 100 (0-99 are reserved for the framework).
type GameOp uint16

const (
	GameOpGuess GameOp = 100
)

// Event opcodes (server -> client): also >= 100.
type GameEvent uint16

const (
	GameEventWordGuessed  GameEvent = 100
	GameEventRoundStarted GameEvent = 101
)

type GuessRequest struct {
	Word string
}

type GuessResponse struct {
	Correct bool
}

type WordGuessed struct {
	PlayerID uint64
	Word     string
}

type RoundStarted struct {
	N uint32
}
```

Generate with `go generate ./internal/messages` (or `make generate` in your
project). You get `schema_gen.go` with `Marshal/Unmarshal` methods and — from
the same schema — TypeScript/C#/Lua bindings for the client (`-out-ts`,
`-out-cs`, `-out-lua`). Commit the generated code; pin the tool via
`tools.go` (see `tools/tools.go` in this repository for a sample).

arpack constraints apply to **wire types only**: explicit-width integers
(`uint16`, not `int`), no pointers, maps, or nested collections. Room game
**state** (section 4) does not need to serialize — anything goes there.

Validation is a `Validate() error` method on the request type (in a separate
file of the package so generation does not overwrite it); the framework
calls it automatically and returns 400 on failure:

```go
// internal/messages/validate.go
package messages

import "errors"

func (m *GuessRequest) Validate() error {
	if m.Word == "" {
		return errors.New("word is required")
	}
	return nil
}
```

## 2. Authentication

Implement the interface:

```go
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (*session.Session, error)
}
```

`ctx` is bounded by `AuthTimeout` and canceled when the connection closes —
a token check against an external platform is cancelable like any other
outgoing call.

The client sends `auth` with a token as its first message; on failure it
gets 401 and the connection is closed — the same happens on timeout (10 s).
The reply is `AuthResult{SessionID, UserID}`. `Session.UserID` is used
everywhere downstream: room singleton modes, `ctx.Session` in handlers.
`Session.Metadata` (`Get`/`Set`) holds profile, permissions, etc.

`partyx.DevAuth()` is the dev variant: the token is the userID, and every
connection gets a fresh session. If no `Authenticator` is configured at all,
the framework substitutes DevAuth and logs a warning — always set it
explicitly in production.

## 3. Global Commands (RPC)

A global command is registered with a generic method on the app — no manual
payload parsing:

```go
app.Handle(uint16(messages.GameOpGuess), func(ctx *partyx.Context, req *messages.GuessRequest) (*messages.GuessResponse, error) {
	// req is already decoded and validated (Validate, if implemented)
	return &messages.GuessResponse{Correct: req.Word == "party"}, nil
})
```

What the framework does:

1. Decodes the payload into `Req` (an empty payload → a zero `Req`, so
   methods without a body need no separate type);
2. Calls `Validate()` if `Req` implements it (failure → 400);
3. Calls your handler and encodes the response exactly once (a `nil`
   response → an empty payload).

Errors: `return nil, partyx.NewError(400, "...")` — the code reaches the
client as is; room domain errors map automatically (404/409/410), everything
else becomes 500.

`ctx` provides:

| Field | What it is |
|-------|------------|
| `ctx.Context` (embedded) | The request `context.Context` — canceled when the connection closes or the server shuts down |
| `ctx.Session` | The client session (`UserID`, `Metadata`) |
| `ctx.ClientID` | Connection ID (the player in rooms) |
| `ctx.Bus` | EventBus — publish events |
| `ctx.Rooms` | Room manager (find/create rooms) |
| `ctx.Subscribe(topic)` / `ctx.Unsub(topic)` | Client subscriptions |

Escape hatch for raw bytes — `app.HandleRaw(op, fn)`. An opcode may
have exactly one owner: a duplicate (global or from a room module) panics at
startup.

Handlers run on a per-connection worker outside the read loop, one message
at a time in arrival order: a slow handler (DB, external RPC) delays that
client's subsequent responses but does not stall frame reads or the
connection liveness deadlines. Use the embedded `context.Context` to bound
long work.

## 4. Game Room Modules

A module is a room type. Game state lives **inside the room actor** and is
mutated directly in hooks and handlers — no mutexes, channels, or `Do`. The
type is described with a fluent builder: it reads top to bottom and the
chain ends with registration:

```go
type WordState struct {
	Round int32
	Words map[uint64]string // server-only: maps and pointers are fine, this is not a wire type
}

partyx.Room[WordState]("wordgame"). // CreateRoomRequest.Type -> this type
	State(func() *WordState {
		return &WordState{Round: 1, Words: map[uint64]string{}}
	}).
	MaxPlayers(2).
	Singleton(protocol.SingletonReject).
	OnJoin(func(ctx context.Context, r *room.Room[WordState], p *room.Player) {
		r.State.Words[p.ID] = ""
	}).
	OnLeave(func(ctx context.Context, r *room.Room[WordState], p *room.Player) {
		delete(r.State.Words, p.ID)
	}).
	// Typed room message handler — runs inside the actor.
	Handle(uint16(messages.GameOpGuess),
		func(ctx context.Context, r *room.Room[WordState], p *room.Player, req *messages.GuessRequest) (*messages.GuessResponse, error) {
			correct := req.Word == "party"
			if correct {
				r.State.Round++
				r.Broadcast(uint16(messages.GameEventWordGuessed),
					&messages.WordGuessed{PlayerID: p.ID, Word: req.Word})
			}
			return &messages.GuessResponse{Correct: correct}, nil
		}).
	Register(app)
```

Two notes on the syntax:

- the state type is named explicitly — `partyx.Room[WordState]("wordgame")`:
  Go cannot infer it from the subsequent `.State(...)` call;
- `Handle(op, fn)` is a generic method (Go 1.27): all its type
  parameters are inferred from `fn`, and the framework decodes the payload,
  calls `Validate()` when implemented, and encodes the response (a `nil`
  response → an empty payload).

Under the hood the builder assembles a `room.Module` — an internal opaque
struct; its fields are inaccessible from outside, so its internals can
change without breaking changes.

Builder methods (all optional except `Register`):

| Method | When / what |
|--------|-------------|
| `State(fn)` | Initial state factory (without it — the zero value of `S`) |
| `MaxPlayers(n)` | Default player cap (0 = unlimited); the client may override it when creating |
| `Singleton(mode)` | Singleton mode; always from the module, the client cannot override it |
| `OnInit(fn)` | Synchronously when the room is created (before publishing — deterministic) |
| `OnJoin(fn)` | After a player is added (in the same serialized step) |
| `OnLeave(fn)` | After a player is removed |
| `OnClose(fn)` | When the room shuts down — including server shutdown |
| `Tick(rate, fn)` | Game loop: `fn` every `rate` inside the actor (timers, physics, rounds) |
| `Handle(op, fn)` | Typed message handler (decoded/validated/encoded by the framework); panics on a duplicate opcode |
| `HandleRaw(op, h)` | Raw message handler (bytes in, Marshaler out); panics on a duplicate opcode |
| `Register(app)` | Registers the type; panics on a duplicate type or an opcode conflict with a global method |

All hooks and handlers receive a `context.Context` as the first argument.
Hooks get the room's lifetime context: it is canceled when the room shuts
down, so `OnClose` always observes it in the done state. Message handlers
get the request context, canceled when the connection closes or the server
shuts down.

Available inside the actor: `r.State` (your state), `r.PlayerList()`,
`r.HasPlayerID(id)`, `r.PlayerByUserID(userID)`, `r.Options()`,
`r.RoomInfo()` — direct
accessors you may call from hooks, handlers and `OnTick`; `r.Broadcast(op,
msg)` / `r.BroadcastBytes` are safe anywhere. The blocking variants
`r.Players()`, `r.HasPlayer()`, `r.Info()`, `r.IsOpen()` are for callers
**outside** the actor (they submit to the inbox and wait) — calling them
from a hook or handler deadlocks; `r.Close()`/`r.Open()` are blocking too.
A panic in a hook/handler is logged (to `Config.Logger`) and does not take
down the actor.

**Addressed sends.** Beside the room-wide `r.Broadcast`, the room can
deliver personal events — published to the target's `client:<id>` topic, so
other room members never see the payload. All of these read the live player
list, so like the direct accessors they must be called from **inside** the
actor:

| Method | Delivery |
|--------|----------|
| `r.Send(p, op, msg)` | To one player. The live connection is resolved by `p.UserID`, so a `*Player` captured before a reconnect still reaches the user's current connection. No live player — the message is dropped. |
| `r.SendTo(userIDs, op, msg)` | To a subset of members, by `userID` (an offer to two sides, a turn reminder). Unknown users are skipped. |
| `r.BroadcastExcept(except, op, msg)` | To everyone except the listed `userID`s. |
| `r.BroadcastFunc(op, fn)` | Per-player payloads: `fn(p)` returns that player's own snapshot (hidden wallets, private hands); returning `false` skips the player. |

Clients distinguish personal events from room events by the `channel` field
(`client:<id>` vs `room:<id>`).

**Room-scoped message routing.** The client simply sends a request with an
opcode — no roomID. The framework finds the room: opcode → module type →
the calling client's rooms of that type. Exactly one — the handler runs in
it; none — 400 (`not in a room...`); several — 400 (`ambiguous`). To keep
addressing unambiguous, keep your room types singleton (`reject`/`replace`)
— that is the typical case anyway.

**Creating rooms.** The client sends `room.create` with `type: "wordgame"`
— the module provides the state and the default config (`SingletonMode`
always comes from the module; `name`/`maxPlayers` may be overridden by the
client). An unregistered type creates an "empty" room without state — lobby
and singleton modes still work.

**Room options.** `room.create` also carries opaque key-value `options`
(`CreateOption[]`). partyx never interprets them — the module does. They
are handed to the room config untouched (the request's options replace the
module defaults as a whole) and are readable from inside the actor:

```go
OnJoin(func(ctx context.Context, r *room.Room[WordState], p *room.Player) {
    if r.Options()["mode"] == "blitz" { r.State.RoundSeconds = 10 }
})
```

**Lifecycle:** the last player leaving removes the room automatically
(`OnClose` → a `room.removed` event in `lobby`). A client disconnect is
handled by the framework (leave from all rooms). When the server shuts down
(`app.Run(ctx)` context canceled), the framework closes every client
connection, runs the disconnect cleanup, and shuts down all room actors —
`OnClose` fires for every room.

## 5. Events

EventBus is pub/sub over string topics; an event = opcode + an
already-encoded payload (marshaled once per publish). Topics:

| Topic | Published by |
|-------|--------------|
| `room:<id>` | The room (`player.joined`/`player.left` + your game events via `r.Broadcast`) |
| `client:<id>` | Personal messages to a client: `room.kicked` and room-initiated sends (`r.Send`/`r.SendTo`/`r.BroadcastExcept`/`r.BroadcastFunc`) |
| `lobby` | `room.created`, `room.removed` |

The client subscribes itself (a `subscribe` message with a channel) or via
`ctx.Subscribe` in a handler. The built-in handlers subscribe the client to
the room topic *before* joining, so it misses no events — do the same in
yours.

Publishing to an arbitrary topic from anywhere:

```go
ctx.Bus.Publish("lobby", eventbus.NewEvent(uint16(messages.GameEventTournamentStarted), &messages.TournamentStarted{}))
```

A custom subscriber (a bot, metrics) implements
`Subscriber{ ID() uint64; Send(topic string, event Event) }`. Empty topics
are removed automatically; a panicking subscriber is logged and does not
break the publish.

## 6. Sessions

`app.Sessions()` — a thread-safe in-memory `id -> Session` store. The
gateway puts a session on auth and removes it on disconnect.
`Session.Metadata` is a thread-safe map for arbitrary data
(`sess.Set("rating", 1450)`).

## 7. Wire Protocol (Brief)

The full picture — `protocol/schema.go` and `docs/architecture.md`. One
endpoint (`/ws` by default — `WSPath`), binary `ClientMessage`/`ServerMessage`
frames. Types:
`auth` / `subscribe` / `unsubscribe` / `request` from the client;
`response` / `error` / `event` from the server. Error codes:
400/401/404/409/410/500. Connection limits (frame size, auth/pong timeouts,
send buffer) are the `Config` fields listed in the beginning — the numbers
there are the defaults.

Client side: generate bindings from `protocol/schema.go` + your schema
(`arpack -out-ts ...`) and send binary frames with
`WebSocket.binaryType = "arraybuffer"`.

## 8. Production Checklist

- [ ] Your own `Authenticator` (JWT/OAuth/signature), not `DevAuth`;
- [ ] `CheckOrigin: nil` (same-origin) or a domain allowlist;
- [ ] `GIN_MODE=release` in the environment;
- [ ] Gateway limits revisited for your traffic;
- [ ] In-memory stores (`session.Store`, `room.Manager`) replaced/wrapped if
      you need persistence or multiple instances — they do not leave a
      single process;
- [ ] Game opcodes documented in the schema (the client is generated from
      it too — no drift possible);
- [ ] Graceful shutdown: `app.Run(ctx)` + `signal.NotifyContext` (as in the
      example above).

## 9. Testing Your Project

Samples in this repository:

- unit: `room/module_test.go` (hooks, tick, typed handlers — the actor is
  tested synchronously: `Join`/`Leave`/`HandleMessage` are deterministic),
  `room/manager_test.go` (singleton modes, routing),
  `eventbus/eventbus_test.go`;
- integration: `gateway/gateway_test.go` — builds a gin engine, hands it to
  `gateway.Config` and starts it via `httptest.NewServer(engine)`, driving a
  real WebSocket client with
  binary frames through the whole protocol, including a room-scoped module
  (`TestRoomModuleFlow`). Copy this pattern for your own commands.