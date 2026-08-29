# Partyx Architecture

Reusable game server framework. WebSocket is just transport — the server is
built around opcode RPC and events. The wire format is binary, defined as an
[arpack](https://github.com/edmand46/arpack) schema with code generation for
Go (server) and TypeScript/C#/Lua (clients).

## Layers

```
                 Client
                   |
        WebSocket (binary frames)
                   |
           +----------------+
           | Gateway Server |
           +----------------+
                   |
         +---------+----------+
         |                    |
    Event Bus             Command RPC
         |                    |
  +------+------+        +----+-----+
  |             |        |          |
Lobby       Room[S]   Global    Room-scoped
Module      actors    handlers  module handlers
```

The root package `partyx` is the facade: `partyx.New(Config)` builds and
wires all subsystems, accessors expose them (`App.Rooms()`, `App.Bus()`,
`App.Commands()`, `App.Sessions()`, `App.Engine()`), and helpers
(`App.Handle`, the `partyx.Room[S]` builder) register game logic.

## Wire Protocol

One WebSocket endpoint (`/ws`). Every binary frame is one arpack-encoded
message — the envelope is itself in the schema (`protocol/schema.go`):

```go
type ClientMessage struct {
    Type    MessageType // auth | subscribe | unsubscribe | request
    ID      uint32      // request: echoed in response/error
    Op      uint16      // request: method opcode
    Token   string      // auth
    Channel string      // subscribe/unsubscribe
    Payload []uint8     // request: arpack-encoded method payload
}

type ServerMessage struct {
    Type    MessageType // response | error | event
    ID      uint32      // response/error: the request id
    Code    uint16      // error: 400/401/404/409/410/500
    Op      uint16      // event: event opcode
    Channel string      // event: topic, e.g. "room:<id>"
    Error   string      // error message
    Payload []uint8     // response/event: arpack-encoded payload
}
```

Opcodes are explicit constants declared in the schema (enums `MethodOp`,
`EventOp`), so clients generated from the same schema share them. Ranges:

| Range | Owner |
|-------|-------|
| 0–99 | Framework (`MethodRoomCreate` …, `EventPlayerJoined` …) |
| 100+ | Games (their own schema enums, see `UserMethodOpBase`/`UserEventOpBase`) |

Payloads are decoded per-opcode: the global registry (`command.Registry`)
maps method opcodes to handlers; unclaimed opcodes fall through to
room-scoped routing (see below).

## Code Generation

- `protocol/schema.go` is the single source of truth for the wire format.
- `make generate` runs arpack (pinned via `tools/tools.go`) and rewrites
  `protocol/schema_gen.go`, which is committed — the tool is a dev-time
  dependency only, the generated code has zero runtime deps.
- Games define their own messages in their own schema file and run arpack the
  same way (optionally with `-out-ts`/`-out-cs`/`-out-lua` for clients).
- arpack constraints apply to wire types only: explicit-width integers (no
  `int`), no pointers, no maps, no nested collections. Server-side state
  (e.g. `Room.State`) is unconstrained.

Generated Go API per struct: `func (m *T) Marshal(buf []byte) []byte` and
`func (m *T) Unmarshal(data []byte) (int, error)`. The framework abstracts
them as `protocol.Marshaler` / `protocol.Unmarshaler`.

## Package Map

```
partyx (root)        Facade: App, Config, Room[S] builder (fluent room-type
                     definition + Register), App.Handle[Req,Resp]/HandleRaw,
                     RegisterRoomType[S] (low-level), DevAuth, aliases (Context, Error...)
├── protocol/        Wire schema + generated code, Marshaler/Unmarshaler,
│                    Encode/Decode helpers, Error, reserved opcodes
├── eventbus/        Topic pub/sub; Event = {Op uint16, Payload []byte},
│                    payload encoded once per publish
├── session/         User session storage (in-memory)
├── command/         Global RPC registry (uint16 opcodes), Context
├── gateway/         Gin HTTP + Gorilla WebSocket, auth, dispatcher, client
├── room/            Room[S] actor, Module[S] (hooks/tick/handlers), Manager
├── lobby/           List rooms
├── internal/
│   └── handlers/    Built-in methods (room.create/join/leave, lobby.list)
tools/               tools.go pinning the arpack code generator
```

## Concurrency Model

| Entity | Model | Notes |
|--------|-------|-------|
| Room[S] | Actor (goroutine + inbox channel) | State, players, hooks, tick — all serialized |
| room.Manager | sync.RWMutex | Short CRUD ops, opcode routing table |
| EventBus.Topic | sync.RWMutex | Publish reads (RLock), sub/unsub writes |
| SessionStore | sync.RWMutex | get/set/delete |
| Client | goroutines (readLoop + writeLoop) | Actor-like via channels |

## Room Actor and Game Modules

A room type is defined fluently; the builder assembles an opaque
`room.Module[S]` (its fields are internal) and registers it:

```go
partyx.Room[WordState]("wordgame").
	State(func() *WordState { return &WordState{Words: map[uint64]string{}} }).
	MaxPlayers(2).
	Singleton(protocol.SingletonReject).
	OnInit(func(r *room.Room[WordState]) { ... }).
	OnJoin(func(r *room.Room[WordState], p *room.Player) { ... }).
	OnLeave(func(r *room.Room[WordState], p *room.Player) { ... }).
	OnClose(func(r *room.Room[WordState]) { ... }).
	Tick(100*time.Millisecond, func(r *room.Room[WordState], dt time.Duration) { ... }).
	HandleTyped(op, guessHandler).
	Register(app)
```

Everything game-facing runs **inside the room actor**, so `r.State` is
mutated directly with no locks and no user-visible `Do`:

- `OnInit` runs synchronously at construction (deterministic, room not yet
  shared);
- `OnJoin`/`OnLeave` run inside the same serialized step as the player
  add/remove;
- `OnTick` runs in the actor loop every `Tick(rate, fn)` interval;
- message handlers (`HandleTyped(op, fn)`) are invoked through the actor
  inbox, and the caller (gateway) blocks until completion.

Typed handlers decode the payload, auto-call `Validate() error` when
implemented (failure → 400), and encode the response — a typed-nil response
means an empty payload. Panics in hooks/handlers are recovered and logged
(`safe`), so one bug cannot kill the actor or the process.

### Room-scoped message routing

Request dispatch order:

1. Global registry (`App.Handle`) — wins on opcode conflicts (registration
   panics instead: an opcode may have exactly one owner);
2. Room modules: opcode → room type (`Manager.opTypes`) → the calling
   client's joined rooms of that type:
   - exactly one → handler runs in that room's actor;
   - none → 400 (`ErrNotInRoom`);
   - several → 400 (`ErrAmbiguousRoom`) — use singleton modes or distinct
     types to stay unambiguous;
   - opcode unknown to any module → 404 "method not found".

### Room lifecycle

`OnEmpty` → `Manager.Remove` → `Shutdown` → `OnClose`. `room.create` with an
unknown type yields a plain stateless room (`EmptyState`), keeping lobby and
singleton enforcement usable without a module. For module-backed rooms the
module config wins for `SingletonMode`; `Name`/`MaxPlayers` may be
overridden by the create request.

## Room Singleton Enforcement

Per room type, per user (across connections):

| Mode | Behavior |
|------|----------|
| `allow` (0) | No restrictions (default) |
| `reject` (1) | 409 if already in a room of this type |
| `replace` (2) | Kick old session (`EventRoomKicked` on its personal channel), allow new join |

Rejoining the same room with the same client is idempotent in every mode and
publishes no duplicate `player.joined`.

## Events

`eventbus.Event{Op uint16, Payload []byte}` — the payload is encoded once at
publish time and shared by all subscribers. Topics: `room:<id>` (room events
+ game events via `r.Broadcast(op, msg)`), `client:<id>` (personal, e.g.
kicks), `lobby` (room.created/removed). Delivery iterates a subscriber
snapshot; a panicking subscriber is recovered and logged.

## Auth

```go
type Authenticator interface {
    Authenticate(token string) (*session.Session, error)
}
```

The client sends `auth` as its first message; the gateway validates via the
Authenticator and replies with `AuthResult{SessionID, UserID}`. `nil` in
`partyx.Config` falls back to `DevAuth()` with a warning log.

## Connection Limits (gateway)

| Setting | Value | Notes |
|---------|-------|-------|
| Read limit | 64 KB | Per frame (matches arpack uint16 length prefixes) |
| Binary only | — | Text frames rejected with 400 |
| Auth timeout | 10 s | Connection closed if not authenticated in time |
| Ping / pong | 30 s / 60 s | Dead connections dropped after missed pongs |
| Send buffer | 256 msgs | Slow consumers are disconnected, never silently dropped |
| Close | graceful | Pending messages flushed (2 s budget) before close |
