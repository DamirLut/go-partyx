# partyx

A Go game server framework for fast prototyping. Binary WebSocket protocol on
[arpack](https://github.com/edmand46/arpack) (code generation, cross-language
clients: Go/TypeScript/C#/Lua), opcode-based RPC, pub/sub events, room actors
with typed state and a game loop (Colyseus-style), and a lobby.

Documentation:
- [docs/usage.md](docs/usage.md) — how to build your game: the `partyx.App`
  facade, authentication, typed commands, game room modules, code generation;
- [docs/architecture.md](docs/architecture.md) — internals: layers, protocol,
  concurrency.

## Quick Start

```sh
go get github.com/damirlut/go-partyx
```

A minimal server:

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/gin-gonic/gin"

	partyx "github.com/damirlut/go-partyx"
)

func main() {
	engine := gin.Default() // your engine: middleware and HTTP routes
	app := partyx.New(partyx.Config{
		Engine:        engine,
		Addr:          ":8080",
		Authenticator: partyx.DevAuth(), // dev only
		CheckOrigin:   func(r *http.Request) bool { return true }, // dev only
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	log.Fatal(app.Run(ctx))
}
```

Developing the library: `make build` / `make test` / `make vet` / `make lint`;
`make generate` regenerates `protocol` after edits to `schema.go`.

Layout: the root `partyx` package is the facade (App, Handle, the Room
builder, DevAuth); subpackages are subsystems (`protocol`, `gateway`,
`command`, `eventbus`, `room`, `lobby`, `session`); built-in methods live in
`internal/handlers`; `tools` pins the arpack code generator.

## Protocol

One endpoint — `ws://localhost:8080/ws` (path: `Config.WSPath`). Every WS
binary frame is one
arpack message: `ClientMessage` from the client, `ServerMessage` from the
server. The schema lives in `protocol/schema.go`; client bindings
(TS/C#/Lua) are generated from it too.

```
ClientMessage { type, id, op, token, channel, payload }
ServerMessage { type, id, code, op, channel, error, payload }
```

- `type`: auth / subscribe / unsubscribe / request from the client;
  response / error / event from the server;
- `op`: numeric opcode of a method (on request) or an event (on event);
  the 0–99 range is reserved for the framework, game opcodes start at 100;
- `payload`: arpack bytes of the concrete message type.

The first message must be authentication (`auth` with a token), otherwise
the connection is closed by timeout (10 s).

## Built-in Methods

| Opcode | Method       | Request payload    | Response payload |
|--------|--------------|--------------------|------------------|
| 1      | `room.create` | `CreateRoomRequest` | `RoomInfo`     |
| 2      | `room.join`   | `JoinRoomRequest`   | `RoomInfo`     |
| 3      | `room.leave`  | `LeaveRoomRequest`  | —              |
| 4      | `lobby.list`  | —                   | `RoomList`     |

Built-in events: `player.joined` (1), `player.left` (2), `room.kicked` (3),
`room.created` (4), `room.removed` (5) — see `EventOp` in the schema.

`singletonMode`: `allow` (0) — no restrictions; `reject` (1) — error 409 if
already in a room of this type; `replace` (2) — kick the old connection.

## Error Codes

| Code | Meaning                                                                    |
|------|----------------------------------------------------------------------------|
| 400  | Invalid payload / message format / not in a room for a room-scoped opcode  |
| 401  | Authentication required                                                    |
| 404  | Method or room not found                                                   |
| 409  | Room is full / already in a room of this type                              |
| 410  | Room is closed                                                             |
| 500  | Internal error                                                             |
