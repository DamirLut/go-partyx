// Package partyx is a game server framework for fast prototyping: a binary
// WebSocket protocol (arpack), opcode-based RPC, pub/sub events, and
// Colyseus-like room modules with typed state, lifecycle hooks and a game
// loop. Build an App with New, register your room types and commands, and
// call Run.
package partyx

import (
	"context"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/gateway"
	"github.com/damirlut/go-partyx/internal/handlers"
	"github.com/damirlut/go-partyx/lobby"
	"github.com/damirlut/go-partyx/room"
	"github.com/damirlut/go-partyx/session"
)

// Config configures an App.
type Config struct {
	// Addr is the HTTP listen address. Default ":8080".
	Addr string
	// Authenticator validates the token sent as the first client message.
	// nil falls back to DevAuth() with a warning — never ship that.
	Authenticator Authenticator
	// CheckOrigin controls the WebSocket origin check. nil means the safe
	// same-origin default; allow all origins only in dev.
	CheckOrigin func(r *http.Request) bool
	// DisableDefaultHandlers turns off the built-in room.*/lobby.* methods
	// (room.create, room.join, room.leave, lobby.list).
	DisableDefaultHandlers bool
}

// App is the framework facade: it owns the gateway and all subsystems.
// Subsystems are exposed via accessors for advanced wiring.
type App struct {
	bus      *eventbus.EventBus
	sessions *session.Store
	rooms    *room.Manager
	lobby    *lobby.Lobby
	commands *command.Registry
	gateway  *gateway.Gateway
}

func New(cfg Config) *App {
	bus := eventbus.New()
	sessions := session.NewStore()
	rooms := room.NewManager(bus)
	l := lobby.New(rooms)
	commands := command.NewRegistry()

	if !cfg.DisableDefaultHandlers {
		handlers.RegisterRoomHandlers(commands, rooms)
		handlers.RegisterLobbyHandlers(commands, l)
	}

	addr := cfg.Addr
	if addr == "" {
		addr = ":8080"
	}

	auth := cfg.Authenticator
	if auth == nil {
		log.Println("partyx: no Authenticator configured, falling back to DevAuth — do not use in production")
		auth = DevAuth()
	}

	gw := gateway.New(gateway.Config{
		Addr:          addr,
		Bus:           bus,
		Commands:      commands,
		Sessions:      sessions,
		Authenticator: auth,
		Rooms:         rooms,
		CheckOrigin:   cfg.CheckOrigin,
	})

	return &App{
		bus:      bus,
		sessions: sessions,
		rooms:    rooms,
		lobby:    l,
		commands: commands,
		gateway:  gw,
	}
}

// Bus is the shared event bus (publish/subscribe to arbitrary topics).
func (a *App) Bus() *eventbus.EventBus {
	return a.bus
}

// Sessions is the session store of authenticated clients.
func (a *App) Sessions() *session.Store {
	return a.sessions
}

// Rooms is the room manager (create/find/list rooms, singleton enforcement).
func (a *App) Rooms() *room.Manager {
	return a.rooms
}

// Lobby lists live rooms.
func (a *App) Lobby() *lobby.Lobby {
	return a.lobby
}

// Commands is the global RPC registry (opcode -> handler).
func (a *App) Commands() *command.Registry {
	return a.commands
}

// Engine exposes the underlying gin engine for custom HTTP routes and
// middleware.
func (a *App) Engine() *gin.Engine {
	return a.gateway.Engine()
}

// Run serves until ctx is canceled, then shuts down gracefully:
//
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
//	defer stop()
//	log.Fatal(app.Run(ctx))
func (a *App) Run(ctx context.Context) error {
	return a.gateway.Run(ctx)
}

// RegisterRoomType registers a room module: rooms created with
// CreateRoomRequest.Type == module.Type get the module's game state, hooks,
// game loop and message handlers. It panics on a duplicate type, or when one
// of the module's opcodes is already registered globally.
//
// This is the low-level path; most code should define room types with the
// fluent Room[S] builder instead (its Register calls this function).
func RegisterRoomType[S any](app *App, m *room.Module[S]) {
	for _, op := range m.Ops() {
		if app.commands.Has(op) {
			panicf("partyx: opcode %d is already registered as a global method", op)
		}
	}
	app.rooms.RegisterModule(m)
}
