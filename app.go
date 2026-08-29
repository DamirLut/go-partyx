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

type Config struct {
	// The gin engine the framework mounts its WebSocket route on. The
	// consumer owns routing: middleware and custom HTTP routes are attached
	// to the engine before New — middleware registered there also wraps the
	// WS route. Required.
	Engine *gin.Engine
	// Default ":8080".
	Addr string
	// nil falls back to DevAuth() with a warning — never ship that.
	Authenticator Authenticator
	// nil means the safe same-origin default; allow all origins only in dev.
	CheckOrigin func(r *http.Request) bool
	// WebSocket endpoint path. Empty means "/ws".
	WSPath string
	// Turns off the built-in room.*/lobby.* methods.
	DisableDefaultHandlers bool
}

// App is the framework facade owning the gateway and all subsystems.
type App struct {
	bus      *eventbus.EventBus
	sessions *session.Store
	rooms    *room.Manager
	lobby    *lobby.Lobby
	commands *command.Registry
	gateway  *gateway.Gateway
	engine   *gin.Engine
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
		Engine:        cfg.Engine,
		WSPath:        cfg.WSPath,
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
		engine:   cfg.Engine,
	}
}

func (a *App) Bus() *eventbus.EventBus {
	return a.bus
}

func (a *App) Sessions() *session.Store {
	return a.sessions
}

func (a *App) Rooms() *room.Manager {
	return a.rooms
}

func (a *App) Lobby() *lobby.Lobby {
	return a.lobby
}

func (a *App) Commands() *command.Registry {
	return a.commands
}

// Engine returns the gin engine passed to New — custom HTTP routes and
// middleware live there.
func (a *App) Engine() *gin.Engine {
	return a.engine
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
// CreateRoomRequest.Type == module.Type get the module's state, hooks, game
// loop and handlers. Panics on a duplicate type or a global opcode conflict.
// Most code should define room types with the fluent Room[S] builder instead.
func RegisterRoomType[S any](app *App, m *room.Module[S]) {
	for _, op := range m.Ops() {
		if app.commands.Has(op) {
			panicf("partyx: opcode %d is already registered as a global method", op)
		}
	}
	app.rooms.RegisterModule(m)
}
