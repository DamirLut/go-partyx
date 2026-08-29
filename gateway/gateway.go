package gateway

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/room"
	"github.com/damirlut/go-partyx/session"
)

type Config struct {
	// The consumer-owned gin engine. The gateway mounts its WebSocket route
	// on it; middleware and custom HTTP routes are attached to the engine
	// before New. Required.
	Engine *gin.Engine
	// WebSocket endpoint path. Empty means "/ws".
	WSPath string

	Addr          string
	Bus           *eventbus.EventBus
	Commands      *command.Registry
	Sessions      *session.Store
	Authenticator Authenticator
	Rooms         *room.Manager
	// nil means the gorilla/websocket same-origin default. Allow all origins
	// only in dev.
	CheckOrigin func(r *http.Request) bool
}

// Gateway is the transport layer: HTTP + WebSocket, auth, dispatching and
// the connection lifecycle.
type Gateway struct {
	config     Config
	dispatcher *Dispatcher
	upgrader   websocket.Upgrader
}

func New(config Config) *Gateway {
	if config.Engine == nil {
		panic("partyx/gateway: Config.Engine is required — the consumer owns the gin engine")
	}
	if config.WSPath == "" {
		config.WSPath = "/ws"
	}
	g := &Gateway{
		config: config,
		dispatcher: NewDispatcher(
			config.Commands,
			config.Bus,
			config.Sessions,
			config.Authenticator,
			config.Rooms,
		),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     config.CheckOrigin,
		},
	}
	g.setupRoutes()
	return g
}

func (g *Gateway) setupRoutes() {
	g.config.Engine.GET(g.config.WSPath, g.handleWebSocket)
}

func (g *Gateway) handleWebSocket(c *gin.Context) {
	conn, err := g.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	client := NewClient(conn, g.config.Bus)
	go client.ReadLoop(g.dispatcher)
}

// Run serves until ctx is canceled, then shuts down gracefully.
func (g *Gateway) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:    g.config.Addr,
		Handler: g.config.Engine,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
