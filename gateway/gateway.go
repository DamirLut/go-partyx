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

// Config wires a Gateway. Built once by partyx.New in typical apps.
type Config struct {
	Addr          string
	Bus           *eventbus.EventBus
	Commands      *command.Registry
	Sessions      *session.Store
	Authenticator Authenticator
	Rooms         *room.Manager
	// CheckOrigin controls the WebSocket origin check. nil means the safe
	// gorilla/websocket default (same-origin). Only allow all origins in dev.
	CheckOrigin func(r *http.Request) bool
}

// Gateway is the transport layer: HTTP + WebSocket, auth, dispatching, and
// the connection lifecycle.
type Gateway struct {
	config     Config
	engine     *gin.Engine
	dispatcher *Dispatcher
	upgrader   websocket.Upgrader
}

func New(config Config) *Gateway {
	g := &Gateway{
		config: config,
		engine: gin.Default(),
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
	g.engine.GET("/ws", g.handleWebSocket)
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
		Handler: g.engine,
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

// Engine exposes the gin engine for custom HTTP routes and middleware.
func (g *Gateway) Engine() *gin.Engine {
	return g.engine
}
