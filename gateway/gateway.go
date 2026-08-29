package gateway

import (
	"context"
	"log/slog"
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

	// nil falls back to slog.Default().
	Logger *slog.Logger

	// Connection tuning; zero values apply the defaults in newTuning.
	MaxMessageSize  int
	AuthTimeout     time.Duration
	PongWait        time.Duration
	WriteWait       time.Duration
	PingPeriod      time.Duration
	SendBufferSize  int
	ShutdownTimeout time.Duration
}

// tuning is the connection configuration with defaults applied.
type tuning struct {
	writeWait       time.Duration
	pongWait        time.Duration
	pingPeriod      time.Duration
	authTimeout     time.Duration
	maxMessageSize  int64
	sendBufferSize  int
	shutdownTimeout time.Duration
}

func newTuning(c Config) tuning {
	return tuning{
		writeWait:       c.WriteWait,
		pongWait:        c.PongWait,
		pingPeriod:      c.PingPeriod,
		authTimeout:     c.AuthTimeout,
		maxMessageSize:  int64(c.MaxMessageSize),
		sendBufferSize:  c.SendBufferSize,
		shutdownTimeout: c.ShutdownTimeout,
	}.withDefaults()
}

func (t tuning) withDefaults() tuning {
	if t.writeWait <= 0 {
		t.writeWait = defaultWriteWait
	}
	if t.pongWait <= 0 {
		t.pongWait = defaultPongWait
	}
	// The ping period only works when it is shorter than the pong wait.
	if t.pingPeriod <= 0 || t.pingPeriod >= t.pongWait {
		t.pingPeriod = defaultPingPeriod
	}
	if t.authTimeout <= 0 {
		t.authTimeout = defaultAuthTimeout
	}
	if t.maxMessageSize <= 0 {
		t.maxMessageSize = defaultMaxMessageSize
	}
	if t.sendBufferSize <= 0 {
		t.sendBufferSize = defaultSendBufferSize
	}
	if t.shutdownTimeout <= 0 {
		t.shutdownTimeout = defaultShutdownTimeout
	}
	return t
}

// Gateway is the transport layer: HTTP + WebSocket, auth, dispatching and
// the connection lifecycle.
type Gateway struct {
	config     Config
	tuning     tuning
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
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	t := newTuning(config)
	g := &Gateway{
		config: config,
		tuning: t,
		dispatcher: NewDispatcher(
			config.Commands,
			config.Bus,
			config.Sessions,
			config.Authenticator,
			config.Rooms,
			t,
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

	client := NewClient(conn, g.config.Bus, g.tuning)
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
