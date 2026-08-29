package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
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

	// baseCtx is the context passed to Run; every connection derives its
	// context from it. Defaults to context.Background so the gateway also
	// works when the engine is driven directly (tests) without Run.
	baseCtx context.Context

	mu      sync.Mutex
	clients map[*Client]struct{}
	srv     *http.Server
	wg      sync.WaitGroup // active client read loops
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
		baseCtx: context.Background(),
		clients: make(map[*Client]struct{}),
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

	client := NewClient(g.baseCtx, conn, g.config.Bus, g.tuning)
	g.register(client)
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		client.ReadLoop(g.dispatcher)
		g.unregister(client)
	}()
}

func (g *Gateway) register(c *Client) {
	g.mu.Lock()
	g.clients[c] = struct{}{}
	g.mu.Unlock()
}

func (g *Gateway) unregister(c *Client) {
	g.mu.Lock()
	delete(g.clients, c)
	g.mu.Unlock()
}

// Run serves until ctx is canceled, then tears everything down gracefully:
// the HTTP listener stops, every client connection is closed (read loops
// unwind and OnDisconnect moves players out of rooms), and finally all room
// actors are shut down so their OnClose hooks run. Returns nil after a
// graceful shutdown.
func (g *Gateway) Run(ctx context.Context) error {
	g.baseCtx = ctx
	srv := &http.Server{
		Addr:    g.config.Addr,
		Handler: g.config.Engine,
	}
	g.mu.Lock()
	g.srv = srv
	g.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		g.shutdown()
		return nil
	case err := <-errCh:
		// The server died (port busy, listener closed): tear down whatever
		// came up and report the failure.
		g.shutdown()
		return err
	}
}

// shutdown unwinds the gateway in dependency order: HTTP listener, then
// client connections, then room actors. It is safe to call once, from Run.
func (g *Gateway) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), g.tuning.shutdownTimeout)
	defer cancel()

	if g.srv != nil {
		// Stops accepting and waits for plain HTTP handlers. WebSocket
		// connections are hijacked — they are closed explicitly below.
		g.srv.Shutdown(ctx)
	}

	g.mu.Lock()
	clients := make([]*Client, 0, len(g.clients))
	for c := range g.clients {
		clients = append(clients, c)
	}
	g.clients = make(map[*Client]struct{})
	g.mu.Unlock()

	// Close is graceful: each WriteLoop flushes queued messages, then the
	// connection closes and the read loop unwinds into OnDisconnect.
	for _, c := range clients {
		c.Close()
	}
	g.wg.Wait()

	// Rooms last, so in-flight handlers still find their room actors alive.
	g.config.Rooms.ShutdownAll()
}
