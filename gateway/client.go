package gateway

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
	"github.com/damirlut/go-partyx/session"
)

const (
	defaultWriteWait       = 10 * time.Second
	defaultPongWait        = 60 * time.Second
	defaultPingPeriod      = 30 * time.Second // must stay below pongWait
	defaultAuthTimeout     = 10 * time.Second
	defaultMaxMessageSize  = 64 * 1024
	defaultSendBufferSize  = 256
	defaultShutdownTimeout = 5 * time.Second
	flushWait              = 2 * time.Second // budget for draining pending messages on close
	// Backpressure: when this many messages await the handler goroutine, the
	// read loop stalls — the connection then dies on its read deadline
	// instead of growing the queue without bound.
	handleBufferSize = 64
)

var nextClientID atomic.Uint64

// Client is a WebSocket connection: an actor-like pair of read/write loops
// plus subscription state. All wire messages are binary arpack-encoded
// protocol.ClientMessage / protocol.ServerMessage frames.
type Client struct {
	id      uint64
	conn    *websocket.Conn
	send    chan []byte
	handles chan *protocol.ClientMessage
	topics  map[string]struct{}
	roomIDs map[string]struct{}
	bus     *eventbus.EventBus
	tuning  tuning
	logger  *slog.Logger

	// ctx lives for the connection; canceled by Close. Request handlers get
	// it (directly or derived).
	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.RWMutex
	session       *session.Session
	authenticated bool

	done      chan struct{}
	closeOnce sync.Once
}

// NewClient starts a connection scoped to parent: when the gateway shuts
// down (parent canceled) or the client closes, ctx is canceled. Missing
// tuning fields fall back to the package defaults.
func NewClient(parent context.Context, conn *websocket.Conn, bus *eventbus.EventBus, t tuning) *Client {
	t = t.withDefaults()
	id := nextClientID.Add(1)
	ctx, cancel := context.WithCancel(parent)
	c := &Client{
		id:      id,
		conn:    conn,
		send:    make(chan []byte, t.sendBufferSize),
		handles: make(chan *protocol.ClientMessage, handleBufferSize),
		topics:  make(map[string]struct{}),
		roomIDs: make(map[string]struct{}),
		bus:     bus,
		tuning:  t,
		logger:  slog.Default(),
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	personalTopic := "client:" + strconv.FormatUint(id, 10)
	bus.Subscribe(personalTopic, c)
	c.topics[personalTopic] = struct{}{}

	return c
}

func (c *Client) ID() uint64 {
	return c.id
}

// Context returns the connection context: canceled when the connection
// closes or the gateway shuts down.
func (c *Client) Context() context.Context {
	return c.ctx
}

// Send implements eventbus.Subscriber.
func (c *Client) Send(topic string, event eventbus.Event) {
	c.SendServerMessage(&protocol.ServerMessage{
		Type:    protocol.MessageEvent,
		Channel: topic,
		Op:      event.Op,
		Payload: event.Payload,
	})
}

func (c *Client) SendServerMessage(msg *protocol.ServerMessage) {
	data := msg.Marshal(make([]byte, 0, 128))
	select {
	case c.send <- data:
	case <-c.done:
	default:
		// Slow consumer policy: disconnect instead of silently dropping messages.
		c.logger.Warn("client: send buffer full, disconnecting", "client", c.id)
		c.Close()
	}
}

func (c *Client) SendResponse(id uint32, payload protocol.Marshaler) {
	c.SendServerMessage(&protocol.ServerMessage{
		Type:    protocol.MessageResponse,
		ID:      id,
		Payload: protocol.Encode(payload),
	})
}

func (c *Client) SendError(id uint32, errMsg string, code int) {
	c.SendServerMessage(&protocol.ServerMessage{
		Type:  protocol.MessageError,
		ID:    id,
		Error: errMsg,
		Code:  uint16(code),
	})
}

func (c *Client) Subscribe(topic string) {
	c.mu.Lock()
	if _, ok := c.topics[topic]; ok {
		c.mu.Unlock()
		return
	}
	c.topics[topic] = struct{}{}
	if strings.HasPrefix(topic, "room:") {
		c.roomIDs[strings.TrimPrefix(topic, "room:")] = struct{}{}
	}
	c.mu.Unlock()
	c.bus.Subscribe(topic, c)
}

func (c *Client) Unsubscribe(topic string) {
	c.mu.Lock()
	delete(c.topics, topic)
	delete(c.roomIDs, strings.TrimPrefix(topic, "room:"))
	c.mu.Unlock()
	c.bus.Unsubscribe(topic, c)
}

// RoomIDs returns the IDs of the rooms the client is subscribed to; used for
// room-scoped message routing.
func (c *Client) RoomIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.roomIDs))
	for id := range c.roomIDs {
		ids = append(ids, id)
	}
	return ids
}

func (c *Client) SetSession(sess *session.Session) {
	c.mu.Lock()
	c.session = sess
	c.authenticated = true
	c.mu.Unlock()
	// Auth deadline is no longer relevant; switch to pong-based liveness.
	c.conn.SetReadDeadline(time.Now().Add(c.tuning.pongWait))
}

func (c *Client) Session() *session.Session {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.session
}

func (c *Client) IsAuthenticated() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.authenticated
}

// WriteLoop is the only writer to the websocket connection.
func (c *Client) WriteLoop() {
	ticker := time.NewTicker(c.tuning.pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case data := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(c.tuning.writeWait))
			if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(c.tuning.writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.done:
			// Flush pending messages with a bounded overall deadline, then exit.
			deadline := time.Now().Add(flushWait)
			for {
				select {
				case data := <-c.send:
					c.conn.SetWriteDeadline(deadline)
					if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
						return
					}
				default:
					return
				}
			}
		}
	}
}

func (c *Client) ReadLoop(dispatcher *Dispatcher) {
	defer func() {
		c.Close() // signals WriteLoop to flush and close the connection
		dispatcher.OnDisconnect(c)
	}()

	c.conn.SetReadLimit(c.tuning.maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(c.tuning.authTimeout))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(c.tuning.pongWait))
		return nil
	})

	go c.WriteLoop()
	go c.handleLoop(dispatcher)

	for {
		messageType, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		// Any readable frame proves liveness. Without this refresh, a
		// handler still busy on a previous message could outlive pongWait.
		wait := c.tuning.authTimeout
		if c.IsAuthenticated() {
			wait = c.tuning.pongWait
		}
		c.conn.SetReadDeadline(time.Now().Add(wait))

		if messageType != websocket.BinaryMessage {
			c.SendError(0, "binary messages only", 400)
			continue
		}

		var msg protocol.ClientMessage
		if _, err := msg.Unmarshal(raw); err != nil {
			c.SendError(0, "invalid message format", 400)
			continue
		}

		// Application-level ping: echo it back so the client can verify the
		// server is alive. Allowed before auth; never routed to the dispatcher.
		if msg.Type == protocol.MessagePing {
			c.SendServerMessage(&protocol.ServerMessage{Type: protocol.MessagePing, ID: msg.ID})
			continue
		}

		if !c.IsAuthenticated() && msg.Type != protocol.MessageAuth {
			c.SendError(msg.ID, "authentication required", 401)
			continue
		}

		// Dispatch off the read loop: a slow handler must not stall frame
		// reads. handleLoop processes them sequentially in arrival order.
		select {
		case c.handles <- &msg:
		case <-c.done:
			return
		}
	}
}

// handleLoop runs the dispatcher for one connection, outside the read loop,
// one message at a time in arrival order.
func (c *Client) handleLoop(dispatcher *Dispatcher) {
	for {
		select {
		case msg := <-c.handles:
			dispatcher.Dispatch(c, msg)
		case <-c.done:
			return
		}
	}
}

// Close is idempotent: it signals WriteLoop to flush pending messages and
// close the connection (so queued messages are not lost), and cancels the
// connection context.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.cancel()
	})
}
