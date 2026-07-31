package gateway

import (
	"log"
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
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second // must be less than pongWait
	authTimeout    = 10 * time.Second
	maxMessageSize = 64 * 1024
	sendBufferSize = 256
	flushWait      = 2 * time.Second // budget for draining pending messages on close
)

var nextClientID atomic.Uint64

// Client is a WebSocket connection: an actor-like pair of read/write loops
// plus subscription state. All wire messages are binary arpack-encoded
// protocol.ClientMessage / protocol.ServerMessage frames.
type Client struct {
	id      uint64
	conn    *websocket.Conn
	send    chan []byte
	topics  map[string]struct{}
	roomIDs map[string]struct{}
	bus     *eventbus.EventBus

	mu            sync.RWMutex
	session       *session.Session
	authenticated bool

	done      chan struct{}
	closeOnce sync.Once
}

func NewClient(conn *websocket.Conn, bus *eventbus.EventBus) *Client {
	id := nextClientID.Add(1)
	c := &Client{
		id:      id,
		conn:    conn,
		send:    make(chan []byte, sendBufferSize),
		topics:  make(map[string]struct{}),
		roomIDs: make(map[string]struct{}),
		bus:     bus,
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
		log.Printf("client %d: send buffer full, disconnecting", c.id)
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

// RoomIDs returns the IDs of the rooms the client is subscribed to; used to
// route room-scoped messages.
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
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
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
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case data := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
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

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(authTimeout))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	go c.WriteLoop()

	for {
		messageType, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		if messageType != websocket.BinaryMessage {
			c.SendError(0, "binary messages only", 400)
			continue
		}

		var msg protocol.ClientMessage
		if _, err := msg.Unmarshal(raw); err != nil {
			c.SendError(0, "invalid message format", 400)
			continue
		}

		if !c.IsAuthenticated() && msg.Type != protocol.MessageAuth {
			c.SendError(msg.ID, "authentication required", 401)
			continue
		}

		dispatcher.Dispatch(c, &msg)
	}
}

// Close is idempotent: it signals WriteLoop (via done) to flush pending
// messages and close the connection. The connection itself is closed by
// WriteLoop, so queued messages are not lost.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
	})
}
