package gateway

import (
	"errors"

	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
	"github.com/damirlut/go-partyx/room"
	"github.com/damirlut/go-partyx/session"
)

// Dispatcher routes decoded client messages: auth, subscriptions, global RPC
// (command.Registry), and room-scoped RPC (room.Manager).
type Dispatcher struct {
	commands      *command.Registry
	bus           *eventbus.EventBus
	sessions      *session.Store
	authenticator Authenticator
	rooms         *room.Manager
}

func NewDispatcher(
	commands *command.Registry,
	bus *eventbus.EventBus,
	sessions *session.Store,
	authenticator Authenticator,
	rooms *room.Manager,
) *Dispatcher {
	return &Dispatcher{
		commands:      commands,
		bus:           bus,
		sessions:      sessions,
		authenticator: authenticator,
		rooms:         rooms,
	}
}

func (d *Dispatcher) Dispatch(c *Client, msg *protocol.ClientMessage) {
	switch msg.Type {
	case protocol.MessageAuth:
		d.handleAuth(c, msg)
	case protocol.MessageSubscribe:
		c.Subscribe(msg.Channel)
	case protocol.MessageUnsubscribe:
		c.Unsubscribe(msg.Channel)
	case protocol.MessageRequest:
		d.handleRequest(c, msg)
	default:
		c.SendError(msg.ID, "unknown message type", 400)
	}
}

func (d *Dispatcher) handleAuth(c *Client, msg *protocol.ClientMessage) {
	if c.IsAuthenticated() {
		c.SendError(msg.ID, "already authenticated", 400)
		return
	}

	sess, err := d.authenticator.Authenticate(msg.Token)
	if err != nil {
		c.SendError(msg.ID, err.Error(), 401)
		// Close is graceful: WriteLoop flushes the queued error before
		// closing the connection.
		c.Close()
		return
	}

	c.SetSession(sess)
	d.sessions.Set(sess)

	c.SendResponse(msg.ID, &protocol.AuthResult{
		SessionID: sess.ID,
		UserID:    sess.UserID,
	})
}

func (d *Dispatcher) handleRequest(c *Client, msg *protocol.ClientMessage) {
	// Global handlers win over room-scoped routing.
	if handler, ok := d.commands.Get(msg.Op); ok {
		ctx := &command.Context{
			Session:   c.Session(),
			ClientID:  c.ID(),
			Bus:       d.bus,
			Rooms:     d.rooms,
			Subscribe: c.Subscribe,
			Unsub:     c.Unsubscribe,
		}
		result, err := handler(ctx, msg.Payload)
		d.respond(c, msg.ID, result, err)
		return
	}

	result, err, handled := d.rooms.DispatchRoomMessage(c.ID(), msg.Op, msg.Payload, c.RoomIDs())
	if !handled {
		c.SendError(msg.ID, "method not found", 404)
		return
	}
	d.respond(c, msg.ID, result, err)
}

func (d *Dispatcher) respond(c *Client, id uint32, result protocol.Marshaler, err error) {
	if err != nil {
		c.SendError(id, err.Error(), errorCode(err))
		return
	}
	c.SendResponse(id, result)
}

// errorCode maps domain errors to wire protocol codes.
func errorCode(err error) int {
	var perr *protocol.Error
	if errors.As(err, &perr) {
		return perr.Code
	}
	switch {
	case errors.Is(err, room.ErrRoomNotFound):
		return 404
	case errors.Is(err, room.ErrRoomFull), errors.Is(err, room.ErrAlreadyInRoomOfType):
		return 409
	case errors.Is(err, room.ErrRoomClosed):
		return 410
	case errors.Is(err, room.ErrNotInRoom), errors.Is(err, room.ErrAmbiguousRoom):
		return 400
	}
	return 500
}

func (d *Dispatcher) OnDisconnect(c *Client) {
	sess := c.Session()

	c.mu.RLock()
	var roomIDs []string
	for rid := range c.roomIDs {
		roomIDs = append(roomIDs, rid)
	}
	topics := make([]string, 0, len(c.topics))
	for t := range c.topics {
		topics = append(topics, t)
	}
	c.mu.RUnlock()

	if sess != nil {
		for _, rid := range roomIDs {
			d.rooms.LeaveRoom(sess.UserID, c.id, rid)
		}
	}

	for _, t := range topics {
		c.Unsubscribe(t)
	}

	if sess != nil {
		d.sessions.Delete(sess.ID)
	}
}
