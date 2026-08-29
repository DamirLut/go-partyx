package command

import (
	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/room"
	"github.com/damirlut/go-partyx/session"
)

// Context carries per-request state for global command handlers.
type Context struct {
	// Session is the authenticated session of the calling client.
	Session *session.Session
	// ClientID identifies the connection; it is also the player ID in rooms.
	ClientID uint64
	// Bus is the shared event bus for publishing to arbitrary topics.
	Bus *eventbus.EventBus
	// Rooms is the room manager (finding/listing rooms from global handlers).
	Rooms *room.Manager
	// Subscribe/Unsub manage the calling client's topic subscriptions.
	Subscribe func(topic string)
	Unsub     func(topic string)
}
