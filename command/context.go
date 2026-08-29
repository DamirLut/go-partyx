package command

import (
	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/room"
	"github.com/damirlut/go-partyx/session"
)

// Context carries per-request state for global command handlers.
type Context struct {
	Session *session.Session
	// ClientID identifies the connection; it is also the player ID in rooms.
	ClientID uint64
	Bus      *eventbus.EventBus
	Rooms    *room.Manager
	// Subscribe/Unsub manage the calling client's topic subscriptions.
	Subscribe func(topic string)
	Unsub     func(topic string)
}
