package command

import (
	"context"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/room"
	"github.com/damirlut/go-partyx/session"
)

// Context carries per-request state for global command handlers. It embeds
// the request context.Context: canceled when the connection closes, so
// handlers can bound cancellation-sensitive work (outgoing RPCs, DB calls).
type Context struct {
	context.Context

	Session *session.Session
	// ClientID identifies the connection; it is also the player ID in rooms.
	ClientID uint64
	Bus      *eventbus.EventBus
	Rooms    *room.Manager
	// Subscribe/Unsub manage the calling client's topic subscriptions.
	Subscribe func(topic string)
	Unsub     func(topic string)
}
