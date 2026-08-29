package lobby

import (
	"github.com/damirlut/go-partyx/protocol"
	"github.com/damirlut/go-partyx/room"
)

// Lobby lists live rooms. Room lifecycle events (room.created/room.removed)
// are published by the room.Manager on the "lobby" topic.
type Lobby struct {
	rooms *room.Manager
}

func New(rooms *room.Manager) *Lobby {
	return &Lobby{rooms: rooms}
}

func (l *Lobby) List() []protocol.RoomInfo {
	return l.rooms.List()
}
