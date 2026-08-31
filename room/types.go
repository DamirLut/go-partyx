package room

import "github.com/damirlut/go-partyx/protocol"

// Player is a connected client inside a room. Fields are set once at join
// and never mutated, so snapshots are safe to read from anywhere.
type Player struct {
	ID       uint64
	UserID   string
	JoinedAt int64
}

// RoomConfig configures a room. Type selects the registered module
// (Manager.RegisterModule); modules provide defaults for the other fields.
type RoomConfig struct {
	Name          string // empty = Type
	Type          string
	MaxPlayers    uint16 // 0 = unlimited
	SingletonMode protocol.SingletonMode
	// Options is opaque creation data handed to the module untouched
	// (Room.Options); partyx never interprets it.
	Options map[string]string
}

func (c *RoomConfig) ApplyDefaults() {
	if c.Name == "" {
		c.Name = c.Type
	}
}
