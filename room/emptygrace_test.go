package room

import (
	"testing"
	"time"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

// A room with an empty grace survives its last leave briefly, so a
// reconnecting player re-joins their seat instead of finding the room gone.
func TestEmptyGraceHoldsRoomForRejoin(t *testing.T) {
	r, _ := newTestRoom(RoomConfig{Name: "arena", Type: "test", EmptyGrace: 80 * time.Millisecond})
	defer r.Shutdown()

	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}
	r.Leave(1)

	if err := r.Join(2, "alice"); err != nil {
		t.Fatalf("rejoin during grace: %v", err)
	}
	info, err := r.Info()
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.PlayerCount != 1 {
		t.Fatalf("playerCount = %d, want 1", info.PlayerCount)
	}
}

// A join cancels the pending removal; once the room empties again and the
// grace runs out, the manager removes it.
func TestEmptyGraceRemovesStillEmptyRoom(t *testing.T) {
	bus := eventbus.New(nil)
	m := NewManager(bus, nil)
	sub := &captureSub{id: 1}
	bus.Subscribe("lobby", sub)

	r := m.Create(RoomConfig{Name: "arena", Type: "test", EmptyGrace: 40 * time.Millisecond})
	r.Join(1, "alice")
	r.Leave(1)

	if err := r.Join(2, "bob"); err != nil {
		t.Fatalf("rejoin during grace: %v", err)
	}
	r.Leave(2)

	waitFor(t, "empty-grace removal", func() bool {
		for _, op := range sub.ops() {
			if op == uint16(protocol.EventRoomRemoved) {
				return true
			}
		}
		return false
	})

	if _, err := r.Info(); err == nil {
		t.Fatal("the emptied room survived its grace")
	}
}

// Without a grace the room is removed the moment it empties — the default.
func TestZeroGraceRemovesImmediately(t *testing.T) {
	bus := eventbus.New(nil)
	m := NewManager(bus, nil)
	sub := &captureSub{id: 1}
	bus.Subscribe("lobby", sub)

	r := m.Create(RoomConfig{Name: "arena", Type: "test"})
	r.Join(1, "alice")
	r.Leave(1)

	waitFor(t, "immediate removal", func() bool {
		for _, op := range sub.ops() {
			if op == uint16(protocol.EventRoomRemoved) {
				return true
			}
		}
		return false
	})
}
