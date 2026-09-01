package room

import (
	"testing"
	"time"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

// A kept room survives its last leave — even past an empty grace — and a
// returning player re-joins their seat: the match-keeps-running case.
func TestKeepOnEmptySurvivesLastLeave(t *testing.T) {
	bus := eventbus.New(nil)
	m := NewManager(bus, nil)

	r := m.Create(RoomConfig{Name: "arena", Type: "test", EmptyGrace: 40 * time.Millisecond})
	r.Join(1, "alice")
	r.SetKeepOnEmpty(true)
	r.Leave(1)

	// The grace runs out but the room must still be there, empty and open.
	time.Sleep(80 * time.Millisecond)
	info, err := r.Info()
	if err != nil {
		t.Fatalf("kept room was removed: %v", err)
	}
	if info.PlayerCount != 0 || !info.IsOpen {
		t.Fatalf("info = %+v, want an open empty room", info)
	}

	if err := r.Join(2, "alice"); err != nil {
		t.Fatalf("rejoin into the kept room: %v", err)
	}
	if info, _ := r.Info(); info.PlayerCount != 1 {
		t.Fatalf("playerCount = %d, want 1", info.PlayerCount)
	}
}

// Turning the keep off on an emptied room resumes the removal — e.g. the
// match ended while everyone was away.
func TestKeepOffRemovesEmptiedRoom(t *testing.T) {
	bus := eventbus.New(nil)
	m := NewManager(bus, nil)
	sub := &captureSub{id: 1}
	bus.Subscribe("lobby", sub)

	r := m.Create(RoomConfig{Name: "arena", Type: "test"})
	r.Join(1, "alice")
	r.SetKeepOnEmpty(true)
	r.Leave(1)

	r.SetKeepOnEmpty(false)
	waitFor(t, "removal after keep off", func() bool {
		for _, op := range sub.ops() {
			if op == uint16(protocol.EventRoomRemoved) {
				return true
			}
		}
		return false
	})
	if _, err := r.Info(); err == nil {
		t.Fatal("the emptied room survived the keep being turned off")
	}
}

// An explicit removal still closes a kept room.
func TestRemoveClosesKeptRoom(t *testing.T) {
	bus := eventbus.New(nil)
	m := NewManager(bus, nil)
	sub := &captureSub{id: 1}
	bus.Subscribe("lobby", sub)

	r := m.Create(RoomConfig{Name: "arena", Type: "test"})
	r.Join(1, "alice")
	r.SetKeepOnEmpty(true)
	r.Leave(1)

	m.Remove(r.ID())
	waitFor(t, "explicit removal", func() bool {
		for _, op := range sub.ops() {
			if op == uint16(protocol.EventRoomRemoved) {
				return true
			}
		}
		return false
	})
}
