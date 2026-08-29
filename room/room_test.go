package room

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

type captureSub struct {
	id     uint64
	mu     sync.Mutex
	events []eventbus.Event
}

func (s *captureSub) ID() uint64 { return s.id }

func (s *captureSub) Send(topic string, e eventbus.Event) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

func (s *captureSub) ops() []uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ops := make([]uint16, 0, len(s.events))
	for _, e := range s.events {
		ops = append(ops, e.Op)
	}
	return ops
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

func newTestRoom(config RoomConfig) (*Room[EmptyState], *eventbus.EventBus) {
	bus := eventbus.New()
	config.ApplyDefaults()
	return newRoom(config, plainModule, bus), bus
}

func TestJoinAndLeave(t *testing.T) {
	r, bus := newTestRoom(RoomConfig{Name: "arena", Type: "test"})
	defer r.Shutdown()

	sub := &captureSub{id: 100}
	bus.Subscribe(r.topic(), sub)

	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}
	if info := r.Info(); info.PlayerCount != 1 {
		t.Fatalf("playerCount = %d, want 1", info.PlayerCount)
	}

	r.Leave(1)
	if info := r.Info(); info.PlayerCount != 0 {
		t.Fatalf("playerCount = %d, want 0", info.PlayerCount)
	}

	got := sub.ops()
	want := []uint16{uint16(protocol.EventPlayerJoined), uint16(protocol.EventPlayerLeft)}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

func TestJoinIsIdempotentForSameClient(t *testing.T) {
	r, bus := newTestRoom(RoomConfig{Name: "arena", Type: "test"})
	defer r.Shutdown()

	sub := &captureSub{id: 100}
	bus.Subscribe(r.topic(), sub)

	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	if info := r.Info(); info.PlayerCount != 1 {
		t.Fatalf("playerCount = %d, want 1", info.PlayerCount)
	}
	// No duplicate player.joined event on idempotent rejoin.
	if got := sub.ops(); len(got) != 1 {
		t.Fatalf("events = %v, want exactly one player.joined", got)
	}
}

func TestRoomFull(t *testing.T) {
	r, _ := newTestRoom(RoomConfig{Name: "arena", Type: "test", MaxPlayers: 1})
	defer r.Shutdown()

	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := r.Join(2, "bob"); !errors.Is(err, ErrRoomFull) {
		t.Fatalf("err = %v, want ErrRoomFull", err)
	}
}

func TestCloseRejectsJoin(t *testing.T) {
	r, _ := newTestRoom(RoomConfig{Name: "arena", Type: "test"})
	defer r.Shutdown()

	r.Close()
	if err := r.Join(1, "alice"); !errors.Is(err, ErrRoomClosed) {
		t.Fatalf("err = %v, want ErrRoomClosed", err)
	}
	if r.IsOpen() {
		t.Fatal("room should not be open")
	}
	r.Open()
	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join after reopen: %v", err)
	}
}

func TestJoinAfterShutdown(t *testing.T) {
	r, _ := newTestRoom(RoomConfig{Name: "arena", Type: "test"})
	r.Shutdown()

	// Must not hang and must report failure instead of a ghost join.
	if err := r.Join(1, "alice"); !errors.Is(err, ErrRoomClosed) {
		t.Fatalf("err = %v, want ErrRoomClosed", err)
	}
}

func TestLeaveNonMemberPublishesNothing(t *testing.T) {
	r, bus := newTestRoom(RoomConfig{Name: "arena", Type: "test"})
	defer r.Shutdown()

	sub := &captureSub{id: 100}
	bus.Subscribe(r.topic(), sub)

	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}
	r.Leave(2) // not a member

	if got := sub.ops(); len(got) != 1 || got[0] != uint16(protocol.EventPlayerJoined) {
		t.Fatalf("events = %v, want [player.joined]", got)
	}
	if info := r.Info(); info.PlayerCount != 1 {
		t.Fatalf("playerCount = %d, want 1", info.PlayerCount)
	}
}

// Regression: Shutdown while an actor operation is in flight must not
// deadlock the room goroutine (Shutdown would hang in wg.Wait).
func TestShutdownWithInFlightOp(t *testing.T) {
	r, _ := newTestRoom(RoomConfig{Name: "arena", Type: "test"})

	started := make(chan struct{})
	release := make(chan struct{})
	doResult := make(chan bool, 1)
	go func() {
		doResult <- r.do(func(r *Room[EmptyState]) {
			close(started)
			<-release // simulate a long operation
		})
	}()

	<-started
	shutdownDone := make(chan struct{})
	go func() {
		r.Shutdown()
		close(shutdownDone)
	}()
	close(release)

	select {
	case <-doResult:
	case <-time.After(2 * time.Second):
		t.Fatal("do hung")
	}
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown hung")
	}
}

func TestBroadcast(t *testing.T) {
	r, bus := newTestRoom(RoomConfig{Name: "arena", Type: "test"})
	defer r.Shutdown()

	sub := &captureSub{id: 100}
	bus.Subscribe(r.topic(), sub)

	const op = 100
	r.Broadcast(op, &protocol.PlayerLeft{PlayerID: 7})

	sub.mu.Lock()
	defer sub.mu.Unlock()
	if len(sub.events) != 1 {
		t.Fatalf("events = %v, want 1 event", sub.events)
	}
	e := sub.events[0]
	if e.Op != op {
		t.Fatalf("op = %d, want %d", e.Op, op)
	}
	var decoded protocol.PlayerLeft
	if _, err := decoded.Unmarshal(e.Payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if decoded.PlayerID != 7 {
		t.Fatalf("PlayerID = %d, want 7", decoded.PlayerID)
	}
}

// Regression: a panicking module hook must not kill the room actor.
func TestPanickingHandlerKeepsRoomAlive(t *testing.T) {
	mod := NewModule[EmptyState]("test")
	mod.Handle(100, func(r *Room[EmptyState], p *Player, payload []byte) (protocol.Marshaler, error) {
		panic("boom")
	})
	r := newRoom(RoomConfig{Name: "arena", Type: "test"}, mod, eventbus.New())
	defer r.Shutdown()

	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}
	_, err := r.HandleMessage(1, 100, nil)
	var perr *protocol.Error
	if !errors.As(err, &perr) || perr.Code != 500 {
		t.Fatalf("err = %v, want 500 protocol.Error", err)
	}
	// Room still works after the panic.
	if info := r.Info(); info.PlayerCount != 1 {
		t.Fatalf("playerCount = %d, want 1 (actor survived)", info.PlayerCount)
	}
}

func TestHandleMessageRequiresMembership(t *testing.T) {
	mod := NewModule[EmptyState]("test")
	mod.Handle(100, func(r *Room[EmptyState], p *Player, payload []byte) (protocol.Marshaler, error) {
		return nil, nil
	})
	r := newRoom(RoomConfig{Name: "arena", Type: "test"}, mod, eventbus.New())
	defer r.Shutdown()

	if _, err := r.HandleMessage(1, 100, nil); !errors.Is(err, ErrNotInRoom) {
		t.Fatalf("err = %v, want ErrNotInRoom", err)
	}
}

func TestRoomIDsAreUnique(t *testing.T) {
	r1, _ := newTestRoom(RoomConfig{Name: "arena", Type: "test"})
	defer r1.Shutdown()
	r2, _ := newTestRoom(RoomConfig{Name: "arena", Type: "test"})
	defer r2.Shutdown()

	if r1.ID() == r2.ID() {
		t.Fatalf("duplicate room id: %s", r1.ID())
	}
}
