package room

import (
	"context"
	"errors"
	"testing"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

func newTestManager() *Manager {
	return NewManager(eventbus.New(nil), nil)
}

func TestJoinUnknownRoom(t *testing.T) {
	m := newTestManager()
	if _, err := m.JoinRoom("alice", 1, "missing"); !errors.Is(err, ErrRoomNotFound) {
		t.Fatalf("err = %v, want ErrRoomNotFound", err)
	}
}

func TestSingletonReject(t *testing.T) {
	m := newTestManager()
	r1 := m.Create(RoomConfig{Name: "a", Type: "duel", SingletonMode: protocol.SingletonReject})
	r2 := m.Create(RoomConfig{Name: "b", Type: "duel", SingletonMode: protocol.SingletonReject})
	defer m.Remove(r1.ID())
	defer m.Remove(r2.ID())

	if _, err := m.JoinRoom("alice", 1, r1.ID()); err != nil {
		t.Fatalf("join r1: %v", err)
	}
	if _, err := m.JoinRoom("alice", 1, r2.ID()); !errors.Is(err, ErrAlreadyInRoomOfType) {
		t.Fatalf("join r2 err = %v, want ErrAlreadyInRoomOfType", err)
	}
	// Rejoin with the same client into the same room is idempotent.
	if _, err := m.JoinRoom("alice", 1, r1.ID()); err != nil {
		t.Fatalf("rejoin r1: %v", err)
	}
	// Same room but a different connection is still a conflict.
	if _, err := m.JoinRoom("alice", 2, r1.ID()); !errors.Is(err, ErrAlreadyInRoomOfType) {
		t.Fatalf("join r1 as client 2 err = %v, want ErrAlreadyInRoomOfType", err)
	}
	// A different user is not affected.
	if _, err := m.JoinRoom("bob", 3, r2.ID()); err != nil {
		t.Fatalf("bob join r2: %v", err)
	}
}

func TestSingletonReplace(t *testing.T) {
	bus := eventbus.New(nil)
	m := NewManager(bus, nil)
	r1 := m.Create(RoomConfig{Name: "a", Type: "duel", SingletonMode: protocol.SingletonReplace})
	r2 := m.Create(RoomConfig{Name: "b", Type: "duel", SingletonMode: protocol.SingletonReplace})
	defer m.Remove(r2.ID())

	kicked := &captureSub{id: 1}
	bus.Subscribe(clientTopic(1), kicked)

	if _, err := m.JoinRoom("alice", 1, r1.ID()); err != nil {
		t.Fatalf("join r1: %v", err)
	}
	if _, err := m.JoinRoom("alice", 1, r2.ID()); err != nil {
		t.Fatalf("join r2: %v", err)
	}

	// The old client was kicked with a notification.
	if got := kicked.ops(); len(got) != 1 || got[0] != uint16(protocol.EventRoomKicked) {
		t.Fatalf("kicked events = %v, want [room.kicked]", got)
	}
	// The old room became empty and was removed.
	waitFor(t, "r1 removal", func() bool {
		_, ok := m.Find(r1.ID())
		return !ok
	})
	// Singleton record points at r2: joining another duel room must replace again.
	r3 := m.Create(RoomConfig{Name: "c", Type: "duel", SingletonMode: protocol.SingletonReplace})
	defer m.Remove(r3.ID())
	if _, err := m.JoinRoom("alice", 1, r3.ID()); err != nil {
		t.Fatalf("join r3: %v", err)
	}
}

// Regression: after a same-room replace with a new client, disconnecting the
// old client must not delete the singleton record of the new one.
func TestLeaveByOldClientKeepsSingletonRecord(t *testing.T) {
	m := newTestManager()
	rA := m.Create(RoomConfig{Name: "a", Type: "duel", SingletonMode: protocol.SingletonReplace})
	rB := m.Create(RoomConfig{Name: "b", Type: "duel", SingletonMode: protocol.SingletonReject})
	defer m.Remove(rA.ID())
	defer m.Remove(rB.ID())

	if _, err := m.JoinRoom("alice", 1, rA.ID()); err != nil {
		t.Fatalf("join A: %v", err)
	}
	// Rejoin the same room with a new connection: client 1 is replaced.
	if _, err := m.JoinRoom("alice", 2, rA.ID()); err != nil {
		t.Fatalf("rejoin A as client 2: %v", err)
	}
	// Old connection disconnects.
	m.LeaveRoom("alice", 1, rA.ID())

	// The singleton record must still belong to client 2.
	if _, err := m.JoinRoom("alice", 2, rB.ID()); !errors.Is(err, ErrAlreadyInRoomOfType) {
		t.Fatalf("join B err = %v, want ErrAlreadyInRoomOfType", err)
	}
	// Client 2 is still a player in room A.
	info, err := rA.Info()
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.PlayerCount != 1 {
		t.Fatalf("playerCount = %d, want 1", info.PlayerCount)
	}
}

// Regression: a failed join must roll back the optimistic singleton record,
// but must not delete a record that existed before.
func TestFailedJoinRollsBackSingleton(t *testing.T) {
	m := newTestManager()
	r1 := m.Create(RoomConfig{Name: "a", Type: "duel", SingletonMode: protocol.SingletonReject, MaxPlayers: 1})
	r2 := m.Create(RoomConfig{Name: "b", Type: "duel", SingletonMode: protocol.SingletonReject})
	defer m.Remove(r1.ID())
	defer m.Remove(r2.ID())

	if _, err := m.JoinRoom("owner", 9, r1.ID()); err != nil {
		t.Fatalf("owner join: %v", err)
	}
	// r1 is full; alice's join fails and must not leave a stale record.
	if _, err := m.JoinRoom("alice", 1, r1.ID()); !errors.Is(err, ErrRoomFull) {
		t.Fatalf("err = %v, want ErrRoomFull", err)
	}
	if _, err := m.JoinRoom("alice", 1, r2.ID()); err != nil {
		t.Fatalf("join r2 after failed join: %v", err)
	}
}

func TestLeaveRoomReleasesSingleton(t *testing.T) {
	m := newTestManager()
	r1 := m.Create(RoomConfig{Name: "a", Type: "duel", SingletonMode: protocol.SingletonReject})
	r2 := m.Create(RoomConfig{Name: "b", Type: "duel", SingletonMode: protocol.SingletonReject})
	defer m.Remove(r2.ID())

	if _, err := m.JoinRoom("alice", 1, r1.ID()); err != nil {
		t.Fatalf("join r1: %v", err)
	}
	m.LeaveRoom("alice", 1, r1.ID())

	// r1 became empty and is removed asynchronously.
	waitFor(t, "r1 removal", func() bool {
		_, ok := m.Find(r1.ID())
		return !ok
	})
	if _, err := m.JoinRoom("alice", 1, r2.ID()); err != nil {
		t.Fatalf("join r2 after leave: %v", err)
	}
}

func TestAllowModeHasNoSingleton(t *testing.T) {
	m := newTestManager()
	r1 := m.Create(RoomConfig{Name: "a", Type: "duel", SingletonMode: protocol.SingletonAllow})
	r2 := m.Create(RoomConfig{Name: "b", Type: "duel", SingletonMode: protocol.SingletonAllow})
	defer m.Remove(r1.ID())
	defer m.Remove(r2.ID())

	if _, err := m.JoinRoom("alice", 1, r1.ID()); err != nil {
		t.Fatalf("join r1: %v", err)
	}
	if _, err := m.JoinRoom("alice", 1, r2.ID()); err != nil {
		t.Fatalf("join r2: %v", err)
	}
}

func TestRemoveCleansUpSingletonRecords(t *testing.T) {
	m := newTestManager()
	r1 := m.Create(RoomConfig{Name: "a", Type: "duel", SingletonMode: protocol.SingletonReject})
	r2 := m.Create(RoomConfig{Name: "b", Type: "duel", SingletonMode: protocol.SingletonReject})
	defer m.Remove(r2.ID())

	if _, err := m.JoinRoom("alice", 1, r1.ID()); err != nil {
		t.Fatalf("join r1: %v", err)
	}
	m.Remove(r1.ID())
	if _, err := m.JoinRoom("alice", 1, r2.ID()); err != nil {
		t.Fatalf("join r2 after r1 removal: %v", err)
	}
}

func TestLobbyEvents(t *testing.T) {
	bus := eventbus.New(nil)
	m := NewManager(bus, nil)

	sub := &captureSub{id: 1}
	bus.Subscribe("lobby", sub)

	r := m.Create(RoomConfig{Name: "a", Type: "duel"})
	m.Remove(r.ID())

	got := sub.ops()
	want := []uint16{uint16(protocol.EventRoomCreated), uint16(protocol.EventRoomRemoved)}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("lobby events = %v, want %v", got, want)
	}
}

func TestList(t *testing.T) {
	m := newTestManager()
	r1 := m.Create(RoomConfig{Name: "a", Type: "duel"})
	r2 := m.Create(RoomConfig{Name: "b", Type: "chess"})
	defer m.Remove(r1.ID())
	defer m.Remove(r2.ID())

	if _, err := m.JoinRoom("alice", 1, r1.ID()); err != nil {
		t.Fatalf("join: %v", err)
	}

	list := m.List()
	if len(list) != 2 {
		t.Fatalf("len(list) = %d, want 2", len(list))
	}
	for _, info := range list {
		if info.ID == r1.ID() && info.PlayerCount != 1 {
			t.Fatalf("r1 playerCount = %d, want 1", info.PlayerCount)
		}
	}
}

func TestModuleBackedRoomUsesModuleConfig(t *testing.T) {
	m := newTestManager()
	m.RegisterModule(NewModule[gameState]("duel").
		MaxPlayers(2).
		Singleton(protocol.SingletonReject))

	// The request may override MaxPlayers; SingletonMode comes from the module.
	r := m.Create(RoomConfig{Type: "duel", MaxPlayers: 5, SingletonMode: protocol.SingletonAllow})
	defer m.Remove(r.ID())

	info, err := r.Info()
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.MaxPlayers != 5 {
		t.Fatalf("maxPlayers = %d, want 5 (request override)", info.MaxPlayers)
	}
	if info.SingletonMode != protocol.SingletonReject {
		t.Fatalf("singletonMode = %v, want reject (module wins)", info.SingletonMode)
	}
	if info.Name != "duel" {
		t.Fatalf("name = %q, want duel (defaulted to type)", info.Name)
	}
}

func TestDispatchRoomMessage(t *testing.T) {
	m := newTestManager()
	mod := NewModule[gameState]("duel").
		Handle(100, func(ctx context.Context, r *Room[gameState], p *Player, req *guessReq) (*guessResp, error) {
			return &guessResp{OK: true}, nil
		})
	m.RegisterModule(mod)

	r := m.Create(RoomConfig{Type: "duel"})
	defer m.Remove(r.ID())
	if _, err := m.JoinRoom("alice", 1, r.ID()); err != nil {
		t.Fatalf("join: %v", err)
	}

	roomIDs := []string{r.ID()}

	resp, err, handled := m.DispatchRoomMessage(context.Background(), 1, 100, protocol.Encode(&guessReq{Word: "x"}), roomIDs)
	if !handled || err != nil {
		t.Fatalf("handled = %v, err = %v", handled, err)
	}
	var got guessResp
	if _, err := got.Unmarshal(protocol.Encode(resp)); err != nil || !got.OK {
		t.Fatalf("resp = %v, err = %v", got, err)
	}

	// Unknown opcode: not handled at all.
	if _, _, handled := m.DispatchRoomMessage(context.Background(), 1, 999, nil, roomIDs); handled {
		t.Fatal("op 999 should not be handled")
	}

	// Known opcode but the client is in no room of that type.
	if _, err, handled := m.DispatchRoomMessage(context.Background(), 2, 100, nil, roomIDs); !handled || !errors.Is(err, ErrNotInRoom) {
		t.Fatalf("handled = %v, err = %v, want ErrNotInRoom", handled, err)
	}
}

func TestDispatchRoomMessageAmbiguous(t *testing.T) {
	m := newTestManager()
	mod := NewModule[gameState]("duel").
		Singleton(protocol.SingletonAllow).
		HandleRaw(100, func(ctx context.Context, r *Room[gameState], p *Player, payload []byte) (protocol.Marshaler, error) {
			return nil, nil
		})
	m.RegisterModule(mod)

	r1 := m.Create(RoomConfig{Type: "duel"})
	r2 := m.Create(RoomConfig{Type: "duel"})
	defer m.Remove(r1.ID())
	defer m.Remove(r2.ID())

	for _, id := range []string{r1.ID(), r2.ID()} {
		if _, err := m.JoinRoom("alice", 1, id); err != nil {
			t.Fatalf("join %s: %v", id, err)
		}
	}

	// The client is in two rooms of the same type: routing is ambiguous.
	_, err, handled := m.DispatchRoomMessage(context.Background(), 1, 100, nil, []string{r1.ID(), r2.ID()})
	if !handled || !errors.Is(err, ErrAmbiguousRoom) {
		t.Fatalf("handled = %v, err = %v, want ErrAmbiguousRoom", handled, err)
	}
}

func TestDuplicateModuleTypePanics(t *testing.T) {
	m := newTestManager()
	m.RegisterModule(NewModule[gameState]("duel"))
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate module type")
		}
	}()
	m.RegisterModule(NewModule[gameState]("duel"))
}

func TestDuplicateModuleOpcodePanics(t *testing.T) {
	m := newTestManager()
	a := NewModule[gameState]("a")
	a.HandleRaw(100, func(ctx context.Context, r *Room[gameState], p *Player, payload []byte) (protocol.Marshaler, error) {
		return nil, nil
	})
	m.RegisterModule(a)

	b := NewModule[gameState]("b")
	b.HandleRaw(100, func(ctx context.Context, r *Room[gameState], p *Player, payload []byte) (protocol.Marshaler, error) {
		return nil, nil
	})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate opcode across modules")
		}
	}()
	m.RegisterModule(b)
}
