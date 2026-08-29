package room

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

type gameState struct {
	players []string
	ticks   int
}

func readState[S any](r *Room[S], fn func(s *S)) {
	r.do(func(r *Room[S]) { fn(r.State) })
}

func TestModuleLifecycleHooks(t *testing.T) {
	inited := false
	closed := make(chan struct{})
	mod := NewModule[gameState]("test").
		State(func() *gameState {
			return &gameState{players: []string{}}
		}).
		OnInit(func(ctx context.Context, r *Room[gameState]) {
			inited = true
		}).
		OnJoin(func(ctx context.Context, r *Room[gameState], p *Player) {
			r.State.players = append(r.State.players, p.UserID)
		}).
		OnLeave(func(ctx context.Context, r *Room[gameState], p *Player) {
			for i, id := range r.State.players {
				if id == p.UserID {
					r.State.players = append(r.State.players[:i], r.State.players[i+1:]...)
					return
				}
			}
		}).
		OnClose(func(ctx context.Context, r *Room[gameState]) {
			close(closed)
		})

	r := newRoom(RoomConfig{Name: "g", Type: "test"}, mod, eventbus.New(nil), nil)

	if !inited {
		t.Fatal("OnInit was not called")
	}
	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}
	readState(r, func(s *gameState) {
		if len(s.players) != 1 || s.players[0] != "alice" {
			t.Fatalf("players = %v, want [alice]", s.players)
		}
	})

	r.Leave(1)
	readState(r, func(s *gameState) {
		if len(s.players) != 0 {
			t.Fatalf("players = %v, want empty", s.players)
		}
	})

	r.Shutdown()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("OnClose was not called")
	}
}

func TestModuleTick(t *testing.T) {
	mod := NewModule[gameState]("test").
		Tick(10*time.Millisecond, func(ctx context.Context, r *Room[gameState], dt time.Duration) {
			r.State.ticks++
		})
	r := newRoom(RoomConfig{Name: "g", Type: "test"}, mod, eventbus.New(nil), nil)
	defer r.Shutdown()

	waitFor(t, "at least 2 ticks", func() bool {
		var n int
		readState(r, func(s *gameState) { n = s.ticks })
		return n >= 2
	})
}

// guessReq is a hand-written protocol.Marshaler/Unmarshaler with Validate,
// used to test typed message handling without new codegen.
type guessReq struct {
	Word string
	Fail bool
}

func (m *guessReq) Marshal(buf []byte) []byte { return append(buf, m.Word...) }

func (m *guessReq) Unmarshal(data []byte) (int, error) {
	m.Word = string(data)
	return len(data), nil
}

func (m *guessReq) Validate() error {
	if m.Word == "" {
		return errors.New("word is required")
	}
	return nil
}

type guessResp struct {
	OK bool
}

func (m *guessResp) Marshal(buf []byte) []byte {
	if m.OK {
		return append(buf, 1)
	}
	return append(buf, 0)
}

func (m *guessResp) Unmarshal(data []byte) (int, error) {
	m.OK = len(data) > 0 && data[0] == 1
	return len(data), nil
}

func TestTypedHandle(t *testing.T) {
	mod := NewModule[gameState]("test").
		Handle(100, func(ctx context.Context, r *Room[gameState], p *Player, req *guessReq) (*guessResp, error) {
			return &guessResp{OK: req.Word == "слово" && p.ID == 1}, nil
		})

	r := newRoom(RoomConfig{Name: "g", Type: "test"}, mod, eventbus.New(nil), nil)
	defer r.Shutdown()
	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}

	resp, err := r.HandleMessage(context.Background(), 1, 100, protocol.Encode(&guessReq{Word: "слово"}))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	var got guessResp
	if _, err := got.Unmarshal(protocol.Encode(resp)); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !got.OK {
		t.Fatal("response OK = false, want true")
	}

	_, err = r.HandleMessage(context.Background(), 1, 100, protocol.Encode(&guessReq{Word: ""}))
	var perr *protocol.Error
	if !errors.As(err, &perr) || perr.Code != 400 {
		t.Fatalf("err = %v, want 400 protocol.Error", err)
	}
}

func TestTypedHandleDecodeFailure(t *testing.T) {
	// protocol.PlayerJoined is a real arpack type: Unmarshal rejects garbage.
	mod := NewModule[gameState]("test").
		Handle(100, func(ctx context.Context, r *Room[gameState], p *Player, req *protocol.PlayerJoined) (*guessResp, error) {
			return &guessResp{OK: true}, nil
		})

	r := newRoom(RoomConfig{Name: "g", Type: "test"}, mod, eventbus.New(nil), nil)
	defer r.Shutdown()
	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}

	_, err := r.HandleMessage(context.Background(), 1, 100, []byte{0xff, 0xff, 0xff, 0xff})
	var perr *protocol.Error
	if !errors.As(err, &perr) || perr.Code != 400 {
		t.Fatalf("err = %v, want 400 protocol.Error", err)
	}
}

func TestNilResponseYieldsEmptyPayload(t *testing.T) {
	mod := NewModule[gameState]("test").
		Handle(100, func(ctx context.Context, r *Room[gameState], p *Player, req *guessReq) (*guessResp, error) {
			return nil, nil
		})

	r := newRoom(RoomConfig{Name: "g", Type: "test"}, mod, eventbus.New(nil), nil)
	defer r.Shutdown()
	if err := r.Join(1, "alice"); err != nil {
		t.Fatalf("join: %v", err)
	}

	resp, err := r.HandleMessage(context.Background(), 1, 100, protocol.Encode(&guessReq{Word: "x"}))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if resp != nil {
		t.Fatalf("resp = %v, want nil", resp)
	}
}

func TestDuplicateOpcodePanics(t *testing.T) {
	mod := NewModule[gameState]("test")
	mod.HandleRaw(100, func(ctx context.Context, r *Room[gameState], p *Player, payload []byte) (protocol.Marshaler, error) {
		return nil, nil
	})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate opcode")
		}
	}()
	mod.HandleRaw(100, func(ctx context.Context, r *Room[gameState], p *Player, payload []byte) (protocol.Marshaler, error) {
		return nil, nil
	})
}
