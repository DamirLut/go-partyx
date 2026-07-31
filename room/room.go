package room

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

// Room is an actor: a goroutine plus an inbox of functions. All mutations of
// the player list and of the game State happen inside that goroutine, so
// module hooks, message handlers and OnTick never need locks.
type Room[S any] struct {
	id     string
	config RoomConfig
	module *Module[S]

	// State is the game state owned by this room. It must only be touched
	// from module hooks, message handlers and OnTick — those run inside the
	// actor. Accessing it from anywhere else is a data race.
	State *S

	players map[uint64]*Player
	isOpen  bool

	inbox   chan func(*Room[S])
	bus     *eventbus.EventBus
	onEmpty func(string)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newRoom[S any](config RoomConfig, module *Module[S], bus *eventbus.EventBus) *Room[S] {
	ctx, cancel := context.WithCancel(context.Background())
	state := new(S)
	if module.newState != nil {
		if s := module.newState(); s != nil {
			state = s
		}
	}
	r := &Room[S]{
		id:      config.Name + "_" + uuid.NewString(),
		config:  config,
		module:  module,
		State:   state,
		players: make(map[uint64]*Player),
		isOpen:  true,
		inbox:   make(chan func(*Room[S]), 64),
		bus:     bus,
		ctx:     ctx,
		cancel:  cancel,
	}
	r.wg.Add(1)
	// OnInit runs synchronously before the actor starts: the room is not
	// shared with anyone yet, so this is race-free and deterministic.
	if module.onInit != nil {
		r.safe("OnInit", func() { module.onInit(r) })
	}
	go r.loop()
	return r
}

func (r *Room[S]) loop() {
	defer r.wg.Done()

	var ticker *time.Ticker
	if r.module.tickRate > 0 && r.module.onTick != nil {
		ticker = time.NewTicker(r.module.tickRate)
	}
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
		if r.module.onClose != nil {
			r.safe("OnClose", func() { r.module.onClose(r) })
		}
	}()

	var tick <-chan time.Time
	if ticker != nil {
		tick = ticker.C
	}

	last := time.Now()
	for {
		select {
		case fn, ok := <-r.inbox:
			if !ok {
				return
			}
			r.safe("inbox", func() { fn(r) })
		case now := <-tick:
			dt := now.Sub(last)
			last = now
			r.safe("OnTick", func() { r.module.onTick(r, dt) })
		case <-r.ctx.Done():
			return
		}
	}
}

// safe runs fn and recovers panics, so a buggy hook or handler cannot kill
// the room actor (and with it, the process).
func (r *Room[S]) safe(what string, fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("room %s: panic in %s: %v", r.id, what, rec)
		}
	}()
	fn()
}

// do runs fn inside the actor and waits for it to finish. It reports whether
// fn actually ran: after Shutdown pending and subsequent operations are
// skipped and do returns false, so callers (e.g. Join) can fail instead of
// silently succeeding on a dead room.
func (r *Room[S]) do(fn func(*Room[S])) bool {
	done := make(chan struct{}, 1)
	// done is signaled via defer so a panicking fn (recovered by safe in the
	// loop) still releases the caller.
	wrapped := func(r *Room[S]) {
		defer func() { done <- struct{}{} }()
		fn(r)
	}
	select {
	case r.inbox <- wrapped:
		select {
		case <-done:
			return true
		case <-r.ctx.Done():
			return false
		}
	case <-r.ctx.Done():
		return false
	}
}

func (r *Room[S]) ID() string {
	return r.id
}

func (r *Room[S]) Config() RoomConfig {
	return r.config
}

// Join adds a player. Rejoining with the same client is idempotent and
// publishes no duplicate event.
func (r *Room[S]) Join(clientID uint64, userID string) error {
	var joinErr error
	var joined bool
	executed := r.do(func(r *Room[S]) {
		switch {
		case !r.isOpen:
			joinErr = ErrRoomClosed
		case r.config.MaxPlayers > 0 && uint16(len(r.players)) >= r.config.MaxPlayers:
			joinErr = ErrRoomFull
		case r.hasPlayer(clientID):
			// Idempotent rejoin.
		default:
			p := &Player{ID: clientID, UserID: userID, JoinedAt: time.Now().Unix()}
			r.players[clientID] = p
			joined = true
			if r.module.onJoin != nil {
				r.module.onJoin(r, p)
			}
		}
	})
	if !executed {
		// Room was shut down concurrently (e.g. removed as empty).
		return ErrRoomClosed
	}
	if joinErr != nil {
		return joinErr
	}
	if joined {
		r.bus.Publish(r.topic(), eventbus.NewEvent(uint16(protocol.EventPlayerJoined),
			&protocol.PlayerJoined{PlayerID: clientID, UserID: userID}))
	}
	return nil
}

// JoinReplace atomically swaps oldClientID for clientID as a single
// serialized operation: the old player is removed and the new one added
// without the room ever becoming transiently empty (which would trigger
// auto-removal via onEmpty).
func (r *Room[S]) JoinReplace(oldClientID, clientID uint64, userID string) error {
	var joinErr error
	var becameEmpty bool
	executed := r.do(func(r *Room[S]) {
		if !r.isOpen {
			joinErr = ErrRoomClosed
			return
		}
		if old, ok := r.players[oldClientID]; ok {
			delete(r.players, oldClientID)
			if r.module.onLeave != nil {
				r.module.onLeave(r, old)
			}
		}
		if r.config.MaxPlayers > 0 && uint16(len(r.players)) >= r.config.MaxPlayers {
			joinErr = ErrRoomFull
			becameEmpty = len(r.players) == 0
			return
		}
		p := &Player{ID: clientID, UserID: userID, JoinedAt: time.Now().Unix()}
		r.players[clientID] = p
		if r.module.onJoin != nil {
			r.module.onJoin(r, p)
		}
	})
	if !executed {
		return ErrRoomClosed
	}
	if joinErr != nil {
		if becameEmpty && r.onEmpty != nil {
			go r.onEmpty(r.id)
		}
		return joinErr
	}
	r.bus.Publish(r.topic(), eventbus.NewEvent(uint16(protocol.EventPlayerLeft),
		&protocol.PlayerLeft{PlayerID: oldClientID}))
	r.bus.Publish(r.topic(), eventbus.NewEvent(uint16(protocol.EventPlayerJoined),
		&protocol.PlayerJoined{PlayerID: clientID, UserID: userID}))
	return nil
}

func (r *Room[S]) Leave(clientID uint64) {
	var empty, existed bool
	executed := r.do(func(r *Room[S]) {
		p, ok := r.players[clientID]
		if !ok {
			return
		}
		existed = true
		delete(r.players, clientID)
		if r.module.onLeave != nil {
			r.module.onLeave(r, p)
		}
		empty = len(r.players) == 0
	})
	if !executed || !existed {
		return
	}
	r.bus.Publish(r.topic(), eventbus.NewEvent(uint16(protocol.EventPlayerLeft),
		&protocol.PlayerLeft{PlayerID: clientID}))
	if empty && r.onEmpty != nil {
		go r.onEmpty(r.id)
	}
}

// Close rejects new joins; existing players are kept.
func (r *Room[S]) Close() {
	r.do(func(r *Room[S]) { r.isOpen = false })
}

func (r *Room[S]) Open() {
	r.do(func(r *Room[S]) { r.isOpen = true })
}

func (r *Room[S]) IsOpen() bool {
	var open bool
	r.do(func(r *Room[S]) { open = r.isOpen })
	return open
}

// Info returns a snapshot of the room. A zero protocol.RoomInfo is returned
// if the room is already shut down.
func (r *Room[S]) Info() protocol.RoomInfo {
	var info protocol.RoomInfo
	r.do(func(r *Room[S]) {
		info = protocol.RoomInfo{
			ID:            r.id,
			Name:          r.config.Name,
			Type:          r.config.Type,
			MaxPlayers:    r.config.MaxPlayers,
			PlayerCount:   uint16(len(r.players)),
			IsOpen:        r.isOpen,
			SingletonMode: r.config.SingletonMode,
		}
	})
	return info
}

// Players returns a snapshot of the current players. Player structs are
// immutable after join, so the snapshot is safe to read anywhere.
func (r *Room[S]) Players() []*Player {
	var out []*Player
	r.do(func(r *Room[S]) {
		out = make([]*Player, 0, len(r.players))
		for _, p := range r.players {
			out = append(out, p)
		}
	})
	return out
}

func (r *Room[S]) HasPlayer(clientID uint64) bool {
	var ok bool
	r.do(func(r *Room[S]) { ok = r.hasPlayer(clientID) })
	return ok
}

func (r *Room[S]) hasPlayer(clientID uint64) bool {
	_, ok := r.players[clientID]
	return ok
}

// Broadcast encodes msg and publishes it to every subscriber of the room
// topic. Safe to call from anywhere, inside or outside the actor.
func (r *Room[S]) Broadcast(op uint16, msg protocol.Marshaler) {
	r.bus.Publish(r.topic(), eventbus.NewEvent(op, msg))
}

// BroadcastBytes publishes a pre-encoded payload to the room topic.
func (r *Room[S]) BroadcastBytes(op uint16, payload []byte) {
	r.bus.Publish(r.topic(), eventbus.NewEventBytes(op, payload))
}

// HandlesOp reports whether the room's module has a handler for op.
func (r *Room[S]) HandlesOp(op uint16) bool {
	_, ok := r.module.handlers[op]
	return ok
}

// HandleMessage runs the module handler for op inside the actor and returns
// its response. Callers must check HandlesOp first.
func (r *Room[S]) HandleMessage(clientID uint64, op uint16, payload []byte) (resp protocol.Marshaler, err error) {
	h := r.module.handlers[op]
	executed := r.do(func(r *Room[S]) {
		p, ok := r.players[clientID]
		if !ok {
			err = ErrNotInRoom
			return
		}
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("room %s: panic in message handler (op %d): %v", r.id, op, rec)
				resp = nil
				err = protocol.NewError(500, "internal error")
			}
		}()
		resp, err = h(r, p, payload)
	})
	if !executed {
		return nil, ErrRoomClosed
	}
	return resp, err
}

func (r *Room[S]) Shutdown() {
	r.cancel()
	r.wg.Wait()
}

func (r *Room[S]) topic() string {
	return "room:" + r.id
}

func (r *Room[S]) setOnEmpty(fn func(string)) {
	r.onEmpty = fn
}
