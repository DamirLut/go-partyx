package room

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

// Room is an actor: a goroutine plus an inbox of functions. All mutations of
// the player list and of the game State happen inside that goroutine, so
// module hooks, message handlers and OnTick never need locks.
//
// Its methods come in two groups. The blocking ones (Join, Leave, Players,
// HasPlayer, Info, IsOpen, ...) submit a closure to the inbox and wait for
// the actor to run them — they are for callers outside the actor. Calling a
// blocking method from a hook, message handler or OnTick deadlocks: the
// actor is busy running the very code that waits on it. Code running inside
// the actor must use the direct accessors instead: PlayerList, HasPlayerID,
// PlayerByUserID, Config, Options and RoomInfo read the state without the
// inbox, and Send,
// SendTo, BroadcastExcept and BroadcastFunc resolve targets against the live
// player list.
type Room[S any] struct {
	id     string
	config RoomConfig
	module *Module[S]

	// State must only be touched from module hooks, message handlers and
	// OnTick — they run inside the actor. Anywhere else is a data race.
	State *S

	players map[uint64]*Player
	isOpen  bool

	inbox   chan func(*Room[S])
	bus     *eventbus.EventBus
	onEmpty func(string)
	logger  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newRoom[S any](config RoomConfig, module *Module[S], bus *eventbus.EventBus, logger *slog.Logger) *Room[S] {
	if logger == nil {
		logger = slog.Default()
	}
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
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
	}
	r.wg.Add(1)
	// OnInit runs before the actor starts, so the room is not shared yet.
	if module.onInit != nil {
		r.safe("OnInit", func() { module.onInit(ctx, r) })
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
			r.safe("OnClose", func() { r.module.onClose(r.ctx, r) })
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
			r.safe("OnTick", func() { r.module.onTick(r.ctx, r, dt) })
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
			r.logger.Error("room: panic in hook", "room", r.id, "hook", what, "panic", rec)
		}
	}()
	fn()
}

// do runs fn inside the actor and waits for it to finish. It reports whether
// fn ran: after Shutdown operations are skipped and do returns false, so
// callers fail instead of silently succeeding on a dead room.
func (r *Room[S]) do(fn func(*Room[S])) bool {
	done := make(chan struct{}, 1)
	// Deferred signal: a panicking fn (recovered in the loop) still
	// releases the caller.
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

// Options returns the opaque creation options the room was created with.
// Direct accessor: config is immutable after construction, so it is safe
// from inside the actor (hooks, handlers, OnTick).
func (r *Room[S]) Options() map[string]string {
	return r.config.Options
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
				r.module.onJoin(r.ctx, r, p)
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

// JoinReplace atomically swaps oldClientID for clientID in one serialized
// step, so the room never becomes transiently empty (which would trigger
// auto-removal).
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
				r.module.onLeave(r.ctx, r, old)
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
			r.module.onJoin(r.ctx, r, p)
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
			r.module.onLeave(r.ctx, r, p)
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

// IsOpen reports whether the room accepts joins. It runs on the room actor;
// calling it from a hook, message handler or OnTick deadlocks — read
// RoomInfo().IsOpen there instead.
func (r *Room[S]) IsOpen() bool {
	var open bool
	r.do(func(r *Room[S]) { open = r.isOpen })
	return open
}

// Info returns a snapshot of the room. It runs on the room actor; calling it
// from a hook, message handler or OnTick deadlocks — use RoomInfo there. If
// the room is already shut down it returns ErrRoomClosed instead of a silent
// zero value.
func (r *Room[S]) Info() (protocol.RoomInfo, error) {
	var info protocol.RoomInfo
	executed := r.do(func(r *Room[S]) {
		info = r.roomInfo()
	})
	if !executed {
		return protocol.RoomInfo{}, ErrRoomClosed
	}
	return info, nil
}

// Players returns a snapshot of the current players. Player structs are
// immutable after join, so the snapshot is safe to read anywhere. It runs on
// the room actor; calling it from a hook, message handler or OnTick
// deadlocks — use PlayerList there.
func (r *Room[S]) Players() []*Player {
	var out []*Player
	r.do(func(r *Room[S]) {
		out = r.playerList()
	})
	return out
}

// HasPlayer reports whether clientID is in the room. It runs on the room
// actor; calling it from a hook, message handler or OnTick deadlocks — use
// HasPlayerID there.
func (r *Room[S]) HasPlayer(clientID uint64) bool {
	var ok bool
	r.do(func(r *Room[S]) { ok = r.hasPlayer(clientID) })
	return ok
}

// PlayerList returns a snapshot of the current players. It reads the room
// state directly, so it must only be called from inside the actor — module
// hooks, message handlers and OnTick. From outside the actor use Players.
func (r *Room[S]) PlayerList() []*Player {
	return r.playerList()
}

// HasPlayerID reports whether clientID is in the room. It reads the room
// state directly, so it must only be called from inside the actor. From
// outside the actor use HasPlayer.
func (r *Room[S]) HasPlayerID(clientID uint64) bool {
	return r.hasPlayer(clientID)
}

// PlayerByUserID returns the live player of userID. If several connections
// of the same user are in the room (possible in SingletonAllow rooms), one
// of them is returned. It reads the room state directly, so it must only be
// called from inside the actor.
func (r *Room[S]) PlayerByUserID(userID string) (*Player, bool) {
	for _, p := range r.players {
		if p.UserID == userID {
			return p, true
		}
	}
	return nil, false
}

// RoomInfo returns the same snapshot as Info. It reads the room state
// directly, so it must only be called from inside the actor. From outside
// the actor use Info.
func (r *Room[S]) RoomInfo() protocol.RoomInfo {
	return r.roomInfo()
}

func (r *Room[S]) playerList() []*Player {
	out := make([]*Player, 0, len(r.players))
	for _, p := range r.players {
		out = append(out, p)
	}
	return out
}

func (r *Room[S]) roomInfo() protocol.RoomInfo {
	return protocol.RoomInfo{
		ID:            r.id,
		Name:          r.config.Name,
		Type:          r.config.Type,
		MaxPlayers:    r.config.MaxPlayers,
		PlayerCount:   uint16(len(r.players)),
		IsOpen:        r.isOpen,
		SingletonMode: r.config.SingletonMode,
	}
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

// Send delivers a personal event to the player: it is published to the
// player's personal topic instead of the room topic, so other room members
// never see it. The live connection is resolved by p.UserID inside the
// actor, so a Player captured before a reconnect (JoinReplace swaps the
// clientID) still reaches the user's current connection. If the user has no
// live player in the room the message is dropped. It reads the room state
// directly, so it must only be called from inside the actor — module hooks,
// message handlers and OnTick.
func (r *Room[S]) Send(p *Player, op uint16, msg protocol.Marshaler) {
	r.sendToUserIDs([]string{p.UserID}, op, msg)
}

// SendTo delivers a personal event to a subset of room members, addressed by
// stable userID (see Send). Users without a live player in the room are
// skipped. Must only be called from inside the actor.
func (r *Room[S]) SendTo(userIDs []string, op uint16, msg protocol.Marshaler) {
	r.sendToUserIDs(userIDs, op, msg)
}

// BroadcastExcept delivers a room-wide event to every player except the
// listed userIDs (see Send for addressing and delivery). Must only be called
// from inside the actor.
func (r *Room[S]) BroadcastExcept(except []string, op uint16, msg protocol.Marshaler) {
	payload := protocol.Encode(msg)
	skip := make(map[string]struct{}, len(except))
	for _, id := range except {
		skip[id] = struct{}{}
	}
	for _, p := range r.players {
		if _, ok := skip[p.UserID]; ok {
			continue
		}
		r.bus.Publish(clientTopic(p.ID), eventbus.NewEventBytes(op, payload))
	}
}

// BroadcastFunc runs fn for every player and delivers each returned payload
// only to that player's personal topic, so every member can receive its own
// snapshot of the state. Returning false skips the player. Must only be
// called from inside the actor.
func (r *Room[S]) BroadcastFunc(op uint16, fn func(p *Player) (protocol.Marshaler, bool)) {
	for _, p := range r.players {
		msg, ok := fn(p)
		if !ok {
			continue
		}
		r.bus.Publish(clientTopic(p.ID), eventbus.NewEvent(op, msg))
	}
}

// sendToUserIDs encodes the payload once and publishes it to the personal
// topic of every connection whose player matches one of userIDs.
func (r *Room[S]) sendToUserIDs(userIDs []string, op uint16, msg protocol.Marshaler) {
	payload := protocol.Encode(msg)
	want := make(map[string]struct{}, len(userIDs))
	for _, id := range userIDs {
		want[id] = struct{}{}
	}
	for _, p := range r.players {
		if _, ok := want[p.UserID]; ok {
			r.bus.Publish(clientTopic(p.ID), eventbus.NewEventBytes(op, payload))
		}
	}
}

func (r *Room[S]) HandlesOp(op uint16) bool {
	_, ok := r.module.handlers[op]
	return ok
}

// HandleMessage runs the module handler for op inside the actor and returns
// its response. ctx is passed through to the handler; it is canceled when
// the connection closes or the server shuts down. Callers must check
// HandlesOp first.
func (r *Room[S]) HandleMessage(ctx context.Context, clientID uint64, op uint16, payload []byte) (resp protocol.Marshaler, err error) {
	h := r.module.handlers[op]
	executed := r.do(func(r *Room[S]) {
		p, ok := r.players[clientID]
		if !ok {
			err = ErrNotInRoom
			return
		}
		defer func() {
			if rec := recover(); rec != nil {
				r.logger.Error("room: panic in message handler", "room", r.id, "op", op, "panic", rec)
				resp = nil
				err = protocol.NewError(500, "internal error")
			}
		}()
		resp, err = h(ctx, r, p, payload)
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
