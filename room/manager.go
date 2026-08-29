package room

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/protocol"
)

type sessionInfo struct {
	RoomID   string
	ClientID uint64
}

// Manager owns all rooms and room types (modules): creation, lookup,
// singleton enforcement per user+type, room-scoped message routing, and
// auto-removal of empty rooms.
type Manager struct {
	mu           sync.RWMutex
	rooms        map[string]AnyRoom
	userSessions map[string]map[string]*sessionInfo // userID -> roomType -> session
	bus          *eventbus.EventBus
	modules      map[string]AnyModule
	opTypes      map[uint16]string // opcode -> room type
	logger       *slog.Logger
}

// NewManager builds a Manager. A nil logger falls back to slog.Default().
func NewManager(bus *eventbus.EventBus, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		rooms:        make(map[string]AnyRoom),
		userSessions: make(map[string]map[string]*sessionInfo),
		bus:          bus,
		modules:      make(map[string]AnyModule),
		opTypes:      make(map[uint16]string),
		logger:       logger,
	}
}

// RegisterModule registers a room type. Panics on a duplicate type or
// opcode: a collision is a programming error best caught at startup.
func (m *Manager) RegisterModule(mod AnyModule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := mod.RoomType()
	if t == "" {
		panic("room: module type is required")
	}
	if _, dup := m.modules[t]; dup {
		panic(fmt.Sprintf("room: module type %q already registered", t))
	}
	for _, op := range mod.Ops() {
		if owner, dup := m.opTypes[op]; dup {
			panic(fmt.Sprintf("room: opcode %d claimed by both %q and %q", op, owner, t))
		}
		m.opTypes[op] = t
	}
	m.modules[t] = mod
}

// OpType reports the room type whose module handles op (room-scoped routing).
func (m *Manager) OpType(op uint16) (string, bool) {
	m.mu.RLock()
	t, ok := m.opTypes[op]
	m.mu.RUnlock()
	return t, ok
}

// Create builds a room, wires auto-removal when its last player leaves, and
// publishes room.created to the lobby topic. If config.Type matches a
// registered module, the module provides the game state and default config
// (the request may only override Name and MaxPlayers); otherwise a plain
// stateless room is created.
func (m *Manager) Create(config RoomConfig) AnyRoom {
	m.mu.RLock()
	mod := m.modules[config.Type]
	m.mu.RUnlock()

	var r AnyRoom
	if mod != nil {
		base := mod.BaseConfig()
		base.Type = config.Type
		if config.Name != "" {
			base.Name = config.Name
		}
		if config.MaxPlayers != 0 {
			base.MaxPlayers = config.MaxPlayers
		}
		base.ApplyDefaults()
		r = mod.Create(base, m.bus, m.logger)
	} else {
		config.ApplyDefaults()
		r = newRoom(config, plainModule, m.bus, m.logger)
	}
	r.setOnEmpty(func(id string) { m.Remove(id) })

	m.mu.Lock()
	m.rooms[r.ID()] = r
	m.mu.Unlock()

	if info, err := r.Info(); err == nil {
		m.bus.Publish("lobby", eventbus.NewEvent(uint16(protocol.EventRoomCreated), &info))
	}
	return r
}

func (m *Manager) Find(id string) (AnyRoom, bool) {
	m.mu.RLock()
	r, ok := m.rooms[id]
	m.mu.RUnlock()
	return r, ok
}

func (m *Manager) List() []protocol.RoomInfo {
	m.mu.RLock()
	rooms := make([]AnyRoom, 0, len(m.rooms))
	for _, r := range m.rooms {
		rooms = append(rooms, r)
	}
	m.mu.RUnlock()

	// Info() blocks on each room actor; do it outside the manager lock.
	result := make([]protocol.RoomInfo, 0, len(rooms))
	for _, r := range rooms {
		info, err := r.Info()
		if err != nil {
			continue // room was removed concurrently
		}
		result = append(result, info)
	}
	return result
}

func (m *Manager) Remove(id string) {
	m.mu.Lock()
	room, ok := m.rooms[id]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.rooms, id)
	roomType := room.Config().Type
	m.cleanupSessionType(roomType, id)
	m.mu.Unlock()

	room.Shutdown()
	m.bus.Publish("lobby", eventbus.NewEvent(uint16(protocol.EventRoomRemoved), &protocol.RoomRemoved{ID: id}))
}

// ShutdownAll shuts down every room (their OnClose hooks run) and clears the
// manager state. Used on server shutdown: lobby events are not published —
// the process is going down and nobody is listening.
func (m *Manager) ShutdownAll() {
	m.mu.Lock()
	rooms := make([]AnyRoom, 0, len(m.rooms))
	for id, r := range m.rooms {
		rooms = append(rooms, r)
		delete(m.rooms, id)
	}
	m.userSessions = make(map[string]map[string]*sessionInfo)
	m.mu.Unlock()

	for _, r := range rooms {
		r.Shutdown()
	}
}

func (m *Manager) JoinRoom(userID string, clientID uint64, roomID string) (protocol.RoomInfo, error) {
	m.mu.Lock()
	r, ok := m.rooms[roomID]
	if !ok {
		m.mu.Unlock()
		return protocol.RoomInfo{}, ErrRoomNotFound
	}

	singletonMode := r.Config().SingletonMode
	roomType := r.Config().Type

	var replaced *sessionInfo
	if singletonMode != protocol.SingletonAllow {
		if m.userSessions[userID] == nil {
			m.userSessions[userID] = make(map[string]*sessionInfo)
		}
		oldSess, exists := m.userSessions[userID][roomType]
		// A rejoin with the same client into the same room is idempotent.
		// Anything else means the user already occupies this room type.
		if exists && (oldSess.RoomID != roomID || oldSess.ClientID != clientID) {
			switch singletonMode {
			case protocol.SingletonReject:
				m.mu.Unlock()
				return protocol.RoomInfo{}, ErrAlreadyInRoomOfType
			case protocol.SingletonReplace:
				replaced = oldSess
			}
		}
		// Optimistic record; rolled back if r.Join fails.
		m.userSessions[userID][roomType] = &sessionInfo{RoomID: roomID, ClientID: clientID}
	}
	m.mu.Unlock()

	var joinErr error
	if replaced != nil && replaced.RoomID == roomID {
		// Same room: atomic swap, so it never becomes transiently empty
		// and is not auto-removed under our feet.
		joinErr = r.JoinReplace(replaced.ClientID, clientID, userID)
		if joinErr == nil {
			m.bus.Publish(clientTopic(replaced.ClientID),
				eventbus.NewEvent(uint16(protocol.EventRoomKicked),
					&protocol.Kicked{Reason: "replaced", RoomID: roomID}))
		}
	} else {
		// Join first; kick the old client only on success, so a failed
		// join does not leave the user without any room.
		joinErr = r.Join(clientID, userID)
		if joinErr == nil && replaced != nil {
			if oldRoom, found := m.Find(replaced.RoomID); found {
				oldRoom.Leave(replaced.ClientID)
			}
			m.bus.Publish(clientTopic(replaced.ClientID),
				eventbus.NewEvent(uint16(protocol.EventRoomKicked),
					&protocol.Kicked{Reason: "replaced", RoomID: roomID}))
		}
	}

	if joinErr != nil {
		if singletonMode != protocol.SingletonAllow {
			m.mu.Lock()
			// Roll back only if the record is still ours — a concurrent
			// join may have already replaced it.
			if cur := m.userSessions[userID][roomType]; cur != nil && cur.ClientID == clientID {
				delete(m.userSessions[userID], roomType)
				if len(m.userSessions[userID]) == 0 {
					delete(m.userSessions, userID)
				}
			}
			m.mu.Unlock()
		}
		return protocol.RoomInfo{}, joinErr
	}

	info, err := r.Info()
	if err != nil {
		// The room was removed right after a successful join.
		return protocol.RoomInfo{}, ErrRoomNotFound
	}
	return info, nil
}

func (m *Manager) LeaveRoom(userID string, clientID uint64, roomID string) {
	m.mu.Lock()
	r, ok := m.rooms[roomID]
	if !ok {
		m.mu.Unlock()
		return
	}

	roomType := r.Config().Type
	if r.Config().SingletonMode != protocol.SingletonAllow {
		if sessions, exists := m.userSessions[userID]; exists {
			// Delete the singleton record only if it still belongs to this
			// client — it may have been taken over by a newer connection.
			if sess, ok := sessions[roomType]; ok && sess.RoomID == roomID && sess.ClientID == clientID {
				delete(sessions, roomType)
				if len(sessions) == 0 {
					delete(m.userSessions, userID)
				}
			}
		}
	}
	m.mu.Unlock()

	r.Leave(clientID)
}

// DispatchRoomMessage routes a room-scoped request to the client's room of
// the opcode's type; ctx is passed through to the module handler. handled is
// false when no module claims the opcode — the caller then reports "method
// not found".
func (m *Manager) DispatchRoomMessage(ctx context.Context, clientID uint64, op uint16, payload []byte, clientRoomIDs []string) (resp protocol.Marshaler, err error, handled bool) {
	typ, ok := m.OpType(op)
	if !ok {
		return nil, nil, false
	}

	m.mu.RLock()
	var match AnyRoom
	count := 0
	for _, id := range clientRoomIDs {
		if r, ok := m.rooms[id]; ok && r.Config().Type == typ {
			match = r
			count++
		}
	}
	m.mu.RUnlock()

	switch count {
	case 0:
		return nil, ErrNotInRoom, true
	case 1:
		resp, err := match.HandleMessage(ctx, clientID, op, payload)
		return resp, err, true
	default:
		return nil, ErrAmbiguousRoom, true
	}
}

func (m *Manager) cleanupSessionType(roomType, roomID string) {
	for userID, sessions := range m.userSessions {
		for typ, sess := range sessions {
			if typ == roomType && sess.RoomID == roomID {
				delete(sessions, typ)
				if len(sessions) == 0 {
					delete(m.userSessions, userID)
				}
			}
		}
	}
}

func clientTopic(clientID uint64) string {
	return "client:" + strconv.FormatUint(clientID, 10)
}
