// Package protocol defines the binary wire format shared by the server and
// its clients. All types in schema.go are compiled by arpack into
// schema_gen.go (run `make generate` after editing); clients generate
// TypeScript/C#/Lua bindings from the same schema.
package protocol

//go:generate go run github.com/edmand46/arpack/cmd/arpack -in schema.go -out-go .

// MessageType identifies the envelope kind.
type MessageType uint8

const (
	// MessagePing is the zero value used as a keepalive/heartbeat message.
	MessagePing        MessageType = 0
	MessageAuth        MessageType = 1
	MessageSubscribe   MessageType = 2
	MessageUnsubscribe MessageType = 3
	MessageRequest     MessageType = 4
	MessageResponse    MessageType = 5
	MessageError       MessageType = 6
	MessageEvent       MessageType = 7
)

// MethodOp identifies a client -> server RPC method. Values 0-99 are
// reserved for the framework; games must register opcodes from 100.
type MethodOp uint16

const (
	MethodRoomCreate MethodOp = 1
	MethodRoomJoin   MethodOp = 2
	MethodRoomLeave  MethodOp = 3
	MethodLobbyList  MethodOp = 4
)

// EventOp identifies a server -> client event. Values 0-99 are reserved for
// the framework; games must use event opcodes from 100.
type EventOp uint16

const (
	EventPlayerJoined EventOp = 1
	EventPlayerLeft   EventOp = 2
	EventRoomKicked   EventOp = 3
	EventRoomCreated  EventOp = 4
	EventRoomRemoved  EventOp = 5
)

// SingletonMode controls how many rooms of the same type a user may occupy
// concurrently (across all connections).
type SingletonMode uint8

const (
	SingletonAllow   SingletonMode = 0 // no restrictions (default)
	SingletonReject  SingletonMode = 1 // error 409 if already in a room of this type
	SingletonReplace SingletonMode = 2 // kick the old session, allow the new join
)

// ClientMessage is the only frame a client sends: one binary arpack-encoded
// message per WebSocket binary frame.
type ClientMessage struct {
	Type    MessageType
	ID      uint32  // request: echoed back in response/error
	Op      uint16  // request: method opcode (MethodOp or game-defined)
	Token   string  // auth
	Channel string  // subscribe/unsubscribe
	Payload []uint8 // request: arpack-encoded method payload
}

// ServerMessage is the only frame the server sends.
type ServerMessage struct {
	Type    MessageType
	ID      uint32  // response/error: the request id
	Code    uint16  // error: 400/401/404/409/410/500
	Op      uint16  // event: event opcode (EventOp or game-defined)
	Channel string  // event: topic, e.g. "room:<id>", "lobby", "client:<id>"
	Error   string  // error: human-readable message
	Payload []uint8 // response/event: arpack-encoded payload
}

// AuthResult is the response payload for a successful auth message.
type AuthResult struct {
	SessionID string
	UserID    string
}

// CreateRoomRequest is the payload of MethodRoomCreate. The created room is
// joined automatically. Type selects the registered room type
// (partyx.Room); unknown types create a plain stateless room.
type CreateRoomRequest struct {
	Name       string // empty = same as Type
	Type       string // required
	MaxPlayers uint16 // 0 = module default, which itself defaults to unlimited
	// SingletonMode applies only to plain rooms. When Type matches a
	// registered module, the mode always comes from module.Singleton and
	// this field is ignored.
	SingletonMode SingletonMode
	// Options carries opaque key-value pairs handed to the room's module
	// untouched (Room.Options); partyx never interprets them.
	Options []CreateOption
}

// CreateOption is one key-value pair of CreateRoomRequest.Options.
type CreateOption struct {
	Key   string
	Value string
}

// JoinRoomRequest is the payload of MethodRoomJoin.
type JoinRoomRequest struct {
	RoomID string
}

// LeaveRoomRequest is the payload of MethodRoomLeave.
type LeaveRoomRequest struct {
	RoomID string
}

// RoomInfo describes a room. Used as the response payload of
// room.create/room.join, inside RoomList, and as the EventRoomCreated payload.
type RoomInfo struct {
	ID            string
	Name          string
	Type          string
	MaxPlayers    uint16
	PlayerCount   uint16
	IsOpen        bool
	SingletonMode SingletonMode
}

// RoomList is the response payload of MethodLobbyList.
type RoomList struct {
	Rooms []RoomInfo
}

// PlayerJoined is the payload of EventPlayerJoined on the room channel.
type PlayerJoined struct {
	PlayerID uint64
	UserID   string
}

// PlayerLeft is the payload of EventPlayerLeft on the room channel.
type PlayerLeft struct {
	PlayerID uint64
}

// Kicked is the payload of EventRoomKicked, sent to the personal channel of
// the client that was replaced by a newer connection (SingletonReplace).
type Kicked struct {
	Reason string
	RoomID string
}

// RoomRemoved is the payload of EventRoomRemoved on the lobby channel.
type RoomRemoved struct {
	ID string
}
