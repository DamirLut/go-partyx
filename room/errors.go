package room

import "errors"

var (
	ErrRoomNotFound        = errors.New("room not found")
	ErrRoomFull            = errors.New("room is full")
	ErrRoomClosed          = errors.New("room is closed")
	ErrAlreadyInRoomOfType = errors.New("already in a room of this type")
	ErrNotInRoom           = errors.New("not in a room that handles this opcode")
	ErrAmbiguousRoom       = errors.New("multiple joined rooms handle this opcode")
)
