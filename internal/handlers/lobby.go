package handlers

import (
	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/lobby"
	"github.com/damirlut/go-partyx/protocol"
)

func RegisterLobbyHandlers(registry *command.Registry, l *lobby.Lobby) {
	registry.Register(uint16(protocol.MethodLobbyList), func(ctx *command.Context, payload []byte) (protocol.Marshaler, error) {
		return &protocol.RoomList{Rooms: l.List()}, nil
	})
}
