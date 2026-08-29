package handlers

import (
	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/protocol"
	"github.com/damirlut/go-partyx/room"
)

func RegisterRoomHandlers(registry *command.Registry, mgr *room.Manager) {
	registry.Register(uint16(protocol.MethodRoomCreate), createRoomHandler(mgr))
	registry.Register(uint16(protocol.MethodRoomJoin), joinRoomHandler(mgr))
	registry.Register(uint16(protocol.MethodRoomLeave), leaveRoomHandler(mgr))
}

func createRoomHandler(mgr *room.Manager) command.Handler {
	return func(ctx *command.Context, payload []byte) (protocol.Marshaler, error) {
		req, err := protocol.Decode[protocol.CreateRoomRequest](payload)
		if err != nil {
			return nil, protocol.NewError(400, "invalid payload")
		}
		if req.Type == "" {
			return nil, protocol.NewError(400, "type is required")
		}

		r := mgr.Create(room.RoomConfig{
			Name:          req.Name,
			Type:          req.Type,
			MaxPlayers:    req.MaxPlayers,
			SingletonMode: req.SingletonMode,
		})

		// Subscribe before joining so no room events are missed.
		ctx.Subscribe("room:" + r.ID())
		info, err := mgr.JoinRoom(ctx.Session.UserID, ctx.ClientID, r.ID())
		if err != nil {
			ctx.Unsub("room:" + r.ID())
			mgr.Remove(r.ID()) // the room is empty; don't leak it
			return nil, err
		}

		return &info, nil
	}
}

func joinRoomHandler(mgr *room.Manager) command.Handler {
	return func(ctx *command.Context, payload []byte) (protocol.Marshaler, error) {
		req, err := protocol.Decode[protocol.JoinRoomRequest](payload)
		if err != nil {
			return nil, protocol.NewError(400, "invalid payload")
		}
		if req.RoomID == "" {
			return nil, protocol.NewError(400, "roomId is required")
		}

		// Subscribe before joining so no room events are missed.
		ctx.Subscribe("room:" + req.RoomID)
		info, err := mgr.JoinRoom(ctx.Session.UserID, ctx.ClientID, req.RoomID)
		if err != nil {
			ctx.Unsub("room:" + req.RoomID)
			return nil, err
		}

		return &info, nil
	}
}

func leaveRoomHandler(mgr *room.Manager) command.Handler {
	return func(ctx *command.Context, payload []byte) (protocol.Marshaler, error) {
		req, err := protocol.Decode[protocol.LeaveRoomRequest](payload)
		if err != nil {
			return nil, protocol.NewError(400, "invalid payload")
		}
		if req.RoomID == "" {
			return nil, protocol.NewError(400, "roomId is required")
		}

		mgr.LeaveRoom(ctx.Session.UserID, ctx.ClientID, req.RoomID)
		ctx.Unsub("room:" + req.RoomID)

		return nil, nil
	}
}
