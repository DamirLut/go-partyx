package gateway

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/eventbus"
	"github.com/damirlut/go-partyx/internal/handlers"
	"github.com/damirlut/go-partyx/lobby"
	"github.com/damirlut/go-partyx/protocol"
	"github.com/damirlut/go-partyx/room"
	"github.com/damirlut/go-partyx/session"
)

type testAuth struct{}

func (testAuth) Authenticate(token string) (*session.Session, error) {
	if token == "" {
		return nil, errors.New("token required")
	}
	return session.New("sess-"+token, token), nil
}

// newTestServer builds a gateway over httptest; setup may register extra
// modules and commands before serving.
func newTestServer(t *testing.T, setup func(rooms *room.Manager, commands *command.Registry)) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)

	bus := eventbus.New()
	rooms := room.NewManager(bus)
	l := lobby.New(rooms)
	commands := command.NewRegistry()
	handlers.RegisterRoomHandlers(commands, rooms)
	handlers.RegisterLobbyHandlers(commands, l)

	if setup != nil {
		setup(rooms, commands)
	}

	gw := New(Config{
		Addr:          "127.0.0.1:0",
		Bus:           bus,
		Commands:      commands,
		Sessions:      session.NewStore(),
		Authenticator: testAuth{},
		Rooms:         rooms,
	})

	srv := httptest.NewServer(gw.Engine())
	t.Cleanup(srv.Close)
	return srv
}

func dialWS(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func sendMsg(t *testing.T, conn *websocket.Conn, msg *protocol.ClientMessage) {
	t.Helper()
	data := msg.Marshal(make([]byte, 0, 64))
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readMsg(t *testing.T, conn *websocket.Conn) *protocol.ServerMessage {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg protocol.ServerMessage
	if _, err := msg.Unmarshal(raw); err != nil {
		t.Fatalf("decode server message: %v", err)
	}
	return &msg
}

func readUntilID(t *testing.T, conn *websocket.Conn, id uint32) *protocol.ServerMessage {
	t.Helper()
	for {
		msg := readMsg(t, conn)
		if (msg.Type == protocol.MessageResponse || msg.Type == protocol.MessageError) && msg.ID == id {
			return msg
		}
	}
}

func readUntilEvent(t *testing.T, conn *websocket.Conn, channel string, op uint16) *protocol.ServerMessage {
	t.Helper()
	for {
		msg := readMsg(t, conn)
		if msg.Type == protocol.MessageEvent && msg.Channel == channel && msg.Op == op {
			return msg
		}
	}
}

func auth(t *testing.T, conn *websocket.Conn, token string) {
	t.Helper()
	sendMsg(t, conn, &protocol.ClientMessage{Type: protocol.MessageAuth, Token: token})
	msg := readUntilID(t, conn, 0)
	if msg.Type != protocol.MessageResponse {
		t.Fatalf("auth failed: %+v", msg)
	}
	var result protocol.AuthResult
	if _, err := result.Unmarshal(msg.Payload); err != nil {
		t.Fatalf("decode auth result: %v", err)
	}
	if result.UserID != token {
		t.Fatalf("userId = %q, want %q", result.UserID, token)
	}
}

func request(t *testing.T, conn *websocket.Conn, id uint32, op uint16, payload protocol.Marshaler) {
	t.Helper()
	sendMsg(t, conn, &protocol.ClientMessage{
		Type:    protocol.MessageRequest,
		ID:      id,
		Op:      op,
		Payload: protocol.Encode(payload),
	})
}

func decodePayload[T any, PT interface {
	*T
	protocol.Unmarshaler
}](t *testing.T, msg *protocol.ServerMessage) *T {
	t.Helper()
	v, err := protocol.Decode[T, PT](msg.Payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return v
}

func createRoom(t *testing.T, conn *websocket.Conn, id uint32, req *protocol.CreateRoomRequest) *protocol.RoomInfo {
	t.Helper()
	request(t, conn, id, uint16(protocol.MethodRoomCreate), req)
	msg := readUntilID(t, conn, id)
	if msg.Type != protocol.MessageResponse {
		t.Fatalf("room.create failed: %+v", msg)
	}
	return decodePayload[protocol.RoomInfo](t, msg)
}

func TestRoomFlow(t *testing.T) {
	srv := newTestServer(t, nil)

	c1 := dialWS(t, srv)
	auth(t, c1, "alice")

	created := createRoom(t, c1, 1, &protocol.CreateRoomRequest{
		Name: "arena", Type: "duel", MaxPlayers: 2, SingletonMode: protocol.SingletonReject,
	})
	roomID := created.ID
	if created.PlayerCount != 1 {
		t.Fatalf("playerCount = %d, want 1", created.PlayerCount)
	}

	request(t, c1, 2, uint16(protocol.MethodLobbyList), nil)
	msg := readUntilID(t, c1, 2)
	if msg.Type != protocol.MessageResponse {
		t.Fatalf("lobby.list failed: %+v", msg)
	}
	if list := decodePayload[protocol.RoomList](t, msg); len(list.Rooms) != 1 {
		t.Fatalf("lobby has %d rooms, want 1", len(list.Rooms))
	}

	c2 := dialWS(t, srv)
	auth(t, c2, "bob")

	request(t, c2, 3, uint16(protocol.MethodRoomJoin), &protocol.JoinRoomRequest{RoomID: roomID})
	msg = readUntilID(t, c2, 3)
	if msg.Type != protocol.MessageResponse {
		t.Fatalf("room.join failed: %+v", msg)
	}
	if joined := decodePayload[protocol.RoomInfo](t, msg); joined.PlayerCount != 2 {
		t.Fatalf("playerCount = %d, want 2", joined.PlayerCount)
	}

	// Alice is notified about Bob joining.
	ev := readUntilEvent(t, c1, "room:"+roomID, uint16(protocol.EventPlayerJoined))
	if pj := decodePayload[protocol.PlayerJoined](t, ev); pj.UserID != "bob" {
		t.Fatalf("player.joined userId = %q, want bob", pj.UserID)
	}

	// Bob disconnects; Alice is notified and stays in the room.
	c2.Close()
	readUntilEvent(t, c1, "room:"+roomID, uint16(protocol.EventPlayerLeft))

	request(t, c1, 4, uint16(protocol.MethodLobbyList), nil)
	msg = readUntilID(t, c1, 4)
	list := decodePayload[protocol.RoomList](t, msg)
	if len(list.Rooms) != 1 {
		t.Fatalf("lobby has %d rooms, want 1", len(list.Rooms))
	}
	if got := list.Rooms[0].PlayerCount; got != 1 {
		t.Fatalf("playerCount = %d, want 1", got)
	}
}

func TestErrorCodes(t *testing.T) {
	srv := newTestServer(t, nil)

	c1 := dialWS(t, srv)
	auth(t, c1, "alice")

	created := createRoom(t, c1, 1, &protocol.CreateRoomRequest{
		Name: "full", Type: "duel", MaxPlayers: 1,
	})

	c2 := dialWS(t, srv)
	auth(t, c2, "bob")

	request(t, c2, 3, uint16(protocol.MethodRoomJoin), &protocol.JoinRoomRequest{RoomID: created.ID})
	if msg := readUntilID(t, c2, 3); msg.Type != protocol.MessageError || msg.Code != 409 {
		t.Fatalf("full room: %+v, want error 409", msg)
	}

	request(t, c2, 4, uint16(protocol.MethodRoomJoin), &protocol.JoinRoomRequest{RoomID: "missing"})
	if msg := readUntilID(t, c2, 4); msg.Type != protocol.MessageError || msg.Code != 404 {
		t.Fatalf("missing room: %+v, want error 404", msg)
	}

	request(t, c2, 5, 999, nil)
	if msg := readUntilID(t, c2, 5); msg.Type != protocol.MessageError || msg.Code != 404 {
		t.Fatalf("unknown method: %+v, want error 404", msg)
	}

	request(t, c2, 6, uint16(protocol.MethodRoomCreate), &protocol.CreateRoomRequest{Name: "x"})
	if msg := readUntilID(t, c2, 6); msg.Type != protocol.MessageError || msg.Code != 400 {
		t.Fatalf("missing type: %+v, want error 400", msg)
	}

	sendMsg(t, c2, &protocol.ClientMessage{
		Type:    protocol.MessageRequest,
		ID:      7,
		Op:      uint16(protocol.MethodRoomJoin),
		Payload: []byte{0xff, 0xff, 0xff},
	})
	if msg := readUntilID(t, c2, 7); msg.Type != protocol.MessageError || msg.Code != 400 {
		t.Fatalf("garbage payload: %+v, want error 400", msg)
	}
}

func TestRequestBeforeAuth(t *testing.T) {
	srv := newTestServer(t, nil)
	c := dialWS(t, srv)

	request(t, c, 1, uint16(protocol.MethodLobbyList), nil)
	if msg := readUntilID(t, c, 1); msg.Type != protocol.MessageError || msg.Code != 401 {
		t.Fatalf("%+v, want error 401", msg)
	}
}

func TestFailedAuthClosesConnection(t *testing.T) {
	srv := newTestServer(t, nil)
	c := dialWS(t, srv)

	sendMsg(t, c, &protocol.ClientMessage{Type: protocol.MessageAuth, Token: ""})
	if msg := readUntilID(t, c, 0); msg.Type != protocol.MessageError || msg.Code != 401 {
		t.Fatalf("%+v, want error 401", msg)
	}

	// The server must flush the error and then close the connection.
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("expected connection to be closed after failed auth")
	}
}

func TestGarbageMessage(t *testing.T) {
	srv := newTestServer(t, nil)
	c := dialWS(t, srv)

	if err := c.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xff, 0xff}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if msg := readMsg(t, c); msg.Type != protocol.MessageError || msg.Code != 400 {
		t.Fatalf("%+v, want error 400", msg)
	}
}

func TestTextFrameRejected(t *testing.T) {
	srv := newTestServer(t, nil)
	c := dialWS(t, srv)

	if err := c.WriteMessage(websocket.TextMessage, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if msg := readMsg(t, c); msg.Type != protocol.MessageError || msg.Code != 400 {
		t.Fatalf("%+v, want error 400", msg)
	}
}

type echoState struct {
	seen int
}

// TestRoomModuleFlow exercises room-scoped messages end to end: a typed
// module handler running inside the room actor, plus broadcasts.
func TestRoomModuleFlow(t *testing.T) {
	const opEcho = 100

	srv := newTestServer(t, func(rooms *room.Manager, commands *command.Registry) {
		mod := room.NewModule[echoState]("echo").
			HandleTyped(opEcho, func(r *room.Room[echoState], p *room.Player, req *protocol.Kicked) (*protocol.Kicked, error) {
				r.State.seen++
				r.Broadcast(opEcho, &protocol.Kicked{Reason: "seen", RoomID: r.ID()})
				return &protocol.Kicked{Reason: req.Reason, RoomID: req.RoomID}, nil
			})
		rooms.RegisterModule(mod)
	})

	c1 := dialWS(t, srv)
	auth(t, c1, "alice")
	created := createRoom(t, c1, 1, &protocol.CreateRoomRequest{Type: "echo"})

	// Room-scoped message. The handler broadcasts before responding, so the
	// event may arrive first; collect both.
	request(t, c1, 2, opEcho, &protocol.Kicked{Reason: "hello", RoomID: "x"})
	var respMsg, evMsg *protocol.ServerMessage
	for respMsg == nil || evMsg == nil {
		msg := readMsg(t, c1)
		switch {
		case msg.Type == protocol.MessageResponse && msg.ID == 2:
			respMsg = msg
		case msg.Type == protocol.MessageEvent && msg.Channel == "room:"+created.ID && msg.Op == opEcho:
			evMsg = msg
		}
	}
	if resp := decodePayload[protocol.Kicked](t, respMsg); resp.Reason != "hello" || resp.RoomID != "x" {
		t.Fatalf("echo resp = %+v", resp)
	}
	if b := decodePayload[protocol.Kicked](t, evMsg); b.Reason != "seen" {
		t.Fatalf("broadcast = %+v", b)
	}

	// A client that is not in an echo room gets a 400.
	c2 := dialWS(t, srv)
	auth(t, c2, "bob")
	request(t, c2, 3, opEcho, &protocol.Kicked{Reason: "hello"})
	if msg := readUntilID(t, c2, 3); msg.Type != protocol.MessageError || msg.Code != 400 {
		t.Fatalf("not in room: %+v, want error 400", msg)
	}
}

func TestPingEcho(t *testing.T) {
	srv := newTestServer(t, nil)
	c := dialWS(t, srv)
	auth(t, c, "alice")

	sendMsg(t, c, &protocol.ClientMessage{Type: protocol.MessagePing, ID: 42})
	msg := readMsg(t, c)
	if msg.Type != protocol.MessagePing {
		t.Fatalf("echo type = %v, want MessagePing", msg.Type)
	}
	if msg.ID != 42 {
		t.Fatalf("echo id = %d, want 42", msg.ID)
	}

	// Pings are also answered before auth.
	c2 := dialWS(t, srv)
	sendMsg(t, c2, &protocol.ClientMessage{Type: protocol.MessagePing, ID: 7})
	msg = readMsg(t, c2)
	if msg.Type != protocol.MessagePing || msg.ID != 7 {
		t.Fatalf("pre-auth ping echo = %+v, want ping 7", msg)
	}
}

func TestSubscribeReceivesLobbyEvents(t *testing.T) {
	srv := newTestServer(t, nil)

	c1 := dialWS(t, srv)
	auth(t, c1, "alice")

	sendMsg(t, c1, &protocol.ClientMessage{Type: protocol.MessageSubscribe, Channel: "lobby"})

	c2 := dialWS(t, srv)
	auth(t, c2, "bob")
	created := createRoom(t, c2, 1, &protocol.CreateRoomRequest{Type: "duel", Name: "arena"})

	ev := readUntilEvent(t, c1, "lobby", uint16(protocol.EventRoomCreated))
	if info := decodePayload[protocol.RoomInfo](t, ev); info.ID != created.ID {
		t.Fatalf("room.created id = %q, want %q", info.ID, created.ID)
	}
}
