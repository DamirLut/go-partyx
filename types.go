package partyx

import (
	"github.com/damirlut/go-partyx/command"
	"github.com/damirlut/go-partyx/gateway"
	"github.com/damirlut/go-partyx/protocol"
	"github.com/damirlut/go-partyx/session"
)

type (
	// Context is the per-request context passed to global command handlers.
	Context = command.Context
	// Error is an RPC error carrying a wire code.
	Error = protocol.Error
	// Authenticator validates the token sent as the first client message.
	Authenticator = gateway.Authenticator
	// Session is an authenticated user session.
	Session = session.Session
)

// NewError returns an RPC error with the given wire code
// (400/401/404/409/410/500).
var NewError = protocol.NewError
