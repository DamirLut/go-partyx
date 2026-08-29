package gateway

import (
	"context"

	"github.com/damirlut/go-partyx/session"
)

// Authenticator verifies the connect token. ctx is bounded by the auth
// timeout and canceled when the connection closes, so token verification
// against an external platform can be canceled.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (*session.Session, error)
}
