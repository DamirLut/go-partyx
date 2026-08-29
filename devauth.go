package partyx

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/damirlut/go-partyx/session"
)

// DevAuth is a development Authenticator: the token is the user ID, every
// connection gets a fresh session. Not for production.
func DevAuth() Authenticator {
	return devAuth{}
}

type devAuth struct{}

func (devAuth) Authenticate(ctx context.Context, token string) (*session.Session, error) {
	if token == "" {
		return nil, errors.New("token required")
	}
	return session.New(uuid.New().String(), token), nil
}
