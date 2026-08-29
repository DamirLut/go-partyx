package partyx

import (
	"errors"

	"github.com/google/uuid"

	"github.com/damirlut/go-partyx/session"
)

// DevAuth returns an Authenticator for development: the token is used
// directly as the user ID and every connection gets a fresh session.
// Replace with a real implementation (JWT, OAuth, ...) in production.
func DevAuth() Authenticator {
	return devAuth{}
}

type devAuth struct{}

func (devAuth) Authenticate(token string) (*session.Session, error) {
	if token == "" {
		return nil, errors.New("token required")
	}
	return session.New(uuid.New().String(), token), nil
}
