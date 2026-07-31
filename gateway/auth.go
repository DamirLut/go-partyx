package gateway

import (
	"github.com/damirlut/go-partyx/session"
)

type Authenticator interface {
	Authenticate(token string) (*session.Session, error)
}
