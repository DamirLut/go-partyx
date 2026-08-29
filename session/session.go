package session

import "sync"

type Session struct {
	ID       string
	UserID   string
	Metadata map[string]interface{}
	mu       sync.RWMutex
}

func New(id, userID string) *Session {
	return &Session{
		ID:       id,
		UserID:   userID,
		Metadata: make(map[string]interface{}),
	}
}

func (s *Session) Set(key string, value interface{}) {
	s.mu.Lock()
	s.Metadata[key] = value
	s.mu.Unlock()
}

func (s *Session) Get(key string) (interface{}, bool) {
	s.mu.RLock()
	v, ok := s.Metadata[key]
	s.mu.RUnlock()
	return v, ok
}
