package session

import "sync"

type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewStore() *Store {
	return &Store{
		sessions: make(map[string]*Session),
	}
}

func (s *Store) Set(session *Session) {
	s.mu.Lock()
	s.sessions[session.ID] = session
	s.mu.Unlock()
}

// Get looks up a session by ID, e.g. from business logic; the gateway
// itself does not use it.
func (s *Store) Get(id string) (*Session, bool) {
	s.mu.RLock()
	sess, ok := s.sessions[id]
	s.mu.RUnlock()
	return sess, ok
}

func (s *Store) Delete(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}
