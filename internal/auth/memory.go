package auth

import (
	"context"
	"strings"
	"sync"
)

// Memory is an in-memory UserStore and SessionStore.
//
// It is the second implementation that earns the interfaces above: with it,
// the Service, the middleware and every HTTP handler are testable without a
// database, and only internal/postgres needs docker.
type Memory struct {
	mu       sync.Mutex
	byEmail  map[string]User
	byID     map[string]User
	sessions map[string]Session
}

func NewMemory() *Memory {
	return &Memory{
		byEmail:  map[string]User{},
		byID:     map[string]User{},
		sessions: map[string]Session{},
	}
}

func (m *Memory) Create(_ context.Context, u User) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	email := strings.ToLower(u.Email)
	if _, taken := m.byEmail[email]; taken {
		return ErrEmailTaken
	}
	m.byEmail[email], m.byID[u.ID] = u, u
	return nil
}

func (m *Memory) ByEmail(_ context.Context, email string) (User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.byEmail[strings.ToLower(email)]
	if !ok {
		return User{}, ErrNotFound
	}
	return u, nil
}

func (m *Memory) CreateSession(_ context.Context, s Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[s.ID] = s
	return nil
}

func (m *Memory) Get(_ context.Context, id string) (Session, User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return Session{}, User{}, ErrNotFound
	}
	u, ok := m.byID[s.UserID]
	if !ok {
		return Session{}, User{}, ErrNotFound
	}
	return s, u, nil
}

func (m *Memory) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, id)
	return nil
}

// Sessions adapts Memory to SessionStore, whose Create has the same name as
// UserStore's but a different argument. One type cannot have both methods.
func (m *Memory) Sessions() SessionStore { return memSessions{m} }

type memSessions struct{ m *Memory }

func (s memSessions) Create(ctx context.Context, x Session) error { return s.m.CreateSession(ctx, x) }
func (s memSessions) Get(ctx context.Context, id string) (Session, User, error) {
	return s.m.Get(ctx, id)
}
func (s memSessions) Delete(ctx context.Context, id string) error { return s.m.Delete(ctx, id) }
