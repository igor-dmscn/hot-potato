package presence

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Memory is presence for a single instance.
type Memory struct {
	mu    sync.Mutex
	users map[string]*entry
	o     Options
}

type entry struct {
	u User
	// grace is non-nil while the User holds no Streams and has not yet been
	// announced offline.
	grace *time.Timer
}

func NewMemory(o Options) *Memory {
	if o.Announce == nil {
		o.Announce = func(string, User) {}
	}
	return &Memory{users: map[string]*entry{}, o: o}
}

func (m *Memory) Online(_ context.Context, u User) error {
	m.mu.Lock()
	if e, ok := m.users[u.ID]; ok {
		// Already present. If a grace window was running, this is a refresh
		// landing inside it: cancel and say nothing.
		if e.grace != nil {
			e.grace.Stop()
			e.grace = nil
		}
		e.u = u
		m.mu.Unlock()
		return nil
	}
	m.users[u.ID] = &entry{u: u}
	m.mu.Unlock()

	// Announce outside the lock: an announcement reaches the bus, which reaches
	// the Stream registry, and holding this lock through all of that is how
	// deadlocks are made.
	m.o.Announce(EventOnline, u)
	return nil
}

func (m *Memory) Offline(_ context.Context, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.users[userID]
	if !ok || e.grace != nil {
		return nil
	}
	e.grace = time.AfterFunc(m.o.Grace, func() { m.expire(userID) })
	return nil
}

// expire runs when a grace window ends. It checks the window is still the live
// one, because Online may have cancelled it between the timer firing and this
// taking the lock.
func (m *Memory) expire(userID string) {
	m.mu.Lock()
	e, ok := m.users[userID]
	if !ok || e.grace == nil {
		m.mu.Unlock()
		return
	}
	delete(m.users, userID)
	m.mu.Unlock()

	m.o.Announce(EventOffline, e.u)
}

func (m *Memory) List(_ context.Context) ([]User, error) {
	m.mu.Lock()
	out := make([]User, 0, len(m.users))
	for _, e := range m.users {
		out = append(out, e.u)
	}
	m.mu.Unlock()

	// Stable order, so a snapshot does not reshuffle the list on every reload.
	sort.Slice(out, func(i, j int) bool { return out[i].DisplayName < out[j].DisplayName })
	return out, nil
}

func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.users {
		if e.grace != nil {
			e.grace.Stop()
		}
	}
	return nil
}
