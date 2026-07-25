package bus

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
)

// ErrClosed is returned by a closed bus.
var ErrClosed = errors.New("bus is closed")

// Options configures an implementation. Buffer is how many events a subscriber
// may fall behind by before its events start being dropped.
type Options struct {
	Buffer int
}

// Memory is the single-instance bus: a fan-out over channels.
type Memory struct {
	mu     sync.Mutex
	subs   map[int]chan Event
	nextID int
	buffer int
	closed bool
	seq    atomic.Uint64
}

func NewMemory(o Options) *Memory {
	if o.Buffer <= 0 {
		o.Buffer = 256
	}
	return &Memory{subs: map[int]chan Event{}, buffer: o.Buffer}
}

// Publish fans out without blocking. A subscriber that has fallen behind loses
// events rather than stalling the publisher — that is what at-most-once means,
// and the SSE layer above turns a lost event into a reconnect and a fresh
// snapshot.
func (m *Memory) Publish(_ context.Context, e Event) error {
	if e.ID == 0 {
		e.ID = m.seq.Add(1)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	for id, ch := range m.subs {
		select {
		case ch <- e:
		default:
			slog.Warn("bus subscriber is behind; dropping event", "sub", id, "event", e.Name)
		}
	}
	return nil
}

func (m *Memory) Subscribe(ctx context.Context) (<-chan Event, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	id := m.nextID
	m.nextID++
	ch := make(chan Event, m.buffer)
	m.subs[id] = ch
	m.mu.Unlock()

	// Unsubscribe on ctx, or leak a channel per dead subscriber. Unregistering
	// under the same lock Publish holds is what makes closing ch safe.
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		delete(m.subs, id)
		m.mu.Unlock()
		close(ch)
	}()
	return ch, nil
}

func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
