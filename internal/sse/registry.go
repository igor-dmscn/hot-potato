package sse

import (
	"context"
	"log/slog"
	"sync"

	"hotpotato/internal/bus"
)

// EventDraining tells every Stream that this instance is going away and they
// should reconnect — somewhere else, if there is somewhere else.
const EventDraining = "server.draining"

type Options struct {
	// Buffer is how many events a Stream may fall behind by before it is
	// dropped. DESIGN §9 says 32.
	Buffer int
}

// Registry holds every Stream this instance is serving, indexed by User so a
// broadcast to one User reaches all their tabs.
type Registry struct {
	mu      sync.RWMutex
	byUser  map[string]map[string]*Stream
	buffer  int
	drained bool
}

func New(o Options) *Registry {
	if o.Buffer <= 0 {
		o.Buffer = 32
	}
	return &Registry{byUser: map[string]map[string]*Stream{}, buffer: o.Buffer}
}

// Open registers a Stream for userID. The second result reports whether this is
// that User's first Stream, which is what makes them online.
//
// It returns nil once the instance is draining: a new Stream on a process that
// is shutting down would be closed a moment later.
//
// The plan sketched this as Open(userID) *Stream with presence worked out by
// the caller. It reports "first" instead because the count has to be read under
// the same lock that inserts, or two tabs opening at once both believe they are
// first and the User is announced online twice.
func (r *Registry) Open(userID string) (*Stream, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.drained {
		return nil, false
	}
	s := newStream(userID, r.buffer)
	byID := r.byUser[userID]
	if byID == nil {
		byID = map[string]*Stream{}
		r.byUser[userID] = byID
	}
	byID[s.ID] = s
	return s, len(byID) == 1
}

// Close deregisters a Stream. The second result reports whether it was that
// User's last, which starts the presence grace window.
func (r *Registry) Close(s *Stream) bool {
	if s == nil {
		return false
	}
	s.close()

	r.mu.Lock()
	defer r.mu.Unlock()
	byID := r.byUser[s.UserID]
	delete(byID, s.ID)
	if len(byID) == 0 {
		delete(r.byUser, s.UserID)
		return true
	}
	return false
}

// Deliver fans one event out to the local Streams it is addressed to. It is
// called from the single goroutine that reads the bus, and must never block:
// Send drops an overflowing Stream instead.
func (r *Registry) Deliver(e bus.Event) {
	r.mu.RLock()
	// Collect first: Send can close a Stream, and its handler will then call
	// Close, which wants the write lock.
	var targets []*Stream
	for userID, byID := range r.byUser {
		if !e.ForUser(userID) {
			continue
		}
		for _, s := range byID {
			targets = append(targets, s)
		}
	}
	r.mu.RUnlock()

	for _, s := range targets {
		s.Send(e)
	}
}

// Owns reports whether streamID is one of userID's Streams. An accept carries
// the Stream that made it, and that value is echoed to the User's other tabs.
func (r *Registry) Owns(userID, streamID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.byUser[userID][streamID]
	return ok
}

// Count is the number of live Streams, for /metrics.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, byID := range r.byUser {
		n += len(byID)
	}
	return n
}

// Drain announces server.draining to every Stream and closes them all.
//
// It must run before Server.Shutdown, which otherwise waits forever on an open
// SSE handler (Go issue #41344). Each Stream's pump flushes what is queued —
// including the announcement — before its handler returns.
func (r *Registry) Drain(_ context.Context) {
	e, err := bus.NewEvent(EventDraining, nil, struct{}{})
	if err != nil {
		slog.Error("build draining event", "err", err)
	}

	r.mu.Lock()
	r.drained = true
	var all []*Stream
	for _, byID := range r.byUser {
		for _, s := range byID {
			all = append(all, s)
		}
	}
	r.mu.Unlock()

	slog.Info("draining streams", "streams", len(all))
	for _, s := range all {
		s.Send(e)
		s.close()
	}
}
