package sse

import (
	"sync"
	"time"
)

// Heartbeat is one clock for every Stream.
//
// One goroutine per Stream is fine at ten thousand. One *timer* per Stream is
// not: ten thousand runtime timers all firing on the same interval is a heap
// operation per beat per Stream, for a job every Stream wants done at the same
// moment anyway.
//
// The broadcast is a channel that gets closed and replaced. Closing wakes every
// waiter at once, which is the only way one signal reaches N selects.
type Heartbeat struct {
	mu   sync.Mutex
	beat chan struct{}
	stop chan struct{}
	done chan struct{}
}

func NewHeartbeat(every time.Duration) *Heartbeat {
	h := &Heartbeat{
		beat: make(chan struct{}),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		defer close(h.done)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-ticker.C:
				h.mu.Lock()
				closed := h.beat
				h.beat = make(chan struct{})
				h.mu.Unlock()
				close(closed)
			}
		}
	}()
	return h
}

// Beats returns a channel closed at the next beat. Call it fresh on every pass
// of a select loop: each beat installs a new one.
func (h *Heartbeat) Beats() <-chan struct{} {
	if h == nil {
		return nil // a nil channel in a select never fires
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.beat
}

func (h *Heartbeat) Close() {
	if h == nil {
		return
	}
	select {
	case <-h.stop:
		return // already closed
	default:
	}
	close(h.stop)
	<-h.done
}
