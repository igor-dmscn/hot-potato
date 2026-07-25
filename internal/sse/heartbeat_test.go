package sse

import (
	"sync"
	"testing"
	"time"
)

// The point of a shared clock is that one tick reaches every waiter. A channel
// send would reach exactly one of them.
func TestOneBeatWakesEveryWaiter(t *testing.T) {
	t.Parallel()
	h := NewHeartbeat(10 * time.Millisecond)
	defer h.Close()

	const waiters = 50
	var wg sync.WaitGroup
	woke := make(chan struct{}, waiters)
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-h.Beats():
				woke <- struct{}{}
			case <-time.After(2 * time.Second):
			}
		}()
	}
	wg.Wait()

	if len(woke) != waiters {
		t.Errorf("%d of %d waiters woke on one beat", len(woke), waiters)
	}
}

// Each beat installs a fresh channel, so a loop that fetches it again keeps
// beating.
func TestBeatsKeepComing(t *testing.T) {
	t.Parallel()
	h := NewHeartbeat(5 * time.Millisecond)
	defer h.Close()

	for i := range 3 {
		select {
		case <-h.Beats():
		case <-time.After(2 * time.Second):
			t.Fatalf("beat %d never arrived", i)
		}
	}
}

func TestNilHeartbeatNeverFires(t *testing.T) {
	t.Parallel()
	var h *Heartbeat

	select {
	case <-h.Beats():
		t.Error("a nil Heartbeat fired")
	case <-time.After(50 * time.Millisecond):
	}
	h.Close() // and is safe to close
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	h := NewHeartbeat(time.Hour)
	h.Close()
	h.Close()
}
