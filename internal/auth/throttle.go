package auth

import (
	"sync"
	"time"
)

// maxThrottleKeys bounds the map so a spray of made-up emails cannot grow it
// without limit. When it is reached, expired entries go first; if they are all
// live, the throttle stops admitting new keys rather than forgetting old ones —
// failing closed is the right direction for a login limiter.
const maxThrottleKeys = 50_000

// throttle counts recent failures per key in a fixed window. It is in-process
// on purpose: a distributed limiter would need Redis on the login path, and an
// attacker spread across every instance still hits each one's limit.
//
// ponytail: fixed window, not sliding. A burst can straddle the boundary and
// get 2×max attempts. Swap in a token bucket if that ever matters.
type throttle struct {
	mu     sync.Mutex
	window time.Duration
	max    int
	now    func() time.Time
	seen   map[string]*attempts
}

type attempts struct {
	count int
	since time.Time
}

func newThrottle(max int, window time.Duration, now func() time.Time) *throttle {
	return &throttle{window: window, max: max, now: now, seen: map[string]*attempts{}}
}

func (t *throttle) blocked(key string) bool {
	if t.max <= 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.seen[key]
	return a != nil && a.count >= t.max && t.now().Sub(a.since) < t.window
}

func (t *throttle) fail(key string) {
	if t.max <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()

	a := t.seen[key]
	if a == nil {
		if len(t.seen) >= maxThrottleKeys {
			t.evictExpired(now)
		}
		if len(t.seen) >= maxThrottleKeys {
			return
		}
		t.seen[key] = &attempts{count: 1, since: now}
		return
	}
	if now.Sub(a.since) >= t.window {
		a.count, a.since = 0, now
	}
	a.count++
}

func (t *throttle) succeed(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.seen, key)
}

func (t *throttle) evictExpired(now time.Time) {
	for k, a := range t.seen {
		if now.Sub(a.since) >= t.window {
			delete(t.seen, k)
		}
	}
}
