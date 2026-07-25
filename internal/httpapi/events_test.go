package httpapi

import (
	"net/http"
	"runtime"
	"testing"
	"time"

	"hotpotato/internal/presence"
	"hotpotato/internal/sse"
)

func TestEventsRequiresASession(t *testing.T) {
	t.Parallel()
	x := newHarness(t)

	if rec := x.do(t, "GET", "/events", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /events without a cookie = %d, want 401", rec.Code)
	}
}

// ADR 0003: the first thing on a Stream is the whole picture.
func TestEventsOpensWithASnapshot(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	ana := x.signup(t, "ana@example.com", "ana")

	s := x.openStream(t, ana)
	f := s.next(t)
	if f.Event != EventSnapshot {
		t.Fatalf("first frame = %q, want %q", f.Event, EventSnapshot)
	}
	snap := decodeSnapshot(t, f)
	if snap.Self.DisplayName != "ana" {
		t.Errorf("snapshot self = %+v", snap.Self)
	}
	if snap.StreamID == "" || snap.Instance != "inst-test" {
		t.Errorf("snapshot = %+v, want a stream ID and the instance", snap)
	}
	if len(snap.Users) != 1 || snap.Users[0].DisplayName != "ana" {
		t.Errorf("snapshot users = %+v, want the opener to be in their own list", snap.Users)
	}
	if snap.Transfers == nil {
		t.Error("snapshot transfers is null, want an empty array")
	}
}

func TestTwoUsersSeeEachOtherArrive(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	ana := x.signup(t, "ana@example.com", "ana")
	bea := x.signup(t, "bea@example.com", "bea")

	anaStream := x.openStream(t, ana)
	anaStream.await(t, EventSnapshot)

	beaStream := x.openStream(t, bea)
	beaSnap := decodeSnapshot(t, beaStream.await(t, EventSnapshot))
	if len(beaSnap.Users) != 2 {
		t.Errorf("bea's snapshot has %d users, want ana and bea", len(beaSnap.Users))
	}

	anaStream.awaitUser(t, presence.EventOnline, "bea")
}

func TestEveryTabOfOneUserReceivesTheBroadcast(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	ana := x.signup(t, "ana@example.com", "ana")
	bea := x.signup(t, "bea@example.com", "bea")

	tabs := []*stream{x.openStream(t, ana), x.openStream(t, ana)}
	for _, s := range tabs {
		s.await(t, EventSnapshot)
	}

	x.openStream(t, bea).await(t, EventSnapshot)

	for _, s := range tabs {
		s.awaitUser(t, presence.EventOnline, "bea")
	}
}

// A refresh drops a Stream and opens another. The grace window exists so that
// does not make the User flicker out of everyone's list.
func TestRefreshWithinTheGraceWindowEmitsNoOffline(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) { o.grace = 500 * time.Millisecond })
	ana := x.signup(t, "ana@example.com", "ana")
	bea := x.signup(t, "bea@example.com", "bea")

	watcher := x.openStream(t, ana)
	watcher.await(t, EventSnapshot)

	first := x.openStream(t, bea)
	first.await(t, EventSnapshot)
	watcher.awaitUser(t, presence.EventOnline, "bea")

	// The refresh: close and immediately reopen, well inside 500ms.
	first.close()
	second := x.openStream(t, bea)
	second.await(t, EventSnapshot)

	watcher.quiet(t, presence.EventOffline, 700*time.Millisecond)

	// Leaving for good does announce it.
	second.close()
	watcher.await(t, presence.EventOffline)
}

func TestDrainingRefusesNewStreamsAndClosesLiveOnes(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	ana := x.signup(t, "ana@example.com", "ana")

	s := x.openStream(t, ana)
	s.await(t, EventSnapshot)

	x.draining.Store(true)
	x.streams.Drain(t.Context())

	if f := s.await(t, sse.EventDraining); f.Event != sse.EventDraining {
		t.Fatalf("live Stream got %q, want %q", f.Event, sse.EventDraining)
	}
	if rec := x.do(t, "GET", "/events", "", ana); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("GET /events while draining = %d, want 503", rec.Code)
	}
}

// One goroutine per Stream is fine; one that outlives its Stream is a leak that
// only shows up at ten thousand of them.
func TestNoGoroutineOrStreamLeakAfterDisconnect(t *testing.T) {
	x := newHarness(t)
	ana := x.signup(t, "ana@example.com", "ana")
	srv := x.server(t)

	baseline := runtime.NumGoroutine()

	for range 5 {
		s := x.openStream(t, ana)
		s.await(t, EventSnapshot)
		s.close()
	}

	// Close blocks until every outstanding handler has returned, which is what
	// makes this assertion deterministic instead of a poll with a sleep.
	srv.Close()
	x.srv = nil

	if n := x.streams.Count(); n != 0 {
		t.Errorf("Registry still holds %d Streams", n)
	}
	// Slack of two: the runtime's own bookkeeping goroutines come and go.
	if after := runtime.NumGoroutine(); after > baseline+2 {
		t.Errorf("goroutines went from %d to %d", baseline, after)
	}
}

func TestHeartbeatKeepsTheStreamAlive(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) { o.heartbeat = 20 * time.Millisecond })
	ana := x.signup(t, "ana@example.com", "ana")

	s := x.openStream(t, ana)
	s.await(t, EventSnapshot)

	deadline := time.After(3 * time.Second)
	for {
		select {
		case f, ok := <-s.frames:
			if !ok {
				t.Fatal("the stream closed before any heartbeat")
			}
			if f.Comment == "hb" {
				return
			}
		case <-deadline:
			t.Fatal("no heartbeat comment within three seconds")
		}
	}
}
