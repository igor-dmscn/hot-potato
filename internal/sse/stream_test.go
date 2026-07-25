package sse

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"hotpotato/internal/bus"
)

// sink is a Sink over a buffer, with a channel that reports every flush so a
// test can wait for output instead of sleeping.
type sink struct {
	mu       sync.Mutex
	b        strings.Builder
	flushed  chan struct{}
	deadline time.Time
	failNext error
}

func newSink() *sink { return &sink{flushed: make(chan struct{}, 64)} }

func (s *sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext != nil {
		return 0, s.failNext
	}
	return s.b.Write(p)
}

func (s *sink) Flush() error {
	select {
	case s.flushed <- struct{}{}:
	default:
	}
	return nil
}

func (s *sink) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadline = t
	return nil
}

func (s *sink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *sink) waitFlushes(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-s.flushed:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for flush %d", n)
		}
	}
}

func opts() PumpOptions {
	return PumpOptions{
		Heartbeat:     10 * time.Millisecond,
		WriteDeadline: time.Second,
		Retry:         3 * time.Second,
	}
}

func TestPumpWritesRetryThenSnapshotThenEvents(t *testing.T) {
	t.Parallel()

	s := newStream("u_1", 4)
	out := newSink()
	o := opts()
	o.Heartbeat = time.Hour // no heartbeat noise in this one
	snap, _ := bus.NewEvent("snapshot", nil, map[string]int{"users": 1})
	o.First = []bus.Event{snap}

	done := make(chan error, 1)
	go func() { done <- s.Pump(context.Background(), out, o) }()

	out.waitFlushes(t, 2) // retry, snapshot
	s.Send(bus.Event{ID: 9, Name: "user.online", Data: []byte(`{}`)})
	out.waitFlushes(t, 1)

	s.close()
	if err := <-done; err != nil {
		t.Fatalf("Pump: %v", err)
	}

	got := out.String()
	wantOrder := []string{"retry: 3000", "event: snapshot", "event: user.online"}
	at := -1
	for _, want := range wantOrder {
		i := strings.Index(got, want)
		if i <= at {
			t.Fatalf("output is out of order at %q:\n%s", want, got)
		}
		at = i
	}
}

func TestPumpHeartbeats(t *testing.T) {
	t.Parallel()

	s := newStream("u_1", 4)
	out := newSink()
	done := make(chan error, 1)
	go func() { done <- s.Pump(context.Background(), out, opts()) }()

	out.waitFlushes(t, 3) // retry, then at least two heartbeats
	s.close()
	<-done

	if !strings.Contains(out.String(), ":hb\n\n") {
		t.Errorf("no heartbeat comment in:\n%q", out.String())
	}
	if out.deadline.IsZero() {
		t.Error("no write deadline was ever set")
	}
}

// On a drain the last thing queued is server.draining, and the client needs it.
func TestPumpFlushesQueuedEventsAfterClose(t *testing.T) {
	t.Parallel()

	s := newStream("u_1", 4)
	out := newSink()
	o := opts()
	o.Heartbeat = time.Hour

	// Queue before the pump starts, then close: the pump must still drain.
	s.Send(bus.Event{ID: 1, Name: EventDraining, Data: []byte(`{}`)})
	s.close()

	if err := s.Pump(context.Background(), out, o); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	if !strings.Contains(out.String(), "event: "+EventDraining) {
		t.Errorf("the draining event was dropped:\n%q", out.String())
	}
}

func TestPumpReturnsWhenClientVanishes(t *testing.T) {
	t.Parallel()

	s := newStream("u_1", 4)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Pump(ctx, newSink(), opts()) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Pump: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pump ignored a cancelled request context")
	}
}

func TestSendClosesAnOverflowingStream(t *testing.T) {
	t.Parallel()

	s := newStream("u_1", 2)
	e := bus.Event{Name: "x", Data: []byte(`{}`)}

	if !s.Send(e) || !s.Send(e) {
		t.Fatal("Send refused an event that fits in the buffer")
	}
	if s.Send(e) {
		t.Fatal("Send accepted a third event into a buffer of two")
	}
	select {
	case <-s.Closed():
	case <-time.After(time.Second):
		t.Fatal("an overflowing Stream was not closed")
	}
	if s.Send(e) {
		t.Error("Send accepted an event on a closed Stream")
	}
}
