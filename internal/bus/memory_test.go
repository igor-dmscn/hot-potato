package bus

import (
	"context"
	"errors"
	"testing"
	"time"
)

// recv waits for one event with a deadline. No test in this tree sleeps.
func recv(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case e, ok := <-ch:
		if !ok {
			t.Fatal("channel closed while waiting for an event")
		}
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an event")
		return Event{}
	}
}

func TestPublishReachesEverySubscriber(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewMemory(Options{Buffer: 4})
	a, err := b.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := b.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	e, err := NewEvent("user.online", nil, map[string]string{"id": "u_1"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	if err := b.Publish(ctx, e); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	for i, ch := range []<-chan Event{a, c} {
		got := recv(t, ch)
		if got.Name != "user.online" {
			t.Errorf("subscriber %d got %q", i, got.Name)
		}
		if got.ID == 0 {
			t.Errorf("subscriber %d got ID 0; the bus assigns one", i)
		}
		if string(got.Data) != `{"id":"u_1"}` {
			t.Errorf("subscriber %d data = %s", i, got.Data)
		}
	}
}

func TestPublishAssignsMonotonicIDs(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewMemory(Options{Buffer: 4})
	ch, _ := b.Subscribe(ctx)
	for range 3 {
		e, _ := NewEvent("x", nil, nil)
		b.Publish(ctx, e)
	}
	var last uint64
	for range 3 {
		got := recv(t, ch)
		if got.ID <= last {
			t.Fatalf("ID %d did not advance past %d", got.ID, last)
		}
		last = got.ID
	}
}

// The bus must never block on a slow subscriber: it drops instead. A dropped
// event costs a reconnect and a fresh snapshot, which is exactly what ADR 0003
// buys.
func TestSlowSubscriberDoesNotBlockPublish(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewMemory(Options{Buffer: 1})
	slow, _ := b.Subscribe(ctx)
	quick, _ := b.Subscribe(ctx)

	done := make(chan struct{})
	go func() {
		for range 50 {
			e, _ := NewEvent("flood", nil, nil)
			b.Publish(ctx, e)
			<-quick // keep this one drained so only `slow` falls behind
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a subscriber that never reads")
	}
	if len(slow) != 1 {
		t.Errorf("slow subscriber holds %d events, want its buffer of 1", len(slow))
	}
}

func TestSubscribeUnsubscribesOnContext(t *testing.T) {
	t.Parallel()
	root := context.Background()
	b := NewMemory(Options{Buffer: 1})

	ctx, cancel := context.WithCancel(root)
	ch, _ := b.Subscribe(ctx)
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel yielded an event after its context was cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel was not closed when its context was cancelled")
	}

	// And publishing afterwards must not panic on a closed channel.
	e, _ := NewEvent("after", nil, nil)
	if err := b.Publish(root, e); err != nil {
		t.Errorf("Publish after an unsubscribe: %v", err)
	}
}

func TestClosedBusRefusesWork(t *testing.T) {
	t.Parallel()
	b := NewMemory(Options{})
	b.Close()

	if err := b.Publish(context.Background(), Event{Name: "x"}); !errors.Is(err, ErrClosed) {
		t.Errorf("Publish on a closed bus = %v, want ErrClosed", err)
	}
	if _, err := b.Subscribe(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Subscribe on a closed bus = %v, want ErrClosed", err)
	}
}

func TestForUser(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		audience []string
		user     string
		want     bool
	}{
		"empty audience is everyone": {nil, "u_1", true},
		"addressed":                  {[]string{"u_1", "u_2"}, "u_2", true},
		"not addressed":              {[]string{"u_1"}, "u_2", false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := (Event{Audience: tc.audience}).ForUser(tc.user); got != tc.want {
				t.Errorf("ForUser(%q) = %v, want %v", tc.user, got, tc.want)
			}
		})
	}
}
