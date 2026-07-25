package sse

import (
	"context"
	"testing"
	"time"

	"hotpotato/internal/bus"
)

func drain(t *testing.T, s *Stream) []bus.Event {
	t.Helper()
	var got []bus.Event
	for {
		select {
		case e := <-s.ch:
			got = append(got, e)
		case <-time.After(50 * time.Millisecond):
			return got
		}
	}
}

func TestOpenReportsTheFirstStreamAndCloseTheLast(t *testing.T) {
	t.Parallel()
	r := New(Options{Buffer: 4})

	first, isFirst := r.Open("u_1")
	if !isFirst {
		t.Error("the first Stream did not report itself as first")
	}
	second, isFirst := r.Open("u_1")
	if isFirst {
		t.Error("the second Stream for one User reported itself as first")
	}
	if first.ID == second.ID {
		t.Error("two Streams share an ID")
	}
	if r.Count() != 2 {
		t.Errorf("Count = %d, want 2", r.Count())
	}

	if r.Close(first) {
		t.Error("closing one of two Streams reported itself as last")
	}
	if !r.Close(second) {
		t.Error("closing the last Stream did not report itself as last")
	}
	if r.Count() != 0 {
		t.Errorf("Count = %d after closing everything, want 0", r.Count())
	}
}

func TestDeliverReachesEveryTabOfOneUser(t *testing.T) {
	t.Parallel()
	r := New(Options{Buffer: 4})
	tabA, _ := r.Open("u_1")
	tabB, _ := r.Open("u_1")

	r.Deliver(bus.Event{ID: 1, Name: "user.online", Audience: []string{"u_1"}, Data: []byte(`{}`)})

	for name, s := range map[string]*Stream{"tab A": tabA, "tab B": tabB} {
		if got := drain(t, s); len(got) != 1 || got[0].Name != "user.online" {
			t.Errorf("%s got %+v, want one user.online", name, got)
		}
	}
}

func TestDeliverFiltersByAudience(t *testing.T) {
	t.Parallel()
	r := New(Options{Buffer: 4})
	mine, _ := r.Open("u_1")
	theirs, _ := r.Open("u_2")

	r.Deliver(bus.Event{ID: 1, Name: "transfer.offered", Audience: []string{"u_1"}, Data: []byte(`{}`)})

	if got := drain(t, mine); len(got) != 1 {
		t.Errorf("the addressed User got %d events, want 1", len(got))
	}
	if got := drain(t, theirs); len(got) != 0 {
		t.Errorf("an unaddressed User got %+v, want nothing", got)
	}
}

// A slow client must not stall the goroutine feeding every other Stream.
func TestDeliverDropsAnOverflowingStreamWithoutBlocking(t *testing.T) {
	t.Parallel()
	r := New(Options{Buffer: 1})
	slow, _ := r.Open("u_1")
	quick, _ := r.Open("u_2")

	done := make(chan struct{})
	go func() {
		for i := range 10 {
			r.Deliver(bus.Event{ID: uint64(i + 1), Name: "flood", Audience: []string{bus.Everyone}, Data: []byte(`{}`)})
			<-quick.ch // keep this one drained
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver blocked on a Stream nobody is reading")
	}
	select {
	case <-slow.Closed():
	case <-time.After(time.Second):
		t.Error("the overflowing Stream is still open")
	}
}

func TestDrainAnnouncesThenClosesAndRefusesNewStreams(t *testing.T) {
	t.Parallel()
	r := New(Options{Buffer: 4})
	s, _ := r.Open("u_1")

	r.Drain(context.Background())

	got := drain(t, s)
	if len(got) != 1 || got[0].Name != EventDraining {
		t.Errorf("Streams received %+v, want one %s", got, EventDraining)
	}
	select {
	case <-s.Closed():
	case <-time.After(time.Second):
		t.Error("Drain left a Stream open, so Shutdown would hang (Go issue #41344)")
	}
	if next, _ := r.Open("u_2"); next != nil {
		t.Error("a draining Registry handed out a new Stream")
	}
}
