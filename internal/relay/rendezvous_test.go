package relay

import (
	"context"
	"errors"
	"testing"
	"time"
)

const id = "inst-a.deadbeef"

func TestParkOnceAndOnlyOnce(t *testing.T) {
	t.Parallel()
	rv := NewRendezvous()

	if err := rv.Park(id, NewSink()); err != nil {
		t.Fatalf("Park: %v", err)
	}
	if err := rv.Park(id, NewSink()); !errors.Is(err, ErrAlreadyParked) {
		t.Fatalf("a second GET = %v, want ErrAlreadyParked", err)
	}
	if rv.Parked() != 1 {
		t.Errorf("Parked = %d, want 1", rv.Parked())
	}

	rv.Unpark(id)
	if err := rv.Park(id, NewSink()); err != nil {
		t.Errorf("Park after Unpark: %v", err)
	}
}

// ADR 0002: a Sender arriving first is refused, not parked.
func TestHandoffBeforeAnybodyIsParked(t *testing.T) {
	t.Parallel()
	rv := NewRendezvous()

	if err := rv.Handoff(id, NewSource(nil, context.Background())); !errors.Is(err, ErrNotParked) {
		t.Fatalf("Handoff with nobody parked = %v, want ErrNotParked", err)
	}
}

func TestHandoffDeliversToTheParkedRecipient(t *testing.T) {
	t.Parallel()
	rv := NewRendezvous()
	sink := NewSink()
	if err := rv.Park(id, sink); err != nil {
		t.Fatal(err)
	}

	src := NewSource(nil, context.Background())
	if err := rv.Handoff(id, src); err != nil {
		t.Fatalf("Handoff: %v", err)
	}

	select {
	case got := <-sink.Ready():
		if got != src {
			t.Error("the Recipient was handed a different Source")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the Source never reached the Recipient")
	}

	// The Sender's handler blocks until the Recipient reports.
	done := make(chan Result, 1)
	go func() { done <- src.Wait() }()
	src.Report(Result{Bytes: 42, Complete: true})

	select {
	case res := <-done:
		if res.Bytes != 42 || !res.Complete {
			t.Errorf("result = %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait never returned")
	}
}

func TestASecondSenderIsRefusedWhileOneIsInFlight(t *testing.T) {
	t.Parallel()
	rv := NewRendezvous()
	if err := rv.Park(id, NewSink()); err != nil {
		t.Fatal(err)
	}

	if err := rv.Handoff(id, NewSource(nil, context.Background())); err != nil {
		t.Fatalf("first Handoff: %v", err)
	}
	if err := rv.Handoff(id, NewSource(nil, context.Background())); !errors.Is(err, ErrBusy) {
		t.Fatalf("second Handoff = %v, want ErrBusy", err)
	}
}

func TestCancelSignalsTheRelayAndIsSafeWhenNothingIsParked(t *testing.T) {
	t.Parallel()
	rv := NewRendezvous()
	sink := NewSink()
	if err := rv.Park(id, sink); err != nil {
		t.Fatal(err)
	}

	rv.Cancel(id)
	select {
	case <-sink.Canceled():
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel did not signal the relay")
	}

	// Twice, and for something that was never there.
	rv.Cancel(id)
	rv.Cancel("inst-a.nothing")
}

func TestReportIsIdempotent(t *testing.T) {
	t.Parallel()
	src := NewSource(nil, context.Background())

	// Both the copy and a deferred cleanup may want to report; the Sender's
	// handler is only waiting for one, and the second must not block.
	src.Report(Result{Bytes: 1})
	src.Report(Result{Bytes: 2})

	if res := src.Wait(); res.Bytes != 1 {
		t.Errorf("Wait = %+v, want the first report", res)
	}
}
