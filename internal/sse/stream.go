package sse

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"sync"
	"time"

	"hotpotato/internal/bus"
)

// Sink is what a Stream writes into: an io.Writer that can be flushed and given
// a per-write deadline. httpapi satisfies it with an http.ResponseController.
//
// The indirection is what keeps net/http out of this package and lets the pump
// be tested against a buffer with millisecond timings.
type Sink interface {
	io.Writer
	Flush() error
	SetWriteDeadline(time.Time) error
}

// Stream is one live SSE connection held by one User. A User may hold several
// at once, one per browser tab.
type Stream struct {
	ID     string
	UserID string

	ch     chan bus.Event
	closed chan struct{}
	once   sync.Once
}

func newStream(userID string, buffer int) *Stream {
	return &Stream{
		ID:     "s_" + rand.Text(),
		UserID: userID,
		ch:     make(chan bus.Event, buffer),
		closed: make(chan struct{}),
	}
}

// Send queues an event without blocking. False means the buffer was full, and
// the Stream has been closed as a result: a slow client must never stall the
// goroutine feeding the bus into every other Stream (ADR 0003). The client
// reconnects and gets a fresh snapshot.
func (s *Stream) Send(e bus.Event) bool {
	select {
	case <-s.closed:
		return false
	default:
	}
	select {
	case s.ch <- e:
		return true
	case <-s.closed:
		return false
	default:
		slog.Warn("stream overflowed; closing it", "stream", s.ID, "user", s.UserID, "event", e.Name)
		s.close()
		return false
	}
}

// Closed is signalled when the Stream is dropped, by overflow or by a drain.
func (s *Stream) Closed() <-chan struct{} { return s.closed }

func (s *Stream) close() { s.once.Do(func() { close(s.closed) }) }

// PumpOptions carries everything the pump needs. Every duration here is a
// config field, which is what lets these tests run in milliseconds.
type PumpOptions struct {
	// Beats is shared by every Stream on this instance. A nil one means no
	// heartbeat, which is what most tests want.
	Beats         *Heartbeat
	WriteDeadline time.Duration
	Retry         time.Duration
	// First is written before anything queued — the snapshot (ADR 0003).
	First []bus.Event
}

// Pump writes this Stream to out until the client goes away, the Stream is
// closed, or a write fails. It returns nil for the ordinary endings; only a
// write error is an error.
func (s *Stream) Pump(ctx context.Context, out Sink, o PumpOptions) error {
	write := func(fn func(io.Writer) error) error {
		// A per-write deadline is the substitute for Server.WriteTimeout, which
		// has to stay 0 or it would kill this connection on a schedule. A Sink
		// that cannot set deadlines says so by returning nil here.
		if err := out.SetWriteDeadline(time.Now().Add(o.WriteDeadline)); err != nil {
			return err
		}
		if err := fn(out); err != nil {
			return err
		}
		return out.Flush()
	}

	if err := write(func(w io.Writer) error { return WriteRetry(w, o.Retry) }); err != nil {
		return err
	}
	for _, e := range o.First {
		if err := write(func(w io.Writer) error { return WriteEvent(w, e) }); err != nil {
			return err
		}
	}

	for {
		select {
		case e := <-s.ch:
			if err := write(func(w io.Writer) error { return WriteEvent(w, e) }); err != nil {
				return err
			}
		// Fetched fresh each pass: every beat installs a new channel.
		case <-o.Beats.Beats():
			if err := write(func(w io.Writer) error { return WriteComment(w, "hb") }); err != nil {
				return err
			}
		case <-s.closed:
			// Flush what is already queued before returning: on a drain the
			// last event is server.draining, and the client needs it in order
			// to know to reconnect somewhere else.
			return s.flushQueued(write)
		case <-ctx.Done():
			return nil // the client vanished
		}
	}
}

func (s *Stream) flushQueued(write func(func(io.Writer) error) error) error {
	for {
		select {
		case e := <-s.ch:
			if err := write(func(w io.Writer) error { return WriteEvent(w, e) }); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}
