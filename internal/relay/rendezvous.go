// Package relay moves Payload bytes from the Sender's request into the
// Recipient's response as they arrive, holding nothing but a copy buffer.
//
// Almost all of it is byte plumbing over io.Reader and io.Writer. net/http
// appears in exactly one signature — Response.ApplyTo — because the response
// shape follows from the declared Payload, and everything else can be tested
// with a bytes.Buffer.
package relay

import (
	"context"
	"errors"
	"mime/multipart"
	"sync"

	"hotpotato/internal/transfer"
)

var (
	// ErrAlreadyParked is a second GET for one Transfer.
	ErrAlreadyParked = errors.New("recipient is already attached")
	// ErrNotParked is a Sender arriving before the Recipient (ADR 0002).
	ErrNotParked = errors.New("recipient is not attached")
	// ErrBusy is a second POST while the first is still streaming.
	ErrBusy = errors.New("a chunk is already in flight")
)

// Source is what the Sender's handler registers: a live multipart body and the
// position in the Payload the Sender believes it is starting from.
type Source struct {
	Parts *multipart.Reader
	Ctx   context.Context

	// EntryIndex and EntryOffset are the resume position claimed by the Sender.
	// Both zero for a Transfer that has not been interrupted.
	EntryIndex  int
	EntryOffset int64

	done chan Result
	once sync.Once
}

// Result is what the Recipient's goroutine tells the Sender's handler once it
// has finished with a Source.
type Result struct {
	// Bytes relayed from this Source.
	Bytes int64
	// Err is nil when the Source was consumed to its end.
	Err error
	// Complete reports whether the Payload is now whole.
	Complete bool
	// Expected* are the position the Sender should have started from, filled in
	// when Err is ErrOffsetMismatch.
	ExpectedIndex  int
	ExpectedOffset int64
}

func NewSource(parts *multipart.Reader, ctx context.Context) *Source {
	return &Source{Parts: parts, Ctx: ctx, done: make(chan Result, 1)}
}

// Report hands the outcome back to the Sender's handler. Safe to call once.
func (s *Source) Report(r Result) { s.once.Do(func() { s.done <- r }) }

// Wait blocks until the Recipient reports. The Sender's handler must not return
// before this: the bytes are being written to a ResponseWriter that belongs to
// the other handler, and it needs to still be there.
func (s *Source) Wait() Result { return <-s.done }

// Sink is what the Recipient's handler registers while it waits for a Sender.
type Sink struct {
	ready    chan *Source
	canceled chan struct{}
	once     sync.Once
}

func NewSink() *Sink {
	return &Sink{ready: make(chan *Source, 1), canceled: make(chan struct{})}
}

// Ready yields each Source handed to this Sink.
func (s *Sink) Ready() <-chan *Source { return s.ready }

// Canceled is closed when the Transfer is cancelled, so a relay already in
// flight can be abandoned at the next buffer boundary instead of only when the
// Payload runs out.
func (s *Sink) Canceled() <-chan struct{} { return s.canceled }

func (s *Sink) cancel() { s.once.Do(func() { close(s.canceled) }) }

// Rendezvous is where a waiting Recipient and an arriving Sender meet. It holds
// live readers and writers, which is why a Transfer cannot move between
// instances (ADR 0007).
type Rendezvous struct {
	mu    sync.Mutex
	sinks map[transfer.ID]*Sink
}

func NewRendezvous() *Rendezvous {
	return &Rendezvous{sinks: map[transfer.ID]*Sink{}}
}

// Park registers a waiting Recipient.
func (rv *Rendezvous) Park(id transfer.ID, s *Sink) error {
	rv.mu.Lock()
	defer rv.mu.Unlock()
	if _, taken := rv.sinks[id]; taken {
		return ErrAlreadyParked
	}
	rv.sinks[id] = s
	return nil
}

// Unpark removes a Recipient. It belongs in a defer in the Recipient's handler.
func (rv *Rendezvous) Unpark(id transfer.ID) {
	rv.mu.Lock()
	defer rv.mu.Unlock()
	delete(rv.sinks, id)
}

// Handoff gives a Source to the parked Recipient. It does not block: if nobody
// is parked the Sender is early, and if the Sink is already holding a Source
// then two POSTs are racing.
func (rv *Rendezvous) Handoff(id transfer.ID, src *Source) error {
	rv.mu.Lock()
	sink, parked := rv.sinks[id]
	rv.mu.Unlock()

	if !parked {
		return ErrNotParked
	}
	select {
	case sink.ready <- src:
		return nil
	default:
		return ErrBusy
	}
}

// Cancel abandons a relay in flight. It is a no-op when nothing is parked,
// which is the common case: most cancellations happen before any bytes move.
func (rv *Rendezvous) Cancel(id transfer.ID) {
	rv.mu.Lock()
	sink := rv.sinks[id]
	rv.mu.Unlock()
	if sink != nil {
		sink.cancel()
	}
}

// Parked is the number of waiting Recipients, for /metrics.
func (rv *Rendezvous) Parked() int {
	rv.mu.Lock()
	defer rv.mu.Unlock()
	return len(rv.sinks)
}
