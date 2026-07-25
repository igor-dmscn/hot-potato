package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"hotpotato/internal/auth"
	"hotpotato/internal/relay"
	"hotpotato/internal/transfer"
)

// errRendezvousTimeout is a Sender who never arrived.
var errRendezvousTimeout = errors.New("the sender did not arrive")

// download is GET /d/{id}: the Recipient's half of the data plane.
//
// The handler parks, and when a Sender arrives the copy runs on *this*
// goroutine. That is not a style choice: a ResponseWriter is invalid the instant
// its handler returns, so the only way to write a Payload into this response is
// to still be inside this function while it happens.
func (a *api) download(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())
	id := transfer.ID(r.PathValue("id"))

	if a.draining.Load() {
		writeError(w, http.StatusServiceUnavailable, codeDraining,
			"this instance is shutting down; retry")
		return
	}

	t, err := a.transfers.Mutate(id, func(t *transfer.Transfer) error {
		// The Recipient's identity is verified here, on every request: a
		// Transfer ID is not an authorization token.
		if u.ID != t.Recipient {
			return transfer.ErrForbidden
		}
		return t.AttachRecipient(a.now())
	})
	if err != nil {
		a.transferError(w, r, err)
		return
	}

	sink := relay.NewSink()
	if err := a.rendezvous.Park(id, sink); err != nil {
		writeError(w, http.StatusConflict, codeAlreadyAttached, err.Error())
		return
	}
	defer a.rendezvous.Unpark(id)

	// Only now is the Sender told to start. No status line has been written yet,
	// which is the entire reason the Recipient goes first: an HTTP status cannot
	// be retracted, so committing 200 on arrival would leave no way to report a
	// Sender who never shows up (ADR 0002).
	a.emit(r.Context(), EventTransferReady, []string{t.Sender}, map[string]any{"id": t.ID})

	wait := time.NewTimer(a.rendezvousWait)
	defer wait.Stop()

	// The request context is cancelled the moment this handler returns, and the
	// outcome still has to reach both parties.
	announce := context.WithoutCancel(r.Context())

	select {
	case src := <-sink.Ready():
		a.pump(w, r, t, src, sink)
	case <-r.Context().Done():
		a.finish(announce, id, 0, a.now(), relay.WriteError{Err: r.Context().Err()})
	case <-wait.C:
		writeError(w, http.StatusGatewayTimeout, codeRendezvousTimeout, errRendezvousTimeout.Error())
		a.finish(announce, id, 0, a.now(), errRendezvousTimeout)
	}
}

// pump runs the relay and reports the outcome to everybody who needs it.
func (a *api) pump(w http.ResponseWriter, r *http.Request, t transfer.Transfer, src *relay.Source, sink *relay.Sink) {
	announce := context.WithoutCancel(r.Context())
	started := a.now()

	if _, err := a.transfers.Get(t.ID); err != nil {
		src.Report(relay.Result{Err: err})
		return
	}

	// The response shape follows from the declared Payload, so it is settled
	// before a byte arrives.
	relay.Describe(t.Payload).ApplyTo(w)

	rc := http.NewResponseController(w)
	dst := relay.NewDeadlineWriter(w, deadlineController{rc}, a.relayWriteDeadline)

	var counted atomic.Int64
	stopProgress := a.reportProgress(announce, t, &counted, started)

	n, copyErr := relay.Copy(dst, src.Parts, t.Payload, relay.Options{
		Count:    func(n int64) { counted.Add(n) },
		Stop:     sink.Canceled(),
		Buffer:   a.relayBuffer,
		Modified: t.CreatedAt,
	})
	stopProgress()
	// Flush the tail: a zip's central directory is written by Close, after the
	// last entry's bytes.
	if err := rc.Flush(); err != nil {
		slog.Debug("final flush", "transfer", t.ID, "err", err)
	}

	src.Report(relay.Result{Bytes: n, Err: copyErr, Complete: copyErr == nil})
	a.finish(announce, t.ID, n, started, copyErr)
}

// upload is POST /d/{id}: the Sender's half.
//
// It hands its body to the parked Recipient and then blocks until told the copy
// is done, because the Recipient is writing into a response that only exists
// while its own handler is running.
func (a *api) upload(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())
	id := transfer.ID(r.PathValue("id"))

	mt, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mt, "multipart/form-data") || params["boundary"] == "" {
		writeError(w, http.StatusBadRequest, codeBadRequest,
			"expected multipart/form-data with a boundary")
		return
	}

	t, err := a.transfers.Mutate(id, func(t *transfer.Transfer) error {
		if u.ID != t.Sender {
			return transfer.ErrForbidden
		}
		return t.AttachSender(a.now())
	})
	if err != nil {
		// ErrRecipientNotAttached lands here as 409 recipient_not_attached: the
		// Sender is early and should wait for transfer.ready.
		a.transferError(w, r, err)
		return
	}

	src := relay.NewSource(multipart.NewReader(r.Body, params["boundary"]), r.Context())
	if err := a.rendezvous.Handoff(id, src); err != nil {
		// The Recipient's handler returned between the state transition and
		// this line. Nobody is going to read the body.
		a.finish(context.WithoutCancel(r.Context()), id, 0, a.now(), relay.WriteError{Err: err})
		writeError(w, http.StatusConflict, codeRecipientNotAttached, err.Error())
		return
	}

	res := src.Wait()
	if res.Err != nil {
		writeError(w, http.StatusBadGateway, codeInternal, res.Err.Error())
		return
	}
	slog.Info("relayed", "transfer", t.ID, "bytes", res.Bytes, "kind", t.Payload.Kind)
	w.WriteHeader(http.StatusNoContent)
}

// finish applies the terminal transition and announces it. It is the only place
// a data-plane outcome is published, so there is exactly one of them per
// Transfer however the relay ended.
func (a *api) finish(ctx context.Context, id transfer.ID, bytes int64, started time.Time, copyErr error) {
	now := a.now()
	final, err := a.transfers.Mutate(id, func(t *transfer.Transfer) error {
		if copyErr != nil {
			return t.Fail(reasonFor(copyErr), bytes, now)
		}
		return t.Complete(bytes, now)
	})
	if err != nil && final.State != transfer.StateFailed {
		// Already terminal — a cancellation got here first, and it has already
		// been announced. Nothing to add.
		slog.Debug("no terminal transition left to make",
			"transfer", id, "state", final.State, "err", err)
		return
	}

	switch final.State {
	case transfer.StateCompleted:
		a.emit(ctx, EventTransferCompleted, parties(final), map[string]any{
			"id":         id,
			"bytes":      final.BytesRelayed,
			"durationMs": now.Sub(started).Milliseconds(),
		})
	case transfer.StateFailed:
		a.emit(ctx, EventTransferFailed, parties(final), map[string]any{
			"id":           id,
			"reason":       final.FailureReason,
			"bytesRelayed": final.BytesRelayed,
		})
	}
}

// reasonFor turns a relay failure into one of DESIGN §7's reasons. Which side of
// the copy broke is what decides who gets blamed, which is why relay.Copy
// distinguishes a read failure from a write one.
func reasonFor(err error) string {
	var readErr relay.ReadError
	var writeErr relay.WriteError
	switch {
	case errors.Is(err, relay.ErrPayloadMismatch):
		return transfer.ReasonPayloadMismatch
	case errors.Is(err, errRendezvousTimeout):
		return transfer.ReasonRendezvousTimeout
	case errors.As(err, &writeErr):
		return transfer.ReasonRecipientDisconnected
	case errors.As(err, &readErr):
		return transfer.ReasonSenderDisconnected
	default:
		return transfer.ReasonInternal
	}
}

// deadlineController adapts http.ResponseController to relay.Deadliner, and
// tolerates a writer that cannot do deadlines at all.
type deadlineController struct{ rc *http.ResponseController }

func (d deadlineController) SetWriteDeadline(t time.Time) error {
	return optional(d.rc.SetWriteDeadline(t))
}
