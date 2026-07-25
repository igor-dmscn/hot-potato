package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"hotpotato/internal/auth"
	"hotpotato/internal/obs"
	"hotpotato/internal/relay"
	"hotpotato/internal/transfer"
)

var (
	// errRendezvousTimeout is a Sender who never arrived.
	errRendezvousTimeout = errors.New("the sender did not arrive")
	// errResumeTimeout is a Sender who arrived, stopped, and did not come back.
	errResumeTimeout = errors.New("the sender did not resume")
	// errUnsatisfiableRange is a Range this design cannot serve.
	errUnsatisfiableRange = errors.New("that range cannot be served")
)

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

	// The Range is settled before anything is claimed. A Recipient only gets one
	// attach, and a request that is going to be refused must not spend it — an
	// earlier version checked the Range after attaching and left the Transfer
	// unusable by the retry that would have worked.
	//
	// The value is the Recipient's own count of what it holds, not the Owner's:
	// the Owner counts bytes written into a socket, which after a disconnection is
	// larger by whatever was in flight.
	known, err := a.transfers.Get(id)
	if err != nil {
		a.transferError(w, r, err)
		return
	}
	if u.ID != known.Recipient {
		// Re-checked under the lock below; this is the cheap early answer, and it
		// keeps an unauthorized caller from learning anything by timing.
		writeError(w, http.StatusForbidden, codeForbidden, "that transfer is not yours to receive")
		return
	}
	resumeFrom, ok := relay.ParseRangeStart(r.Header.Get("Range"))
	if !ok || (resumeFrom > 0 && known.Payload.Kind != transfer.KindFile) {
		// A folder cannot be resumed into a new response: the archive was being
		// built as it streamed, and that stream went with the socket.
		writeError(w, http.StatusRequestedRangeNotSatisfiable, codeBadRequest,
			"only `Range: bytes=N-` on a single file can be resumed")
		return
	}

	t, err := a.mutate(r.Context(), id, func(t *transfer.Transfer) error {
		// The Recipient's identity is verified here too, under the lock: a
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

	// Everything from here on belongs to the Transfer's own trace, which started
	// when it was proposed — on another request, and possibly another instance.
	ctx, span := obs.Start(obs.Continue(r.Context(), t.Trace), "relay.recipient",
		obs.Attr("transfer.id", string(id)), obs.Attr("payload.kind", string(t.Payload.Kind)))
	defer span.End()
	r = r.WithContext(ctx)

	sink := relay.NewSink()
	if err := a.rendezvous.Park(id, sink); err != nil {
		writeError(w, http.StatusConflict, codeAlreadyAttached, err.Error())
		return
	}
	defer a.rendezvous.Unpark(id)
	parked := a.now()

	// Only now is the Sender told to start. No status line has been written yet,
	// which is the entire reason the Recipient goes first: an HTTP status cannot
	// be retracted, so committing 200 on arrival would leave no way to report a
	// Sender who never shows up (ADR 0002).
	//
	// The invitation always carries a position, even the first one. A Sender has
	// no other way to learn that a Recipient came back with a Range and rewound
	// it — and "start here" is a better instruction than "start".
	a.emit(r.Context(), EventTransferReady, []string{t.Sender}, map[string]any{
		"id":     t.ID,
		"entry":  0,
		"offset": resumeFrom,
	})

	wait := time.NewTimer(a.rendezvousWait)
	defer wait.Stop()

	// The request context is cancelled the moment this handler returns, and the
	// outcome still has to reach both parties.
	announce := context.WithoutCancel(r.Context())

	select {
	case src := <-sink.Ready():
		a.metrics.RendezvousWait(a.now().Sub(parked).Seconds())
		a.pump(w, r, t, src, sink, resumeFrom)
	case <-r.Context().Done():
		a.finish(announce, id, 0, a.now(), relay.WriteError{Err: r.Context().Err()})
	case <-wait.C:
		a.metrics.RendezvousWait(a.now().Sub(parked).Seconds())
		writeError(w, http.StatusGatewayTimeout, codeRendezvousTimeout, errRendezvousTimeout.Error())
		a.finish(announce, id, 0, a.now(), errRendezvousTimeout)
	}
}

// pump runs the relay to its end, across as many Sender requests as it takes,
// and reports the outcome to everybody who needs it.
//
// The Session — including a folder's live zip.Writer and its half-written entry —
// lives in this function's frame for the whole Transfer. That is the only state a
// resumed Transfer needs, and it is also why resume cannot survive the Owner's
// death (ADR 0008).
func (a *api) pump(w http.ResponseWriter, r *http.Request, t transfer.Transfer, src *relay.Source, sink *relay.Sink, resumeFrom int64) {
	announce := context.WithoutCancel(r.Context())
	started := a.now()

	// The response shape follows from the declared Payload, so it is settled
	// before a byte arrives.
	relay.Describe(t.Payload).ResumedFrom(resumeFrom).ApplyTo(w)

	rc := http.NewResponseController(w)
	dst := relay.NewDeadlineWriter(w, deadlineController{rc}, a.relayWriteDeadline)

	var counted atomic.Int64
	counted.Store(resumeFrom) // progress carries on from where the last attempt stopped
	stopProgress := a.reportProgress(announce, t, &counted, started)
	defer stopProgress()

	session := relay.NewSession(dst, t.Payload, relay.Options{
		Count:    func(n int64) { counted.Add(n) },
		Stop:     sink.Canceled(),
		Buffer:   a.relayBuffer,
		Modified: t.CreatedAt,
	})
	if resumeFrom > 0 {
		if err := session.SkipTo(resumeFrom); err != nil {
			src.Report(relay.Result{Err: err})
			a.finish(announce, t.ID, 0, started, err)
			return
		}
	}

	end := func(err error) {
		stopProgress()
		// Flush the tail: a zip's central directory is written by Close, after
		// the last entry's bytes.
		if flushErr := rc.Flush(); flushErr != nil {
			slog.Debug("final flush", "transfer", t.ID, "err", flushErr)
		}
		relayed := session.Total() - resumeFrom
		a.metrics.RelayedBytes(relayed)
		if elapsed := a.now().Sub(started).Seconds(); elapsed > 0 && err == nil {
			a.metrics.Throughput(float64(relayed) / elapsed)
		}
		a.finish(announce, t.ID, session.Total(), started, err)
	}

	for {
		chunkErr := a.relayChunk(session, src)
		switch {
		case session.Complete():
			end(session.Close())
			return
		case isWriteFailure(chunkErr):
			// The Recipient's socket is gone. If it can come back with a Range —
			// which only a single file can — the Transfer waits for it instead of
			// failing.
			stopProgress()
			a.recipientLeft(announce, t, session, resumeFrom, started)
			return
		case a.resumeWindow <= 0 && chunkErr != nil:
			end(chunkErr)
			return
		case !resumable(chunkErr):
			end(chunkErr)
			return
		}

		// Let the Sender come back. The Transfer stays live: the Recipient is
		// still here and so is the archive position.
		if _, err := a.mutate(announce, t.ID, func(x *transfer.Transfer) error {
			return x.DetachSender(a.now(), a.resumeWindow)
		}); err != nil {
			slog.Debug("detach sender", "transfer", t.ID, "err", err)
		}
		// The invitation carries the position, not just the fact.
		//
		// A Sender whose upload died usually never sees its own response: the HTTP
		// client reports the broken request body instead. Putting the position on
		// the control plane is what makes resume usable rather than theoretical —
		// and the control plane is exactly where "here is the current state of your
		// Transfer" belongs.
		entry, offset := session.Position()
		a.emit(announce, EventTransferReady, []string{t.Sender}, map[string]any{
			"id":     t.ID,
			"resume": true,
			"entry":  entry,
			"offset": offset,
		})

		next := time.NewTimer(a.resumeWindow)
		select {
		case src = <-sink.Ready():
			next.Stop()
		case <-r.Context().Done():
			next.Stop()
			stopProgress()
			a.recipientLeft(announce, t, session, resumeFrom, started)
			return
		case <-next.C:
			end(relay.ReadError{Err: errResumeTimeout})
			return
		}
	}
}

// relayChunk consumes one Sender request. It returns nil when the chunk ended
// cleanly, whether or not the Payload is now whole.
func (a *api) relayChunk(session *relay.Session, src *relay.Source) error {
	// Where the Sender thinks it is, against where the relay actually is. The
	// Sender cannot work this out for itself: what it wrote into a socket is not
	// what the Recipient read out of one.
	if err := session.Continue(src.EntryIndex, src.EntryOffset); err != nil {
		var mismatch relay.OffsetMismatchError
		errors.As(err, &mismatch)
		src.Report(relay.Result{
			Err:            err,
			ExpectedIndex:  mismatch.ExpectedEntry,
			ExpectedOffset: mismatch.ExpectedOffset,
		})
		return err
	}

	n, err := session.Consume(src.Parts)
	entry, offset := session.Position()
	src.Report(relay.Result{
		Bytes:          n,
		Err:            err,
		Complete:       session.Complete(),
		ExpectedIndex:  entry,
		ExpectedOffset: offset,
	})
	return err
}

// resumable reports whether waiting for another chunk could help. A Sender that
// stopped or asked for the wrong offset can try again; a Recipient whose socket
// is gone cannot be written to by anybody.
func resumable(err error) bool {
	if err == nil {
		return true
	}
	var readErr relay.ReadError
	var mismatch relay.OffsetMismatchError
	return errors.As(err, &readErr) || errors.As(err, &mismatch)
}

func isWriteFailure(err error) bool {
	var writeErr relay.WriteError
	return errors.As(err, &writeErr)
}

// recipientLeft handles a Recipient whose response is gone.
//
// A single file can be picked up again with `Range: bytes=N-`, so the Transfer is
// detached rather than failed and the reaper enforces the deadline. A folder
// cannot: the archive was being built as it streamed, and that stream has gone
// with the socket.
func (a *api) recipientLeft(ctx context.Context, t transfer.Transfer, session *relay.Session, resumeFrom int64, started time.Time) {
	relayed := session.Total() - resumeFrom
	a.metrics.RelayedBytes(relayed)

	if a.resumeWindow <= 0 || t.Payload.Kind != transfer.KindFile {
		a.finish(ctx, t.ID, session.Total(), started,
			relay.WriteError{Err: errors.New("the recipient went away")})
		return
	}

	_, offset := session.Position()
	if _, err := a.mutate(ctx, t.ID, func(x *transfer.Transfer) error {
		x.BytesRelayed = offset
		return x.DetachRecipient(a.now(), a.resumeWindow)
	}); err != nil {
		slog.Debug("detach recipient", "transfer", t.ID, "err", err)
		return
	}
	slog.Info("recipient left mid-relay; holding it open",
		"transfer", t.ID, "at", offset, "window", a.resumeWindow)
	// Both parties are told where it stopped, so the Recipient knows what to ask
	// for with Range and the Sender knows to expect a fresh request.
	a.emit(ctx, EventTransferProgress, parties(t), map[string]any{
		"id":          t.ID,
		"bytes":       offset,
		"total":       t.Payload.TotalBytes,
		"bytesPerSec": 0,
	})
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

	entry, offset, err := resumePosition(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, codeBadRequest, err.Error())
		return
	}

	t, err := a.mutate(r.Context(), id, func(t *transfer.Transfer) error {
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
	src.EntryIndex, src.EntryOffset = entry, offset

	if err := a.rendezvous.Handoff(id, src); err != nil {
		// The Recipient's handler returned between the state transition and
		// this line. Nobody is going to read the body.
		a.finish(context.WithoutCancel(r.Context()), id, 0, a.now(), relay.WriteError{Err: err})
		writeError(w, http.StatusConflict, codeRecipientNotAttached, err.Error())
		return
	}

	res := src.Wait()
	var mismatch relay.OffsetMismatchError
	switch {
	case errors.As(res.Err, &mismatch):
		// The one error a Sender can act on, so it is told exactly where to
		// start. There is no manifest to check an entry against (ADR 0004): a
		// wrong offset that got through would surface as a CRC error at unzip
		// time, which is far too late.
		w.Header().Set(relay.HeaderExpectedEntry, strconv.Itoa(mismatch.ExpectedEntry))
		w.Header().Set(relay.HeaderExpectedOffset, strconv.FormatInt(mismatch.ExpectedOffset, 10))
		writeError(w, http.StatusConflict, codeIllegalState, mismatch.Error())
		return
	case res.Err != nil:
		// The Recipient may still be waiting for another attempt; the headers say
		// where from.
		w.Header().Set(relay.HeaderExpectedEntry, strconv.Itoa(res.ExpectedIndex))
		w.Header().Set(relay.HeaderExpectedOffset, strconv.FormatInt(res.ExpectedOffset, 10))
		writeError(w, http.StatusBadGateway, codeInternal, res.Err.Error())
		return
	}
	slog.Info("relayed", "transfer", t.ID, "bytes", res.Bytes,
		"kind", t.Payload.Kind, "complete", res.Complete)
	w.WriteHeader(http.StatusNoContent)
}

// resumePosition reads where the Sender claims to be starting. Absent headers
// mean the beginning, which is what an ordinary first attempt sends.
func resumePosition(r *http.Request) (entry int, offset int64, err error) {
	if v := r.Header.Get(relay.HeaderEntryIndex); v != "" {
		if entry, err = strconv.Atoi(v); err != nil || entry < 0 {
			return 0, 0, fmt.Errorf("%s must be a non-negative integer", relay.HeaderEntryIndex)
		}
	}
	if v := r.Header.Get(relay.HeaderEntryOffset); v != "" {
		if offset, err = strconv.ParseInt(v, 10, 64); err != nil || offset < 0 {
			return 0, 0, fmt.Errorf("%s must be a non-negative integer", relay.HeaderEntryOffset)
		}
	}
	return entry, offset, nil
}

// finish applies the terminal transition and announces it. It is the only place
// a data-plane outcome is published, so there is exactly one of them per
// Transfer however the relay ended.
func (a *api) finish(ctx context.Context, id transfer.ID, bytes int64, started time.Time, copyErr error) {
	now := a.now()
	final, err := a.mutate(ctx, id, func(t *transfer.Transfer) error {
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
