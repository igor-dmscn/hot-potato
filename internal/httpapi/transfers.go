package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"hotpotato/internal/auth"
	"hotpotato/internal/bus"
	"hotpotato/internal/obs"
	"hotpotato/internal/presence"
	"hotpotato/internal/transfer"
)

// maxStreamIDLength bounds a value the client supplies and the server echoes.
const maxStreamIDLength = 64

// The control plane's Transfer events, from DESIGN §7.
const (
	EventTransferCreated   = "transfer.created"
	EventTransferOffered   = "transfer.offered"
	EventTransferAccepted  = "transfer.accepted"
	EventTransferDenied    = "transfer.denied"
	EventTransferCanceled  = "transfer.canceled"
	EventTransferReady     = "transfer.ready"
	EventTransferProgress  = "transfer.progress"
	EventTransferCompleted = "transfer.completed"
	EventTransferFailed    = "transfer.failed"
)

// createTransfer is POST /api/transfers: the Sender proposes.
func (a *api) createTransfer(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())

	var body struct {
		To         string        `json:"to"`
		Name       string        `json:"name"`
		Kind       transfer.Kind `json:"kind"`
		TotalBytes int64         `json:"totalBytes"`
		EntryCount int           `json:"entryCount"`
	}
	if !decode(w, r, &body) {
		return
	}

	// Offering to somebody who is not there would create a Transfer that can
	// only expire.
	online, err := a.presence.List(r.Context())
	if err != nil {
		internalError(w, r, err)
		return
	}
	if !slices.ContainsFunc(online, func(u presence.User) bool { return u.ID == body.To }) {
		writeError(w, http.StatusNotFound, codeNotFound, "that user is not online")
		return
	}

	now := a.now()
	t := transfer.New(
		transfer.NewID(a.instance),
		u.ID, body.To,
		transfer.Payload{
			Name:       body.Name,
			Kind:       body.Kind,
			TotalBytes: body.TotalBytes,
			EntryCount: body.EntryCount,
		},
		now, a.limits.OfferTTL,
	)
	// The trace this Transfer belongs to, carried for the rest of its life. Its
	// accept, its relay and its completion are separate requests, on possibly
	// separate instances, and this is what puts them on one trace.
	t.Trace = obs.Traceparent(r.Context())

	if err := a.transfers.Create(t, a.limits, now); err != nil {
		a.transferError(w, r, err)
		return
	}
	a.mirror(r.Context(), *t)
	a.metrics.TransferReached(string(t.State))

	// The Sender's other tabs learn about it too, which is what makes the
	// outbound list the same on every one of them.
	a.emit(r.Context(), EventTransferCreated, []string{t.Sender}, t)
	a.emit(r.Context(), EventTransferOffered, []string{t.Recipient}, t)

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":        t.ID,
		"expiresAt": t.ExpiresAt,
	})
}

// acceptTransfer is POST /api/transfers/{id}/accept.
func (a *api) acceptTransfer(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())
	var body struct {
		StreamID string `json:"streamId"`
	}
	if !decode(w, r, &body) {
		return
	}
	// streamId is a hint for the Recipient's *other* tabs, so they can clear a
	// prompt that was answered elsewhere.
	//
	// It is deliberately not checked against the Stream registry. In a
	// multi-instance deployment the Recipient's Stream lives on whichever
	// instance they are attached to, and this request has been redirected to the
	// instance that owns the Transfer — usually a different one, which has never
	// heard of that Stream. It is not an authorization input either: the worst a
	// wrong value achieves is failing to clear one of the caller's own prompts.
	// Only its length is bounded, because it is echoed into an event.
	if len(body.StreamID) > maxStreamIDLength {
		writeError(w, http.StatusBadRequest, codeBadRequest, "streamId is too long")
		return
	}

	now := a.now()
	t, err := a.mutate(r.Context(), transfer.ID(r.PathValue("id")), func(t *transfer.Transfer) error {
		return t.Accept(u.ID, body.StreamID, now)
	})
	if err != nil {
		a.transferError(w, r, err)
		return
	}

	// Both parties, and the winning Stream, so the Recipient's other tabs know
	// the prompt was answered elsewhere.
	a.emit(r.Context(), EventTransferAccepted, parties(t), map[string]any{
		"id":       t.ID,
		"at":       now,
		"byStream": t.AcceptedByStream,
	})
	w.WriteHeader(http.StatusAccepted)
}

// denyTransfer is POST /api/transfers/{id}/deny.
func (a *api) denyTransfer(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())
	now := a.now()

	t, err := a.mutate(r.Context(), transfer.ID(r.PathValue("id")), func(t *transfer.Transfer) error {
		return t.Deny(u.ID, now)
	})
	if err != nil {
		a.transferError(w, r, err)
		return
	}
	a.emit(r.Context(), EventTransferDenied, parties(t), map[string]any{"id": t.ID, "at": now})
	w.WriteHeader(http.StatusNoContent)
}

// cancelTransfer is POST /api/transfers/{id}/cancel, open to either party.
func (a *api) cancelTransfer(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())
	now := a.now()

	t, err := a.mutate(r.Context(), transfer.ID(r.PathValue("id")), func(t *transfer.Transfer) error {
		return t.Cancel(u.ID, now)
	})
	if err != nil {
		a.transferError(w, r, err)
		return
	}
	// A relay already in flight is abandoned at the next buffer boundary. When
	// nothing is streaming this is a no-op, which is the common case.
	a.rendezvous.Cancel(t.ID)

	a.emit(r.Context(), EventTransferCanceled, parties(t), map[string]any{"id": t.ID, "by": u.ID})
	w.WriteHeader(http.StatusNoContent)
}

// Reap expires unanswered offers on a ticker and tells both parties.
//
// DESIGN §7 has no transfer.expired event: the outcome travels as
// transfer.failed with reason offer_expired, which is also what a client that
// only handles failures needs to see.
func (a *api) Reap(ctx context.Context, every, keepTerminal time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, t := range a.transfers.ReapExpired(a.now(), keepTerminal) {
				a.mirror(ctx, t)
				a.metrics.TransferReached(string(t.State))
				// An unanswered offer and an abandoned relay both travel as
				// transfer.failed; only the reason differs.
				reason := t.FailureReason
				if t.State == transfer.StateExpired {
					reason = transfer.ReasonOfferExpired
				}
				a.emit(ctx, EventTransferFailed, parties(t), map[string]any{
					"id":           t.ID,
					"reason":       reason,
					"bytesRelayed": t.BytesRelayed,
				})
			}
		}
	}
}

// mutate applies a transition and mirrors the result into the read model.
//
// Every state change goes through here, which is what keeps the mirror honest
// without a Put next to each transition. The mirror is written even when the
// transition was refused: it costs one round trip and removes the question of
// whether this particular failure changed anything (Complete on a short payload
// does).
func (a *api) mutate(ctx context.Context, id transfer.ID, fn func(*transfer.Transfer) error) (transfer.Transfer, error) {
	var before transfer.State
	t, err := a.transfers.Mutate(id, func(x *transfer.Transfer) error {
		before = x.State
		return fn(x)
	})
	if t.ID != "" {
		a.mirror(ctx, t)
		if t.State != before {
			a.metrics.TransferReached(string(t.State))
		}
	}
	return t, err
}

func (a *api) mirror(ctx context.Context, t transfer.Transfer) {
	if err := a.readModel.Put(ctx, t, a.readModelTTL); err != nil {
		// A snapshot that misses a Transfer is a worse page, not a broken one.
		slog.Error("mirror transfer", "transfer", t.ID, "err", err)
	}
}

// emit publishes one control-plane event. A failure to publish is logged and
// swallowed: the bus is at-most-once by contract, and the client's next
// reconnect brings a fresh snapshot (ADR 0003).
func (a *api) emit(ctx context.Context, name string, audience []string, payload any) {
	e, err := bus.NewEvent(name, audience, payload)
	if err != nil {
		slog.Error("build event", "event", name, "err", err)
		return
	}
	// The traceparent travels with the event, so a trace can cross the instance
	// boundary the bus exists to span.
	e.Trace = obs.Traceparent(ctx)

	started := a.now()
	if err := a.bus.Publish(ctx, e); err != nil {
		slog.Error("publish event", "event", name, "err", err)
	}
	a.metrics.BusPublish(a.now().Sub(started).Seconds())
}

// parties is the audience for anything that concerns both ends.
func parties(t transfer.Transfer) []string { return []string{t.Sender, t.Recipient} }

// transferError maps the domain's errors onto DESIGN §6's envelope.
//
// An unknown ID is 404 and someone else's Transfer is 403 — the ID is 64 bits
// of randomness, so there is nothing to enumerate and no reason to lie about
// which case it is.
func (a *api) transferError(w http.ResponseWriter, r *http.Request, err error) {
	var illegal transfer.IllegalTransitionError
	switch {
	case errors.Is(err, transfer.ErrNotFound):
		writeError(w, http.StatusNotFound, codeNotFound, "no such transfer")
	case errors.Is(err, transfer.ErrForbidden):
		writeError(w, http.StatusForbidden, codeForbidden, "that transfer is not yours to change")
	case errors.As(err, &illegal):
		writeError(w, http.StatusConflict, codeIllegalState, illegal.Error())
	case errors.Is(err, transfer.ErrExpired):
		writeError(w, http.StatusConflict, codeIllegalState, "that offer has expired")
	case errors.Is(err, transfer.ErrAlreadyAttached):
		writeError(w, http.StatusConflict, codeAlreadyAttached, "that transfer already has both ends")
	case errors.Is(err, transfer.ErrRecipientNotAttached):
		writeError(w, http.StatusConflict, codeRecipientNotAttached,
			"the recipient has not opened the download yet")
	case errors.Is(err, transfer.ErrTooManyTransfers):
		writeError(w, http.StatusTooManyRequests, codeTooManyTransfers, err.Error())
	case errors.Is(err, transfer.ErrRateLimited):
		writeError(w, http.StatusTooManyRequests, codeRateLimited, err.Error())
	case errors.Is(err, transfer.ErrPayloadTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, codePayloadTooLarge, err.Error())
	case errors.Is(err, transfer.ErrInvalidPayload):
		writeError(w, http.StatusBadRequest, codeBadRequest, err.Error())
	default:
		internalError(w, r, err)
	}
}
