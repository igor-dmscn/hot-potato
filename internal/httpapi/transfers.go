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
	"hotpotato/internal/presence"
	"hotpotato/internal/transfer"
)

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
	if err := a.transfers.Create(t, a.limits, now); err != nil {
		a.transferError(w, r, err)
		return
	}

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
	// The Stream has to be one of this User's own: the ID is echoed to their
	// other tabs, and a value from elsewhere would clear the wrong prompt.
	if !a.streams.Owns(u.ID, body.StreamID) {
		writeError(w, http.StatusBadRequest, codeBadRequest, "streamId is not one of your streams")
		return
	}

	now := a.now()
	t, err := a.transfers.Mutate(transfer.ID(r.PathValue("id")), func(t *transfer.Transfer) error {
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

	t, err := a.transfers.Mutate(transfer.ID(r.PathValue("id")), func(t *transfer.Transfer) error {
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

	t, err := a.transfers.Mutate(transfer.ID(r.PathValue("id")), func(t *transfer.Transfer) error {
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
				a.emit(ctx, EventTransferFailed, parties(t), map[string]any{
					"id":           t.ID,
					"reason":       transfer.ReasonOfferExpired,
					"bytesRelayed": t.BytesRelayed,
				})
			}
		}
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
	if err := a.bus.Publish(ctx, e); err != nil {
		slog.Error("publish event", "event", name, "err", err)
	}
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
