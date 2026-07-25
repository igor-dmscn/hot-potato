package httpapi

import (
	"encoding/json"
	"net/http"

	"hotpotato/internal/auth"
	"hotpotato/internal/transfer"
)

// EventTransferSignal carries an opaque blob from one party to the other.
const EventTransferSignal = "transfer.signal"

// maxSignal bounds a blob. An SDP offer is a few kilobytes; anything much larger
// is somebody using the control plane as the data plane.
const maxSignal = 16 << 10

// signal is POST /api/transfers/{id}/signal: the whole of the WebRTC data plane's
// server-side involvement.
//
// The entire control plane is reused unchanged — offer, accept, deny, cancel,
// presence, snapshots — and only the bytes move somewhere else. That is the point
// of the comparison: what the server gives up is not the coordination, it is the
// ability to see, measure or enforce anything about the Payload.
//
// The blob is never parsed. SDP and ICE are between the peers; the server is a
// postbox with an authorization check.
func (a *api) signal(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())
	id := transfer.ID(r.PathValue("id"))

	var body struct {
		// Signal is opaque. Whatever the peers agree on goes here.
		Signal json.RawMessage `json:"signal"`
		// Outcome is the peers reporting how it went.
		//
		// Peer-reported, and therefore unverifiable: the server relayed no bytes,
		// so it has nothing to check this against. The declared-total check still
		// runs, but its input now comes from the party with an interest in the
		// answer. That is the price of the bytes not passing through here.
		Outcome *struct {
			Bytes  int64  `json:"bytes"`
			Failed bool   `json:"failed"`
			Reason string `json:"reason"`
		} `json:"outcome,omitempty"`
	}
	if !decode(w, r, &body) {
		return
	}
	if len(body.Signal) > maxSignal {
		writeError(w, http.StatusRequestEntityTooLarge, codePayloadTooLarge,
			"a signal is for SDP and ICE, not for payload")
		return
	}

	now := a.now()
	t, err := a.mutate(r.Context(), id, func(t *transfer.Transfer) error {
		if !t.Party(u.ID) {
			return transfer.ErrForbidden
		}
		// A peer signalling is a peer attaching. The existing transitions apply:
		// the Recipient goes first, exactly as in ADR 0002, because the Recipient
		// is the one who answers the offer.
		switch {
		case u.ID == t.Recipient && !t.RecipientAttached && t.State == transfer.StateAccepted:
			return t.AttachRecipient(now)
		case u.ID == t.Sender && !t.SenderAttached && t.RecipientAttached && t.State == transfer.StateAccepted:
			return t.AttachSender(now)
		}
		return nil
	})
	if err != nil {
		a.transferError(w, r, err)
		return
	}

	if body.Outcome != nil {
		a.peerOutcome(r, t, u.ID, body.Outcome.Bytes, body.Outcome.Failed, body.Outcome.Reason)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Forwarded to the other party only. Both parties' *other* tabs have no use
	// for half a peer connection.
	a.emit(r.Context(), EventTransferSignal, []string{counterparty(t, u.ID)}, map[string]any{
		"id":     t.ID,
		"from":   u.ID,
		"signal": body.Signal,
	})
	w.WriteHeader(http.StatusNoContent)
}

// peerOutcome ends a Transfer on the peers' word.
func (a *api) peerOutcome(r *http.Request, t transfer.Transfer, by string, bytes int64, failed bool, reason string) {
	now := a.now()
	final, err := a.mutate(r.Context(), t.ID, func(x *transfer.Transfer) error {
		if failed {
			return x.Fail(peerReason(reason), bytes, now)
		}
		return x.Complete(bytes, now)
	})
	if err != nil && final.State != transfer.StateFailed {
		return
	}

	switch final.State {
	case transfer.StateCompleted:
		a.emit(r.Context(), EventTransferCompleted, parties(final), map[string]any{
			"id":    final.ID,
			"bytes": final.BytesRelayed,
			// No durationMs: the server never saw the transfer start.
			"reportedBy": by,
		})
	case transfer.StateFailed:
		a.emit(r.Context(), EventTransferFailed, parties(final), map[string]any{
			"id":           final.ID,
			"reason":       final.FailureReason,
			"bytesRelayed": final.BytesRelayed,
			"reportedBy":   by,
		})
	}
}

// peerReason keeps a peer from inventing reasons. DESIGN §7's list is the list.
func peerReason(reason string) string {
	switch reason {
	case transfer.ReasonSenderDisconnected, transfer.ReasonRecipientDisconnected,
		transfer.ReasonPayloadMismatch, transfer.ReasonInternal:
		return reason
	default:
		return transfer.ReasonInternal
	}
}

func counterparty(t transfer.Transfer, me string) string {
	if me == t.Sender {
		return t.Recipient
	}
	return t.Sender
}
