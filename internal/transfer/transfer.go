// Package transfer is the Transfer state machine: the rules about who may do
// what, and when, expressed without a clock, a mutex or an HTTP request.
//
// There is no net/http import here, and there is a test that fails if one
// appears. `now` is a parameter to every transition, so expiry is testable
// without sleeping. Locking belongs to the Registry, one layer up.
package transfer

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
)

// ID is "<instance>.<random>". The instance prefix is the whole of ADR 0007:
// any instance can work out which one owns a Transfer, and redirect to it,
// without asking anybody.
type ID string

// NewID mints an ID owned by instance.
func NewID(instance string) ID {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failing is not a condition this can carry on past
	}
	return ID(instance + "." + hex.EncodeToString(b[:]))
}

// OwnerOf is the instance that holds this Transfer's live state.
func OwnerOf(id ID) string {
	instance, _, found := strings.Cut(string(id), ".")
	if !found {
		return ""
	}
	return instance
}

// Payload is what a Transfer carries: the bytes, plus the name, size and shape
// declared for them. Everything here is the Sender's claim until the relay has
// counted what actually arrived.
type Payload struct {
	Name       string `json:"name"`
	Kind       Kind   `json:"kind"`
	TotalBytes int64  `json:"totalBytes"`
	EntryCount int    `json:"entryCount"`
}

// Transfer is one attempt by a Sender to send a Payload to a Recipient, from
// the moment it is proposed until it ends.
type Transfer struct {
	ID        ID      `json:"id"`
	Sender    string  `json:"sender"`
	Recipient string  `json:"recipient"`
	Payload   Payload `json:"payload"`
	State     State   `json:"state"`

	CreatedAt time.Time `json:"createdAt"`
	ExpiresAt time.Time `json:"expiresAt"`
	// EndedAt is when this Transfer reached a terminal state. Snapshots include
	// terminal Transfers for a short window afterwards, so a brief
	// disconnection cannot swallow a one-shot outcome such as a denial.
	EndedAt time.Time `json:"endedAt,omitzero"`

	// AcceptedByStream is the Stream that won the accept. It travels in
	// transfer.accepted so the User's other tabs know to clear their prompt.
	AcceptedByStream string `json:"acceptedByStream,omitempty"`

	SenderAttached    bool `json:"senderAttached"`
	RecipientAttached bool `json:"recipientAttached"`

	BytesRelayed  int64  `json:"bytesRelayed"`
	FailureReason string `json:"failureReason,omitempty"`

	// ResumeBy is when an interrupted Transfer stops being resumable. It is set
	// when a party detaches mid-relay and cleared when one attaches, and the
	// reaper is what enforces it — otherwise a Transfer whose Sender never comes
	// back is held open by nobody, forever.
	ResumeBy time.Time `json:"resumeBy,omitzero"`

	// Trace is the W3C traceparent of the request that proposed this Transfer.
	// Its accept, its relay and its outcome are separate requests on possibly
	// separate instances; carrying the header is what puts them on one trace.
	Trace string `json:"trace,omitempty"`
}

// New returns a pending Transfer.
func New(id ID, sender, recipient string, p Payload, now time.Time, ttl time.Duration) *Transfer {
	return &Transfer{
		ID:        id,
		Sender:    sender,
		Recipient: recipient,
		Payload:   p,
		State:     StatePending,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
}

// IsTerminal reports whether this Transfer will never change again.
func (t Transfer) IsTerminal() bool { return terminal[t.State] }

// Party reports whether userID is the Sender or the Recipient.
func (t Transfer) Party(userID string) bool {
	return userID == t.Sender || userID == t.Recipient
}

// Accept is the Recipient's decision. Exactly one ever succeeds: a second, from
// another of their tabs, is an illegal transition, and the winning stream ID is
// recorded so the losing tabs can clear their prompt.
func (t *Transfer) Accept(by, streamID string, now time.Time) error {
	if by != t.Recipient {
		return ErrForbidden
	}
	if err := t.mustBe("accept", StatePending); err != nil {
		return err
	}
	// The reaper runs every few seconds, so an offer can be past its TTL and
	// still be pending. Accepting it would start a Transfer the Sender has
	// stopped expecting.
	if !now.Before(t.ExpiresAt) {
		return ErrExpired
	}
	t.State = StateAccepted
	t.AcceptedByStream = streamID
	return nil
}

func (t *Transfer) Deny(by string, now time.Time) error {
	if by != t.Recipient {
		return ErrForbidden
	}
	if err := t.mustBe("deny", StatePending); err != nil {
		return err
	}
	if !now.Before(t.ExpiresAt) {
		return ErrExpired
	}
	t.State = StateDenied
	t.EndedAt = now
	return nil
}

// Cancel is either party changing their mind, at any point before the end.
//
// Unlike Accept it tolerates an offer that is past its TTL but not yet reaped:
// accepting a dead offer would start something nobody expects, while
// cancelling one is what the caller wants regardless.
func (t *Transfer) Cancel(by string, now time.Time) error {
	if !t.Party(by) {
		return ErrForbidden
	}
	return t.transition("cancel", StateCanceled, now, StatePending, StateAccepted, StateStreaming)
}

// AttachRecipient parks the Recipient's GET. It happens before the Sender is
// told to start, which is what makes a no-show reportable as a 504 rather than
// a truncated 200 (ADR 0002).
func (t *Transfer) AttachRecipient(_ time.Time) error {
	if err := t.mustBe("attach recipient to", StateAccepted); err != nil {
		return err
	}
	if t.RecipientAttached {
		return ErrAlreadyAttached
	}
	t.RecipientAttached = true
	t.ResumeBy = time.Time{}
	return nil
}

// AttachSender joins the Sender's POST to the waiting Recipient, and is what
// starts the relay.
func (t *Transfer) AttachSender(_ time.Time) error {
	if err := t.mustBe("attach sender to", StateAccepted); err != nil {
		return err
	}
	if !t.RecipientAttached {
		return ErrRecipientNotAttached
	}
	if t.SenderAttached {
		return ErrAlreadyAttached
	}
	t.SenderAttached = true
	t.State = StateStreaming
	t.ResumeBy = time.Time{}
	return nil
}

// DetachSender lets a Sender that stopped mid-relay come back.
//
// The Transfer returns to accepted rather than failing, because the Recipient is
// still holding its response open and the Owner is still holding the archive
// position — the only state a resumed Transfer needs, and the reason resume
// cannot survive the Owner's death (ADR 0008).
func (t *Transfer) DetachSender(now time.Time, window time.Duration) error {
	if err := t.mustBe("detach the sender from", StateStreaming); err != nil {
		return err
	}
	t.SenderAttached = false
	t.State = StateAccepted
	t.ResumeBy = now.Add(window)
	return nil
}

// DetachRecipient lets a Recipient reconnect, with Range, to a Transfer that is
// still in flight.
func (t *Transfer) DetachRecipient(now time.Time, window time.Duration) error {
	if err := t.mustBe("detach the recipient from", StateStreaming, StateAccepted); err != nil {
		return err
	}
	t.RecipientAttached = false
	t.SenderAttached = false
	t.State = StateAccepted
	t.ResumeBy = now.Add(window)
	return nil
}

// Complete ends a Transfer that delivered every declared byte. A short or long
// delivery fails as payload_mismatch instead: the server has no copy to check
// against, so the declared total is the only thing it can enforce.
func (t *Transfer) Complete(bytes int64, now time.Time) error {
	if err := t.mustBe("complete", StateStreaming); err != nil {
		return err
	}
	if bytes != t.Payload.TotalBytes {
		_ = t.Fail(ReasonPayloadMismatch, bytes, now)
		return ErrPayloadMismatch
	}
	t.State = StateCompleted
	t.BytesRelayed = bytes
	t.EndedAt = now
	return nil
}

// Fail ends a Transfer with a machine-readable reason and the byte count it
// reached. Every failure produces one of these, addressed to both parties: no
// connection is ever left hanging without an explanation.
func (t *Transfer) Fail(reason string, bytes int64, now time.Time) error {
	if t.IsTerminal() {
		return IllegalTransitionError{From: t.State, Action: "fail"}
	}
	t.State = StateFailed
	t.FailureReason = reason
	t.BytesRelayed = bytes
	t.EndedAt = now
	return nil
}

// Expire is the reaper's transition, and applies only to an offer nobody
// answered. Once accepted, a Transfer has no deadline.
func (t *Transfer) Expire(now time.Time) error {
	return t.transition("expire", StateExpired, now, StatePending)
}

// ResumeExpiredAt reports whether an interrupted Transfer has waited long enough
// for the party that walked away.
func (t Transfer) ResumeExpiredAt(now time.Time) bool {
	return !t.IsTerminal() && !t.ResumeBy.IsZero() && !now.Before(t.ResumeBy)
}

// ExpiredAt reports whether an unanswered offer is past its TTL at now. It is
// the reaper's predicate, kept next to the rule it enforces.
func (t Transfer) ExpiredAt(now time.Time) bool {
	return t.State == StatePending && !now.Before(t.ExpiresAt)
}

func (t *Transfer) transition(action string, to State, now time.Time, from ...State) error {
	if err := t.mustBe(action, from...); err != nil {
		return err
	}
	t.State = to
	if terminal[to] {
		t.EndedAt = now
	}
	return nil
}

func (t *Transfer) mustBe(action string, allowed ...State) error {
	for _, s := range allowed {
		if t.State == s {
			return nil
		}
	}
	return IllegalTransitionError{From: t.State, Action: action}
}
