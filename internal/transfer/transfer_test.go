package transfer

import (
	"errors"
	"testing"
	"time"
)

const (
	sender    = "u_sender"
	recipient = "u_recipient"
	stranger  = "u_stranger"
	total     = int64(1024)
	ttl       = time.Minute
)

var now = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func pending() *Transfer {
	return New("inst-a.deadbeef", sender, recipient,
		Payload{Name: "docs.zip", Kind: KindFile, TotalBytes: total}, now, ttl)
}

// inState drives a Transfer to s through real transitions, so the fixture is
// itself a test of the happy paths.
func inState(t *testing.T, s State) *Transfer {
	t.Helper()
	tr := pending()
	var err error
	switch s {
	case StatePending:
	case StateAccepted:
		err = tr.Accept(recipient, "s_1", now)
	case StateStreaming:
		if err = tr.Accept(recipient, "s_1", now); err == nil {
			if err = tr.AttachRecipient(now); err == nil {
				err = tr.AttachSender(now)
			}
		}
	case StateCompleted:
		tr = inState(t, StateStreaming)
		err = tr.Complete(total, now)
	case StateDenied:
		err = tr.Deny(recipient, now)
	case StateExpired:
		err = tr.Expire(now.Add(2 * ttl))
	case StateCanceled:
		err = tr.Cancel(sender, now)
	case StateFailed:
		err = tr.Fail(ReasonInternal, 0, now)
	default:
		t.Fatalf("no fixture for state %q", s)
	}
	if err != nil {
		t.Fatalf("building a %s transfer: %v", s, err)
	}
	if tr.State != s {
		t.Fatalf("fixture is %s, want %s", tr.State, s)
	}
	return tr
}

// The expected outcome of an action, as a class rather than an exact error.
type want int

const (
	succeeds want = iota
	illegal
	forbidden
	staleOffer
	attached
	unattached
	mismatch
)

func (w want) check(t *testing.T, got error) {
	t.Helper()
	switch w {
	case succeeds:
		if got != nil {
			t.Errorf("got %v, want success", got)
		}
	case illegal:
		var e IllegalTransitionError
		if !errors.As(got, &e) {
			t.Errorf("got %v, want an IllegalTransitionError", got)
		}
	case forbidden:
		if !errors.Is(got, ErrForbidden) {
			t.Errorf("got %v, want ErrForbidden", got)
		}
	case staleOffer:
		if !errors.Is(got, ErrExpired) {
			t.Errorf("got %v, want ErrExpired", got)
		}
	case attached:
		if !errors.Is(got, ErrAlreadyAttached) {
			t.Errorf("got %v, want ErrAlreadyAttached", got)
		}
	case unattached:
		if !errors.Is(got, ErrRecipientNotAttached) {
			t.Errorf("got %v, want ErrRecipientNotAttached", got)
		}
	case mismatch:
		if !errors.Is(got, ErrPayloadMismatch) {
			t.Errorf("got %v, want ErrPayloadMismatch", got)
		}
	}
}

// actions, each run by the party entitled to run it.
var actions = map[string]func(tr *Transfer) error{
	"accept":          func(tr *Transfer) error { return tr.Accept(recipient, "s_1", now) },
	"deny":            func(tr *Transfer) error { return tr.Deny(recipient, now) },
	"cancel":          func(tr *Transfer) error { return tr.Cancel(sender, now) },
	"attachRecipient": func(tr *Transfer) error { return tr.AttachRecipient(now) },
	"attachSender":    func(tr *Transfer) error { return tr.AttachSender(now) },
	"complete":        func(tr *Transfer) error { return tr.Complete(total, now) },
	"fail":            func(tr *Transfer) error { return tr.Fail(ReasonInternal, 0, now) },
	"expire":          func(tr *Transfer) error { return tr.Expire(now.Add(2 * ttl)) },
	"detachSender":    func(tr *Transfer) error { return tr.DetachSender(now, time.Minute) },
	"detachRecipient": func(tr *Transfer) error { return tr.DetachRecipient(now, time.Minute) },
}

// Every state against every action. The exhaustiveness is the point: a rule
// nobody wrote down is a rule the next reader has to guess.
func TestEveryStateAgainstEveryAction(t *testing.T) {
	t.Parallel()

	table := map[State]map[string]want{
		StatePending: {
			"accept": succeeds, "deny": succeeds, "cancel": succeeds,
			"attachRecipient": illegal, "attachSender": illegal,
			"complete": illegal, "fail": succeeds, "expire": succeeds,
			"detachSender": illegal, "detachRecipient": illegal,
		},
		StateAccepted: {
			"accept": illegal, "deny": illegal, "cancel": succeeds,
			"attachRecipient": succeeds, "attachSender": unattached,
			"complete": illegal, "fail": succeeds, "expire": illegal,
			// A Sender cannot detach from a Transfer it has not attached to; a
			// Recipient can, which is how a Range reconnect gets its slot back.
			"detachSender": illegal, "detachRecipient": succeeds,
		},
		StateStreaming: {
			"accept": illegal, "deny": illegal, "cancel": succeeds,
			"attachRecipient": illegal, "attachSender": illegal,
			"complete": succeeds, "fail": succeeds, "expire": illegal,
			"detachSender": succeeds, "detachRecipient": succeeds,
		},
		StateCompleted: allIllegal(),
		StateDenied:    allIllegal(),
		StateExpired:   allIllegal(),
		StateCanceled:  allIllegal(),
		StateFailed:    allIllegal(),
	}

	for state, expected := range table {
		if len(expected) != len(actions) {
			t.Fatalf("%s covers %d of %d actions", state, len(expected), len(actions))
		}
		for name, run := range actions {
			t.Run(string(state)+"/"+name, func(t *testing.T) {
				t.Parallel()
				expected[name].check(t, run(inState(t, state)))
			})
		}
	}
}

func allIllegal() map[string]want {
	out := map[string]want{}
	for name := range actions {
		out[name] = illegal
	}
	return out
}

// Only the Recipient decides; either party may cancel; a stranger may do
// nothing. Authorization is checked before state, so the answer does not depend
// on where the Transfer happens to be.
func TestActorRules(t *testing.T) {
	t.Parallel()

	for _, state := range []State{StatePending, StateAccepted, StateCompleted} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			for _, actor := range []string{sender, stranger} {
				forbidden.check(t, inState(t, state).Accept(actor, "s_1", now))
				forbidden.check(t, inState(t, state).Deny(actor, now))
			}
			forbidden.check(t, inState(t, state).Cancel(stranger, now))
		})
	}

	// Both parties may cancel a live Transfer.
	for _, actor := range []string{sender, recipient} {
		succeeds.check(t, inState(t, StateAccepted).Cancel(actor, now))
	}
}

// Two tabs of the Recipient accept at the same moment. The Registry serialises
// them; the state machine has to make the second one lose, and record which
// Stream won so the other tab can clear its prompt.
func TestOnlyOneAcceptEverWins(t *testing.T) {
	t.Parallel()
	tr := pending()

	if err := tr.Accept(recipient, "s_first", now); err != nil {
		t.Fatalf("first accept: %v", err)
	}
	illegal.check(t, tr.Accept(recipient, "s_second", now))
	if tr.AcceptedByStream != "s_first" {
		t.Errorf("AcceptedByStream = %q, want s_first", tr.AcceptedByStream)
	}
}

func TestAcceptAndDenyAfterExpiry(t *testing.T) {
	t.Parallel()
	late := now.Add(ttl + time.Second)

	// Still pending — the reaper has not run — but the offer is gone.
	staleOffer.check(t, pending().Accept(recipient, "s_1", late))
	staleOffer.check(t, pending().Deny(recipient, late))

	// Cancelling something already dead is what the caller wants anyway.
	succeeds.check(t, pending().Cancel(sender, late))
}

func TestRendezvousIsRecipientFirst(t *testing.T) {
	t.Parallel()

	// ADR 0002: the Sender may not attach until the Recipient is parked.
	tr := inState(t, StateAccepted)
	unattached.check(t, tr.AttachSender(now))
	if tr.State != StateAccepted {
		t.Errorf("a refused sender attach moved the state to %s", tr.State)
	}

	succeeds.check(t, tr.AttachRecipient(now))
	attached.check(t, tr.AttachRecipient(now))

	succeeds.check(t, tr.AttachSender(now))
	if tr.State != StateStreaming {
		t.Fatalf("state = %s after both attached, want streaming", tr.State)
	}
	// A second POST for the same Transfer.
	illegal.check(t, tr.AttachSender(now))
}

func TestCompleteEnforcesTheDeclaredTotal(t *testing.T) {
	t.Parallel()

	for name, delivered := range map[string]int64{
		"short": total - 1,
		"long":  total + 1,
		"none":  0,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr := inState(t, StateStreaming)
			mismatch.check(t, tr.Complete(delivered, now))

			// A mismatch is not a refusal to act: the Transfer fails, with the
			// count it actually reached, so both parties can be told.
			if tr.State != StateFailed {
				t.Errorf("state = %s, want failed", tr.State)
			}
			if tr.FailureReason != ReasonPayloadMismatch {
				t.Errorf("reason = %q, want %q", tr.FailureReason, ReasonPayloadMismatch)
			}
			if tr.BytesRelayed != delivered {
				t.Errorf("BytesRelayed = %d, want %d", tr.BytesRelayed, delivered)
			}
		})
	}

	exact := inState(t, StateStreaming)
	succeeds.check(t, exact.Complete(total, now))
	if exact.State != StateCompleted || exact.BytesRelayed != total {
		t.Errorf("exact delivery = %s with %d bytes", exact.State, exact.BytesRelayed)
	}
}

func TestFailCarriesTheReasonAndByteCount(t *testing.T) {
	t.Parallel()
	tr := inState(t, StateStreaming)

	if err := tr.Fail(ReasonSenderDisconnected, 4096, now); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if tr.State != StateFailed || tr.FailureReason != ReasonSenderDisconnected || tr.BytesRelayed != 4096 {
		t.Errorf("failed transfer = %+v", tr)
	}
	// Terminal means terminal, even for another failure.
	illegal.check(t, tr.Fail(ReasonInternal, 0, now))
}

func TestIsTerminal(t *testing.T) {
	t.Parallel()

	for _, s := range []State{StatePending, StateAccepted, StateStreaming} {
		if inState(t, s).IsTerminal() {
			t.Errorf("%s reports itself terminal", s)
		}
	}
	for _, s := range []State{StateCompleted, StateDenied, StateExpired, StateCanceled, StateFailed} {
		if !inState(t, s).IsTerminal() {
			t.Errorf("%s does not report itself terminal", s)
		}
	}
}

func TestExpiredAt(t *testing.T) {
	t.Parallel()

	if pending().ExpiredAt(now.Add(ttl - time.Nanosecond)) {
		t.Error("an offer inside its TTL reports itself expired")
	}
	if !pending().ExpiredAt(now.Add(ttl)) {
		t.Error("an offer at its TTL does not report itself expired")
	}
	// Only unanswered offers expire; an accepted Transfer has no deadline.
	if inState(t, StateAccepted).ExpiredAt(now.Add(100 * ttl)) {
		t.Error("an accepted Transfer reports itself expired")
	}
}

func TestIDCarriesItsOwner(t *testing.T) {
	t.Parallel()

	id := NewID("inst-b")
	if OwnerOf(id) != "inst-b" {
		t.Errorf("OwnerOf(%q) = %q, want inst-b", id, OwnerOf(id))
	}
	if id == NewID("inst-b") {
		t.Error("two IDs came out identical")
	}
	if OwnerOf("nodots") != "" {
		t.Error("an ID with no owner prefix claimed an owner")
	}
}
