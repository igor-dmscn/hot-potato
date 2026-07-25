package transfer

import (
	"errors"
	"fmt"
)

var (
	// ErrForbidden means the actor is not a party to this Transfer, or not the
	// party allowed to do this. A Transfer ID is not an authorization token.
	ErrForbidden = errors.New("actor may not do that")
	// ErrExpired is an offer acted on after its TTL but before the reaper got
	// to it.
	ErrExpired = errors.New("offer has expired")
	// ErrAlreadyAttached is a second GET or a second POST on the data plane. The
	// protocol gives it its own code, so it gets its own error rather than hiding
	// inside an illegal transition.
	ErrAlreadyAttached = errors.New("already attached")
	// ErrRecipientNotAttached is a Sender arriving before the Recipient has
	// parked. The rendezvous is Recipient-first by design (ADR 0002).
	ErrRecipientNotAttached = errors.New("recipient has not attached")
	// ErrPayloadMismatch is delivered bytes disagreeing with the declared
	// total. The Transfer fails rather than completing.
	ErrPayloadMismatch = errors.New("delivered bytes do not match the declared total")
)

// IllegalTransitionError is an action the current state does not permit.
type IllegalTransitionError struct {
	From   State
	Action string
}

func (e IllegalTransitionError) Error() string {
	return fmt.Sprintf("cannot %s a transfer that is %s", e.Action, e.From)
}
