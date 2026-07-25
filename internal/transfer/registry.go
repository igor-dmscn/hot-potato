package transfer

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

var (
	// ErrNotFound is an unknown Transfer ID.
	ErrNotFound = errors.New("no such transfer")
	// ErrTooManyTransfers is one of the per-User limits.
	ErrTooManyTransfers = errors.New("too many transfers")
	// ErrRateLimited is the offer rate limit.
	ErrRateLimited = errors.New("too many offers")
	// ErrPayloadTooLarge is a declared size above the ceiling.
	ErrPayloadTooLarge = errors.New("payload too large")
	// ErrInvalidPayload is a declaration that does not describe anything.
	ErrInvalidPayload = errors.New("invalid payload")
)

// Limits is the policy envelope, minus the timings that belong to other
// packages. main fills it from config (docs/protocol.md).
type Limits struct {
	OfferTTL          time.Duration
	MaxOutbound       int   // non-terminal Transfers a User may be sending
	MaxPendingInbound int   // unanswered offers a User may be holding
	MaxPayloadBytes   int64 // declared size ceiling
	MaxEntries        int   // entries in a folder
	OfferRate         int   // offers per window, per Sender
	OfferRateWindow   time.Duration
}

// Validate checks a declared Payload against the limits. It rejects rather than
// rewrites: a name the Sender did not mean to send is worth an error, and the
// value is echoed to the Recipient either way.
func (p Payload) Validate(l Limits) error {
	switch p.Kind {
	case KindFile, KindFolder:
	default:
		return fmt.Errorf("%w: kind must be %q or %q", ErrInvalidPayload, KindFile, KindFolder)
	}
	if err := checkName(p.Name); err != nil {
		return err
	}
	if p.TotalBytes <= 0 {
		return fmt.Errorf("%w: totalBytes must be positive", ErrInvalidPayload)
	}
	if l.MaxPayloadBytes > 0 && p.TotalBytes > l.MaxPayloadBytes {
		return fmt.Errorf("%w: %d bytes is over the %d byte limit",
			ErrPayloadTooLarge, p.TotalBytes, l.MaxPayloadBytes)
	}
	switch p.Kind {
	case KindFile:
		if p.EntryCount > 1 {
			return fmt.Errorf("%w: a file has one entry, not %d", ErrInvalidPayload, p.EntryCount)
		}
	case KindFolder:
		// The upper bound is not arbitrary: a zip's central directory is built
		// in memory, so entry count is the one part of a Payload the server
		// does pay for per item.
		if p.EntryCount < 1 || (l.MaxEntries > 0 && p.EntryCount > l.MaxEntries) {
			return fmt.Errorf("%w: entryCount must be between 1 and %d", ErrInvalidPayload, l.MaxEntries)
		}
	}
	return nil
}

func checkName(name string) error {
	switch {
	case name == "" || len(name) > 255:
		return fmt.Errorf("%w: name must be 1–255 bytes", ErrInvalidPayload)
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("%w: name may not contain a path separator", ErrInvalidPayload)
	case name == "." || name == "..":
		return fmt.Errorf("%w: name may not be a directory reference", ErrInvalidPayload)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: name may not contain control characters", ErrInvalidPayload)
		}
	}
	return nil
}

// Registry is this instance's write model: the live state of every Transfer it
// owns. There is no mutex in the state machine because there is one here.
type Registry struct {
	mu     sync.Mutex
	byID   map[ID]*Transfer
	offers map[string]*rateWindow // per-Sender offer rate
}

type rateWindow struct {
	count int
	since time.Time
}

func NewRegistry() *Registry {
	return &Registry{byID: map[ID]*Transfer{}, offers: map[string]*rateWindow{}}
}

// Create registers a pending Transfer, enforcing the per-User limits under the
// same lock that inserts it. Counting outside the lock would let two
// simultaneous offers both see room for one more.
func (r *Registry) Create(t *Transfer, l Limits, now time.Time) error {
	if err := t.Payload.Validate(l); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byID[t.ID]; exists {
		return fmt.Errorf("transfer %s already exists", t.ID)
	}

	// ponytail: a linear scan. Bounded by MaxOutbound × online Users, and this
	// runs once per offer; index by User if a profile ever says otherwise.
	outbound, inbound := 0, 0
	for _, other := range r.byID {
		if other.IsTerminal() {
			continue
		}
		if other.Sender == t.Sender {
			outbound++
		}
		if other.Recipient == t.Recipient && other.State == StatePending {
			inbound++
		}
	}
	if l.MaxOutbound > 0 && outbound >= l.MaxOutbound {
		return fmt.Errorf("%w: %d outbound at once is the limit", ErrTooManyTransfers, l.MaxOutbound)
	}
	// Inbound, not outbound: outbound is already capped at three. This is what
	// stops one User burying another under offers.
	if l.MaxPendingInbound > 0 && inbound >= l.MaxPendingInbound {
		return fmt.Errorf("%w: that user is holding %d unanswered offers", ErrTooManyTransfers, inbound)
	}
	if err := r.allowOffer(t.Sender, l, now); err != nil {
		return err
	}

	r.byID[t.ID] = t
	return nil
}

// allowOffer is a fixed window per Sender. Caller holds the lock.
//
// ponytail: fixed window, so a burst straddling the boundary gets 2×rate. The
// limit exists to stop a loop, not to be exact.
func (r *Registry) allowOffer(sender string, l Limits, now time.Time) error {
	if l.OfferRate <= 0 {
		return nil
	}
	w := r.offers[sender]
	if w == nil || now.Sub(w.since) >= l.OfferRateWindow {
		r.offers[sender] = &rateWindow{count: 1, since: now}
		return nil
	}
	if w.count >= l.OfferRate {
		return fmt.Errorf("%w: %d offers per %s is the limit", ErrRateLimited, l.OfferRate, l.OfferRateWindow)
	}
	w.count++
	return nil
}

// Mutate applies fn to a Transfer with the lock held. That is the whole
// compare-and-swap: two tabs accepting at the same moment are serialised here,
// and exactly one of them sees a nil error.
func (r *Registry) Mutate(id ID, fn func(*Transfer) error) (Transfer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.byID[id]
	if !ok {
		return Transfer{}, ErrNotFound
	}
	if err := fn(t); err != nil {
		return *t, err
	}
	return *t, nil
}

// Get returns a copy.
func (r *Registry) Get(id ID) (Transfer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.byID[id]
	if !ok {
		return Transfer{}, ErrNotFound
	}
	return *t, nil
}

// ForUser is what a snapshot carries: every live Transfer this User is party to,
// plus the ones that ended recently enough that a reconnecting tab still needs
// to hear about them (ADR 0003).
func (r *Registry) ForUser(userID string, now time.Time, keepTerminal time.Duration) []Transfer {
	r.mu.Lock()
	out := []Transfer{}
	for _, t := range r.byID {
		if !t.Party(userID) {
			continue
		}
		if t.IsTerminal() && now.Sub(t.EndedAt) > keepTerminal {
			continue
		}
		out = append(out, *t)
	}
	r.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

// ReapExpired ends what has run out of time and forgets what ended long enough
// ago. It returns everything it made terminal, so the caller can tell both
// parties — an unanswered offer and an abandoned relay are both outcomes somebody
// is waiting to hear about.
func (r *Registry) ReapExpired(now time.Time, keepTerminal time.Duration) []Transfer {
	r.mu.Lock()
	defer r.mu.Unlock()

	var expired []Transfer
	for id, t := range r.byID {
		switch {
		case t.ExpiredAt(now):
			if err := t.Expire(now); err == nil {
				expired = append(expired, *t)
			}
		case t.ResumeExpiredAt(now):
			// Interrupted, and nobody came back for it.
			if err := t.Fail(ReasonSenderDisconnected, t.BytesRelayed, now); err == nil {
				expired = append(expired, *t)
			}
		case t.IsTerminal() && now.Sub(t.EndedAt) > keepTerminal:
			delete(r.byID, id)
		}
	}
	for sender, w := range r.offers {
		if now.Sub(w.since) >= r.rateWindowFloor() {
			delete(r.offers, sender)
		}
	}
	return expired
}

// rateWindowFloor is how long an idle rate-limit entry is kept. It is longer
// than any sane window, and the sweep is what keeps the map from growing with
// every Sender that ever sent one offer.
func (r *Registry) rateWindowFloor() time.Duration { return time.Hour }

// Count is the number of Transfers held, for /metrics.
func (r *Registry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byID)
}
