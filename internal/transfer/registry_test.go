package transfer

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func limits() Limits {
	return Limits{
		OfferTTL:          time.Minute,
		MaxOutbound:       3,
		MaxPendingInbound: 10,
		MaxPayloadBytes:   10 << 30,
		MaxEntries:        10_000,
		OfferRate:         10,
		OfferRateWindow:   time.Minute,
	}
}

func offer(from, to string) *Transfer {
	return New(NewID("inst-a"), from, to,
		Payload{Name: "docs", Kind: KindFile, TotalBytes: total}, now, time.Minute)
}

func TestCreateEnforcesOutboundLimit(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	l := limits()

	for i := range l.MaxOutbound {
		if err := r.Create(offer(sender, recipient), l, now); err != nil {
			t.Fatalf("offer %d: %v", i, err)
		}
	}
	err := r.Create(offer(sender, recipient), l, now)
	if !errors.Is(err, ErrTooManyTransfers) {
		t.Fatalf("the fourth outbound offer = %v, want ErrTooManyTransfers", err)
	}

	// A terminal Transfer does not count against the limit.
	if _, err := r.Mutate(firstID(r), func(tr *Transfer) error { return tr.Deny(recipient, now) }); err != nil {
		t.Fatalf("Deny: %v", err)
	}
	if err := r.Create(offer(sender, recipient), l, now); err != nil {
		t.Errorf("after a denial there is room again, but: %v", err)
	}
}

func TestCreateEnforcesInboundPendingLimit(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	l := limits()
	l.MaxOutbound = 0 // not what is under test here
	l.MaxPendingInbound = 2

	// Two different Senders, so this is the inbound limit and not the outbound.
	if err := r.Create(offer("u_a", recipient), l, now); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(offer("u_b", recipient), l, now); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(offer("u_c", recipient), l, now); !errors.Is(err, ErrTooManyTransfers) {
		t.Fatalf("a third unanswered offer = %v, want ErrTooManyTransfers", err)
	}
	// Someone else's inbox is unaffected.
	if err := r.Create(offer("u_c", "u_other"), l, now); err != nil {
		t.Errorf("an offer to a different Recipient: %v", err)
	}
}

func TestCreateRateLimitsOffers(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	l := limits()
	l.MaxOutbound, l.MaxPendingInbound = 0, 0
	l.OfferRate = 3

	for i := range 3 {
		if err := r.Create(offer(sender, recipient), l, now); err != nil {
			t.Fatalf("offer %d: %v", i, err)
		}
	}
	if err := r.Create(offer(sender, recipient), l, now); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("the fourth offer inside the window = %v, want ErrRateLimited", err)
	}
	// Another Sender has their own bucket.
	if err := r.Create(offer("u_other", recipient), l, now); err != nil {
		t.Errorf("a different Sender: %v", err)
	}
	// And the window moves on.
	if err := r.Create(offer(sender, recipient), l, now.Add(l.OfferRateWindow)); err != nil {
		t.Errorf("after the window: %v", err)
	}
}

func TestPayloadValidate(t *testing.T) {
	t.Parallel()
	l := limits()

	for name, tc := range map[string]struct {
		p    Payload
		want error
	}{
		"a file":                 {Payload{Name: "a.bin", Kind: KindFile, TotalBytes: 1}, nil},
		"a folder":               {Payload{Name: "docs", Kind: KindFolder, TotalBytes: 1, EntryCount: 12}, nil},
		"no kind":                {Payload{Name: "a", TotalBytes: 1}, ErrInvalidPayload},
		"no name":                {Payload{Kind: KindFile, TotalBytes: 1}, ErrInvalidPayload},
		"path in name":           {Payload{Name: "../../etc/passwd", Kind: KindFile, TotalBytes: 1}, ErrInvalidPayload},
		"backslash in name":      {Payload{Name: `c:\secrets`, Kind: KindFile, TotalBytes: 1}, ErrInvalidPayload},
		"newline in name":        {Payload{Name: "a\nb", Kind: KindFile, TotalBytes: 1}, ErrInvalidPayload},
		"dotdot name":            {Payload{Name: "..", Kind: KindFile, TotalBytes: 1}, ErrInvalidPayload},
		"empty payload":          {Payload{Name: "a", Kind: KindFile}, ErrInvalidPayload},
		"too large":              {Payload{Name: "a", Kind: KindFile, TotalBytes: l.MaxPayloadBytes + 1}, ErrPayloadTooLarge},
		"file with two entries":  {Payload{Name: "a", Kind: KindFile, TotalBytes: 1, EntryCount: 2}, ErrInvalidPayload},
		"folder with no entries": {Payload{Name: "a", Kind: KindFolder, TotalBytes: 1}, ErrInvalidPayload},
		"folder with too many":   {Payload{Name: "a", Kind: KindFolder, TotalBytes: 1, EntryCount: l.MaxEntries + 1}, ErrInvalidPayload},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := tc.p.Validate(l)
			if tc.want == nil && err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want %v", err, tc.want)
			}
		})
	}
}

// Two tabs of the Recipient accept the same offer at the same moment. Real
// goroutines, because the serialisation is the thing under test.
func TestConcurrentAcceptsYieldExactlyOneWinner(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	tr := offer(sender, recipient)
	if err := r.Create(tr, limits(), now); err != nil {
		t.Fatal(err)
	}

	const tabs = 8
	var wg sync.WaitGroup
	errs := make([]error, tabs)
	start := make(chan struct{})
	for i := range tabs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = r.Mutate(tr.ID, func(x *Transfer) error {
				return x.Accept(recipient, "s_"+string(rune('a'+i)), now)
			})
		}()
	}
	close(start)
	wg.Wait()

	won := 0
	for i, err := range errs {
		switch {
		case err == nil:
			won++
		default:
			var illegal IllegalTransitionError
			if !errors.As(err, &illegal) {
				t.Errorf("tab %d lost with %v, want an IllegalTransitionError", i, err)
			}
		}
	}
	if won != 1 {
		t.Fatalf("%d of %d tabs accepted successfully, want exactly 1", won, tabs)
	}

	got, err := r.Get(tr.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateAccepted || got.AcceptedByStream == "" {
		t.Errorf("transfer = %+v, want accepted with a winning stream", got)
	}
}

func TestMutateUnknownID(t *testing.T) {
	t.Parallel()
	r := NewRegistry()

	if _, err := r.Mutate("inst-a.nope", func(*Transfer) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Errorf("Mutate on an unknown ID = %v, want ErrNotFound", err)
	}
	if _, err := r.Get("inst-a.nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get on an unknown ID = %v, want ErrNotFound", err)
	}
}

func TestForUserIncludesRecentlyEndedTransfers(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	l := limits()
	window := time.Minute

	live := offer(sender, recipient)
	ended := offer(sender, recipient)
	other := offer("u_x", "u_y")
	for _, tr := range []*Transfer{live, ended, other} {
		if err := r.Create(tr, l, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Mutate(ended.ID, func(x *Transfer) error { return x.Deny(recipient, now) }); err != nil {
		t.Fatal(err)
	}

	// Just after the denial, both are the Sender's business.
	if got := r.ForUser(sender, now, window); len(got) != 2 {
		t.Errorf("ForUser = %d transfers, want the live one and the denied one", len(got))
	}
	// Once the window has passed, only the live one.
	late := now.Add(2 * window)
	got := r.ForUser(sender, late, window)
	if len(got) != 1 || got[0].ID != live.ID {
		t.Errorf("ForUser after the window = %+v, want only the live transfer", got)
	}
	// A stranger's Transfer is nobody else's business.
	if got := r.ForUser("u_stranger", now, window); len(got) != 0 {
		t.Errorf("ForUser for a stranger = %+v, want nothing", got)
	}
}

func TestReapExpiresOffersAndForgetsOldOnes(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	l := limits()

	unanswered := offer(sender, recipient)
	accepted := offer(sender, "u_other")
	if err := r.Create(unanswered, l, now); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(accepted, l, now); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Mutate(accepted.ID, func(x *Transfer) error { return x.Accept("u_other", "s_1", now) }); err != nil {
		t.Fatal(err)
	}

	// Inside the TTL, nothing happens.
	if got := r.ReapExpired(now.Add(30*time.Second), time.Minute); len(got) != 0 {
		t.Fatalf("reaped %+v inside the TTL", got)
	}

	past := now.Add(2 * time.Minute)
	got := r.ReapExpired(past, time.Minute)
	if len(got) != 1 || got[0].ID != unanswered.ID {
		t.Fatalf("reaped %+v, want the unanswered offer", got)
	}
	if got[0].State != StateExpired {
		t.Errorf("state = %s, want expired", got[0].State)
	}
	// An accepted Transfer has no deadline.
	if after, _ := r.Get(accepted.ID); after.State != StateAccepted {
		t.Errorf("the accepted transfer became %s", after.State)
	}

	// A second sweep, well past the terminal window, forgets it entirely.
	r.ReapExpired(past.Add(2*time.Minute), time.Minute)
	if _, err := r.Get(unanswered.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("the expired transfer is still held: %v", err)
	}
	if r.Count() != 1 {
		t.Errorf("Count = %d, want only the accepted transfer", r.Count())
	}
}

func firstID(r *Registry) ID {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range r.byID {
		return id
	}
	return ""
}
