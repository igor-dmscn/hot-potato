package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"hotpotato/internal/transfer"
)

// clock is a hand-cranked time source for the expiry tests.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// pair signs up two Users, opens a Stream for each, and returns both ends.
type pair struct {
	senderCookie, recipientCookie *http.Cookie
	sender, recipient             *stream
	senderID, recipientID         string
	senderStreamID                string
	recipientStreamID             string
}

func newPair(t *testing.T, x *harness) *pair {
	t.Helper()
	p := &pair{
		senderCookie:    x.signup(t, "ana@example.com", "ana"),
		recipientCookie: x.signup(t, "bea@example.com", "bea"),
	}
	p.sender = x.openStream(t, p.senderCookie)
	senderSnap := decodeSnapshot(t, p.sender.await(t, EventSnapshot))
	p.senderID, p.senderStreamID = senderSnap.Self.ID, senderSnap.StreamID

	p.recipient = x.openStream(t, p.recipientCookie)
	recipientSnap := decodeSnapshot(t, p.recipient.await(t, EventSnapshot))
	p.recipientID, p.recipientStreamID = recipientSnap.Self.ID, recipientSnap.StreamID

	// Wait until the Sender can see the Recipient, or the offer races presence.
	p.sender.awaitUser(t, "user.online", "bea")
	return p
}

func (p *pair) offerBody(kind transfer.Kind, bytes int64, entries int) string {
	return fmt.Sprintf(`{"to":%q,"name":"docs","kind":%q,"totalBytes":%d,"entryCount":%d}`,
		p.recipientID, kind, bytes, entries)
}

// offer proposes one Transfer and returns its ID.
func (p *pair) offer(t *testing.T, x *harness) transfer.ID {
	t.Helper()
	rec := x.do(t, "POST", "/api/transfers", p.offerBody(transfer.KindFile, 1024, 1), p.senderCookie)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/transfers = %d %s, want 201", rec.Code, rec.Body)
	}
	var created struct {
		ID        transfer.ID `json:"id"`
		ExpiresAt time.Time   `json:"expiresAt"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("create body: %v", err)
	}
	if created.ID == "" || created.ExpiresAt.IsZero() {
		t.Fatalf("create returned %+v", created)
	}
	if transfer.OwnerOf(created.ID) != "inst-test" {
		t.Errorf("ID %q does not carry this instance as its owner", created.ID)
	}
	return created.ID
}

func decodeTransfer(t *testing.T, f frame) transfer.Transfer {
	t.Helper()
	var tr transfer.Transfer
	if err := json.Unmarshal([]byte(f.Data), &tr); err != nil {
		t.Fatalf("transfer payload %q: %v", f.Data, err)
	}
	return tr
}

func TestOfferReachesBothSides(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	id := p.offer(t, x)

	// The Sender's own tabs see it, so every tab shows the same outbound list.
	created := decodeTransfer(t, p.sender.await(t, EventTransferCreated))
	if created.ID != id || created.State != transfer.StatePending {
		t.Errorf("transfer.created = %+v", created)
	}
	offered := decodeTransfer(t, p.recipient.await(t, EventTransferOffered))
	if offered.ID != id || offered.Payload.Name != "docs" {
		t.Errorf("transfer.offered = %+v", offered)
	}
}

func TestOfferToSomebodyOffline(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	ana := x.signup(t, "ana@example.com", "ana")

	body := `{"to":"u_nobody","name":"docs","kind":"file","totalBytes":1024,"entryCount":1}`
	if rec := x.do(t, "POST", "/api/transfers", body, ana); rec.Code != http.StatusNotFound {
		t.Fatalf("offer to an absent User = %d %s, want 404", rec.Code, rec.Body)
	}
}

func TestDenyReachesTheSender(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)
	id := p.offer(t, x)

	rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/deny", "", p.recipientCookie)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("deny = %d %s, want 204", rec.Code, rec.Body)
	}

	f := p.sender.await(t, EventTransferDenied)
	var payload struct{ ID transfer.ID }
	json.Unmarshal([]byte(f.Data), &payload)
	if payload.ID != id {
		t.Errorf("transfer.denied carried %q, wanted %q", payload.ID, id)
	}
	// And the Recipient's other tabs are told too, so the prompt clears.
	p.recipient.await(t, EventTransferDenied)
}

// Two tabs of the Recipient race. Exactly one wins, and the announcement says
// which Stream did, so the loser can clear its prompt.
func TestTwoTabsAcceptAndExactlyOneWins(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	secondTab := x.openStream(t, p.recipientCookie)
	secondSnap := decodeSnapshot(t, secondTab.await(t, EventSnapshot))
	id := p.offer(t, x)

	type result struct {
		code int
		body string
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, streamID := range []string{p.recipientStreamID, secondSnap.StreamID} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/accept",
				fmt.Sprintf(`{"streamId":%q}`, streamID), p.recipientCookie)
			results[i] = result{rec.Code, rec.Body.String()}
		}()
	}
	close(start)
	wg.Wait()

	accepted, conflicted := 0, 0
	for _, r := range results {
		switch r.code {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicted++
			if !strings.Contains(r.body, codeIllegalState) {
				t.Errorf("the losing tab got %s, want illegal_state", r.body)
			}
		default:
			t.Errorf("unexpected status %d: %s", r.code, r.body)
		}
	}
	if accepted != 1 || conflicted != 1 {
		t.Fatalf("%d accepted and %d conflicted, want exactly one of each", accepted, conflicted)
	}

	var announced struct {
		ID       transfer.ID `json:"id"`
		ByStream string      `json:"byStream"`
	}
	json.Unmarshal([]byte(p.sender.await(t, EventTransferAccepted).Data), &announced)
	if announced.ID != id || announced.ByStream == "" {
		t.Errorf("transfer.accepted = %+v, want the winning stream", announced)
	}
}

// The accepting Stream is a hint for the caller's own other tabs, not an
// authorization input — the instance that owns the Transfer has usually never
// heard of it. Only its length is checked, because it is echoed into an event.
func TestAcceptBoundsTheStreamID(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	id := p.offer(t, x)
	rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/accept",
		fmt.Sprintf(`{"streamId":%q}`, strings.Repeat("s", maxStreamIDLength+1)), p.recipientCookie)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("accept with an over-long streamId = %d %s, want 400", rec.Code, rec.Body)
	}

	// A Stream belonging to another of this User's tabs, on another instance,
	// is a perfectly ordinary value.
	id = p.offer(t, x)
	rec = x.do(t, "POST", "/api/transfers/"+string(id)+"/accept",
		`{"streamId":"s_ONANOTHERINSTANCE"}`, p.recipientCookie)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("accept with an unknown streamId = %d %s, want 202", rec.Code, rec.Body)
	}
}

// A Transfer ID is not an authorization token: 404 when it does not exist, 403
// when it exists and is not yours.
func TestUnknownAndSomebodyElsesTransfer(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)
	id := p.offer(t, x)
	stranger := x.signup(t, "cara@example.com", "cara")

	if rec := x.do(t, "POST", "/api/transfers/inst-test.deadbeef/deny", "", p.recipientCookie); rec.Code != http.StatusNotFound {
		t.Errorf("deny of an unknown ID = %d, want 404", rec.Code)
	}
	if rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/deny", "", stranger); rec.Code != http.StatusForbidden {
		t.Errorf("a stranger denying = %d, want 403", rec.Code)
	}
	if rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/cancel", "", stranger); rec.Code != http.StatusForbidden {
		t.Errorf("a stranger cancelling = %d, want 403", rec.Code)
	}
	// The Sender denying their own offer is forbidden, not merely illegal.
	if rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/deny", "", p.senderCookie); rec.Code != http.StatusForbidden {
		t.Errorf("the sender denying = %d, want 403", rec.Code)
	}
}

func TestEitherPartyMayCancel(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	for name, cookie := range map[string]*http.Cookie{
		"sender":    p.senderCookie,
		"recipient": p.recipientCookie,
	} {
		id := p.offer(t, x)
		rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/cancel", "", cookie)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s cancelling = %d %s, want 204", name, rec.Code, rec.Body)
		}
		p.sender.await(t, EventTransferCanceled)
		p.recipient.await(t, EventTransferCanceled)
	}
}

// An unanswered offer expires and both parties are told, as transfer.failed
// with reason offer_expired — there is no separate expired event.
func TestReaperExpiresUnansweredOffers(t *testing.T) {
	t.Parallel()
	clk := &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	x := newHarness(t, func(o *harnessOpts) { o.now = clk.now })
	p := newPair(t, x)
	id := p.offer(t, x)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go x.api.Reap(ctx, 5*time.Millisecond, time.Minute)

	// Nothing happens inside the TTL.
	p.recipient.quiet(t, EventTransferFailed, 50*time.Millisecond)

	clk.advance(2 * time.Minute)

	for _, s := range []*stream{p.sender, p.recipient} {
		var payload struct {
			ID     transfer.ID `json:"id"`
			Reason string      `json:"reason"`
		}
		json.Unmarshal([]byte(s.await(t, EventTransferFailed).Data), &payload)
		if payload.ID != id || payload.Reason != transfer.ReasonOfferExpired {
			t.Errorf("transfer.failed = %+v, want %s for %s", payload, transfer.ReasonOfferExpired, id)
		}
	}

	// And accepting it afterwards is a conflict, not a surprise.
	rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/accept",
		fmt.Sprintf(`{"streamId":%q}`, p.recipientStreamID), p.recipientCookie)
	if rec.Code != http.StatusConflict {
		t.Errorf("accepting an expired offer = %d %s, want 409", rec.Code, rec.Body)
	}
}

func TestOfferLimits(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) { o.limits.MaxOutbound = 2 })
	p := newPair(t, x)

	p.offer(t, x)
	p.offer(t, x)
	rec := x.do(t, "POST", "/api/transfers", p.offerBody(transfer.KindFile, 1024, 1), p.senderCookie)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("a third concurrent offer = %d %s, want 429", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), codeTooManyTransfers) {
		t.Errorf("body = %s, want %s", rec.Body, codeTooManyTransfers)
	}
}

func TestOfferRejectsAnImpossiblePayload(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) { o.limits.MaxPayloadBytes = 1 << 20 })
	p := newPair(t, x)

	for name, tc := range map[string]struct {
		body string
		want int
	}{
		"too large":    {p.offerBody(transfer.KindFile, 2<<20, 1), http.StatusRequestEntityTooLarge},
		"no bytes":     {p.offerBody(transfer.KindFile, 0, 1), http.StatusBadRequest},
		"unknown kind": {p.offerBody("magnet", 1024, 1), http.StatusBadRequest},
		"empty folder": {p.offerBody(transfer.KindFolder, 1024, 0), http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			rec := x.do(t, "POST", "/api/transfers", tc.body, p.senderCookie)
			if rec.Code != tc.want {
				t.Errorf("= %d %s, want %d", rec.Code, rec.Body, tc.want)
			}
		})
	}
}

// A tab that reconnects mid-transfer has to find it in the snapshot, not have
// missed it.
func TestSnapshotCarriesTheUsersTransfers(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)
	id := p.offer(t, x)

	reconnected := x.openStream(t, p.recipientCookie)
	snap := decodeSnapshot(t, reconnected.await(t, EventSnapshot))
	if len(snap.Transfers) != 1 || snap.Transfers[0].ID != id {
		t.Fatalf("snapshot transfers = %+v, want the pending offer", snap.Transfers)
	}

	// And a stranger sees none of it.
	stranger := x.signup(t, "cara@example.com", "cara")
	strangerSnap := decodeSnapshot(t, x.openStream(t, stranger).await(t, EventSnapshot))
	if len(strangerSnap.Transfers) != 0 {
		t.Errorf("a stranger's snapshot = %+v, want no transfers", strangerSnap.Transfers)
	}
}
