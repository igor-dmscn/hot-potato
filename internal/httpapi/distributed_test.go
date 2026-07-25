package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"hotpotato/internal/auth"
	"hotpotato/internal/bus"
	"hotpotato/internal/mirror"
	"hotpotato/internal/presence"
	"hotpotato/internal/relay"
	"hotpotato/internal/sse"
	"hotpotato/internal/transfer"
)

// The distributed suite. Two instances in one process, sharing a real Redis and
// a real NATS, which is what two containers behind a proxy amount to.
//
// These are not parallel and they flush the Redis database, so they get their
// own variables rather than the server's: pointing a suite that truncates
// things at a live store should stay a deliberate act.
func requireBrokers(t *testing.T) (redisURL, natsURL string) {
	t.Helper()
	redisURL, natsURL = os.Getenv("HP_TEST_REDIS_URL"), os.Getenv("HP_TEST_NATS_URL")
	if redisURL == "" || natsURL == "" {
		t.Skip("HP_TEST_REDIS_URL and HP_TEST_NATS_URL not set; start compose to run the distributed suite")
	}
	return redisURL, natsURL
}

// node is one instance.
type node struct {
	id        string
	api       *Server
	srv       *httptest.Server
	streams   *sse.Registry
	presence  presence.Presence
	transfers *transfer.Registry
	readModel transfer.ReadModel
}

type cluster struct {
	nodes []*node
	users *auth.Memory
	rdb   *redis.Client
}

func (c *cluster) node(id string) *node {
	for _, n := range c.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

// newCluster builds instances that share one Redis, one NATS subject and one
// identity store.
func newCluster(t *testing.T, ids ...string) *cluster {
	t.Helper()
	redisURL, natsURL := requireBrokers(t)

	ctx := context.Background()
	rdb, err := mirror.Open(ctx, redisURL)
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	// A clean slate, and no leftovers for the next suite.
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	t.Cleanup(func() {
		rdb.FlushDB(context.Background())
		rdb.Close()
	})

	// A fresh subject per test, so two of these running back to back do not hear
	// each other.
	subject := "hp.test." + strings.ToLower(rand.Text()[:10])
	c := &cluster{users: auth.NewMemory(), rdb: rdb}
	for _, id := range ids {
		c.nodes = append(c.nodes, newNode(t, id, rdb, natsURL, subject, c.users))
	}
	return c
}

func newNode(t *testing.T, id string, rdb *redis.Client, natsURL, subject string, users *auth.Memory) *node {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	svc := auth.New(auth.Options{
		Users:              users,
		Sessions:           users.Sessions(),
		SessionTTL:         time.Hour,
		LoginMaxFailures:   20,
		LoginFailureWindow: time.Minute,
	})

	eventBus, err := bus.NewNATS(natsURL, subject, bus.Options{Buffer: 128})
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(func() { eventBus.Close() })

	streams := sse.New(sse.Options{Buffer: 32})
	events, err := eventBus.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	go func() {
		for e := range events {
			streams.Deliver(e)
		}
	}()

	p := presence.NewRedis(rdb, presence.RedisOptions{
		Options: presence.Options{
			Grace: 50 * time.Millisecond,
			Announce: func(name string, u presence.User) {
				e, err := bus.NewEvent(name, nil, u)
				if err == nil {
					eventBus.Publish(ctx, e)
				}
			},
		},
		Instance: id,
		TTL:      2 * time.Second,
		Refresh:  200 * time.Millisecond,
	})
	t.Cleanup(func() { p.Close() })

	transfers := transfer.NewRegistry()
	instances := mirror.NewInstances(rdb)
	readModel := mirror.NewTransfers(rdb)

	api := New(Options{
		Auth:          svc,
		Streams:       streams,
		Presence:      p,
		Transfers:     transfers,
		ReadModel:     readModel,
		Rendezvous:    relay.NewRendezvous(),
		Directory:     instances,
		Bus:           eventBus,
		WebUI:         http.NotFoundHandler(),
		Draining:      &atomic.Bool{},
		Instance:      id,
		SessionMaxAge: 3600,
		SSE:           SSEOptions{Heartbeat: time.Hour, Retry: time.Second, WriteDeadline: 10 * time.Second},
		Limits: transfer.Limits{
			OfferTTL:          time.Minute,
			MaxOutbound:       3,
			MaxPendingInbound: 10,
			MaxPayloadBytes:   1 << 30,
			MaxEntries:        1000,
			OfferRate:         100,
			OfferRateWindow:   time.Minute,
		},
		TerminalWindow:     time.Minute,
		RendezvousWait:     5 * time.Second,
		RelayWriteDeadline: 10 * time.Second,
		RelayBuffer:        64 << 10,
		ProgressInterval:   50 * time.Millisecond,
		ReadModelTTL:       5 * time.Minute,
	})

	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	// The URL only exists once the listener does, so registration happens here
	// rather than inside New.
	if err := instances.Register(ctx, id, srv.URL, 30*time.Second); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}

	return &node{id: id, api: api, srv: srv, streams: streams, presence: p, transfers: transfers, readModel: readModel}
}

// user is a client with a cookie jar, which is what makes following a 307 across
// instances work at all.
type user struct {
	hc     *http.Client
	cookie *http.Cookie
	id     string
}

func (c *cluster) signup(t *testing.T, n *node, email, name string) *user {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar: %v", err)
	}
	hc := &http.Client{Jar: jar}

	body := fmt.Sprintf(`{"email":%q,"displayName":%q,"password":"hunter2hunter2"}`, email, name)
	req, _ := http.NewRequest("POST", n.srv.URL+"/api/signup", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res, err := hc.Do(req)
	if err != nil {
		t.Fatalf("signup: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("signup = %d", res.StatusCode)
	}
	var dto userDTO
	json.NewDecoder(res.Body).Decode(&dto)

	var cookie *http.Cookie
	for _, ck := range res.Cookies() {
		if ck.Name == auth.CookieName {
			cookie = ck
		}
	}
	if cookie == nil {
		t.Fatal("no session cookie")
	}
	return &user{hc: hc, cookie: cookie, id: dto.ID}
}

// post sends a JSON request and follows whatever redirect comes back.
func (u *user) post(t *testing.T, n *node, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("POST", n.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(u.cookie)
	res, err := u.hc.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return res
}

// A whole Transfer where the two parties are connected to different instances:
// the offer is owned by one, and every request the other makes about it is
// redirected there.
func TestCrossInstanceTransfer(t *testing.T) {
	c := newCluster(t, "inst-a", "inst-b")
	a, b := c.node("inst-a"), c.node("inst-b")

	sender := c.signup(t, a, "ana@example.com", "ana")
	recipient := c.signup(t, b, "bea@example.com", "bea")

	senderStream := openStreamOn(t, a.srv, sender.hc, sender.cookie)
	senderSnap := decodeSnapshot(t, senderStream.await(t, EventSnapshot))
	if senderSnap.Instance != "inst-a" {
		t.Fatalf("the sender's snapshot says instance %q", senderSnap.Instance)
	}
	recipientStream := openStreamOn(t, b.srv, recipient.hc, recipient.cookie)
	recipientSnap := decodeSnapshot(t, recipientStream.await(t, EventSnapshot))
	if recipientSnap.Instance != "inst-b" {
		t.Fatalf("the recipient's snapshot says instance %q", recipientSnap.Instance)
	}

	// Presence is shared, so the Sender can see somebody on another instance.
	senderStream.awaitUser(t, presence.EventOnline, "bea")

	parts := []part{pattern("report.bin", 2<<20)}
	body := fmt.Sprintf(`{"to":%q,"name":"report.bin","kind":"file","totalBytes":%d,"entryCount":1}`,
		recipient.id, sum(parts))
	res := sender.post(t, a, "/api/transfers", body)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("offer = %d", res.StatusCode)
	}
	var created struct {
		ID transfer.ID `json:"id"`
	}
	json.NewDecoder(res.Body).Decode(&created)
	res.Body.Close()
	if transfer.OwnerOf(created.ID) != "inst-a" {
		t.Fatalf("the transfer is owned by %q, want inst-a", transfer.OwnerOf(created.ID))
	}

	// The offer reaches the other instance's Stream over the bus.
	recipientStream.await(t, EventTransferOffered)

	// Accept against inst-b, which does not own it: 307, followed by the client.
	res = recipient.post(t, b, "/api/transfers/"+string(created.ID)+"/accept",
		fmt.Sprintf(`{"streamId":%q}`, recipientSnap.StreamID))
	res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("accept via the wrong instance = %d, want 202 after the redirect", res.StatusCode)
	}
	if !strings.HasPrefix(res.Request.URL.String(), a.srv.URL) {
		t.Errorf("the accept was answered by %s, want it redirected to %s", res.Request.URL, a.srv.URL)
	}
	senderStream.await(t, EventTransferAccepted)

	// The download is requested from inst-b too, and lands on inst-a.
	got := startDownload(t, b.srv, recipient.hc, recipient.cookie, created.ID, downloadOptions{})
	senderStream.await(t, EventTransferReady)

	up, err := upload(t, a.srv, sender.hc, sender.cookie, created.ID, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	up.Body.Close()
	if up.StatusCode != http.StatusNoContent {
		t.Fatalf("upload = %d, want 204", up.StatusCode)
	}

	r := <-got
	if r.err != nil {
		t.Fatalf("download: %v", r.err)
	}
	if r.bytes != sum(parts) {
		t.Fatalf("received %d bytes, want %d", r.bytes, sum(parts))
	}
	if want := hashOf(parts); r.sha != want {
		t.Errorf("sha256 = %s, want %s", r.sha, want)
	}

	// Both parties are told, from whichever instance they are attached to.
	senderStream.await(t, EventTransferCompleted)
	recipientStream.await(t, EventTransferCompleted)
}

// 307 specifically, and with the whole path and query preserved: 302 is allowed
// to turn a POST into a GET, which would silently drop an upload's body.
func TestWrongInstanceIsAnsweredWith307(t *testing.T) {
	c := newCluster(t, "inst-a", "inst-b")
	a, b := c.node("inst-a"), c.node("inst-b")
	u := c.signup(t, a, "ana@example.com", "ana")

	// A client that reports the redirect instead of following it.
	noFollow := &http.Client{
		Jar:           u.hc.Jar,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	for _, path := range []string{
		"/api/transfers/inst-a.deadbeef/accept",
		"/api/transfers/inst-a.deadbeef/cancel",
		"/d/inst-a.deadbeef",
	} {
		method := "POST"
		if strings.HasPrefix(path, "/d/") {
			method = "GET"
		}
		req, _ := http.NewRequest(method, b.srv.URL+path, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(u.cookie)
		res, err := noFollow.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		res.Body.Close()

		if res.StatusCode != http.StatusTemporaryRedirect {
			t.Errorf("%s %s from the wrong instance = %d, want 307", method, path, res.StatusCode)
		}
		if want := a.srv.URL + path; res.Header.Get("Location") != want {
			t.Errorf("Location = %q, want %q", res.Header.Get("Location"), want)
		}
	}
}

// The trap in ADR 0007's redirect: net/http follows a 307 only when it can
// replay the body. A streaming upload has no GetBody, so the client is handed
// the 307 and has to deal with it — which is why spud does exactly that.
func TestAStreamingUploadDoesNotFollowIts307(t *testing.T) {
	c := newCluster(t, "inst-a", "inst-b")
	a, b := c.node("inst-a"), c.node("inst-b")
	sender := c.signup(t, a, "ana@example.com", "ana")
	recipient := c.signup(t, b, "bea@example.com", "bea")

	senderStream := openStreamOn(t, a.srv, sender.hc, sender.cookie)
	senderStream.await(t, EventSnapshot)
	recipientStream := openStreamOn(t, b.srv, recipient.hc, recipient.cookie)
	recipientSnap := decodeSnapshot(t, recipientStream.await(t, EventSnapshot))
	senderStream.awaitUser(t, presence.EventOnline, "bea")

	parts := []part{pattern("report.bin", 1<<20)}
	body := fmt.Sprintf(`{"to":%q,"name":"report.bin","kind":"file","totalBytes":%d,"entryCount":1}`,
		recipient.id, sum(parts))
	res := sender.post(t, a, "/api/transfers", body)
	var created struct {
		ID transfer.ID `json:"id"`
	}
	json.NewDecoder(res.Body).Decode(&created)
	res.Body.Close()
	recipientStream.await(t, EventTransferOffered)

	res = recipient.post(t, b, "/api/transfers/"+string(created.ID)+"/accept",
		fmt.Sprintf(`{"streamId":%q}`, recipientSnap.StreamID))
	res.Body.Close()
	got := startDownload(t, b.srv, recipient.hc, recipient.cookie, created.ID, downloadOptions{})
	senderStream.await(t, EventTransferReady)

	// Upload to inst-b, which does not own it. The body is an io.Pipe.
	up, err := upload(t, b.srv, sender.hc, sender.cookie, created.ID, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	up.Body.Close()
	if up.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("a streaming POST to the wrong instance = %d, want the 307 handed back unfollowed",
			up.StatusCode)
	}
	if !strings.HasPrefix(up.Header.Get("Location"), a.srv.URL) {
		t.Errorf("Location = %q, want inst-a", up.Header.Get("Location"))
	}

	// Doing what a client has to do: repeat the request against the owner, with a
	// fresh body.
	up, err = upload(t, a.srv, sender.hc, sender.cookie, created.ID, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("upload to the owner: %v", err)
	}
	up.Body.Close()
	if up.StatusCode != http.StatusNoContent {
		t.Fatalf("upload to the owner = %d, want 204", up.StatusCode)
	}
	if r := <-got; r.bytes != sum(parts) {
		t.Errorf("received %d bytes, want %d", r.bytes, sum(parts))
	}
}

// An instance that is gone takes its Transfers with it, by design (ADR 0007).
func TestUnknownOwnerIsNotFound(t *testing.T) {
	c := newCluster(t, "inst-a")
	a := c.node("inst-a")
	u := c.signup(t, a, "ana@example.com", "ana")

	res := u.post(t, a, "/api/transfers/inst-gone.deadbeef/cancel", "{}")
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("a transfer owned by a dead instance = %d, want 404", res.StatusCode)
	}
}

// Presence is one key per (User, instance) with a TTL, so a User is visible from
// everywhere and a dead instance needs no cleanup.
func TestPresenceIsSharedAcrossInstances(t *testing.T) {
	c := newCluster(t, "inst-a", "inst-b")
	a, b := c.node("inst-a"), c.node("inst-b")
	ana := c.signup(t, a, "ana@example.com", "ana")

	stream := openStreamOn(t, a.srv, ana.hc, ana.cookie)
	stream.await(t, EventSnapshot)

	// The other instance can see her without being told anything.
	waitFor(t, 3*time.Second, func() bool {
		users, err := b.presence.List(context.Background())
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		return len(users) == 1 && users[0].DisplayName == "ana"
	}, "inst-b never saw ana")

	// She leaves; after the grace window her claim is dropped and inst-b's view
	// catches up.
	stream.close()
	waitFor(t, 3*time.Second, func() bool {
		users, _ := b.presence.List(context.Background())
		return len(users) == 0
	}, "inst-b still lists ana after she left")
}

// Presence survives Redis losing everything: the refresher re-registers.
func TestPresenceSurvivesAnEmptyRedis(t *testing.T) {
	c := newCluster(t, "inst-a")
	a := c.node("inst-a")
	ana := c.signup(t, a, "ana@example.com", "ana")

	stream := openStreamOn(t, a.srv, ana.hc, ana.cookie)
	stream.await(t, EventSnapshot)
	waitFor(t, 3*time.Second, func() bool {
		users, _ := a.presence.List(context.Background())
		return len(users) == 1
	}, "ana never appeared")

	if err := c.rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Nothing re-announces and nothing retries explicitly; the refresh ticker is
	// the whole recovery mechanism.
	waitFor(t, 3*time.Second, func() bool {
		users, _ := a.presence.List(context.Background())
		return len(users) == 1
	}, "presence did not come back after the store was emptied")
}

// A snapshot has to include Transfers owned by other instances, which is the
// only reason the read model exists.
func TestSnapshotIncludesATransferOwnedElsewhere(t *testing.T) {
	c := newCluster(t, "inst-a", "inst-b")
	a, b := c.node("inst-a"), c.node("inst-b")
	sender := c.signup(t, a, "ana@example.com", "ana")
	recipient := c.signup(t, b, "bea@example.com", "bea")

	senderStream := openStreamOn(t, a.srv, sender.hc, sender.cookie)
	senderStream.await(t, EventSnapshot)
	first := openStreamOn(t, b.srv, recipient.hc, recipient.cookie)
	first.await(t, EventSnapshot)
	senderStream.awaitUser(t, presence.EventOnline, "bea")

	body := fmt.Sprintf(`{"to":%q,"name":"docs","kind":"file","totalBytes":1024,"entryCount":1}`, recipient.id)
	res := sender.post(t, a, "/api/transfers", body)
	var created struct {
		ID transfer.ID `json:"id"`
	}
	json.NewDecoder(res.Body).Decode(&created)
	res.Body.Close()

	// A fresh tab on the instance that does not own it.
	reconnected := openStreamOn(t, b.srv, recipient.hc, recipient.cookie)
	snap := decodeSnapshot(t, reconnected.await(t, EventSnapshot))
	if len(snap.Transfers) != 1 || snap.Transfers[0].ID != created.ID {
		t.Fatalf("snapshot from inst-b = %+v, want the transfer owned by inst-a", snap.Transfers)
	}
	if snap.Transfers[0].Payload.Name != "docs" {
		t.Errorf("the mirrored transfer lost its payload: %+v", snap.Transfers[0])
	}
}

// waitFor polls a condition to a deadline. Used only for state that lives in
// another process — everything in-process is signalled on a channel.
func waitFor(t *testing.T, within time.Duration, ok func() bool, complaint string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(complaint)
}
