package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hotpotato/internal/auth"
	"hotpotato/internal/bus"
	"hotpotato/internal/metrics"
	"hotpotato/internal/presence"
	"hotpotato/internal/relay"
	"hotpotato/internal/sse"
	"hotpotato/internal/transfer"
)

// harness is the whole HTTP surface over in-memory stores and an in-memory
// bus: no docker, and every duration is a millisecond.
type harness struct {
	h         http.Handler
	api       *Server
	svc       *auth.Service
	streams   *sse.Registry
	presence  *presence.Memory
	transfers *transfer.Registry
	bus       *bus.Memory
	draining  *atomic.Bool
	srv       *httptest.Server
}

type harnessOpts struct {
	grace            time.Duration
	heartbeat        time.Duration
	rendezvousWait   time.Duration
	progressInterval time.Duration
	readyTimeout     time.Duration
	resumeWindow     time.Duration
	limits           transfer.Limits
	checks           []Check
	now              func() time.Time
}

func newHarness(t *testing.T, tweak ...func(*harnessOpts)) *harness {
	t.Helper()
	o := harnessOpts{
		grace:            20 * time.Millisecond,
		heartbeat:        time.Hour,
		rendezvousWait:   5 * time.Second,
		progressInterval: 20 * time.Millisecond,
		readyTimeout:     2 * time.Second,
		resumeWindow:     0, // most tests want an interruption to be terminal
		limits: transfer.Limits{
			OfferTTL:          time.Minute,
			MaxOutbound:       3,
			MaxPendingInbound: 10,
			MaxPayloadBytes:   10 << 30,
			MaxEntries:        10_000,
			OfferRate:         10,
			OfferRateWindow:   time.Minute,
		},
	}
	for _, fn := range tweak {
		fn(&o)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mem := auth.NewMemory()
	svc := auth.New(auth.Options{
		Users:              mem,
		Sessions:           mem.Sessions(),
		SessionTTL:         time.Hour,
		LoginMaxFailures:   3,
		LoginFailureWindow: time.Minute,
	})

	eventBus := bus.NewMemory(bus.Options{Buffer: 64})
	streams := sse.New(sse.Options{Buffer: 8})
	events, err := eventBus.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go func() {
		for e := range events {
			streams.Deliver(e)
		}
	}()

	p := presence.NewMemory(presence.Options{
		Grace: o.grace,
		Announce: func(name string, u presence.User) {
			e, err := bus.NewEvent(name, nil, u)
			if err != nil {
				return
			}
			eventBus.Publish(ctx, e)
		},
	})
	t.Cleanup(func() { p.Close() })

	// One shared clock, as in production. Most tests set it to an hour so the
	// heartbeat does not turn up in the middle of an assertion.
	heartbeat := sse.NewHeartbeat(o.heartbeat)
	t.Cleanup(heartbeat.Close)

	draining := &atomic.Bool{}
	transfers := transfer.NewRegistry()
	rendezvous := relay.NewRendezvous()
	api := New(Options{
		Auth:          svc,
		Streams:       streams,
		Presence:      p,
		Transfers:     transfers,
		Bus:           eventBus,
		WebUI:         http.NotFoundHandler(),
		Draining:      draining,
		Instance:      "inst-test",
		SessionMaxAge: 3600,
		SSE:           SSEOptions{Retry: 3 * time.Second, WriteDeadline: 2 * time.Second},
		Heartbeat:     heartbeat,
		Metrics: metrics.New(metrics.Options{
			Streams:   func() float64 { return float64(streams.Count()) },
			Transfers: func() float64 { return float64(transfers.Count()) },
			Parked:    func() float64 { return float64(rendezvous.Parked()) },
		}),
		Checks:             o.checks,
		ReadyTimeout:       o.readyTimeout,
		ReadModel:          transfer.NewLocal(transfers, time.Minute),
		Directory:          LocalDirectory{Instance: "inst-test", BaseURL: "http://inst-test.invalid"},
		Limits:             o.limits,
		TerminalWindow:     time.Minute,
		ReadModelTTL:       5 * time.Minute,
		RendezvousWait:     o.rendezvousWait,
		RelayWriteDeadline: 10 * time.Second,
		RelayBuffer:        64 << 10,
		ProgressInterval:   o.progressInterval,
		ResumeWindow:       o.resumeWindow,
		Rendezvous:         rendezvous,
		Now:                o.now,
	})
	return &harness{
		h:         api,
		api:       api,
		svc:       svc,
		streams:   streams,
		presence:  p,
		transfers: transfers,
		bus:       eventBus,
		draining:  draining,
	}
}

// do performs one request against the handler with no network involved.
func (x *harness) do(t *testing.T, method, path, body string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if method != http.MethodGet {
		r.Header.Set("Content-Type", "application/json")
	}
	for _, c := range cookies {
		r.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	x.h.ServeHTTP(rec, r)
	return rec
}

// server starts a real listener. SSE needs one: a ResponseRecorder cannot be
// read while the handler is still writing to it.
func (x *harness) server(t *testing.T) *httptest.Server {
	t.Helper()
	if x.srv == nil {
		x.srv = httptest.NewServer(x.h)
		t.Cleanup(x.srv.Close)
	}
	return x.srv
}

// signup registers a user through the real endpoint and returns its cookie.
func (x *harness) signup(t *testing.T, email, name string) *http.Cookie {
	t.Helper()
	rec := x.do(t, "POST", "/api/signup",
		`{"email":"`+email+`","displayName":"`+name+`","password":"hunter2hunter2"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("signup %s = %d %s", email, rec.Code, rec.Body)
	}
	return session(t, rec)
}

func session(t *testing.T, rec *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range rec.Result().Cookies() {
		if c.Name == auth.CookieName {
			return c
		}
	}
	t.Fatalf("no %s cookie in %v", auth.CookieName, rec.Result().Cookies())
	return nil
}

// frame is one parsed SSE frame.
type frame struct {
	ID      string
	Event   string
	Data    string
	Comment string
}

// stream reads an SSE connection in the background so a test can wait for a
// frame with a deadline rather than blocking on a read.
type stream struct {
	res    *http.Response
	frames chan frame
	fail   chan error
}

func (x *harness) openStream(t *testing.T, cookie *http.Cookie) *stream {
	t.Helper()
	srv := x.server(t)
	return openStreamOn(t, srv, srv.Client(), cookie)
}

func openStreamOn(t *testing.T, srv *httptest.Server, hc *http.Client, cookie *http.Cookie) *stream {
	t.Helper()

	req, err := http.NewRequest("GET", srv.URL+"/events", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(cookie)
	res, err := hc.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		t.Fatalf("GET /events = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache", cc)
	}
	if ab := res.Header.Get("X-Accel-Buffering"); ab != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", ab)
	}

	s := &stream{res: res, frames: make(chan frame, 64), fail: make(chan error, 1)}
	go s.read()
	t.Cleanup(s.close)
	return s
}

func (s *stream) read() {
	defer close(s.frames)
	sc := bufio.NewScanner(s.res.Body)
	var f frame
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if f != (frame{}) {
				s.frames <- f
				f = frame{}
			}
		case strings.HasPrefix(line, ":"):
			f.Comment = line[1:]
		case strings.HasPrefix(line, "id: "):
			f.ID = line[4:]
		case strings.HasPrefix(line, "event: "):
			f.Event = line[7:]
		case strings.HasPrefix(line, "data: "):
			f.Data += line[6:]
		case strings.HasPrefix(line, "retry: "):
			f.Comment = "retry=" + line[7:]
		}
	}
	if err := sc.Err(); err != nil {
		s.fail <- err
	}
}

func (s *stream) close() { s.res.Body.Close() }

// next waits for the next frame that is not a heartbeat or a retry hint.
func (s *stream) next(t *testing.T) frame {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case f, ok := <-s.frames:
			if !ok {
				t.Fatal("the stream closed while waiting for a frame")
			}
			if f.Event == "" {
				continue // heartbeat or retry
			}
			return f
		case err := <-s.fail:
			t.Fatalf("reading the stream: %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for a frame")
		}
	}
}

// await waits for a frame with the given event name, skipping others.
func (s *stream) await(t *testing.T, event string) frame {
	t.Helper()
	for {
		f := s.next(t)
		if f.Event == event {
			return f
		}
	}
}

// awaitUser waits for a presence event about one particular display name,
// skipping the announcements about everyone else — including the reader's own
// arrival, which every Stream sees because presence goes to everyone.
func (s *stream) awaitUser(t *testing.T, event, displayName string) {
	t.Helper()
	for {
		f := s.await(t, event)
		var u presence.User
		if err := json.Unmarshal([]byte(f.Data), &u); err != nil {
			t.Fatalf("%s payload %q: %v", event, f.Data, err)
		}
		if u.DisplayName == displayName {
			return
		}
	}
}

// quiet asserts no frame with this event name arrives within d.
func (s *stream) quiet(t *testing.T, event string, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case f, ok := <-s.frames:
			if !ok {
				return
			}
			if f.Event == event {
				t.Fatalf("unexpected %s: %s", event, f.Data)
			}
		case <-deadline:
			return
		}
	}
}

func decodeSnapshot(t *testing.T, f frame) snapshotView {
	t.Helper()
	var s snapshotView
	if err := json.Unmarshal([]byte(f.Data), &s); err != nil {
		t.Fatalf("snapshot %q: %v", f.Data, err)
	}
	return s
}
