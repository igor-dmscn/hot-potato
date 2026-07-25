package main

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"hotpotato/internal/auth"
	"hotpotato/internal/bus"
	"hotpotato/internal/httpapi"
	"hotpotato/internal/presence"
	"hotpotato/internal/relay"
	"hotpotato/internal/sse"
	"hotpotato/internal/transfer"
)

// A whole server, in this process, with no external dependency: the in-memory
// stores exist for exactly this.
func server(t *testing.T) *httptest.Server {
	t.Helper()
	srv, _ := serverWithRegistry(t)
	return srv
}

func serverWithRegistry(t *testing.T) (*httptest.Server, *sse.Registry) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mem := auth.NewMemory()
	svc := auth.New(auth.Options{
		Users:              mem,
		Sessions:           mem.Sessions(),
		SessionTTL:         time.Hour,
		LoginMaxFailures:   20,
		LoginFailureWindow: time.Minute,
	})

	eventBus := bus.NewMemory(bus.Options{Buffer: 256})
	streams := sse.New(sse.Options{Buffer: 64})
	events, err := eventBus.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	go func() {
		for e := range events {
			streams.Deliver(e)
		}
	}()

	p := presence.NewMemory(presence.Options{
		Grace: 50 * time.Millisecond,
		Announce: func(name string, u presence.User) {
			if e, err := bus.NewEvent(name, nil, u); err == nil {
				eventBus.Publish(ctx, e)
			}
		},
	})
	t.Cleanup(func() { p.Close() })

	transfers := transfer.NewRegistry()
	api := httpapi.New(httpapi.Options{
		Auth:          svc,
		Streams:       streams,
		Presence:      p,
		Transfers:     transfers,
		ReadModel:     transfer.NewLocal(transfers, time.Minute),
		Rendezvous:    relay.NewRendezvous(),
		Directory:     httpapi.LocalDirectory{Instance: "inst-test", BaseURL: "http://inst-test.invalid"},
		Bus:           eventBus,
		WebUI:         http.NotFoundHandler(),
		Draining:      &atomic.Bool{},
		Instance:      "inst-test",
		SessionMaxAge: 3600,
		SSE:           SSEOptionsForTest(),
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
		ProgressInterval:   20 * time.Millisecond,
		ReadModelTTL:       5 * time.Minute,
	})

	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	return srv, streams
}

func SSEOptionsForTest() httpapi.SSEOptions {
	return httpapi.SSEOptions{Retry: time.Second, WriteDeadline: 10 * time.Second}
}

// tree writes a small directory and returns its path plus the entry names a
// browser would have sent for it.
func tree(t *testing.T) (string, map[string][]byte) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "docs")
	want := map[string][]byte{
		"docs/a.txt":             []byte("first"),
		"docs/nested/b.txt":      []byte("second"),
		"docs/nested/deep/c.bin": bytes.Repeat([]byte{9}, 3000),
	}
	for name, data := range want {
		path := filepath.Join(filepath.Dir(root), name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root, want
}

// Not parallel, and none of these are: each dial does a real argon2id signup at
// 64 MiB, and five of those at once contends with every other package's tests
// for memory and cores.
func TestWatchRoundTripsARealEvent(t *testing.T) {
	srv := server(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ana, err := dial(ctx, srv.URL, "ana@example.com", "hunter2hunter2", "ana", true)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	s, err := ana.open(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.close()

	snap, err := s.snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Self.DisplayName != "ana" || snap.StreamID == "" || snap.Instance != "inst-test" {
		t.Fatalf("snapshot = %+v", snap)
	}

	// Somebody else arriving is a real event, produced by the server rather than
	// by the test.
	bea, err := dial(ctx, srv.URL, "bea@example.com", "hunter2hunter2", "bea", true)
	if err != nil {
		t.Fatalf("dial bea: %v", err)
	}
	beaStream, err := bea.open(ctx)
	if err != nil {
		t.Fatalf("open bea: %v", err)
	}
	defer beaStream.close()

	// Presence goes to everyone, so ana hears about her own arrival first.
	for {
		e, err := s.await(ctx, "user.online")
		if err != nil {
			t.Fatalf("await: %v", err)
		}
		if bytes.Contains(e.Data, []byte("bea")) {
			return
		}
	}
}

// A directory sent by spud must arrive as the same archive a browser would have
// produced: entry names are "<folder>/<path within it>", which is exactly what
// webkitRelativePath gives the browser.
func TestSendADirectoryAndReceiveTheZip(t *testing.T) {
	srv := server(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	root, want := tree(t)
	into := t.TempDir()

	sender, err := dial(ctx, srv.URL, "ana@example.com", "hunter2hunter2", "ana", true)
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}
	recipient, err := dial(ctx, srv.URL, "bea@example.com", "hunter2hunter2", "bea", true)
	if err != nil {
		t.Fatalf("dial recipient: %v", err)
	}

	// The Recipient has to be online before the Sender takes its snapshot, or
	// there is nobody to address.
	presenceStream, err := recipient.open(ctx)
	if err != nil {
		t.Fatalf("open recipient: %v", err)
	}
	defer presenceStream.close()
	if _, err := presenceStream.snapshot(ctx); err != nil {
		t.Fatalf("recipient snapshot: %v", err)
	}

	received := make(chan error, 1)
	go func() { received <- recv(ctx, recipient, into) }()

	if err := send(ctx, sender, "bea", root); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := <-received; err != nil {
		t.Fatalf("recv: %v", err)
	}

	archive := filepath.Join(into, "docs.zip")
	info, err := os.Stat(archive)
	if err != nil {
		t.Fatalf("no archive on disk: %v", err)
	}
	zr, err := zip.OpenReader(archive)
	if err != nil {
		t.Fatalf("the archive does not open: %v", err)
	}
	defer zr.Close()

	if len(zr.File) != len(want) {
		t.Fatalf("archive holds %d entries, want %d", len(zr.File), len(want))
	}
	for _, f := range zr.File {
		expected, ok := want[f.Name]
		if !ok {
			t.Errorf("unexpected entry %q — the relative path did not survive", f.Name)
			continue
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("entry %q does not match what was sent", f.Name)
		}
	}
	t.Logf("docs.zip is %d bytes for %d entries", info.Size(), len(zr.File))
}

func TestSendASingleFile(t *testing.T) {
	srv := server(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	file := filepath.Join(t.TempDir(), "report.bin")
	want := bytes.Repeat([]byte{3, 1, 4, 1, 5}, 200_000)
	if err := os.WriteFile(file, want, 0o644); err != nil {
		t.Fatal(err)
	}
	into := t.TempDir()

	sender, err := dial(ctx, srv.URL, "ana@example.com", "hunter2hunter2", "ana", true)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := dial(ctx, srv.URL, "bea@example.com", "hunter2hunter2", "bea", true)
	if err != nil {
		t.Fatal(err)
	}
	presenceStream, err := recipient.open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer presenceStream.close()
	if _, err := presenceStream.snapshot(ctx); err != nil {
		t.Fatal(err)
	}

	received := make(chan error, 1)
	go func() { received <- recv(ctx, recipient, into) }()
	if err := send(ctx, sender, "bea", file); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := <-received; err != nil {
		t.Fatalf("recv: %v", err)
	}

	// A single file passes through untouched.
	got, err := os.ReadFile(filepath.Join(into, "report.bin"))
	if err != nil {
		t.Fatalf("no file on disk: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("received %d bytes, want %d, and they differ", len(got), len(want))
	}
}

// The trap in ADR 0007: net/http will not replay a streaming body across a 307,
// so spud follows it itself. The redirector stands in for an instance that does
// not own the Transfer.
func TestHonours307WithAStreamingBody(t *testing.T) {
	owner := server(t)

	var redirects atomic.Int64
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirects.Add(1)
		http.Redirect(w, r, owner.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	file := filepath.Join(t.TempDir(), "report.bin")
	want := bytes.Repeat([]byte{7}, 500_000)
	if err := os.WriteFile(file, want, 0o644); err != nil {
		t.Fatal(err)
	}
	into := t.TempDir()

	// Everything goes through the redirector: login, the stream, the offer and
	// the upload. Cookies ignore ports, so one jar covers both hosts.
	sender, err := dial(ctx, redirector.URL, "ana@example.com", "hunter2hunter2", "ana", true)
	if err != nil {
		t.Fatalf("dial through the redirector: %v", err)
	}
	recipient, err := dial(ctx, redirector.URL, "bea@example.com", "hunter2hunter2", "bea", true)
	if err != nil {
		t.Fatal(err)
	}
	presenceStream, err := recipient.open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer presenceStream.close()
	if _, err := presenceStream.snapshot(ctx); err != nil {
		t.Fatal(err)
	}

	received := make(chan error, 1)
	go func() { received <- recv(ctx, recipient, into) }()
	if err := send(ctx, sender, "bea", file); err != nil {
		t.Fatalf("send through the redirector: %v", err)
	}
	if err := <-received; err != nil {
		t.Fatalf("recv: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(into, "report.bin"))
	if err != nil {
		t.Fatalf("no file on disk: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("received %d bytes, want %d", len(got), len(want))
	}
	if redirects.Load() == 0 {
		t.Error("nothing was redirected, so this test proved nothing")
	}
	t.Logf("%d requests redirected, including the streaming upload", redirects.Load())
}

// One User holding many Streams at once is the shape phase 10 measures at ten
// thousand. Twenty-five is enough to prove the mechanism.
func TestLoadOpensAndHoldsStreams(t *testing.T) {
	srv, registry := serverWithRegistry(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ana, err := dial(ctx, srv.URL, "ana@example.com", "hunter2hunter2", "ana", true)
	if err != nil {
		t.Fatal(err)
	}

	const streams = 25
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- load(runCtx, ana, streams) }()

	deadline := time.Now().Add(10 * time.Second)
	for registry.Count() < streams && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := registry.Count(); n < streams {
		t.Fatalf("the server holds %d Streams, want %d", n, streams)
	}

	stop()
	if err := <-done; err != nil {
		t.Fatalf("load: %v", err)
	}
	// Every Stream is let go of when load returns: the handlers exit and the
	// registry empties.
	deadline = time.Now().Add(10 * time.Second)
	for registry.Count() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := registry.Count(); n != 0 {
		t.Errorf("the server still holds %d Streams after load returned", n)
	}
}
