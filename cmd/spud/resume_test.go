package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// interrupter truncates the body of the first N uploads, which from the server's
// side is exactly what a dropped connection looks like: a read that fails part
// way through a part.
//
// Breaking the request in front of the handler, rather than killing a socket,
// makes the interruption land at a known byte on every run.
func interrupter(breaks int, after int64) func(http.Handler) http.Handler {
	var broken atomic.Int64
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/d/") &&
				broken.Add(1) <= int64(breaks) {
				r.Body = io.NopCloser(io.MultiReader(
					io.LimitReader(r.Body, after),
					brokenPipe{},
				))
			}
			next.ServeHTTP(w, r)
		})
	}
}

type brokenPipe struct{}

func (brokenPipe) Read([]byte) (int, error) { return 0, errors.New("connection reset by peer") }

// A single file whose upload dies part way, twice, and still arrives intact. The
// Recipient's download never notices: its response stayed open the whole time.
func TestSendResumesAnInterruptedUpload(t *testing.T) {
	srv := server(t, func(o *serverOpts) {
		o.resumeWindow = 10 * time.Second
		o.wrap = interrupter(2, 400_000)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	want := bytes.Repeat([]byte("hot potato "), 100_000) // 1.1 MB
	file := filepath.Join(t.TempDir(), "report.bin")
	if err := os.WriteFile(file, want, 0o644); err != nil {
		t.Fatal(err)
	}
	into := t.TempDir()

	sender, err := dial(ctx, srv.URL, "ana@example.com", "hunter2hunter2", "ana", true, 5)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := dial(ctx, srv.URL, "bea@example.com", "hunter2hunter2", "bea", true, 5)
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
		t.Fatalf("send across two interruptions: %v", err)
	}
	if err := <-received; err != nil {
		t.Fatalf("recv: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(into, "report.bin"))
	if err != nil {
		t.Fatalf("no file on disk: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("received %d bytes, want %d, and they differ — the resumed bytes are wrong",
			len(got), len(want))
	}
}

// The same, mid-entry inside a folder: the archive has to open, which is the only
// way to see that the resumed entry's CRC is right.
func TestSendResumesMidEntryInAFolder(t *testing.T) {
	srv := server(t, func(o *serverOpts) {
		o.resumeWindow = 10 * time.Second
		// Well inside the second entry.
		o.wrap = interrupter(1, 400_000)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	root := filepath.Join(t.TempDir(), "docs")
	want := map[string][]byte{
		"docs/one.bin":   bytes.Repeat([]byte{1}, 300_000),
		"docs/two.bin":   bytes.Repeat([]byte{2}, 400_000),
		"docs/three.bin": bytes.Repeat([]byte{3}, 200_000),
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
	into := t.TempDir()

	sender, err := dial(ctx, srv.URL, "ana@example.com", "hunter2hunter2", "ana", true, 5)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := dial(ctx, srv.URL, "bea@example.com", "hunter2hunter2", "bea", true, 5)
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
	if err := send(ctx, sender, "bea", root); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := <-received; err != nil {
		t.Fatalf("recv: %v", err)
	}

	assertArchive(t, filepath.Join(into, "docs.zip"), want)
}

// truncater makes the first N downloads stop short, which from the server's side
// is the Recipient's socket going away mid-write.
//
// Unwrap is what keeps http.ResponseController working through the wrapper — the
// relay's flush and per-write deadline both go through it.
func truncater(breaks int, after int64) func(http.Handler) http.Handler {
	var broken atomic.Int64
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/d/") &&
				broken.Add(1) <= int64(breaks) {
				w = &cappedWriter{ResponseWriter: w, left: after}
			}
			next.ServeHTTP(w, r)
		})
	}
}

type cappedWriter struct {
	http.ResponseWriter
	left int64
}

func (c *cappedWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }

func (c *cappedWriter) Write(p []byte) (int, error) {
	if c.left <= 0 {
		return 0, errors.New("connection reset by peer")
	}
	if int64(len(p)) > c.left {
		// Part of the write lands and then the socket dies, which is what a real
		// reset mid-write looks like. Returning a short write with a nil error
		// would instead be a broken io.Writer.
		n, _ := c.ResponseWriter.Write(p[:c.left])
		c.left = 0
		return n, errors.New("connection reset by peer")
	}
	c.left -= int64(len(p))
	return c.ResponseWriter.Write(p)
}

// The other direction: the Recipient's download breaks, and it comes back with
// `Range: bytes=N-` for what it does not have. N is what is on disk here, not
// what the server counted — the Owner's count includes bytes that were written
// into a socket and never arrived.
func TestRecvResumesWithRange(t *testing.T) {
	srv := server(t, func(o *serverOpts) {
		o.resumeWindow = 10 * time.Second
		o.wrap = truncater(1, 300_000)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	want := bytes.Repeat([]byte("resume me "), 90_000) // 900 KB
	file := filepath.Join(t.TempDir(), "report.bin")
	if err := os.WriteFile(file, want, 0o644); err != nil {
		t.Fatal(err)
	}
	into := t.TempDir()

	sender, err := dial(ctx, srv.URL, "ana@example.com", "hunter2hunter2", "ana", true, 5)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := dial(ctx, srv.URL, "bea@example.com", "hunter2hunter2", "bea", true, 5)
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
		t.Fatalf("recv across a truncated download: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(into, "report.bin"))
	if err != nil {
		t.Fatalf("no file on disk: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("received %d bytes, want %d, and they differ", len(got), len(want))
	}
}

// With -attempts 1 an interruption is terminal, which is the phase 5 behaviour and
// has to stay reachable.
func TestOneAttemptDoesNotResume(t *testing.T) {
	srv := server(t, func(o *serverOpts) {
		o.resumeWindow = 500 * time.Millisecond
		o.wrap = interrupter(1, 200_000)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	file := filepath.Join(t.TempDir(), "report.bin")
	if err := os.WriteFile(file, bytes.Repeat([]byte{7}, 900_000), 0o644); err != nil {
		t.Fatal(err)
	}

	sender, err := dial(ctx, srv.URL, "ana@example.com", "hunter2hunter2", "ana", true, 1)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := dial(ctx, srv.URL, "bea@example.com", "hunter2hunter2", "bea", true, 1)
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

	go recv(ctx, recipient, t.TempDir())
	err = send(ctx, sender, "bea", file)
	if err == nil {
		t.Fatal("send reported success despite an interruption and no retries")
	}
	if !strings.Contains(err.Error(), "gave up after 1 attempt") {
		t.Errorf("error = %v, want it to say it gave up after one attempt", err)
	}
}
