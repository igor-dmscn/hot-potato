package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"hotpotato/internal/transfer"
)

func TestReadyzReportsEveryDependency(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) {
		o.checks = []Check{
			{Name: "postgres", Ping: func(context.Context) error { return nil }},
			{Name: "redis", Ping: func(context.Context) error { return nil }},
		}
	})

	rec := x.do(t, "GET", "/readyz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz = %d %s, want 200", rec.Code, rec.Body)
	}
	var body struct {
		Ready  bool              `json:"ready"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body, err)
	}
	if !body.Ready || body.Checks["postgres"] != "ok" || body.Checks["redis"] != "ok" {
		t.Errorf("readyz = %+v", body)
	}
}

func TestReadyzFailsWhenADependencyDoes(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) {
		o.checks = []Check{
			{Name: "postgres", Ping: func(context.Context) error { return nil }},
			{Name: "redis", Ping: func(context.Context) error { return errors.New("connection refused") }},
		}
	})

	rec := x.do(t, "GET", "/readyz", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with a broken dependency = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "connection refused") {
		t.Errorf("body = %s, want it to name the failure", rec.Body)
	}
}

// A check that hangs must not hang the probe: the budget covers all of them.
func TestReadyzHonoursItsBudget(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) {
		o.readyTimeout = 50 * time.Millisecond
		o.checks = []Check{
			{Name: "slow", Ping: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}},
		}
	})

	started := time.Now()
	rec := x.do(t, "GET", "/readyz", "")
	elapsed := time.Since(started)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz with a hanging dependency = %d, want 503", rec.Code)
	}
	if elapsed > time.Second {
		t.Errorf("readyz took %s; the budget is meant to bound it", elapsed)
	}
}

// Liveness and readiness answer different questions, and draining is where the
// difference shows: the process is healthy and must stop being sent work.
func TestDrainingIsNotReadyButStaysHealthy(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	x.draining.Store(true)

	if rec := x.do(t, "GET", "/readyz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz while draining = %d, want 503", rec.Code)
	}
	if rec := x.do(t, "GET", "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("healthz while draining = %d, want 200 — the process is fine", rec.Code)
	}
}

// Every series the protocol reference lists, and the one that matters most:
// relay bytes, which is how phase 12 proves the server saw none.
func TestMetricsExposesTheSeries(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	parts := []part{pattern("report.bin", 1<<20)}
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", sum(parts), 1)
	p.accept(t, x, id)
	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)
	res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	res.Body.Close()
	<-got
	p.sender.await(t, EventTransferCompleted)

	rec := x.do(t, "GET", "/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"hp_streams_active",
		"hp_transfers_active",
		"hp_rendezvous_parked",
		"hp_transfers_total",
		"hp_relay_bytes_total",
		"hp_relay_throughput_bytes",
		"hp_bus_publish_seconds",
		"hp_rendezvous_wait_seconds",
		// The Go collector is where a load test reads goroutines and GC pause.
		"go_goroutines",
		"go_gc_duration_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("%s is missing from /metrics", want)
		}
	}

	// The counters actually counted.
	if !strings.Contains(body, `hp_transfers_total{state="completed"} 1`) {
		t.Error("hp_transfers_total does not record the completion")
	}
	relayed := metricValue(t, body, "hp_relay_bytes_total")
	if relayed != float64(sum(parts)) {
		t.Errorf("hp_relay_bytes_total = %v, want %d", relayed, sum(parts))
	}
	if streams := metricValue(t, body, "hp_streams_active"); streams < 2 {
		t.Errorf("hp_streams_active = %v, want at least the two open Streams", streams)
	}
}

// metricValue pulls one unlabelled sample out of the exposition text.
func metricValue(t *testing.T, body, name string) float64 {
	t.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		if !strings.HasPrefix(line, name+" ") {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name+" ")), 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return v
	}
	t.Fatalf("%s not found in /metrics", name)
	return 0
}
