package main

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestServeDrainsThenReturnsInsideGrace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()}

	drained := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, srv, time.Second, func(context.Context) { close(drained) })
	}()

	cancel()

	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain hook never ran")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return inside the grace window")
	}
}

// The drain hook has to run before Shutdown, or Shutdown blocks forever on the
// streams the hook exists to close (Go issue #41344).
func TestServeDrainsBeforeShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()}

	order := make(chan string, 2)
	srv.RegisterOnShutdown(func() { order <- "shutdown" })

	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, srv, time.Second, func(context.Context) { order <- "drain" })
	}()
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if first := <-order; first != "drain" {
		t.Errorf("first hook was %q, want drain", first)
	}
}

func TestServeReturnsListenError(t *testing.T) {
	srv := &http.Server{Addr: "127.0.0.1:not-a-port", Handler: http.NotFoundHandler()}
	if err := serve(context.Background(), srv, time.Second, func(context.Context) {}); err == nil {
		t.Fatal("serve returned nil for an unlistenable address")
	}
}
