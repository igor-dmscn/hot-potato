// Command server runs one Hot Potato instance.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"hotpotato/internal/config"
	"hotpotato/internal/httpapi"
	"hotpotato/internal/webui"
)

func main() {
	if err := run(); err != nil {
		slog.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		// slog is not configured yet, and a broken environment is the one error
		// that has to be readable without a log pipeline in front of it.
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})))
	// Half of all deployment confusion is a process that did not get the
	// variable someone thought they set. Log the resolved config once.
	slog.Info("boot", "config", cfg, "pid", os.Getpid())

	var draining atomic.Bool
	srv := &http.Server{
		Addr: cfg.Addr,
		Handler: httpapi.New(httpapi.Options{
			WebUI:    webui.Handler(),
			Draining: &draining,
		}),
		// WriteTimeout is a deadline on the entire response, so any non-zero
		// value kills an SSE stream and a multi-gigabyte relay on a schedule.
		// Long-lived writes take per-write deadlines from
		// http.ResponseController instead.
		WriteTimeout: 0,
		// ReadHeaderTimeout, never ReadTimeout: a slow upload takes longer than
		// any whole-request deadline worth setting.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return serve(ctx, srv, cfg.ShutdownGrace, func(context.Context) {
		draining.Store(true)
		// The rest of the drain — broadcast server.draining and close every SSE
		// Stream — arrives in phase 2. The hook exists from day one because
		// Server.Shutdown waits forever on an open stream (Go issue #41344),
		// and discovering that after the streams exist is the expensive way.
	})
}

// serve runs srv until ctx is done, then drains and shuts down inside grace.
func serve(ctx context.Context, srv *http.Server, grace time.Duration, drain func(context.Context)) error {
	listening := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", srv.Addr)
		listening <- srv.ListenAndServe()
	}()

	select {
	case err := <-listening:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	// A fresh context: ctx is already cancelled, and the grace window is the
	// whole point of shutting down deliberately rather than exiting.
	sctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()

	slog.Info("draining", "grace", grace)
	drain(sctx)
	return srv.Shutdown(sctx)
}
