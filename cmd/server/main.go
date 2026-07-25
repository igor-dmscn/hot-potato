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

	"hotpotato/internal/auth"
	"hotpotato/internal/bus"
	"hotpotato/internal/config"
	"hotpotato/internal/httpapi"
	"hotpotato/internal/postgres"
	"hotpotato/internal/presence"
	"hotpotato/internal/sse"
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

	// A boot budget: a database that is not there should fail the process, not
	// hang it. Everything after this point is either wired or fatal.
	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelBoot()

	store, err := postgres.Open(bootCtx, cfg.DatabaseURL.Reveal())
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(bootCtx); err != nil {
		return err
	}

	authSvc := auth.New(auth.Options{
		Users:              store.Users(),
		Sessions:           store.Sessions(),
		SessionTTL:         cfg.SessionTTL,
		LoginMaxFailures:   cfg.LoginMaxFailures,
		LoginFailureWindow: cfg.LoginFailureWindow,
	})

	// The control plane. The bus outlives the request-serving context so that
	// events still flow while the server is draining.
	busCtx, stopBus := context.WithCancel(context.Background())
	defer stopBus()

	eventBus := bus.NewMemory(bus.Options{Buffer: cfg.BusBuffer})
	defer eventBus.Close()
	streams := sse.New(sse.Options{Buffer: cfg.StreamBuffer})

	// One subscription per process, feeding every local Stream. Every instance
	// receives every event and filters by audience locally (ADR 0006).
	events, err := eventBus.Subscribe(busCtx)
	if err != nil {
		return err
	}
	go func() {
		for e := range events {
			streams.Deliver(e)
		}
	}()

	presenceSvc := presence.NewMemory(presence.Options{
		Grace: cfg.PresenceGrace,
		Announce: func(name string, u presence.User) {
			e, err := bus.NewEvent(name, nil, u)
			if err != nil {
				slog.Error("build presence event", "event", name, "err", err)
				return
			}
			if err := eventBus.Publish(busCtx, e); err != nil {
				slog.Error("publish presence event", "event", name, "err", err)
			}
		},
	})
	defer presenceSvc.Close()

	var draining atomic.Bool
	srv := &http.Server{
		Addr: cfg.Addr,
		Handler: httpapi.New(httpapi.Options{
			Auth:          authSvc,
			Streams:       streams,
			Presence:      presenceSvc,
			WebUI:         webui.Handler(),
			Draining:      &draining,
			Instance:      cfg.InstanceID,
			SessionMaxAge: int(cfg.SessionTTL.Seconds()),
			SSE: httpapi.SSEOptions{
				Heartbeat:     cfg.SSEHeartbeat,
				Retry:         cfg.SSERetry,
				WriteDeadline: cfg.SSEWriteDeadline,
			},
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

	return serve(ctx, srv, cfg.ShutdownGrace, func(dctx context.Context) {
		draining.Store(true)
		// Announce and close every Stream before Shutdown, which otherwise
		// waits forever on an open SSE handler (Go issue #41344).
		streams.Drain(dctx)
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
