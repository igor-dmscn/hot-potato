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

	"github.com/redis/go-redis/v9"

	"hotpotato/internal/auth"
	"hotpotato/internal/bus"
	"hotpotato/internal/config"
	"hotpotato/internal/httpapi"
	"hotpotato/internal/metrics"
	"hotpotato/internal/mirror"
	"hotpotato/internal/obs"
	"hotpotato/internal/postgres"
	"hotpotato/internal/presence"
	"hotpotato/internal/relay"
	"hotpotato/internal/sse"
	"hotpotato/internal/transfer"
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
	slog.Info("boot", "config", cfg, "pid", os.Getpid(), "distributed", cfg.Distributed())

	// Signals first: everything below registers cleanup against this.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A boot budget: a dependency that is not there should fail the process, not
	// hang it.
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

	// The bus outlives the request-serving context, so events still flow while
	// the server is draining.
	busCtx, stopBus := context.WithCancel(context.Background())
	defer stopBus()

	eventBus, err := openBus(cfg)
	if err != nil {
		return err
	}
	defer eventBus.Close()

	streams := sse.New(sse.Options{Buffer: cfg.StreamBuffer})
	transfers := transfer.NewRegistry()
	rendezvous := relay.NewRendezvous()

	// One clock for every Stream: ten thousand goroutines is fine, ten thousand
	// timers is not.
	heartbeat := sse.NewHeartbeat(cfg.SSEHeartbeat)
	defer heartbeat.Close()

	shutdownTracing, err := obs.Setup(bootCtx, obs.Options{
		Endpoint: cfg.OTLPEndpoint,
		Instance: cfg.InstanceID,
		Sample:   cfg.TraceSample,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			slog.Error("flush traces", "err", err)
		}
	}()

	meters := metrics.New(metrics.Options{
		Streams:   func() float64 { return float64(streams.Count()) },
		Transfers: func() float64 { return float64(transfers.Count()) },
		Parked:    func() float64 { return float64(rendezvous.Parked()) },
	})

	checks := []httpapi.Check{
		{Name: "postgres", Ping: store.Ping},
		{Name: "bus", Ping: func(c context.Context) error {
			// The bus has no ping, so readiness is "a publish works" — which is
			// the thing that has to work. No audience: the probe has to traverse
			// the bus, not arrive anywhere. That used to be spelled as this
			// instance's ID in a field of User IDs, which only reached nobody
			// because the two namespaces happen not to collide.
			e, err := bus.NewEvent("readyz", nil, nil)
			if err != nil {
				return err
			}
			return eventBus.Publish(c, e)
		}},
	}

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

	announce := func(name string, u presence.User) {
		// Presence is public within the app — it is the online list.
		e, err := bus.NewEvent(name, []string{bus.Everyone}, u)
		if err != nil {
			slog.Error("build presence event", "event", name, "err", err)
			return
		}
		if err := eventBus.Publish(busCtx, e); err != nil {
			slog.Error("publish presence event", "event", name, "err", err)
		}
	}

	var (
		rdb         *redis.Client
		presenceSvc presence.Presence
		readModel   transfer.ReadModel
		directory   httpapi.Directory
	)
	if cfg.Distributed() {
		if rdb, err = mirror.Open(bootCtx, cfg.RedisURL); err != nil {
			return err
		}
		defer rdb.Close()

		presenceSvc = presence.NewRedis(rdb, presence.RedisOptions{
			Options:  presence.Options{Grace: cfg.PresenceGrace, Announce: announce},
			Instance: cfg.InstanceID,
			TTL:      cfg.PresenceTTL,
			Refresh:  cfg.PresenceRefresh,
		})
		readModel = mirror.NewTransfers(rdb)
		checks = append(checks, httpapi.Check{Name: "redis", Ping: func(c context.Context) error {
			return rdb.Ping(c).Err()
		}})

		instances := mirror.NewInstances(rdb)
		directory = instances
		// Self-register, and withdraw on the way out so nobody is redirected to
		// a process that has gone.
		go instances.Keep(ctx, cfg.InstanceID, cfg.ExternalURL, cfg.InstanceTTL, cfg.InstanceRefresh)
	} else {
		presenceSvc = presence.NewMemory(presence.Options{Grace: cfg.PresenceGrace, Announce: announce})
		readModel = transfer.NewLocal(transfers, cfg.TerminalWindow)
		directory = httpapi.LocalDirectory{Instance: cfg.InstanceID, BaseURL: cfg.ExternalURL}
	}
	defer presenceSvc.Close()

	var draining atomic.Bool
	api := httpapi.New(httpapi.Options{
		Auth:          authSvc,
		Streams:       streams,
		Presence:      presenceSvc,
		Transfers:     transfers,
		ReadModel:     readModel,
		Rendezvous:    rendezvous,
		Directory:     directory,
		Bus:           eventBus,
		WebUI:         webui.Handler(),
		Metrics:       meters,
		Heartbeat:     heartbeat,
		Checks:        checks,
		ReadyTimeout:  cfg.ReadyTimeout,
		Draining:      &draining,
		Instance:      cfg.InstanceID,
		SessionMaxAge: int(cfg.SessionTTL.Seconds()),
		SSE: httpapi.SSEOptions{
			Retry:         cfg.SSERetry,
			WriteDeadline: cfg.SSEWriteDeadline,
		},
		Limits: transfer.Limits{
			OfferTTL:          cfg.OfferTTL,
			MaxOutbound:       cfg.MaxOutbound,
			MaxPendingInbound: cfg.MaxPendingInbound,
			MaxPayloadBytes:   cfg.MaxPayloadBytes,
			MaxEntries:        cfg.MaxEntries,
			OfferRate:         cfg.OfferRate,
			OfferRateWindow:   cfg.OfferRateWindow,
		},
		TerminalWindow:     cfg.TerminalWindow,
		RendezvousWait:     cfg.RendezvousWait,
		RelayWriteDeadline: cfg.RelayWriteDeadline,
		RelayBuffer:        cfg.RelayBuffer,
		ProgressInterval:   cfg.ProgressInterval,
		ResumeWindow:       cfg.ResumeWindow,
		ReadModelTTL:       cfg.ReadModelTTL,
	})
	go api.Reap(busCtx, cfg.ReapInterval, cfg.TerminalWindow)

	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: api,
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

	return serve(ctx, srv, cfg.ShutdownGrace, func(dctx context.Context) {
		draining.Store(true)
		// Announce and close every Stream before Shutdown, which otherwise
		// waits forever on an open SSE handler (Go issue #41344).
		streams.Drain(dctx)
	})
}

// openBus picks an implementation. The interface is pinned to the lowest common
// denominator of all of them on purpose (ADR 0006).
func openBus(cfg config.Config) (bus.Bus, error) {
	o := bus.Options{Buffer: cfg.BusBuffer}
	switch cfg.Bus {
	case "nats":
		return bus.NewNATS(cfg.NATSURL, cfg.BusSubject, o)
	case "redis":
		if !cfg.Distributed() {
			return nil, errors.New("HP_BUS=redis needs HP_REDIS_URL")
		}
		return bus.NewRedis(cfg.RedisURL, cfg.BusSubject, o)
	case "kafka":
		return bus.NewKafka(cfg.KafkaBrokers, cfg.BusSubject, cfg.InstanceID, o)
	default:
		return bus.NewMemory(o), nil
	}
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
