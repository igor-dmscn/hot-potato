// Package httpapi owns every http.Handler in the system, plus routing and
// middleware. Together with relay's one wrapper it is the only place net/http
// appears.
package httpapi

import (
	"net/http"
	"sync/atomic"
	"time"

	"hotpotato/internal/auth"
	"hotpotato/internal/bus"
	"hotpotato/internal/metrics"
	"hotpotato/internal/obs"
	"hotpotato/internal/presence"
	"hotpotato/internal/relay"
	"hotpotato/internal/sse"
	"hotpotato/internal/transfer"
)

// SSEOptions are the stream timings, all of them config fields. The heartbeat
// interval is not here: it belongs to the shared Heartbeat, because one timer
// per Stream does not survive ten thousand of them.
type SSEOptions struct {
	Retry         time.Duration
	WriteDeadline time.Duration
}

// Options is everything the HTTP surface needs. main fills it in from config;
// no handler reaches for configuration on its own.
type Options struct {
	Auth       *auth.Service
	Streams    *sse.Registry
	Presence   presence.Presence
	Transfers  *transfer.Registry
	ReadModel  transfer.ReadModel
	Rendezvous *relay.Rendezvous
	Directory  Directory
	Bus        bus.Bus
	WebUI      http.Handler

	Metrics   *metrics.Metrics
	Heartbeat *sse.Heartbeat
	// Checks are the dependencies /readyz probes.
	Checks       []Check
	ReadyTimeout time.Duration

	Draining      *atomic.Bool
	Instance      string
	SessionMaxAge int // seconds, for the cookie
	SSE           SSEOptions
	Limits        transfer.Limits
	// TerminalWindow is how long a finished Transfer stays in snapshots.
	TerminalWindow time.Duration
	// RendezvousWait is how long a parked Recipient waits for a Sender.
	RendezvousWait time.Duration
	// RelayWriteDeadline is the per-write deadline on the Recipient's socket.
	RelayWriteDeadline time.Duration
	RelayBuffer        int
	ProgressInterval   time.Duration
	// ResumeWindow is how long a parked Recipient waits for an interrupted
	// Sender to come back. Zero makes every interruption terminal.
	ResumeWindow time.Duration
	// ReadModelTTL is how long mirrored Transfer metadata survives.
	ReadModelTTL time.Duration
	// Now is injected so expiry is testable without waiting for a minute.
	Now func() time.Time
}

type api struct {
	auth       *auth.Service
	streams    *sse.Registry
	presence   presence.Presence
	transfers  *transfer.Registry
	readModel  transfer.ReadModel
	rendezvous *relay.Rendezvous
	directory  Directory
	bus        bus.Bus
	metrics    *metrics.Metrics
	heartbeat  *sse.Heartbeat

	checks       []Check
	readyTimeout time.Duration

	draining           *atomic.Bool
	instance           string
	sessionMaxAge      int
	sse                SSEOptions
	limits             transfer.Limits
	terminalWindow     time.Duration
	rendezvousWait     time.Duration
	relayWriteDeadline time.Duration
	relayBuffer        int
	progressInterval   time.Duration
	resumeWindow       time.Duration
	readModelTTL       time.Duration
	now                func() time.Time
}

// Server is the HTTP surface plus the background loops that belong to it.
type Server struct {
	*api
	handler http.Handler
}

func New(o Options) *Server {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.ReadyTimeout <= 0 {
		o.ReadyTimeout = defaultReadyTimeout
	}
	a := &api{
		auth:               o.Auth,
		streams:            o.Streams,
		presence:           o.Presence,
		transfers:          o.Transfers,
		readModel:          o.ReadModel,
		rendezvous:         o.Rendezvous,
		directory:          o.Directory,
		bus:                o.Bus,
		metrics:            o.Metrics,
		heartbeat:          o.Heartbeat,
		checks:             o.Checks,
		readyTimeout:       o.ReadyTimeout,
		draining:           o.Draining,
		instance:           o.Instance,
		sessionMaxAge:      o.SessionMaxAge,
		sse:                o.SSE,
		limits:             o.Limits,
		terminalWindow:     o.TerminalWindow,
		rendezvousWait:     o.RendezvousWait,
		relayWriteDeadline: o.RelayWriteDeadline,
		relayBuffer:        o.RelayBuffer,
		progressInterval:   o.ProgressInterval,
		resumeWindow:       o.ResumeWindow,
		readModelTTL:       o.ReadModelTTL,
		now:                o.Now,
	}

	// Go 1.22 method patterns mean the mux does the method matching, so no
	// handler starts with a switch on r.Method.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	mux.Handle("GET /metrics", o.Metrics.Handler())

	// requireJSON on every state-changing route; Require on every route that
	// needs an identity.
	mux.Handle("POST /api/signup", requireJSON(http.HandlerFunc(a.signup)))
	mux.Handle("POST /api/login", requireJSON(http.HandlerFunc(a.login)))
	mux.Handle("POST /api/logout", requireJSON(o.Auth.Require(http.HandlerFunc(a.logout))))
	mux.Handle("GET /api/me", o.Auth.Require(http.HandlerFunc(a.me)))

	mux.Handle("GET /events", o.Auth.Require(http.HandlerFunc(a.events)))

	// A new Transfer is minted here and owned here, so it needs no redirect.
	mux.Handle("POST /api/transfers", requireJSON(o.Auth.Require(http.HandlerFunc(a.createTransfer))))

	// Everything about an existing Transfer goes to its Owner first. The
	// ownership check is outermost: a redirect should not cost a session lookup.
	mux.Handle("POST /api/transfers/{id}/accept",
		a.ownership(requireJSON(o.Auth.Require(http.HandlerFunc(a.acceptTransfer)))))
	mux.Handle("POST /api/transfers/{id}/deny",
		a.ownership(requireJSON(o.Auth.Require(http.HandlerFunc(a.denyTransfer)))))
	mux.Handle("POST /api/transfers/{id}/cancel",
		a.ownership(requireJSON(o.Auth.Require(http.HandlerFunc(a.cancelTransfer)))))

	// The data plane. No requireJSON here: these are multipart and a download,
	// and a cross-site form *can* send multipart — SameSite=Lax on the session
	// cookie is what keeps it from carrying an identity.
	mux.Handle("GET /d/{id}", a.ownership(o.Auth.Require(http.HandlerFunc(a.download))))
	mux.Handle("POST /d/{id}", a.ownership(o.Auth.Require(http.HandlerFunc(a.upload))))

	mux.Handle("GET /", o.WebUI)

	// One span per request, outermost, so a redirect or a rejection is on the
	// trace too.
	return &Server{api: a, handler: obs.Middleware(mux)}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }
