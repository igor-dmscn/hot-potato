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
	"hotpotato/internal/presence"
	"hotpotato/internal/sse"
	"hotpotato/internal/transfer"
)

// SSEOptions are the stream timings, all of them config fields.
type SSEOptions struct {
	Heartbeat     time.Duration
	Retry         time.Duration
	WriteDeadline time.Duration
}

// Options is everything the HTTP surface needs. main fills it in from config;
// no handler reaches for configuration on its own.
type Options struct {
	Auth      *auth.Service
	Streams   *sse.Registry
	Presence  presence.Presence
	Transfers *transfer.Registry
	Bus       bus.Bus
	WebUI     http.Handler

	Draining      *atomic.Bool
	Instance      string
	SessionMaxAge int // seconds, for the cookie
	SSE           SSEOptions
	Limits        transfer.Limits
	// TerminalWindow is how long a finished Transfer stays in snapshots.
	TerminalWindow time.Duration
	// Now is injected so expiry is testable without waiting for a minute.
	Now func() time.Time
}

type api struct {
	auth      *auth.Service
	streams   *sse.Registry
	presence  presence.Presence
	transfers *transfer.Registry
	bus       bus.Bus

	draining       *atomic.Bool
	instance       string
	sessionMaxAge  int
	sse            SSEOptions
	limits         transfer.Limits
	terminalWindow time.Duration
	now            func() time.Time
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
	a := &api{
		auth:           o.Auth,
		streams:        o.Streams,
		presence:       o.Presence,
		transfers:      o.Transfers,
		bus:            o.Bus,
		draining:       o.Draining,
		instance:       o.Instance,
		sessionMaxAge:  o.SessionMaxAge,
		sse:            o.SSE,
		limits:         o.Limits,
		terminalWindow: o.TerminalWindow,
		now:            o.Now,
	}

	// Go 1.22 method patterns mean the mux does the method matching, so no
	// handler starts with a switch on r.Method.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)

	// requireJSON on every state-changing route; Require on every route that
	// needs an identity.
	mux.Handle("POST /api/signup", requireJSON(http.HandlerFunc(a.signup)))
	mux.Handle("POST /api/login", requireJSON(http.HandlerFunc(a.login)))
	mux.Handle("POST /api/logout", requireJSON(o.Auth.Require(http.HandlerFunc(a.logout))))
	mux.Handle("GET /api/me", o.Auth.Require(http.HandlerFunc(a.me)))

	mux.Handle("GET /events", o.Auth.Require(http.HandlerFunc(a.events)))

	mux.Handle("POST /api/transfers", requireJSON(o.Auth.Require(http.HandlerFunc(a.createTransfer))))
	mux.Handle("POST /api/transfers/{id}/accept", requireJSON(o.Auth.Require(http.HandlerFunc(a.acceptTransfer))))
	mux.Handle("POST /api/transfers/{id}/deny", requireJSON(o.Auth.Require(http.HandlerFunc(a.denyTransfer))))
	mux.Handle("POST /api/transfers/{id}/cancel", requireJSON(o.Auth.Require(http.HandlerFunc(a.cancelTransfer))))

	mux.Handle("GET /", o.WebUI)
	return &Server{api: a, handler: mux}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }
