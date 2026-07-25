// Package httpapi owns every http.Handler in the system, plus routing and
// middleware. Together with relay's one wrapper it is the only place net/http
// appears.
package httpapi

import (
	"net/http"
	"sync/atomic"
	"time"

	"hotpotato/internal/auth"
	"hotpotato/internal/presence"
	"hotpotato/internal/sse"
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
	Auth          *auth.Service
	Streams       *sse.Registry
	Presence      presence.Presence
	WebUI         http.Handler
	Draining      *atomic.Bool
	Instance      string
	SessionMaxAge int // seconds, for the cookie
	SSE           SSEOptions
}

type api struct {
	auth          *auth.Service
	streams       *sse.Registry
	presence      presence.Presence
	draining      *atomic.Bool
	instance      string
	sessionMaxAge int
	sse           SSEOptions
}

// New wires the routes. Go 1.22 method patterns mean the mux does the method
// matching, so no handler starts with a switch on r.Method.
func New(o Options) http.Handler {
	a := &api{
		auth:          o.Auth,
		streams:       o.Streams,
		presence:      o.Presence,
		draining:      o.Draining,
		instance:      o.Instance,
		sessionMaxAge: o.SessionMaxAge,
		sse:           o.SSE,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)

	// requireJSON on every state-changing route; Require on every route that
	// needs an identity.
	mux.Handle("POST /api/signup", requireJSON(http.HandlerFunc(a.signup)))
	mux.Handle("POST /api/login", requireJSON(http.HandlerFunc(a.login)))
	mux.Handle("POST /api/logout", requireJSON(o.Auth.Require(http.HandlerFunc(a.logout))))
	mux.Handle("GET /api/me", o.Auth.Require(http.HandlerFunc(a.me)))

	mux.Handle("GET /events", o.Auth.Require(http.HandlerFunc(a.events)))

	mux.Handle("GET /", o.WebUI)
	return mux
}
