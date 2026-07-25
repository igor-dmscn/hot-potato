// Package httpapi owns every http.Handler in the system, plus routing and
// middleware. Together with relay's one wrapper it is the only place net/http
// appears.
package httpapi

import (
	"net/http"
	"sync/atomic"

	"hotpotato/internal/auth"
)

// Options is everything the HTTP surface needs. main fills it in from config;
// no handler reaches for configuration on its own.
type Options struct {
	Auth          *auth.Service
	WebUI         http.Handler
	Draining      *atomic.Bool
	SessionMaxAge int // seconds, for the cookie
}

type api struct {
	auth          *auth.Service
	draining      *atomic.Bool
	sessionMaxAge int
}

// New wires the routes. Go 1.22 method patterns mean the mux does the method
// matching, so no handler starts with a switch on r.Method.
func New(o Options) http.Handler {
	a := &api{auth: o.Auth, draining: o.Draining, sessionMaxAge: o.SessionMaxAge}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)

	// requireJSON on every state-changing route; Require on every route that
	// needs an identity.
	mux.Handle("POST /api/signup", requireJSON(http.HandlerFunc(a.signup)))
	mux.Handle("POST /api/login", requireJSON(http.HandlerFunc(a.login)))
	mux.Handle("POST /api/logout", requireJSON(o.Auth.Require(http.HandlerFunc(a.logout))))
	mux.Handle("GET /api/me", o.Auth.Require(http.HandlerFunc(a.me)))

	mux.Handle("GET /", o.WebUI)
	return mux
}
