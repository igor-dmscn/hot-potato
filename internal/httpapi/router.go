// Package httpapi owns every http.Handler in the system, plus routing and
// middleware. Together with relay's one wrapper it is the only place net/http
// appears.
package httpapi

import (
	"net/http"
	"sync/atomic"
)

// Options is everything the HTTP surface needs. main fills it in; no handler
// reaches for configuration on its own.
type Options struct {
	WebUI    http.Handler
	Draining *atomic.Bool
}

// New wires the routes. Go 1.22 method patterns mean the mux does the method
// matching, so no handler starts with a switch on r.Method.
func New(o Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.Handle("GET /", o.WebUI)
	return mux
}
