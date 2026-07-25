package httpapi

import "net/http"

// healthz answers while the process is alive. Liveness only — readiness, which
// depends on Postgres, Redis and the bus, is /readyz.
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("ok\n"))
}
