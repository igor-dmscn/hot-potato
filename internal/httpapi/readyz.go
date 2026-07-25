package httpapi

import (
	"context"
	"net/http"
	"time"
)

// Check is one dependency's liveness probe. main supplies one per service.
type Check struct {
	Name string
	Ping func(context.Context) error
}

// readyz answers whether this instance can serve.
//
// It is not /healthz. Liveness says the process is running, and a restart would
// not help; readiness says it can do its job, and a load balancer should stop
// sending it work if it cannot. Draining is the clearest case of the difference:
// the process is perfectly healthy and must stop receiving requests.
func (a *api) readyz(w http.ResponseWriter, r *http.Request) {
	if a.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ready":  false,
			"reason": codeDraining,
		})
		return
	}

	// One budget for all of them, in parallel. A readiness probe that takes
	// longer than the interval it is polled at is a second outage.
	ctx, cancel := context.WithTimeout(r.Context(), a.readyTimeout)
	defer cancel()

	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(a.checks))
	for _, c := range a.checks {
		go func() {
			results <- result{c.Name, c.Ping(ctx)}
		}()
	}

	ready := true
	status := map[string]any{}
	for range a.checks {
		got := <-results
		if got.err != nil {
			ready = false
			status[got.name] = got.err.Error()
			continue
		}
		status[got.name] = "ok"
	}

	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{"ready": ready, "checks": status})
}

// defaultReadyTimeout is DESIGN §6's two-second budget.
const defaultReadyTimeout = 2 * time.Second
