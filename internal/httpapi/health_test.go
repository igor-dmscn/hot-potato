package httpapi

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHealthz(t *testing.T) {
	t.Parallel()

	h := New(Options{WebUI: http.NotFoundHandler(), Draining: &atomic.Bool{}})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "ok\n" {
		t.Errorf("body = %q, want \"ok\\n\"", got)
	}
}
