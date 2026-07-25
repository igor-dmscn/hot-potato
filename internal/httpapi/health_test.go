package httpapi

import (
	"net/http"
	"testing"
)

func TestHealthz(t *testing.T) {
	t.Parallel()

	rec := newHarness(t).do(t, "GET", "/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "ok\n" {
		t.Errorf("body = %q, want \"ok\\n\"", got)
	}
}
