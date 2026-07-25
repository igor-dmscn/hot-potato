package auth

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Any non-nil TLS state is enough: isLocal only checks whether there is one.
var tlsState tls.ConnectionState

// echo reports which User, if any, the middleware put in the context.
func echo(seen *User) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, ok := UserFrom(r.Context())
		if ok {
			*seen = u
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func TestRequirePassesAnAuthenticatedUser(t *testing.T) {
	t.Parallel()
	svc, _, _ := service(t)

	u, sess, err := svc.Signup(context.Background(), "ana@example.com", "ana", "hunter2hunter2")
	if err != nil {
		t.Fatalf("Signup: %v", err)
	}

	var seen User
	req := httptest.NewRequest("GET", "/api/me", nil)
	req.AddCookie(&http.Cookie{Name: CookieName, Value: sess.ID})
	rec := httptest.NewRecorder()
	svc.Require(echo(&seen)).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if seen.ID != u.ID {
		t.Errorf("context user = %q, want %q", seen.ID, u.ID)
	}
	if seen.PasswordHash == "" {
		t.Error("the context user has no hash, so SessionStore.Get is not joining the user row")
	}
}

func TestRequireRejects(t *testing.T) {
	t.Parallel()

	for name, cookie := range map[string]string{
		"no cookie":   "",
		"unknown id":  "there-is-no-such-session",
		"empty value": " ",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc, _, _ := service(t)
			var seen User
			req := httptest.NewRequest("GET", "/api/me", nil)
			if cookie != "" {
				req.AddCookie(&http.Cookie{Name: CookieName, Value: cookie})
			}
			rec := httptest.NewRecorder()
			svc.Require(echo(&seen)).ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
			var body struct{ Error, Message string }
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body %q is not the JSON envelope: %v", rec.Body, err)
			}
			if body.Error != "unauthorized" {
				t.Errorf("error = %q, want unauthorized", body.Error)
			}
			if seen.ID != "" {
				t.Error("the handler ran anyway")
			}
		})
	}
}

func TestRequireRejectsAnExpiredSessionAndClearsTheCookie(t *testing.T) {
	t.Parallel()
	svc, _, clk := service(t)

	_, sess, err := svc.Signup(context.Background(), "ana@example.com", "ana", "hunter2hunter2")
	if err != nil {
		t.Fatalf("Signup: %v", err)
	}
	clk.advance(7*24*time.Hour + time.Second)

	var seen User
	req := httptest.NewRequest("GET", "/api/me", nil)
	req.AddCookie(&http.Cookie{Name: CookieName, Value: sess.ID})
	rec := httptest.NewRecorder()
	svc.Require(echo(&seen)).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if c := rec.Result().Cookies(); len(c) != 1 || c[0].Name != CookieName || c[0].MaxAge >= 0 {
		t.Errorf("cookies = %+v, want hp_session expired", c)
	}
}

func TestCookieSecurityAttributes(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		host       string
		tls        bool
		wantSecure bool
	}{
		"localhost keeps Secure off": {"localhost:8080", false, false},
		"loopback keeps Secure off":  {"127.0.0.1:8080", false, false},
		"a real host gets Secure":    {"hp.example.com", false, true},
		"tls gets Secure":            {"localhost:8443", true, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest("POST", "/api/login", nil)
			req.Host = tc.host
			if tc.tls {
				req.TLS = &tlsState
			}
			rec := httptest.NewRecorder()
			SetCookie(rec, req, Session{ID: "s1"}, 100)

			c := rec.Result().Cookies()[0]
			if c.Secure != tc.wantSecure {
				t.Errorf("Secure = %v, want %v", c.Secure, tc.wantSecure)
			}
			if !c.HttpOnly {
				t.Error("HttpOnly is off, so JavaScript can read the session")
			}
			if c.SameSite != http.SameSiteLaxMode {
				t.Errorf("SameSite = %v, want Lax", c.SameSite)
			}
		})
	}
}
