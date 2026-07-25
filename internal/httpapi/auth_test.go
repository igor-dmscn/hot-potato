package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSignupLoginMeLogout(t *testing.T) {
	t.Parallel()
	x := newHarness(t)

	rec := x.do(t, "POST", "/api/signup", `{"email":"ana@example.com","displayName":"ana","password":"hunter2hunter2"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("signup = %d %s, want 201", rec.Code, rec.Body)
	}
	var u userDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &u); err != nil {
		t.Fatalf("signup body: %v", err)
	}
	if u.ID == "" || u.DisplayName != "ana" {
		t.Errorf("signup returned %+v", u)
	}
	if strings.Contains(rec.Body.String(), "argon2") {
		t.Error("the response carries the password hash")
	}
	cookie := session(t, rec)

	rec = x.do(t, "GET", "/api/me", "", cookie)
	if rec.Code != http.StatusOK {
		t.Fatalf("me = %d %s, want 200", rec.Code, rec.Body)
	}

	rec = x.do(t, "POST", "/api/logout", "", cookie)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("logout = %d %s, want 204", rec.Code, rec.Body)
	}
	if c := session(t, rec); c.MaxAge >= 0 {
		t.Errorf("logout cookie MaxAge = %d, want negative", c.MaxAge)
	}

	// The session is gone server-side, not just in the browser.
	if rec := x.do(t, "GET", "/api/me", "", cookie); rec.Code != http.StatusUnauthorized {
		t.Errorf("me after logout = %d, want 401", rec.Code)
	}
}

func TestLoginWrongPassword(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	x.do(t, "POST", "/api/signup", `{"email":"ana@example.com","displayName":"ana","password":"hunter2hunter2"}`)

	rec := x.do(t, "POST", "/api/login", `{"email":"ana@example.com","password":"nope"}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("login = %d, want 401", rec.Code)
	}
	var body errorBody
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Error != codeUnauthorized {
		t.Errorf("error = %q, want %q", body.Error, codeUnauthorized)
	}
	// The message must not distinguish a wrong password from an unknown email.
	if !strings.Contains(body.Message, "email or password") {
		t.Errorf("message = %q, want it to be vague about which half was wrong", body.Message)
	}
}

// SameSite=Lax plus this is the CSRF defence: a cross-site form cannot set
// application/json.
func TestStateChangingRoutesRequireJSONContentType(t *testing.T) {
	t.Parallel()
	x := newHarness(t)

	for _, path := range []string{"/api/signup", "/api/login", "/api/logout"} {
		r := httptest.NewRequest("POST", path, strings.NewReader("email=ana"))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		x.h.ServeHTTP(rec, r)

		if rec.Code != http.StatusUnsupportedMediaType {
			t.Errorf("%s with a form content type = %d, want 415", path, rec.Code)
		}
	}
}

func TestSignupRejectsUnknownFieldsAndOversizeBodies(t *testing.T) {
	t.Parallel()
	x := newHarness(t)

	// A field we never declared is an error, not something to ignore quietly.
	// (Note that a *misspelled* one would not be: encoding/json matches field
	// names case-insensitively, so "displayname" still lands in DisplayName.)
	rec := x.do(t, "POST", "/api/signup", `{"email":"ana@example.com","displayName":"ana","password":"hunter2hunter2","admin":true}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("unknown field = %d, want 400", rec.Code)
	}

	huge := `{"email":"ana@example.com","displayName":"ana","password":"` + strings.Repeat("x", maxJSONBody) + `"}`
	rec = x.do(t, "POST", "/api/signup", huge)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize body = %d, want 413", rec.Code)
	}
}

func TestSignupConflictOnDuplicateEmail(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	body := `{"email":"ana@example.com","displayName":"ana","password":"hunter2hunter2"}`

	x.do(t, "POST", "/api/signup", body)
	if rec := x.do(t, "POST", "/api/signup", body); rec.Code != http.StatusConflict {
		t.Errorf("duplicate signup = %d, want 409", rec.Code)
	}
}

func TestLoginRateLimited(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	x.do(t, "POST", "/api/signup", `{"email":"ana@example.com","displayName":"ana","password":"hunter2hunter2"}`)

	for range 3 {
		x.do(t, "POST", "/api/login", `{"email":"ana@example.com","password":"nope"}`)
	}
	rec := x.do(t, "POST", "/api/login", `{"email":"ana@example.com","password":"hunter2hunter2"}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("login after three failures = %d, want 429", rec.Code)
	}
}
