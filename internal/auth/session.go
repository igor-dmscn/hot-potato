package auth

import (
	"net"
	"net/http"
	"strings"
)

// CookieName is the session cookie. HttpOnly so JavaScript cannot read it,
// SameSite=Lax so a cross-site form POST does not carry it — which, together
// with requiring Content-Type: application/json on every state-changing route,
// is this application's CSRF defence.
const CookieName = "hp_session"

// SetCookie writes the session cookie for s.
func SetCookie(w http.ResponseWriter, r *http.Request, s Session, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    s.ID,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   !isLocal(r),
	})
}

// ClearCookie expires the session cookie. Its attributes must match the ones
// used to set it, or the browser keeps the original.
func ClearCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   !isLocal(r),
	})
}

// isLocal decides whether to drop the Secure attribute. A Secure cookie on
// http://localhost is a cookie the browser will never send back, which looks
// exactly like broken auth.
func isLocal(r *http.Request) bool {
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		return false
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	switch strings.ToLower(strings.Trim(host, "[]")) {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return false
}
