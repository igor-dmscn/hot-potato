package auth

import (
	"context"
	"net/http"
)

type ctxKey struct{}

type identity struct {
	user    User
	session Session
}

// UserFrom returns the User the middleware authenticated.
func UserFrom(ctx context.Context) (User, bool) {
	id, ok := ctx.Value(ctxKey{}).(identity)
	return id.user, ok
}

// SessionFrom returns the Session the request arrived on, which logout needs
// in order to delete exactly this browser's session and no other.
func SessionFrom(ctx context.Context) (Session, bool) {
	id, ok := ctx.Value(ctxKey{}).(identity)
	return id.session, ok
}

// Require rejects anything without a live session.
func (s *Service) Require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(CookieName)
		if err != nil || c.Value == "" {
			unauthorized(w)
			return
		}
		sess, u, err := s.sessions.Get(r.Context(), c.Value)
		if err != nil || !sess.ExpiresAt.After(s.now()) {
			// Clear the cookie: a stale session ID that keeps being presented
			// costs a database round trip on every request.
			ClearCookie(w, r)
			unauthorized(w)
			return
		}
		ctx := context.WithValue(r.Context(), ctxKey{}, identity{user: u, session: sess})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// unauthorized writes the error envelope (docs/protocol.md). It is spelled out
// here rather than borrowed from httpapi because httpapi imports this package,
// and 401 is the only status this package ever produces.
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte(`{"error":"unauthorized","message":"sign in first"}` + "\n"))
}
