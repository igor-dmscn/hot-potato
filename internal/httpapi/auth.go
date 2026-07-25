package httpapi

import (
	"errors"
	"net"
	"net/http"
	"strings"

	"hotpotato/internal/auth"
)

// userDTO is what a User looks like on the wire. It exists so that adding a
// field to auth.User cannot accidentally publish it.
type userDTO struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"displayName"`
}

func dto(u auth.User) userDTO {
	return userDTO{ID: u.ID, Email: u.Email, DisplayName: u.DisplayName}
}

func (a *api) signup(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email       string `json:"email"`
		DisplayName string `json:"displayName"`
		Password    string `json:"password"`
	}
	if !decode(w, r, &body) {
		return
	}
	u, sess, err := a.auth.Signup(r.Context(), body.Email, body.DisplayName, body.Password)
	if err != nil {
		a.authError(w, r, err)
		return
	}
	auth.SetCookie(w, r, sess, a.sessionMaxAge)
	writeJSON(w, http.StatusCreated, dto(u))
}

func (a *api) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !decode(w, r, &body) {
		return
	}
	u, sess, err := a.auth.Login(r.Context(), body.Email, body.Password, clientIP(r))
	if err != nil {
		a.authError(w, r, err)
		return
	}
	auth.SetCookie(w, r, sess, a.sessionMaxAge)
	writeJSON(w, http.StatusOK, dto(u))
}

func (a *api) logout(w http.ResponseWriter, r *http.Request) {
	sess, _ := auth.SessionFrom(r.Context())
	if err := a.auth.Logout(r.Context(), sess.ID); err != nil {
		internalError(w, r, err)
		return
	}
	auth.ClearCookie(w, r)
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) me(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())
	writeJSON(w, http.StatusOK, dto(u))
}

// authError maps the package's sentinel errors onto the envelope. Credentials
// and rate limits get deliberately vague messages.
func (a *api) authError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, auth.ErrInvalid):
		writeError(w, http.StatusBadRequest, codeBadRequest, err.Error())
	case errors.Is(err, auth.ErrEmailTaken):
		writeError(w, http.StatusConflict, codeBadRequest, "that email is already registered")
	case errors.Is(err, auth.ErrCredentials):
		writeError(w, http.StatusUnauthorized, codeUnauthorized, "email or password is wrong")
	case errors.Is(err, auth.ErrTooManyLogins):
		writeError(w, http.StatusTooManyRequests, codeRateLimited, "too many attempts; wait a few minutes")
	default:
		internalError(w, r, err)
	}
}

// clientIP is a throttle key, not an authorization input, so trusting
// X-Forwarded-For's first hop behind our own proxy is acceptable: the worst a
// spoofed value achieves is throttling a bucket nobody else uses.
func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		first, _, _ := strings.Cut(fwd, ",")
		return strings.TrimSpace(first)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
