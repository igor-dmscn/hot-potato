// Package auth owns identity: signup, login, sessions and the middleware that
// puts a User in a request's context.
//
// It depends on the two store interfaces declared here, never on a database
// handle, so the Service, its middleware and every handler above it can be
// tested against Memory while only internal/postgres needs a live Postgres.
package auth

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"sync"
	"time"
	"unicode"
)

// User is a registered account.
type User struct {
	ID          string
	Email       string
	DisplayName string
	// PasswordHash never leaves the process. json:"-" so that no handler can
	// leak it by marshalling a User directly.
	PasswordHash string `json:"-"`
	CreatedAt    time.Time
}

// Session is one logged-in browser.
type Session struct {
	ID        string
	UserID    string
	CreatedAt time.Time
	ExpiresAt time.Time
}

type UserStore interface {
	Create(ctx context.Context, u User) error // ErrEmailTaken if the email exists
	ByEmail(ctx context.Context, email string) (User, error)
}

type SessionStore interface {
	Create(ctx context.Context, s Session) error
	Get(ctx context.Context, id string) (Session, User, error) // one query, joined
	Delete(ctx context.Context, id string) error
}

var (
	// ErrNotFound is what a store returns for a row that is not there.
	ErrNotFound = errors.New("not found")
	// ErrEmailTaken is the unique-violation on users.email, translated.
	ErrEmailTaken = errors.New("email already registered")
	// ErrCredentials is deliberately one error for both "no such email" and
	// "wrong password": the caller must not be able to enumerate accounts.
	ErrCredentials   = errors.New("email or password is wrong")
	ErrTooManyLogins = errors.New("too many failed logins")
	// ErrInvalid is wrapped with the detail; callers match with errors.Is.
	ErrInvalid = errors.New("invalid input")
)

// Options is the Service's configuration. main fills it from config; the
// package never reads the environment.
type Options struct {
	Users              UserStore
	Sessions           SessionStore
	SessionTTL         time.Duration
	LoginMaxFailures   int
	LoginFailureWindow time.Duration
	// Now is injected so session expiry is testable without sleeping.
	Now func() time.Time
}

type Service struct {
	users    UserStore
	sessions SessionStore
	ttl      time.Duration
	now      func() time.Time
	throttle *throttle
}

func New(o Options) *Service {
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Service{
		users:    o.Users,
		sessions: o.Sessions,
		ttl:      o.SessionTTL,
		now:      o.Now,
		throttle: newThrottle(o.LoginMaxFailures, o.LoginFailureWindow, o.Now),
	}
}

// Signup creates the account and logs it straight in. It returns the Session
// as well as the User because otherwise the caller would have to call Login
// immediately after, paying for a second 64 MiB argon2id hash to learn what
// this call already knows.
func (s *Service) Signup(ctx context.Context, email, displayName, password string) (User, Session, error) {
	email, err := cleanEmail(email)
	if err != nil {
		return User{}, Session{}, err
	}
	displayName, err = cleanDisplayName(displayName)
	if err != nil {
		return User{}, Session{}, err
	}
	if err := checkPassword(password); err != nil {
		return User{}, Session{}, err
	}

	hash, err := HashPassword(password)
	if err != nil {
		return User{}, Session{}, err
	}
	u := User{
		ID:           "u_" + rand.Text(),
		Email:        email,
		DisplayName:  displayName,
		PasswordHash: hash,
		CreatedAt:    s.now(),
	}
	if err := s.users.Create(ctx, u); err != nil {
		return User{}, Session{}, err
	}
	sess, err := s.mint(ctx, u)
	return u, sess, err
}

// decoy is a real hash of a password nobody has. Verifying against it costs
// exactly what verifying a real one costs, so an unknown email and a wrong
// password take the same time — otherwise the response clock enumerates
// accounts. Computed once, on first use, rather than at init.
var decoy = sync.OnceValue(func() string {
	h, err := HashPassword("there is no user here")
	if err != nil {
		panic(err) // crypto/rand failed; nothing above this can cope either
	}
	return h
})

// Login verifies credentials and mints a Session. ip is used only for the
// failed-login throttle, which is keyed on IP *and* email so that one
// attacker cannot lock out a victim by guessing at their address.
func (s *Service) Login(ctx context.Context, email, password, ip string) (User, Session, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	key := ip + "\x00" + email
	if s.throttle.blocked(key) {
		return User{}, Session{}, ErrTooManyLogins
	}

	u, err := s.users.ByEmail(ctx, email)
	switch {
	case errors.Is(err, ErrNotFound):
		VerifyPassword(decoy(), password)
		s.throttle.fail(key)
		return User{}, Session{}, ErrCredentials
	case err != nil:
		return User{}, Session{}, err
	}
	if !VerifyPassword(u.PasswordHash, password) {
		s.throttle.fail(key)
		return User{}, Session{}, ErrCredentials
	}

	s.throttle.succeed(key)
	sess, err := s.mint(ctx, u)
	return u, sess, err
}

func (s *Service) Logout(ctx context.Context, sessionID string) error {
	return s.sessions.Delete(ctx, sessionID)
}

func (s *Service) mint(ctx context.Context, u User) (Session, error) {
	now := s.now()
	sess := Session{
		ID:        rand.Text(), // 128+ bits of base32: a session ID is a bearer token
		UserID:    u.ID,
		CreatedAt: now,
		ExpiresAt: now.Add(s.ttl),
	}
	if err := s.sessions.Create(ctx, sess); err != nil {
		return Session{}, err
	}
	return sess, nil
}

// Input validation. This is a trust boundary, so the limits are here rather
// than in a handler that might be bypassed by the next caller.

func cleanEmail(raw string) (string, error) {
	e := strings.ToLower(strings.TrimSpace(raw))
	if len(e) == 0 || len(e) > 254 {
		return "", fmt.Errorf("%w: email must be 1–254 characters", ErrInvalid)
	}
	if _, err := mail.ParseAddress(e); err != nil {
		return "", fmt.Errorf("%w: email is not a valid address", ErrInvalid)
	}
	return e, nil
}

func cleanDisplayName(raw string) (string, error) {
	n := strings.TrimSpace(raw)
	if n == "" || len([]rune(n)) > 64 {
		return "", fmt.Errorf("%w: display name must be 1–64 characters", ErrInvalid)
	}
	for _, r := range n {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("%w: display name may not contain control characters", ErrInvalid)
		}
	}
	return n, nil
}

func checkPassword(p string) error {
	// The upper bound is not pedantry: argon2id on a 10 MB password is a
	// denial of service the attacker pays nothing for.
	if len(p) < 8 || len(p) > 1024 {
		return fmt.Errorf("%w: password must be 8–1024 bytes", ErrInvalid)
	}
	return nil
}
