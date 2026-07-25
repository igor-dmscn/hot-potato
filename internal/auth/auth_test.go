package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// clock is a hand-cranked time source. Every duration in the Service is
// injected, so nothing here sleeps.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func service(t *testing.T) (*Service, *Memory, *clock) {
	t.Helper()
	mem := NewMemory()
	clk := newClock()
	return New(Options{
		Users:              mem,
		Sessions:           mem.Sessions(),
		SessionTTL:         7 * 24 * time.Hour,
		LoginMaxFailures:   3,
		LoginFailureWindow: 15 * time.Minute,
		Now:                clk.now,
	}), mem, clk
}

func TestSignupThenLogin(t *testing.T) {
	t.Parallel()
	svc, _, clk := service(t)
	ctx := context.Background()

	u, sess, err := svc.Signup(ctx, "  Ana@Example.COM ", " ana ", "hunter2hunter2")
	if err != nil {
		t.Fatalf("Signup: %v", err)
	}
	if u.Email != "ana@example.com" {
		t.Errorf("Email = %q, want it lowercased and trimmed", u.Email)
	}
	if u.DisplayName != "ana" {
		t.Errorf("DisplayName = %q, want it trimmed", u.DisplayName)
	}
	if sess.UserID != u.ID || sess.ID == "" {
		t.Errorf("Signup returned session %+v, want one belonging to %s", sess, u.ID)
	}
	if want := clk.now().Add(7 * 24 * time.Hour); !sess.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", sess.ExpiresAt, want)
	}

	// Case and whitespace must not create a second identity.
	got, sess2, err := svc.Login(ctx, "ANA@example.com", "hunter2hunter2", "10.0.0.1")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if got.ID != u.ID {
		t.Errorf("Login returned %s, want %s", got.ID, u.ID)
	}
	if sess2.ID == sess.ID {
		t.Error("Login reused the signup session ID; each login is its own session")
	}
}

func TestSignupRejects(t *testing.T) {
	t.Parallel()
	svc, _, _ := service(t)
	ctx := context.Background()

	if _, _, err := svc.Signup(ctx, "ana@example.com", "ana", "hunter2hunter2"); err != nil {
		t.Fatalf("Signup: %v", err)
	}

	for name, tc := range map[string]struct {
		email, display, password string
		want                     error
	}{
		"duplicate email":  {"ana@example.com", "ana2", "hunter2hunter2", ErrEmailTaken},
		"duplicate cased":  {"ANA@example.com", "ana2", "hunter2hunter2", ErrEmailTaken},
		"no email":         {"", "ana", "hunter2hunter2", ErrInvalid},
		"not an address":   {"ana", "ana", "hunter2hunter2", ErrInvalid},
		"no display name":  {"b@example.com", "  ", "hunter2hunter2", ErrInvalid},
		"control in name":  {"b@example.com", "a\nb", "hunter2hunter2", ErrInvalid},
		"short password":   {"b@example.com", "ana", "hunter2", ErrInvalid},
		"endless password": {"b@example.com", "ana", string(make([]byte, 2000)), ErrInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := svc.Signup(ctx, tc.email, tc.display, tc.password)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Signup = %v, want %v", err, tc.want)
			}
		})
	}
}

// An unknown email and a wrong password must be indistinguishable: same error,
// and the same argon2id work, or the response clock enumerates accounts.
func TestLoginDoesNotRevealWhichEmailsExist(t *testing.T) {
	t.Parallel()
	svc, _, _ := service(t)
	ctx := context.Background()

	if _, _, err := svc.Signup(ctx, "ana@example.com", "ana", "hunter2hunter2"); err != nil {
		t.Fatalf("Signup: %v", err)
	}
	if _, _, err := svc.Login(ctx, "ana@example.com", "wrong-password", "10.0.0.1"); !errors.Is(err, ErrCredentials) {
		t.Errorf("wrong password = %v, want ErrCredentials", err)
	}
	if _, _, err := svc.Login(ctx, "nobody@example.com", "wrong-password", "10.0.0.2"); !errors.Is(err, ErrCredentials) {
		t.Errorf("unknown email = %v, want ErrCredentials", err)
	}
	if decoy() == "" {
		t.Error("the decoy hash is empty, so an unknown email skips the work")
	}
}

func TestLoginThrottlesFailuresPerIPAndEmail(t *testing.T) {
	t.Parallel()
	svc, _, clk := service(t)
	ctx := context.Background()

	if _, _, err := svc.Signup(ctx, "ana@example.com", "ana", "hunter2hunter2"); err != nil {
		t.Fatalf("Signup: %v", err)
	}
	for i := range 3 {
		if _, _, err := svc.Login(ctx, "ana@example.com", "nope", "10.0.0.1"); !errors.Is(err, ErrCredentials) {
			t.Fatalf("attempt %d = %v, want ErrCredentials", i, err)
		}
	}
	if _, _, err := svc.Login(ctx, "ana@example.com", "hunter2hunter2", "10.0.0.1"); !errors.Is(err, ErrTooManyLogins) {
		t.Fatalf("fourth attempt = %v, want ErrTooManyLogins even with the right password", err)
	}

	// Another IP guessing at the same email must not be locked out by the
	// first one's failures, or an attacker can lock any account out.
	if _, _, err := svc.Login(ctx, "ana@example.com", "hunter2hunter2", "10.0.0.2"); err != nil {
		t.Fatalf("a different IP = %v, want success", err)
	}

	clk.advance(16 * time.Minute)
	if _, _, err := svc.Login(ctx, "ana@example.com", "hunter2hunter2", "10.0.0.1"); err != nil {
		t.Fatalf("after the window = %v, want success", err)
	}
}

func TestLogoutDeletesOnlyThatSession(t *testing.T) {
	t.Parallel()
	svc, mem, _ := service(t)
	ctx := context.Background()

	_, first, err := svc.Signup(ctx, "ana@example.com", "ana", "hunter2hunter2")
	if err != nil {
		t.Fatalf("Signup: %v", err)
	}
	_, second, err := svc.Login(ctx, "ana@example.com", "hunter2hunter2", "10.0.0.1")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if err := svc.Logout(ctx, first.ID); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if _, _, err := mem.Get(ctx, first.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("the logged-out session is still there: %v", err)
	}
	if _, _, err := mem.Get(ctx, second.ID); err != nil {
		t.Errorf("logout took the other tab's session too: %v", err)
	}
}
