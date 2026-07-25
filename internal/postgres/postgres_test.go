package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	"hotpotato/internal/auth"
)

// The only tests in the tree that need docker. HP_TEST_DATABASE_URL is
// deliberately a different variable from the server's: pointing a test suite
// that truncates nothing at a real database should still be a deliberate act.
func open(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("HP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("HP_TEST_DATABASE_URL not set; start compose to run the Postgres suite")
	}
	ctx := context.Background()
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return store
}

func user() auth.User {
	return auth.User{
		ID:           "u_" + rand.Text(),
		Email:        rand.Text() + "@example.com",
		DisplayName:  "ana",
		PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$c2FsdHNhbHRzYWx0c2Ex$ZGlnZXN0",
		CreatedAt:    time.Now().UTC().Truncate(time.Microsecond),
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	store := open(t)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestUsersRoundTrip(t *testing.T) {
	store := open(t)
	ctx := context.Background()
	users := store.Users()
	want := user()

	if err := users.Create(ctx, want); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := users.ByEmail(ctx, want.Email)
	if err != nil {
		t.Fatalf("ByEmail: %v", err)
	}
	if got.ID != want.ID || got.DisplayName != want.DisplayName || got.PasswordHash != want.PasswordHash {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
}

// citext, not lower(): the column itself is case-insensitive, so a second
// signup differing only in case collides instead of creating a twin.
func TestEmailIsCaseInsensitiveAndUnique(t *testing.T) {
	store := open(t)
	ctx := context.Background()
	users := store.Users()
	first := user()

	if err := users.Create(ctx, first); err != nil {
		t.Fatalf("Create: %v", err)
	}
	shouty := user()
	shouty.Email = upper(first.Email)
	if err := users.Create(ctx, shouty); !errors.Is(err, auth.ErrEmailTaken) {
		t.Fatalf("Create with a differently-cased email = %v, want ErrEmailTaken", err)
	}
	if _, err := users.ByEmail(ctx, upper(first.Email)); err != nil {
		t.Errorf("ByEmail with a differently-cased email: %v", err)
	}
}

func TestByEmailUnknown(t *testing.T) {
	store := open(t)
	if _, err := store.Users().ByEmail(context.Background(), "nobody@example.com"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("ByEmail(unknown) = %v, want ErrNotFound", err)
	}
}

func TestSessionsRoundTripAndDelete(t *testing.T) {
	store := open(t)
	ctx := context.Background()
	u := user()
	if err := store.Users().Create(ctx, u); err != nil {
		t.Fatalf("Create user: %v", err)
	}

	sessions := store.Sessions()
	want := auth.Session{
		ID:        rand.Text(),
		UserID:    u.ID,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		ExpiresAt: time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
	}
	if err := sessions.Create(ctx, want); err != nil {
		t.Fatalf("Create session: %v", err)
	}

	gotSess, gotUser, err := sessions.Get(ctx, want.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gotSess.UserID != u.ID || gotUser.ID != u.ID {
		t.Errorf("Get = %+v / %+v, want both to point at %s", gotSess, gotUser, u.ID)
	}
	if !gotSess.ExpiresAt.Equal(want.ExpiresAt) {
		t.Errorf("ExpiresAt = %v, want %v", gotSess.ExpiresAt, want.ExpiresAt)
	}

	if err := sessions.Delete(ctx, want.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := sessions.Get(ctx, want.ID); !errors.Is(err, auth.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}
