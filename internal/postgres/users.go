package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"hotpotato/internal/auth"
)

// Users implements auth.UserStore.
type Users struct{ pool *pgxpool.Pool }

const uniqueViolation = "23505"

func (u *Users) Create(ctx context.Context, x auth.User) error {
	_, err := u.pool.Exec(ctx,
		`insert into users (id, email, display_name, password_hash, created_at)
		 values ($1, $2, $3, $4, $5)`,
		x.ID, x.Email, x.DisplayName, x.PasswordHash, x.CreatedAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		// Translated at the boundary: nothing above this package should have to
		// know a Postgres error code to tell a taken email from a broken one.
		return auth.ErrEmailTaken
	}
	if err != nil {
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

func (u *Users) ByEmail(ctx context.Context, email string) (auth.User, error) {
	var x auth.User
	err := u.pool.QueryRow(ctx,
		`select id, email, display_name, password_hash, created_at
		   from users where email = $1`, email).
		Scan(&x.ID, &x.Email, &x.DisplayName, &x.PasswordHash, &x.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return auth.User{}, auth.ErrNotFound
	}
	if err != nil {
		return auth.User{}, fmt.Errorf("select user: %w", err)
	}
	return x, nil
}
