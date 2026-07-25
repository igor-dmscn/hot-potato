package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"hotpotato/internal/auth"
)

// Sessions implements auth.SessionStore.
type Sessions struct{ pool *pgxpool.Pool }

func (s *Sessions) Create(ctx context.Context, x auth.Session) error {
	_, err := s.pool.Exec(ctx,
		`insert into sessions (id, user_id, created_at, expires_at) values ($1, $2, $3, $4)`,
		x.ID, x.UserID, x.CreatedAt, x.ExpiresAt)
	if err != nil {
		return fmt.Errorf("insert session: %w", err)
	}
	return nil
}

// Get joins the user, because every authenticated request needs both and two
// round trips per request is one too many.
func (s *Sessions) Get(ctx context.Context, id string) (auth.Session, auth.User, error) {
	var sess auth.Session
	var u auth.User
	err := s.pool.QueryRow(ctx,
		`select s.id, s.user_id, s.created_at, s.expires_at,
		        u.id, u.email, u.display_name, u.password_hash, u.created_at
		   from sessions s join users u on u.id = s.user_id
		  where s.id = $1`, id).
		Scan(&sess.ID, &sess.UserID, &sess.CreatedAt, &sess.ExpiresAt,
			&u.ID, &u.Email, &u.DisplayName, &u.PasswordHash, &u.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return auth.Session{}, auth.User{}, auth.ErrNotFound
	}
	if err != nil {
		return auth.Session{}, auth.User{}, fmt.Errorf("select session: %w", err)
	}
	return sess, u, nil
}

func (s *Sessions) Delete(ctx context.Context, id string) error {
	if _, err := s.pool.Exec(ctx, `delete from sessions where id = $1`, id); err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	return nil
}
