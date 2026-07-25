package transfer

import (
	"context"
	"time"
)

// ReadModel is a mirror of Transfer metadata that any instance can read.
//
// The Owner's memory is the write model (ADR 0007). This exists for exactly one
// job: a snapshot has to include the Transfers a User is party to, and some of
// them are owned by other instances. Nothing decides anything from it.
type ReadModel interface {
	// Put mirrors a Transfer, replacing whatever was there.
	Put(ctx context.Context, t Transfer, ttl time.Duration) error
	// ForUser is every mirrored Transfer this User is party to.
	ForUser(ctx context.Context, userID string, now time.Time) ([]Transfer, error)
}

// Local is the single-instance ReadModel: the write model is in this process, so
// there is nothing to mirror and nothing to read back.
type Local struct {
	registry *Registry
	window   time.Duration
}

func NewLocal(r *Registry, terminalWindow time.Duration) *Local {
	return &Local{registry: r, window: terminalWindow}
}

func (l *Local) Put(context.Context, Transfer, time.Duration) error { return nil }

func (l *Local) ForUser(_ context.Context, userID string, now time.Time) ([]Transfer, error) {
	return l.registry.ForUser(userID, now, l.window), nil
}
