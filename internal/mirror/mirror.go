// Package mirror holds the Redis-backed read models that let an instance answer
// questions about state it does not own.
//
// Two of them: where the other instances are, and what Transfers exist. Both are
// mirrors of authoritative state that lives in some instance's memory, both are
// keyed with a TTL, and neither is ever the basis for a decision — a hard-killed
// instance needs no cleanup because its keys simply expire (ADR 0005, 0007).
package mirror

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Open connects to Redis and verifies it, so a bad URL fails at boot.
func Open(ctx context.Context, url string) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis url: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return client, nil
}
