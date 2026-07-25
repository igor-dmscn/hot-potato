package mirror

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"hotpotato/internal/transfer"
)

const (
	transferKey = "transfer:"
	userZSet    = "user:" // user:<id>:transfers
)

// Transfers is the Transfer read model: a TTL'd mirror of metadata, plus a
// sorted set per User so a snapshot is three round trips instead of a scan.
type Transfers struct {
	client *redis.Client
}

func NewTransfers(client *redis.Client) *Transfers {
	return &Transfers{client: client}
}

func zsetFor(userID string) string { return userZSet + userID + ":transfers" }

// Put mirrors one Transfer under both parties. The score is when the entry stops
// being interesting, which is what lets ForUser drop stale members with a range
// delete rather than reading them first.
func (t *Transfers) Put(ctx context.Context, tr transfer.Transfer, ttl time.Duration) error {
	value, err := json.Marshal(tr)
	if err != nil {
		return fmt.Errorf("marshal transfer: %w", err)
	}
	expiry := float64(tr.CreatedAt.Add(ttl).Unix())

	pipe := t.client.Pipeline()
	pipe.Set(ctx, transferKey+string(tr.ID), value, ttl)
	for _, party := range []string{tr.Sender, tr.Recipient} {
		pipe.ZAdd(ctx, zsetFor(party), redis.Z{Score: expiry, Member: string(tr.ID)})
		// The set itself expires too, or a User who signs up once leaves a key
		// behind forever.
		pipe.Expire(ctx, zsetFor(party), ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis put transfer: %w", err)
	}
	return nil
}

// ForUser reads a User's Transfers: drop what has expired, list the rest, fetch
// them in one MGET.
func (t *Transfers) ForUser(ctx context.Context, userID string, now time.Time) ([]transfer.Transfer, error) {
	key := zsetFor(userID)
	if err := t.client.ZRemRangeByScore(ctx, key, "-inf",
		strconv.FormatInt(now.Unix(), 10)).Err(); err != nil {
		return nil, fmt.Errorf("redis trim transfers: %w", err)
	}

	ids, err := t.client.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis list transfers: %w", err)
	}
	if len(ids) == 0 {
		return []transfer.Transfer{}, nil
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = transferKey + id
	}
	values, err := t.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget transfers: %w", err)
	}

	out := make([]transfer.Transfer, 0, len(values))
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			continue // the metadata expired before the set member did
		}
		var tr transfer.Transfer
		if err := json.Unmarshal([]byte(s), &tr); err != nil {
			slog.Warn("bad transfer in the read model", "err", err)
			continue
		}
		out = append(out, tr)
	}
	return out, nil
}
