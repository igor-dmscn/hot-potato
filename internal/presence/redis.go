package presence

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// keyPrefix is the namespace for presence keys.
const keyPrefix = "presence:"

// RedisOptions configures the distributed implementation.
type RedisOptions struct {
	Options
	// Instance is this process's ID. It is part of the key, not just the value —
	// see the comment on Redis below.
	Instance string
	// TTL is how long a claim survives without a refresh. A hard-killed
	// instance needs no cleanup: its keys simply expire (ADR 0005).
	TTL time.Duration
	// Refresh is how often live claims are renewed. It must be comfortably
	// shorter than TTL.
	Refresh time.Duration
}

// Redis is presence across instances: one key per (User, instance), with a TTL.
//
// DESIGN §9 describes one key per User carrying the instance as a value. That
// breaks for a User with tabs on two instances: the second SET overwrites the
// first, and whichever instance loses its last Stream first deletes a claim
// that is still true. Keying by both, and deriving a User's presence from
// whether *any* claim survives, is the same idea with that hole closed.
type Redis struct {
	client *redis.Client
	o      RedisOptions

	mu    sync.Mutex
	local map[string]*entry // Users holding Streams on this instance

	stop chan struct{}
	done chan struct{}
}

func NewRedis(client *redis.Client, o RedisOptions) *Redis {
	if o.Announce == nil {
		o.Announce = func(string, User) {}
	}
	r := &Redis{
		client: client,
		o:      o,
		local:  map[string]*entry{},
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	// One ticker for every local User. One timer per Stream is what falls over
	// at ten thousand of them.
	go r.refresher()
	return r
}

func (r *Redis) key(userID string) string {
	return keyPrefix + userID + ":" + r.o.Instance
}

func (r *Redis) Online(ctx context.Context, u User) error {
	value, err := json.Marshal(u)
	if err != nil {
		return fmt.Errorf("marshal presence: %w", err)
	}
	if err := r.client.Set(ctx, r.key(u.ID), value, r.o.TTL).Err(); err != nil {
		return fmt.Errorf("redis set presence: %w", err)
	}

	r.mu.Lock()
	e, known := r.local[u.ID]
	if known {
		if e.grace != nil {
			e.grace.Stop()
			e.grace = nil
		}
		e.u = u
	} else {
		r.local[u.ID] = &entry{u: u}
	}
	r.mu.Unlock()

	if !known {
		// Every instance announces its own arrivals; the others hear it on the
		// bus. A User arriving on two instances announces twice, which is
		// idempotent for anybody keeping a set.
		r.o.Announce(EventOnline, u)
	}
	return nil
}

func (r *Redis) Offline(_ context.Context, userID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.local[userID]
	if !ok || e.grace != nil {
		return nil
	}
	e.grace = time.AfterFunc(r.o.Grace, func() { r.expire(userID) })
	return nil
}

// expire drops this instance's claim, and announces the User offline only if no
// other instance still claims them.
func (r *Redis) expire(userID string) {
	r.mu.Lock()
	e, ok := r.local[userID]
	if !ok || e.grace == nil {
		r.mu.Unlock()
		return
	}
	u := e.u
	delete(r.local, userID)
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.client.Del(ctx, r.key(userID)).Err(); err != nil {
		slog.Error("redis del presence", "user", userID, "err", err)
	}
	elsewhere, err := r.claimed(ctx, userID)
	if err != nil {
		slog.Error("redis scan presence", "user", userID, "err", err)
		// Announcing on an unreadable store would remove a User who may still
		// be here. Their key expires on its own if they are not.
		return
	}
	if !elsewhere {
		r.o.Announce(EventOffline, u)
	}
}

// claimed reports whether any instance still holds a claim on userID.
func (r *Redis) claimed(ctx context.Context, userID string) (bool, error) {
	var cursor uint64
	for {
		keys, next, err := r.client.Scan(ctx, cursor, keyPrefix+userID+":*", 100).Result()
		if err != nil {
			return false, err
		}
		if len(keys) > 0 {
			return true, nil
		}
		if next == 0 {
			return false, nil
		}
		cursor = next
	}
}

// List is every User online anywhere, deduplicated across instances.
//
// SCAN, never KEYS: KEYS is O(n) with the whole keyspace blocked for the
// duration, on the one server every instance shares.
func (r *Redis) List(ctx context.Context) ([]User, error) {
	var keys []string
	var cursor uint64
	for {
		batch, next, err := r.client.Scan(ctx, cursor, keyPrefix+"*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("redis scan presence: %w", err)
		}
		keys = append(keys, batch...)
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(keys) == 0 {
		return []User{}, nil
	}

	values, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget presence: %w", err)
	}

	seen := make(map[string]User, len(values))
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			continue // expired between the SCAN and the MGET
		}
		var u User
		if err := json.Unmarshal([]byte(s), &u); err != nil {
			slog.Warn("bad presence value", "err", err)
			continue
		}
		seen[u.ID] = u
	}

	out := make([]User, 0, len(seen))
	for _, u := range seen {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DisplayName < out[j].DisplayName })
	return out, nil
}

// refresher renews every local claim on one ticker.
func (r *Redis) refresher() {
	defer close(r.done)
	ticker := time.NewTicker(r.o.Refresh)
	defer ticker.Stop()

	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.mu.Lock()
			users := make([]User, 0, len(r.local))
			for _, e := range r.local {
				if e.grace == nil { // not on the way out
					users = append(users, e.u)
				}
			}
			r.mu.Unlock()
			if len(users) == 0 {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), r.o.Refresh)
			pipe := r.client.Pipeline()
			for _, u := range users {
				value, err := json.Marshal(u)
				if err != nil {
					continue
				}
				pipe.Set(ctx, r.key(u.ID), value, r.o.TTL)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				// Presence survives a Redis restart because this runs again in
				// Refresh seconds and re-registers everyone.
				slog.Warn("redis presence refresh", "users", len(users), "err", err)
			}
			cancel()
		}
	}
}

func (r *Redis) Close() error {
	select {
	case <-r.stop:
		return nil // already closed
	default:
	}
	close(r.stop)
	<-r.done

	r.mu.Lock()
	keys := make([]string, 0, len(r.local))
	for id, e := range r.local {
		if e.grace != nil {
			e.grace.Stop()
		}
		keys = append(keys, r.key(id))
	}
	r.local = map[string]*entry{}
	r.mu.Unlock()

	if len(keys) == 0 {
		return nil
	}
	// A clean shutdown drops its claims immediately rather than making everyone
	// wait out the TTL.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redis del presence: %w", err)
	}
	return nil
}

// UserFromKey is exported for tests and debugging: presence:<user>:<instance>.
func UserFromKey(key string) string {
	rest := strings.TrimPrefix(key, keyPrefix)
	user, _, _ := strings.Cut(rest, ":")
	return user
}
