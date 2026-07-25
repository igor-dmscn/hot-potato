package mirror

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNoInstance is an instance that is not registered — almost always one that
// has died, taking its Transfers with it (ADR 0007).
var ErrNoInstance = errors.New("no such instance")

const instanceKey = "instance:"

// Instances is the directory every ownership redirect goes through: instance ID
// to the base URL that reaches it.
type Instances struct {
	client *redis.Client
}

func NewInstances(client *redis.Client) *Instances {
	return &Instances{client: client}
}

// Register publishes this instance's whereabouts.
func (i *Instances) Register(ctx context.Context, instance, externalURL string, ttl time.Duration) error {
	if err := i.client.Set(ctx, instanceKey+instance, externalURL, ttl).Err(); err != nil {
		return fmt.Errorf("redis register instance: %w", err)
	}
	return nil
}

// Lookup is one GET. That is the whole cost of resolving an owner.
func (i *Instances) Lookup(ctx context.Context, instance string) (string, error) {
	url, err := i.client.Get(ctx, instanceKey+instance).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return "", ErrNoInstance
	case err != nil:
		return "", fmt.Errorf("redis lookup instance: %w", err)
	}
	return url, nil
}

// Keep re-registers on a ticker until ctx is done, then withdraws.
//
// The withdrawal is what makes a clean shutdown clean: without it, requests
// keep being redirected to a process that has gone until the TTL runs out.
func (i *Instances) Keep(ctx context.Context, instance, externalURL string, ttl, refresh time.Duration) {
	register := func() {
		c, cancel := context.WithTimeout(context.WithoutCancel(ctx), refresh)
		defer cancel()
		if err := i.Register(c, instance, externalURL, ttl); err != nil {
			slog.Warn("register instance", "instance", instance, "err", err)
		}
	}
	register()

	ticker := time.NewTicker(refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := i.client.Del(c, instanceKey+instance).Err(); err != nil {
				slog.Warn("withdraw instance", "instance", instance, "err", err)
			}
			return
		case <-ticker.C:
			register()
		}
	}
}
