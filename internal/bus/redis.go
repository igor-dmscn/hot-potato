package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// Redis is the bus over PUBLISH/SUBSCRIBE on a single channel.
//
// Redis pub/sub is fire-and-forget with no persistence and no replay, which
// happens to be exactly the contract this interface promises (ADR 0006). It is
// also already a hard dependency for presence, so choosing it means one fewer
// service to run.
type Redis struct {
	client  *redis.Client
	channel string
	buffer  int
	seq     atomic.Uint64
}

func NewRedis(url, channel string, o Options) (*Redis, error) {
	if o.Buffer <= 0 {
		o.Buffer = 256
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis url: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Redis{client: client, channel: channel, buffer: o.Buffer}, nil
}

func (r *Redis) Publish(ctx context.Context, e Event) error {
	if e.ID == 0 {
		e.ID = r.seq.Add(1)
	}
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	// Unlike NATS, this waits for the server to acknowledge the command — worth
	// knowing when comparing publish latencies.
	if err := r.client.Publish(ctx, r.channel, data).Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}
	return nil
}

func (r *Redis) Subscribe(ctx context.Context) (<-chan Event, error) {
	ps := r.client.Subscribe(ctx, r.channel)
	// Wait for the subscription to be live, so an event published straight after
	// this returns is not silently dropped.
	if _, err := ps.Receive(ctx); err != nil {
		ps.Close()
		return nil, fmt.Errorf("redis subscribe: %w", err)
	}

	out := make(chan Event, r.buffer)
	go func() {
		defer func() {
			ps.Close()
			close(out)
		}()
		messages := ps.Channel(redis.WithChannelSize(r.buffer))
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				var e Event
				if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
					slog.Error("bad event on the bus", "err", err)
					continue
				}
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (r *Redis) Close() error { return r.client.Close() }
