package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// One suite, four implementations. The interface is pinned to the lowest common
// denominator on purpose (ADR 0006), so any behaviour asserted here has to hold
// for all of them — and anything that does not belong here does not belong in
// the interface.

// busTestBuffer is well above the burst size used in the latency measurement,
// so what that measures is latency rather than each bus's overflow policy. At
// the production default of 32–256 an in-memory bus drops roughly a fifth of a
// 1000-event burst, which is at-most-once working as designed.
const busTestBuffer = 4096

type implementation struct {
	name string
	// open returns a bus, or skips when its broker is not running.
	open func(t *testing.T) Bus
}

func implementations() []implementation {
	return []implementation{
		{"memory", func(t *testing.T) Bus {
			return NewMemory(Options{Buffer: busTestBuffer})
		}},
		{"nats", func(t *testing.T) Bus {
			url := broker(t, "HP_TEST_NATS_URL")
			b, err := NewNATS(url, subject(t), Options{Buffer: busTestBuffer})
			if err != nil {
				t.Fatalf("nats: %v", err)
			}
			t.Cleanup(func() { b.Close() })
			return b
		}},
		{"redis", func(t *testing.T) Bus {
			url := broker(t, "HP_TEST_REDIS_URL")
			b, err := NewRedis(url, subject(t), Options{Buffer: busTestBuffer})
			if err != nil {
				t.Fatalf("redis: %v", err)
			}
			t.Cleanup(func() { b.Close() })
			return b
		}},
		{"kafka", func(t *testing.T) Bus {
			brokers := broker(t, "HP_TEST_KAFKA_BROKERS")
			b, err := NewKafka(brokers, subject(t), "test-"+strings.ToLower(t.Name()), Options{Buffer: busTestBuffer})
			if err != nil {
				t.Fatalf("kafka: %v", err)
			}
			t.Cleanup(func() { b.Close() })
			return b
		}},
	}
}

func broker(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s not set; start compose to run this implementation", key)
	}
	return v
}

// subject isolates a test's traffic from every other test's.
func subject(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '-'
		}
		return r
	}, t.Name())
	return "hp-test-" + strings.ToLower(name)
}

// receive waits for an event whose name matches, ignoring anything else.
//
// It republishes on a cadence until something arrives, because a Kafka consumer
// group takes a moment to join and an event published into that gap is simply
// gone — which is the contract, and the reason the design does not depend on it
// (ADR 0003).
func receive(t *testing.T, ch <-chan Event, republish func(), name string) Event {
	t.Helper()
	deadline := time.After(20 * time.Second)
	retry := time.NewTicker(250 * time.Millisecond)
	defer retry.Stop()

	republish()
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatal("the channel closed while waiting")
			}
			if e.Name == name {
				return e
			}
		case <-retry.C:
			republish()
		case <-deadline:
			t.Fatalf("no %q within twenty seconds", name)
			return Event{}
		}
	}
}

func TestBusDeliversToASubscriber(t *testing.T) {
	for _, impl := range implementations() {
		t.Run(impl.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			b := impl.open(t)
			ch, err := b.Subscribe(ctx)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			want, err := NewEvent("user.online", []string{"u_1"}, map[string]string{"id": "u_1"})
			if err != nil {
				t.Fatalf("NewEvent: %v", err)
			}
			got := receive(t, ch, func() {
				if err := b.Publish(ctx, want); err != nil {
					t.Errorf("Publish: %v", err)
				}
			}, "user.online")

			if got.ID == 0 {
				t.Error("the event arrived with no ID; every implementation assigns one")
			}
			if !slices.Equal(got.Audience, want.Audience) {
				t.Errorf("audience = %v, want %v", got.Audience, want.Audience)
			}
			var payload map[string]string
			if err := json.Unmarshal(got.Data, &payload); err != nil {
				t.Fatalf("data %q: %v", got.Data, err)
			}
			if payload["id"] != "u_1" {
				t.Errorf("data = %v", payload)
			}
		})
	}
}

// Fanout is the whole point: every instance subscribes to everything and filters
// locally, so two subscribers must each get their own copy.
func TestBusFansOutToEverySubscriber(t *testing.T) {
	for _, impl := range implementations() {
		t.Run(impl.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			b := impl.open(t)
			first, err := b.Subscribe(ctx)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			second, err := b.Subscribe(ctx)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			e, _ := NewEvent("transfer.progress", nil, map[string]int{"bytes": 1})
			publish := func() {
				if err := b.Publish(ctx, e); err != nil {
					t.Errorf("Publish: %v", err)
				}
			}
			receive(t, first, publish, "transfer.progress")
			receive(t, second, func() {}, "transfer.progress")
		})
	}
}

func TestBusUnsubscribesOnContext(t *testing.T) {
	for _, impl := range implementations() {
		t.Run(impl.name, func(t *testing.T) {
			b := impl.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			ch, err := b.Subscribe(ctx)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			cancel()

			deadline := time.After(20 * time.Second)
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						return // closed, as promised
					}
				case <-deadline:
					t.Fatal("the channel was not closed when its context was cancelled")
				}
			}
		})
	}
}

// Publish→deliver latency per implementation, measured two ways, because one
// number would be misleading.
//
// Paced is one event at a time, each waited for before the next: that is the
// transport's round trip. Burst is 1000 events published back to back: that is
// mostly queueing, and it is dominated by whether the publish call itself waits
// for anything. Redis's PUBLISH waits for a server reply and so paces itself;
// NATS and Kafka publishes do not, so a burst piles up in front of the
// subscriber and the "latency" is the depth of that pile.
//
// The numbers this prints are what docs/bus-comparison.md records.
func TestBusLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the latency measurement in -short mode")
	}
	const events = 1000

	for _, impl := range implementations() {
		t.Run(impl.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// A buffer well over the burst size, so what is measured is latency
			// rather than the bus's overflow policy.
			b := impl.open(t)
			ch, err := b.Subscribe(ctx)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			// Warm up until the subscription is genuinely live. For Kafka that is
			// the consumer group joining; for the others it is one round trip.
			warm, _ := NewEvent("warmup", nil, nil)
			receive(t, ch, func() { b.Publish(ctx, warm) }, "warmup")
			drain(ch)

			paced := measurePaced(t, ctx, b, ch, events)
			drain(ch)
			burst, delivered := measureBurst(t, ctx, b, ch, events)

			t.Logf("%s paced: p50=%s p99=%s | burst(%d): %d/%d delivered p50=%s p99=%s max=%s",
				impl.name,
				round(percentile(paced, 0.50)), round(percentile(paced, 0.99)),
				events, delivered, events,
				round(percentile(burst, 0.50)), round(percentile(burst, 0.99)),
				round(percentile(burst, 1)))
		})
	}
}

// measurePaced publishes one event at a time and waits for it.
func measurePaced(t *testing.T, ctx context.Context, b Bus, ch <-chan Event, events int) []time.Duration {
	t.Helper()
	out := make([]time.Duration, 0, events)
	for i := range events {
		e, err := NewEvent("bench", nil, time.Now().UnixNano())
		if err != nil {
			t.Fatalf("NewEvent: %v", err)
		}
		if err := b.Publish(ctx, e); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case got, ok := <-ch:
			if !ok {
				t.Fatal("the channel closed mid-measurement")
			}
			if d, ok := latencyOf(got); ok {
				out = append(out, d)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("event %d never arrived", i)
		}
	}
	return out
}

// measureBurst publishes everything and then reads whatever turns up.
func measureBurst(t *testing.T, ctx context.Context, b Bus, ch <-chan Event, events int) ([]time.Duration, int) {
	t.Helper()
	out := make([]time.Duration, 0, events)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range events {
			select {
			case got, ok := <-ch:
				if !ok {
					return
				}
				if d, ok := latencyOf(got); ok {
					out = append(out, d)
				}
			case <-time.After(5 * time.Second):
				return // the rest were dropped, which is allowed
			}
		}
	}()

	for range events {
		e, err := NewEvent("bench", nil, time.Now().UnixNano())
		if err != nil {
			t.Fatalf("NewEvent: %v", err)
		}
		if err := b.Publish(ctx, e); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	<-done
	return out, len(out)
}

func latencyOf(e Event) (time.Duration, bool) {
	if e.Name != "bench" {
		return 0, false
	}
	var sent int64
	if err := json.Unmarshal(e.Data, &sent); err != nil {
		return 0, false
	}
	return time.Duration(time.Now().UnixNano() - sent), true
}

func percentile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(float64(len(s)-1)*q)]
}

func drain(ch <-chan Event) {
	for {
		select {
		case <-ch:
		case <-time.After(200 * time.Millisecond):
			return
		}
	}
}

// round keeps the logged numbers readable.
func round(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return d.String()
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1000)
	default:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
}
