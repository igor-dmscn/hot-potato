package bus

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka is the bus over one topic, with a consumer group per instance.
//
// It is here to be compared, not because it fits. Every instance needs every
// event, so every instance needs its own group; the offsets a group exists to
// track are meaningless for events that are worthless a second later; and a
// rebalance pauses consumption on a path that must never pause. See
// docs/bus-comparison.md for what that costs, measured.
type Kafka struct {
	producer *kgo.Client
	brokers  []string
	topic    string
	group    string
	buffer   int
	seq      atomic.Uint64
	// subs numbers the subscriptions, because each one needs a group of its own.
	// See Subscribe.
	subs atomic.Uint64
}

// NewKafka connects a producer. instance becomes part of the consumer group
// name, which is what makes every instance receive every event rather than
// sharing the partitions out between them.
func NewKafka(brokers, topic, instance string, o Options) (*Kafka, error) {
	if o.Buffer <= 0 {
		o.Buffer = 256
	}
	seeds := splitBrokers(brokers)

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DefaultProduceTopic(topic),
		kgo.AllowAutoTopicCreation(),
		// Latency over batching: these events are small, frequent and stale
		// almost immediately.
		kgo.ProducerLinger(0),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := producer.Ping(ctx); err != nil {
		producer.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}

	return &Kafka{
		producer: producer,
		brokers:  seeds,
		topic:    topic,
		// The group carries a per-process suffix, not just the instance ID.
		//
		// A stable group name looks tidier and costs 45 seconds of silence: when
		// a restarted instance rejoins a group whose previous member has not been
		// evicted yet, the rebalance waits out that member's session timeout
		// (session.timeout.ms, 45s by default) before anybody consumes anything.
		// Measured: with a stable name, a second run of the latency suite saw no
		// events at all for 20 seconds and gave up.
		//
		// A group nobody will ever rejoin is the honest description of what this
		// workload needs, and it is not what consumer groups are for.
		group:  "hp-" + instance + "-" + strings.ToLower(rand.Text()[:8]),
		buffer: o.Buffer,
	}, nil
}

func (k *Kafka) Publish(ctx context.Context, e Event) error {
	if e.ID == 0 {
		e.ID = k.seq.Add(1)
	}
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	// Asynchronous, like a NATS publish, so the comparison is between like and
	// like. A failure to produce is logged: the bus is at-most-once, and a
	// snapshot heals whatever was lost.
	k.producer.Produce(ctx, &kgo.Record{Value: data}, func(_ *kgo.Record, err error) {
		if err != nil {
			slog.Error("kafka produce", "err", err)
		}
	})
	return nil
}

// Subscribe joins a consumer group of its own.
//
// A group *per subscription*, not per instance, and the reason is the whole of
// ADR 0006's argument in one line: members of a Kafka consumer group divide the
// partitions between them, so two subscriptions sharing a group would each
// receive part of the firehose. The interface promises that every subscriber
// sees every event — which NATS, Redis and the in-memory bus all do naturally —
// so the group has to be unique, and every group created here is abandoned
// seconds later with offsets nobody will ever read.
func (k *Kafka) Subscribe(ctx context.Context) (<-chan Event, error) {
	group := fmt.Sprintf("%s-%d", k.group, k.subs.Add(1))

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(k.brokers...),
		kgo.ConsumeTopics(k.topic),
		kgo.ConsumerGroup(group),
		// At the end, always: an instance booting now has no use for events from
		// before it existed, and a snapshot covers what it missed.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}

	out := make(chan Event, k.buffer)
	go func() {
		defer func() {
			// Leave the group deliberately; otherwise the next instance waits out
			// a session timeout before the partitions move.
			consumer.Close()
			close(out)
		}()
		for {
			fetches := consumer.PollFetches(ctx)
			if fetches.IsClientClosed() || ctx.Err() != nil {
				return
			}
			fetches.EachError(func(topic string, partition int32, err error) {
				slog.Error("kafka fetch", "topic", topic, "partition", partition, "err", err)
			})
			var stop bool
			fetches.EachRecord(func(rec *kgo.Record) {
				if stop {
					return
				}
				var e Event
				if err := json.Unmarshal(rec.Value, &e); err != nil {
					slog.Error("bad event on the bus", "err", err)
					return
				}
				select {
				case out <- e:
				case <-ctx.Done():
					stop = true
				}
			})
			if stop {
				return
			}
		}
	}()
	return out, nil
}

func (k *Kafka) Close() error {
	k.producer.Close()
	return nil
}

func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
