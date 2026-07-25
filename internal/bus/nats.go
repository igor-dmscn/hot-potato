package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// NATS is the default multi-instance bus.
//
// One subject, and every instance subscribes to all of it: addressing is a
// field on the Event, not a subject, because Kafka has no per-key subscribe and
// narrowing here would break the swap (ADR 0006).
type NATS struct {
	conn    *nats.Conn
	subject string
	buffer  int
	seq     atomic.Uint64
}

func NewNATS(url, subject string, o Options) (*NATS, error) {
	if o.Buffer <= 0 {
		o.Buffer = 256
	}
	conn, err := nats.Connect(url,
		nats.Name("hotpotato"),
		// Reconnect forever: a bus outage should degrade the control plane, not
		// end the process. Snapshots heal whatever was missed (ADR 0003).
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("nats disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			slog.Info("nats reconnected", "url", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return &NATS{conn: conn, subject: subject, buffer: o.Buffer}, nil
}

func (n *NATS) Publish(_ context.Context, e Event) error {
	if e.ID == 0 {
		// Per-instance, so IDs are unique to a publisher and not globally
		// ordered. Nothing reads them back; they exist for debugging.
		e.ID = n.seq.Add(1)
	}
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if err := n.conn.Publish(n.subject, data); err != nil {
		return fmt.Errorf("nats publish: %w", err)
	}
	return nil
}

func (n *NATS) Subscribe(ctx context.Context) (<-chan Event, error) {
	raw := make(chan *nats.Msg, n.buffer)
	sub, err := n.conn.ChanSubscribe(n.subject, raw)
	if err != nil {
		return nil, fmt.Errorf("nats subscribe: %w", err)
	}

	out := make(chan Event, n.buffer)
	go func() {
		// Unsubscribe on the way out, or leak a subscription per subscriber.
		defer func() {
			if err := sub.Unsubscribe(); err != nil {
				slog.Debug("nats unsubscribe", "err", err)
			}
			close(out)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-raw:
				var e Event
				if err := json.Unmarshal(msg.Data, &e); err != nil {
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

func (n *NATS) Close() error {
	// Drain rather than Close: anything already published gets flushed.
	if err := n.conn.Drain(); err != nil {
		n.conn.Close()
		return fmt.Errorf("nats drain: %w", err)
	}
	return nil
}
