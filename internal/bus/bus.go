// Package bus is the control plane's broadcast. Every instance publishes every
// event and subscribes to every event; addressing is a field on the event, not
// a subject, because Kafka has no per-key subscribe and the swap between
// implementations has to stay honest (ADR 0006).
package bus

import (
	"context"
	"encoding/json"
	"fmt"
)

// Event is one control-plane fact.
//
// The contract is deliberately the lowest common denominator of the four
// implementations: at-most-once delivery, no ordering guarantees across users,
// one firehose. Correctness rests on snapshots (ADR 0003), not on the bus.
type Event struct {
	// ID is for debugging and the SSE id: field. Nobody reads it back —
	// Last-Event-ID is deliberately ignored.
	ID uint64 `json:"id"`
	// Name is the event name, e.g. "user.online" or "transfer.progress".
	Name string `json:"name"`
	// Audience is the set of user IDs that should receive this event. Empty
	// means everyone. Every instance receives every event and filters locally.
	Audience []string `json:"audience,omitempty"`
	// Data is the event's JSON payload, already marshalled.
	Data json.RawMessage `json:"data"`
	// Trace carries a W3C traceparent so a trace can span instances (phase 10).
	Trace string `json:"trace,omitempty"`
}

// ForUser reports whether e should reach userID.
func (e Event) ForUser(userID string) bool {
	if len(e.Audience) == 0 {
		return true
	}
	for _, id := range e.Audience {
		if id == userID {
			return true
		}
	}
	return false
}

// NewEvent marshals payload into an Event. The ID is assigned by the bus at
// publish time.
func NewEvent(name string, audience []string, payload any) (Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal %s: %w", name, err)
	}
	return Event{Name: name, Audience: audience, Data: data}, nil
}

type Bus interface {
	Publish(ctx context.Context, e Event) error
	// Subscribe returns the firehose. The channel is closed when ctx is done.
	Subscribe(ctx context.Context) (<-chan Event, error)
	Close() error
}
