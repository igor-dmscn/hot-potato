package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"hotpotato/internal/relay"
)

// position is where in a Payload the relay expects the next byte to come from:
// which entry, and how far into it.
type position struct {
	entry  int
	offset int64
}

func (p position) String() string { return fmt.Sprintf("entry %d offset %d", p.entry, p.offset) }

// uploadError is a chunk that did not finish. It carries the position the server
// reported, when the server got a word in.
type uploadError struct {
	status   int
	message  string
	expected position
	// mismatch reports that the Sender asked to start in the wrong place, which
	// is the one failure it can fix by itself.
	mismatch bool
	cause    error
}

func (e *uploadError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return fmt.Sprintf("%d: %s", e.status, e.message)
}

func (e *uploadError) Unwrap() error { return e.cause }

// positionFrom reads the expected position out of a 409 or 502.
func positionFrom(res *http.Response) position {
	var p position
	if v := res.Header.Get(relay.HeaderExpectedEntry); v != "" {
		p.entry, _ = strconv.Atoi(v)
	}
	if v := res.Header.Get(relay.HeaderExpectedOffset); v != "" {
		p.offset, _ = strconv.ParseInt(v, 10, 64)
	}
	return p
}

// awaitResume waits for the server to invite this Sender back, and to say where
// from.
//
// Every invitation carries a position, including one for a Recipient that came
// back with a Range and rewound the relay to a byte the Sender had already sent.
// The invitation is the authority, not the failed response. A Sender whose upload
// was interrupted usually never sees its own response at all — net/http reports
// the broken request body instead of whatever the server said — and even when it
// does, the position only becomes meaningful once the Recipient is parked again.
// Both facts arrive together on the control plane, which is what the control
// plane is for.
func awaitResume(ctx context.Context, s *stream, id string) (position, error) {
	for {
		e, err := s.await(ctx, "transfer.ready", "transfer.failed", "transfer.canceled", "transfer.denied")
		if err != nil {
			return position{}, err
		}
		if e.Name != "transfer.ready" {
			return position{}, fmt.Errorf("%s: %s", e.Name, e.Data)
		}

		var ready struct {
			ID     string `json:"id"`
			Resume bool   `json:"resume"`
			Entry  int    `json:"entry"`
			Offset int64  `json:"offset"`
		}
		if err := json.Unmarshal(e.Data, &ready); err != nil {
			return position{}, fmt.Errorf("transfer.ready payload: %w", err)
		}
		if ready.ID != id {
			continue // somebody else's Transfer
		}
		return position{entry: ready.Entry, offset: ready.Offset}, nil
	}
}
