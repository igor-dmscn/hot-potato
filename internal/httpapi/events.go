package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"hotpotato/internal/auth"
	"hotpotato/internal/bus"
	"hotpotato/internal/presence"
	"hotpotato/internal/sse"
	"hotpotato/internal/transfer"
)

// EventSnapshot is the first event on every Stream (ADR 0003): the complete
// current picture, after which everything is a delta.
const EventSnapshot = "snapshot"

// snapshotView is DESIGN §7's snapshot payload.
type snapshotView struct {
	Self      userDTO             `json:"self"`
	StreamID  string              `json:"streamId"`
	Instance  string              `json:"instance"`
	Users     []presence.User     `json:"users"`
	Transfers []transfer.Transfer `json:"transfers"`
}

// events is GET /events: one SSE Stream per browser tab.
func (a *api) events(w http.ResponseWriter, r *http.Request) {
	u, _ := auth.UserFrom(r.Context())

	stream, first := a.streams.Open(u.ID)
	if stream == nil || a.draining.Load() {
		if stream != nil {
			a.streams.Close(stream)
		}
		writeError(w, http.StatusServiceUnavailable, codeDraining,
			"this instance is shutting down; reconnect")
		return
	}
	defer func() {
		if last := a.streams.Close(stream); last {
			// The request context is already cancelled by the time this runs,
			// and the grace window still has to be started.
			if err := a.presence.Offline(context.WithoutCancel(r.Context()), u.ID); err != nil {
				slog.Error("presence offline", "user", u.ID, "err", err)
			}
		}
	}()

	// Online before the snapshot, so the snapshot includes this User.
	if first {
		if err := a.presence.Online(r.Context(), presence.User{ID: u.ID, DisplayName: u.DisplayName}); err != nil {
			internalError(w, r, err)
			return
		}
	}
	snapshot, err := a.snapshot(r.Context(), u, stream.ID)
	if err != nil {
		internalError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Reverse proxies buffer a response body by default, which turns a live
	// stream into a file that arrives once it ends.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	sink := sseSink{w: w, rc: http.NewResponseController(w)}
	if err := stream.Pump(r.Context(), sink, sse.PumpOptions{
		// One shared clock for every Stream on this instance.
		Beats:         a.heartbeat,
		WriteDeadline: a.sse.WriteDeadline,
		Retry:         a.sse.Retry,
		First:         snapshot,
	}); err != nil {
		// A write failure here is a client that left mid-frame: normal, and
		// nothing can be reported to it.
		slog.Debug("stream ended", "stream", stream.ID, "user", u.ID, "err", err)
	}
}

func (a *api) snapshot(ctx context.Context, u auth.User, streamID string) ([]bus.Event, error) {
	users, err := a.presence.List(ctx)
	if err != nil {
		return nil, err
	}
	// Through the read model, not the local Registry: a User's Transfers may be
	// owned by other instances, and a snapshot has to include them (ADR 0007).
	transfers, err := a.readModel.ForUser(ctx, u.ID, a.now())
	if err != nil {
		return nil, err
	}
	e, err := bus.NewEvent(EventSnapshot, []string{u.ID}, snapshotView{
		Self:      dto(u),
		StreamID:  streamID,
		Instance:  a.instance,
		Users:     users,
		Transfers: transfers,
	})
	if err != nil {
		return nil, err
	}
	return []bus.Event{e}, nil
}

// sseSink adapts an http.ResponseWriter to sse.Sink. Together with relay's
// deadline writer it is the entire net/http surface of the streaming paths.
type sseSink struct {
	w  io.Writer
	rc *http.ResponseController
}

func (s sseSink) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s sseSink) Flush() error                { return optional(s.rc.Flush()) }

func (s sseSink) SetWriteDeadline(t time.Time) error {
	return optional(s.rc.SetWriteDeadline(t))
}

// optional swallows ErrNotSupported. A ResponseWriter that cannot flush or set
// deadlines — httptest.ResponseRecorder, or a middleware that wraps without
// forwarding — is a property of the writer, not a failure of the Stream.
func optional(err error) error {
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	return err
}
