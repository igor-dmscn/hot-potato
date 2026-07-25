package httpapi

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"hotpotato/internal/transfer"
)

// reportProgress publishes transfer.progress while a relay is running, and
// returns a function that stops it.
//
// The copy owns the counter and writes to it; this goroutine only reads. That
// separation is why relay.Copy knows nothing about events: it is handed a
// func(int64) and never learns what happens to the number.
//
// The stop function does not return until the goroutine has exited, which is
// what guarantees no progress event can be published after the terminal one.
func (a *api) reportProgress(ctx context.Context, t transfer.Transfer, counted *atomic.Int64, started time.Time) func() {
	if a.progressInterval <= 0 {
		return func() {}
	}

	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(a.progressInterval)
		defer ticker.Stop()

		audience := parties(t)
		last, lastAt := int64(0), started

		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case tick := <-ticker.C:
				n := counted.Load()
				// Suppressed when unchanged: a stalled relay should go quiet,
				// not repeat itself four times a second.
				if n == last {
					continue
				}
				// The rate comes from the delta since the previous *published*
				// tick, so a suppressed gap does not read as a speed-up.
				//
				// Floored at 1: integer division truncates, so a small delta over
				// a long gap — a starved ticker on a busy machine — would report
				// zero bytes per second while bytes were demonstrably moving.
				rate := int64(1)
				if elapsed := tick.Sub(lastAt).Seconds(); elapsed > 0 {
					rate = max(1, int64(math.Round(float64(n-last)/elapsed)))
				}
				last, lastAt = n, tick

				a.emit(ctx, EventTransferProgress, audience, map[string]any{
					"id":          t.ID,
					"bytes":       n,
					"total":       t.Payload.TotalBytes,
					"bytesPerSec": rate,
				})
			}
		}
	}()

	// Idempotent, because the relay has several endings — completion, a write
	// failure, a Recipient walking away — and each of them wants the ticker
	// stopped. Closing a closed channel is a panic, and a panic inside a handler
	// takes the connection with it.
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}
