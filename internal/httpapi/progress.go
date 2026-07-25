package httpapi

import (
	"context"
	"sync/atomic"
	"time"

	"hotpotato/internal/transfer"
)

// reportProgress starts publishing transfer.progress and returns a function
// that stops it. Phase 6 fills this in; until then the counter is read only
// once, when the relay ends.
func (a *api) reportProgress(_ context.Context, _ transfer.Transfer, _ *atomic.Int64, _ time.Time) func() {
	return func() {}
}
