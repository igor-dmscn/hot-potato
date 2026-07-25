package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"hotpotato/internal/transfer"
)

type progressEvent struct {
	ID          transfer.ID `json:"id"`
	Bytes       int64       `json:"bytes"`
	Total       int64       `json:"total"`
	BytesPerSec int64       `json:"bytesPerSec"`
}

// A whole relay's worth of progress events, in order, plus the completion.
func TestProgressIsMonotonicAndEndsAtTheTotal(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) {
		o.progressInterval = time.Millisecond
		// A thousand progress events a second outruns a 32-event Stream buffer,
		// and an overflowing Stream is dropped on purpose (ADR 0003). That is
		// correct behaviour and not what this test is about.
		o.streamBuffer = 4096
	})
	p := newPair(t, x)

	parts := []part{pattern("big.bin", 32<<20)}
	total := sum(parts)
	id := p.offerAs(t, x, transfer.KindFile, "big.bin", total, 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)

	res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("POST /d: %v", err)
	}
	res.Body.Close()
	if r := <-got; r.bytes != total {
		t.Fatalf("received %d bytes, want %d", r.bytes, total)
	}

	// Read the Sender's stream to the completion, collecting progress on the way.
	var progress []progressEvent
	var completions int
	var completedBytes int64
	deadline := time.After(10 * time.Second)

collect:
	for {
		select {
		case f, ok := <-p.sender.frames:
			if !ok {
				t.Fatal("the stream closed before the completion")
			}
			switch f.Event {
			case EventTransferProgress:
				var e progressEvent
				if err := json.Unmarshal([]byte(f.Data), &e); err != nil {
					t.Fatalf("progress payload %q: %v", f.Data, err)
				}
				progress = append(progress, e)
			case EventTransferCompleted:
				var e struct {
					Bytes int64 `json:"bytes"`
				}
				json.Unmarshal([]byte(f.Data), &e)
				completions++
				completedBytes = e.Bytes
				break collect
			case EventTransferFailed:
				t.Fatalf("the transfer failed: %s", f.Data)
			}
		case <-deadline:
			t.Fatal("no completion within ten seconds")
		}
	}

	if len(progress) == 0 {
		t.Fatal("a 32 MB relay produced no progress events")
	}
	var last int64
	for i, e := range progress {
		switch {
		case e.ID != id:
			t.Fatalf("progress %d is about %s, not %s", i, e.ID, id)
		case e.Bytes <= last:
			t.Fatalf("progress went %d → %d at event %d; it must only ever rise", last, e.Bytes, i)
		case e.Bytes > total:
			t.Fatalf("progress reported %d of %d bytes", e.Bytes, total)
		case e.Total != total:
			t.Fatalf("progress reports a total of %d, want %d", e.Total, total)
		case e.BytesPerSec <= 0:
			t.Errorf("progress %d reports a rate of %d", i, e.BytesPerSec)
		}
		last = e.Bytes
	}
	if completions != 1 || completedBytes != total {
		t.Errorf("%d completions carrying %d bytes, want one carrying %d", completions, completedBytes, total)
	}

	// Nothing may follow the terminal event: the ticker is stopped, and waited
	// for, before the outcome is published.
	p.sender.quiet(t, EventTransferProgress, 200*time.Millisecond)
}

// A relay that is over before the first tick still reports its outcome — the
// progress stream is an extra, not the source of truth.
func TestATinyTransferStillCompletesWithoutProgress(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) { o.progressInterval = time.Hour })
	p := newPair(t, x)

	parts := []part{fixed("small.bin", []byte("just a few bytes"))}
	id := p.offerAs(t, x, transfer.KindFile, "small.bin", sum(parts), 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)
	res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("POST /d: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("POST /d = %d, want 204", res.StatusCode)
	}
	<-got

	var done struct {
		Bytes      int64 `json:"bytes"`
		DurationMs int64 `json:"durationMs"`
	}
	json.Unmarshal([]byte(p.recipient.await(t, EventTransferCompleted).Data), &done)
	if done.Bytes != sum(parts) {
		t.Errorf("completed with %d bytes, want %d", done.Bytes, sum(parts))
	}
	if done.DurationMs < 0 {
		t.Errorf("durationMs = %d", done.DurationMs)
	}
}

// A failed relay's last progress event must not claim more than it moved.
func TestNoProgressAfterAFailure(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) {
		o.progressInterval = time.Millisecond
		o.streamBuffer = 4096
	})
	p := newPair(t, x)

	parts := []part{pattern("big.bin", 16<<20)}
	id := p.offerAs(t, x, transfer.KindFile, "big.bin", sum(parts), 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)
	x.upload(t, p.senderCookie, id, parts, uploadOptions{killAfter: 4 << 20})
	<-got

	var failed struct {
		BytesRelayed int64 `json:"bytesRelayed"`
	}
	json.Unmarshal([]byte(p.sender.await(t, EventTransferFailed).Data), &failed)
	if failed.BytesRelayed <= 0 || failed.BytesRelayed > sum(parts) {
		t.Errorf("bytesRelayed = %d, want between 1 and %d", failed.BytesRelayed, sum(parts))
	}
	p.sender.quiet(t, EventTransferProgress, 200*time.Millisecond)
}
