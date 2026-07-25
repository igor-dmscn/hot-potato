package httpapi

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"hotpotato/internal/relay"
	"hotpotato/internal/transfer"
)

// resuming turns the resume window on. With it at zero every interruption is
// terminal, which is what every other test in this package wants.
func resuming(window time.Duration) func(*harnessOpts) {
	return func(o *harnessOpts) { o.resumeWindow = window }
}

// expectedFromHeaders reads the position out of a synchronous 409 or 502.
func expectedFromHeaders(t *testing.T, res *http.Response) (int, int64) {
	t.Helper()
	entry, err := strconv.Atoi(res.Header.Get(relay.HeaderExpectedEntry))
	if err != nil {
		t.Fatalf("%s = %q: %v", relay.HeaderExpectedEntry, res.Header.Get(relay.HeaderExpectedEntry), err)
	}
	offset, err := strconv.ParseInt(res.Header.Get(relay.HeaderExpectedOffset), 10, 64)
	if err != nil {
		t.Fatalf("%s = %q: %v", relay.HeaderExpectedOffset, res.Header.Get(relay.HeaderExpectedOffset), err)
	}
	return entry, offset
}

// awaitResumeFrom waits for the invitation to come back, and takes the position
// from it.
//
// This is what a real client does. A Sender whose upload was interrupted usually
// never sees its own response — net/http reports the broken request body instead
// of whatever the server said — so the position has to arrive on the control
// plane.
func awaitResumeFrom(t *testing.T, s *stream) (int, int64) {
	t.Helper()
	for {
		f := s.await(t, EventTransferReady)
		var ready struct {
			Resume bool  `json:"resume"`
			Entry  int   `json:"entry"`
			Offset int64 `json:"offset"`
		}
		if err := json.Unmarshal([]byte(f.Data), &ready); err != nil {
			t.Fatalf("transfer.ready payload %q: %v", f.Data, err)
		}
		if ready.Resume {
			return ready.Entry, ready.Offset
		}
	}
}

// The Sender is killed at about 40% and comes back. The Recipient never notices:
// its response stayed open the whole time, and the checksum proves the bytes are
// the original ones in the original order.
func TestSenderResumesASingleFile(t *testing.T) {
	t.Parallel()
	x := newHarness(t, resuming(10*time.Second))
	p := newPair(t, x)

	parts := []part{pattern("report.bin", 4<<20)}
	total := sum(parts)
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", total, 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)

	// First attempt dies at roughly 40%. Whether the Sender sees a 502 or just
	// its own broken body is a race it cannot control, so it does not depend on
	// either: the Recipient is still parked, and the invitation to come back
	// carries the position.
	if res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{killAfter: total * 2 / 5}); err == nil {
		res.Body.Close()
	}
	entry, offset := awaitResumeFrom(t, p.sender)
	if entry != 0 || offset <= 0 || offset >= total {
		t.Fatalf("resume position = entry %d offset %d, want something inside the payload", entry, offset)
	}

	// Second attempt: the tail, and only the tail.
	res, err := x.upload(t, p.senderCookie, id, parts,
		uploadOptions{fromEntry: entry, fromOffset: offset, resuming: true})
	if err != nil {
		t.Fatalf("resumed upload: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("resumed upload = %d, want 204", res.StatusCode)
	}

	r := <-got
	if r.err != nil {
		t.Fatalf("download: %v", r.err)
	}
	if r.bytes != total {
		t.Fatalf("received %d bytes, want %d", r.bytes, total)
	}
	if want := hashOf(parts); r.sha != want {
		t.Errorf("sha256 = %s, want %s — the resumed bytes are not the original ones", r.sha, want)
	}
	p.sender.await(t, EventTransferCompleted)
}

// Mid-entry inside a folder, which is the case the plan says to read twice: the
// zip.Writer and its half-written entry survive in the Owner's memory, so the
// resumed entry finishes with the right CRC and the archive opens.
func TestSenderResumesMidEntryInAFolder(t *testing.T) {
	t.Parallel()
	x := newHarness(t, resuming(10*time.Second))
	p := newPair(t, x)

	parts := []part{
		fixed("docs/one.bin", bytes.Repeat([]byte{1}, 300_000)),
		fixed("docs/two.bin", bytes.Repeat([]byte{2}, 400_000)),
		fixed("docs/three.bin", bytes.Repeat([]byte{3}, 500_000)),
	}
	total := sum(parts)
	id := p.offerAs(t, x, transfer.KindFolder, "docs", total, len(parts))
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{keep: true})
	p.sender.await(t, EventTransferReady)

	// Die inside the second entry.
	if res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{killAfter: 500_000}); err == nil {
		res.Body.Close()
	}
	entry, offset := awaitResumeFrom(t, p.sender)
	if entry != 1 {
		t.Fatalf("resume entry = %d, want 1 — the interruption was inside the second entry", entry)
	}
	if offset <= 0 || offset >= parts[1].size {
		t.Fatalf("resume offset = %d, want somewhere inside a %d byte entry", offset, parts[1].size)
	}

	res, err := x.upload(t, p.senderCookie, id, parts,
		uploadOptions{fromEntry: entry, fromOffset: offset, resuming: true})
	if err != nil {
		t.Fatalf("resumed upload: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("resumed upload = %d, want 204", res.StatusCode)
	}

	r := <-got
	if r.err != nil {
		t.Fatalf("download: %v", r.err)
	}
	zr, err := zip.NewReader(bytes.NewReader(r.body), int64(len(r.body)))
	if err != nil {
		t.Fatalf("the resumed archive does not open: %v", err)
	}
	if len(zr.File) != len(parts) {
		t.Fatalf("archive holds %d entries, want %d", len(zr.File), len(parts))
	}
	for i, f := range zr.File {
		if f.Name != parts[i].name {
			t.Errorf("entry %d is %q, want %q", i, f.Name, parts[i].name)
		}
		rc, err := f.Open()
		if err != nil {
			// A CRC mismatch lands here. It is the failure the live zip.Writer
			// exists to prevent, and the only place it can be observed.
			t.Fatalf("open %q: %v", f.Name, err)
		}
		content, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %q: %v", f.Name, err)
		}
		want, _ := io.ReadAll(parts[i].data())
		if !bytes.Equal(content, want) {
			t.Errorf("entry %q does not match what was sent", f.Name)
		}
	}
	p.sender.await(t, EventTransferCompleted)
}

// A Sender that guesses is told exactly where to start. Without this the wrong
// bytes would be spliced in and only surface as a CRC error at unzip time
// (ADR 0004).
func TestAWrongResumeOffsetIsRejectedWithTheRightOne(t *testing.T) {
	t.Parallel()
	x := newHarness(t, resuming(10*time.Second))
	p := newPair(t, x)

	parts := []part{pattern("report.bin", 2<<20)}
	total := sum(parts)
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", total, 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)

	// A first attempt that starts in the wrong place: nothing has been relayed,
	// so the only correct position is zero.
	res, err := x.upload(t, p.senderCookie, id, parts,
		uploadOptions{fromEntry: 0, fromOffset: 12345, resuming: true})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("a wrong offset = %d, want 409", res.StatusCode)
	}
	entry, offset := expectedFromHeaders(t, res)
	if entry != 0 || offset != 0 {
		t.Errorf("expected position = entry %d offset %d, want 0/0", entry, offset)
	}
	var body errorBody
	json.NewDecoder(res.Body).Decode(&body)
	if body.Error != codeIllegalState {
		t.Errorf("error = %q, want %q", body.Error, codeIllegalState)
	}

	// And starting from the position it was given works.
	p.sender.await(t, EventTransferReady)
	res2, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{resuming: true})
	if err != nil {
		t.Fatalf("corrected upload: %v", err)
	}
	res2.Body.Close()
	if res2.StatusCode != http.StatusNoContent {
		t.Fatalf("corrected upload = %d, want 204", res2.StatusCode)
	}
	if r := <-got; r.sha != hashOf(parts) {
		t.Error("the corrected upload did not produce the original bytes")
	}
}

// The Recipient walks away mid-relay and comes back with Range. The Transfer is
// held open rather than failed, and the two halves join up to the original file.
func TestRecipientResumesWithRange(t *testing.T) {
	t.Parallel()
	x := newHarness(t, resuming(10*time.Second))
	p := newPair(t, x)

	parts := []part{pattern("report.bin", 4<<20)}
	total := sum(parts)
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", total, 1)
	p.accept(t, x, id)

	// The Recipient reads a quarter and then closes its connection.
	firstQuarter := total / 4
	first := x.startDownload(t, p.recipientCookie, id,
		downloadOptions{keep: true, stopAfter: firstQuarter})
	p.sender.await(t, EventTransferReady)

	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		if res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{}); err == nil {
			res.Body.Close()
		}
	}()
	head := <-first
	if head.bytes != firstQuarter {
		t.Fatalf("the first attempt received %d bytes, want %d", head.bytes, firstQuarter)
	}
	<-uploadDone

	// The Transfer is not dead: it is waiting to be picked up again.
	waitForState(t, x, id, transfer.StateAccepted)
	live, err := x.transfers.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if live.RecipientAttached || live.ResumeBy.IsZero() {
		t.Fatalf("transfer = %+v, want it detached with a resume deadline", live)
	}

	// A fresh GET for the tail, asking from what the *Recipient* holds — not from
	// what the server counted as relayed. Those differ by whatever was in flight
	// when the socket closed, and the Recipient's number is the only one that
	// describes bytes anybody actually has.
	if live.BytesRelayed <= head.bytes {
		t.Errorf("the server counted %d relayed but the Recipient only received %d; "+
			"the point of asking from the client's offset is that these differ",
			live.BytesRelayed, head.bytes)
	}
	second := x.startDownload(t, p.recipientCookie, id, downloadOptions{
		keep:      true,
		rangeFrom: head.bytes,
	})
	p.sender.await(t, EventTransferReady)

	res, err := x.upload(t, p.senderCookie, id, parts,
		uploadOptions{fromOffset: head.bytes, resuming: true})
	if err != nil {
		t.Fatalf("resumed upload: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("resumed upload = %d, want 204", res.StatusCode)
	}

	tail := <-second
	if tail.status != http.StatusPartialContent {
		t.Errorf("the resumed download = %d, want 206", tail.status)
	}
	if want := fmt.Sprintf("bytes %d-%d/%d", head.bytes, total-1, total); tail.header.Get("Content-Range") != want {
		t.Errorf("Content-Range = %q, want %q", tail.header.Get("Content-Range"), want)
	}

	// The two halves are the original file.
	whole := append(append([]byte{}, head.body...), tail.body...)
	sum := sha256.Sum256(whole)
	if hex.EncodeToString(sum[:]) != hashOf(parts) {
		t.Errorf("the reassembled file is %d bytes and does not match the original", len(whole))
	}
	p.sender.await(t, EventTransferCompleted)
}

// A folder cannot be resumed into a new response: the archive was being built as
// it streamed, and that stream went with the socket.
func TestAFolderRefusesARange(t *testing.T) {
	t.Parallel()
	x := newHarness(t, resuming(10*time.Second))
	p := newPair(t, x)

	parts := []part{fixed("docs/a.bin", bytes.Repeat([]byte{1}, 1000))}
	id := p.offerAs(t, x, transfer.KindFolder, "docs", sum(parts), 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{rangeFrom: 100})
	p.sender.await(t, EventTransferReady)
	if res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{}); err == nil {
		res.Body.Close()
	}

	if r := <-got; r.status != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("a folder asked for a range = %d, want 416", r.status)
	}
}

// Nobody comes back. The reaper is what ends it, so a Transfer nobody is waiting
// on does not sit in memory forever.
func TestAnAbandonedResumeIsReaped(t *testing.T) {
	t.Parallel()
	clk := &clock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	x := newHarness(t, func(o *harnessOpts) {
		o.resumeWindow = 10 * time.Second
		o.now = clk.now
	})
	p := newPair(t, x)

	parts := []part{pattern("report.bin", 2<<20)}
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", sum(parts), 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{stopAfter: 64 << 10})
	p.sender.await(t, EventTransferReady)
	go func() {
		if res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{}); err == nil {
			res.Body.Close()
		}
	}()
	<-got
	waitForState(t, x, id, transfer.StateAccepted)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go x.api.Reap(ctx, 5*time.Millisecond, time.Minute)

	// Inside the window, nothing happens.
	p.sender.quiet(t, EventTransferFailed, 50*time.Millisecond)

	clk.advance(time.Minute)
	var failed struct {
		Reason string `json:"reason"`
	}
	json.Unmarshal([]byte(p.sender.await(t, EventTransferFailed).Data), &failed)
	if failed.Reason != transfer.ReasonSenderDisconnected {
		t.Errorf("reason = %q, want %q", failed.Reason, transfer.ReasonSenderDisconnected)
	}
}

// Progress does not restart from zero across a resume: the counter is seeded with
// what the previous attempt delivered.
func TestProgressStaysMonotonicAcrossAResume(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) {
		o.resumeWindow = 10 * time.Second
		o.progressInterval = time.Millisecond
	})
	p := newPair(t, x)

	parts := []part{pattern("report.bin", 8<<20)}
	total := sum(parts)
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", total, 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)

	if res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{killAfter: total / 3}); err == nil {
		res.Body.Close()
	}
	entry, offset := awaitResumeFrom(t, p.sender)

	res, err := x.upload(t, p.senderCookie, id, parts,
		uploadOptions{fromEntry: entry, fromOffset: offset, resuming: true})
	if err != nil {
		t.Fatalf("resumed upload: %v", err)
	}
	res.Body.Close()
	<-got

	var last int64
	deadline := time.After(10 * time.Second)
	for {
		select {
		case f, ok := <-p.sender.frames:
			if !ok {
				t.Fatal("the stream closed before the completion")
			}
			switch f.Event {
			case EventTransferProgress:
				var e progressEvent
				json.Unmarshal([]byte(f.Data), &e)
				if e.Bytes < last {
					t.Fatalf("progress went backwards, %d then %d, across the resume", last, e.Bytes)
				}
				if e.Bytes > total {
					t.Fatalf("progress reported %d of %d bytes", e.Bytes, total)
				}
				last = e.Bytes
			case EventTransferCompleted:
				if last == 0 {
					t.Error("no progress events at all")
				}
				return
			case EventTransferFailed:
				t.Fatalf("the transfer failed: %s", f.Data)
			}
		case <-deadline:
			t.Fatal("no completion within ten seconds")
		}
	}
}

// waitForState polls the registry, which is the only place the live state is.
func waitForState(t *testing.T, x *harness, id transfer.ID, want transfer.State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got, err := x.transfers.Get(id); err == nil && got.State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := x.transfers.Get(id)
	t.Fatalf("transfer is %s, want %s", got.State, want)
}
