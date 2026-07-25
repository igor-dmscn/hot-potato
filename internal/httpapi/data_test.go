package httpapi

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"hotpotato/internal/transfer"
)

// part is one entry of an upload. The data is produced on demand so a
// gigabyte-sized test never holds a gigabyte.
type part struct {
	name string
	size int64
	data func() io.Reader
}

func fixed(name string, b []byte) part {
	return part{name: name, size: int64(len(b)), data: func() io.Reader { return bytes.NewReader(b) }}
}

// pattern is cheap, position-dependent filler: enough to catch bytes arriving
// in the wrong order, without the cost of a CSPRNG at these sizes.
func pattern(name string, size int64) part {
	block := make([]byte, 64<<10)
	for i := range block {
		block[i] = byte(i*7 + 13)
	}
	return part{name: name, size: size, data: func() io.Reader {
		return io.LimitReader(&repeat{block: block}, size)
	}}
}

type repeat struct {
	block []byte
	at    int
}

func (r *repeat) Read(p []byte) (int, error) {
	n := copy(p, r.block[r.at:])
	r.at = (r.at + n) % len(r.block)
	return n, nil
}

func sum(parts []part) int64 {
	var total int64
	for _, p := range parts {
		total += p.size
	}
	return total
}

// offerAs proposes a Transfer with an arbitrary Payload.
func (p *pair) offerAs(t *testing.T, x *harness, kind transfer.Kind, name string, total int64, entries int) transfer.ID {
	t.Helper()
	body := fmt.Sprintf(`{"to":%q,"name":%q,"kind":%q,"totalBytes":%d,"entryCount":%d}`,
		p.recipientID, name, kind, total, entries)
	rec := x.do(t, "POST", "/api/transfers", body, p.senderCookie)
	if rec.Code != http.StatusCreated {
		t.Fatalf("offer = %d %s, want 201", rec.Code, rec.Body)
	}
	var created struct {
		ID transfer.ID `json:"id"`
	}
	json.Unmarshal(rec.Body.Bytes(), &created)
	return created.ID
}

func (p *pair) accept(t *testing.T, x *harness, id transfer.ID) {
	t.Helper()
	rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/accept",
		fmt.Sprintf(`{"streamId":%q}`, p.recipientStreamID), p.recipientCookie)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("accept = %d %s, want 202", rec.Code, rec.Body)
	}
	p.sender.await(t, EventTransferAccepted)
}

// received is the Recipient's side of a relay.
type received struct {
	status int
	header http.Header
	sha    string
	bytes  int64
	body   []byte // only when keep is set
	err    error
}

type downloadOptions struct {
	// keep retains the body so a test can open the archive.
	keep bool
	// stopAfter closes the response early, modelling a Recipient who walks away.
	stopAfter int64
}

func (x *harness) startDownload(t *testing.T, cookie *http.Cookie, id transfer.ID, o downloadOptions) <-chan received {
	t.Helper()
	srv := x.server(t)
	return startDownload(t, srv, srv.Client(), cookie, id, o)
}

// startDownload issues GET /d/{id} and drains it in the background.
//
// It deliberately does not wait for the request to return: the Recipient's
// headers are withheld until a Sender attaches (ADR 0002), so Do() blocks until
// the relay is under way. The signal that the Recipient has parked is the
// Sender's transfer.ready event, which is what every caller waits for next.
func startDownload(t *testing.T, srv *httptest.Server, hc *http.Client, cookie *http.Cookie, id transfer.ID, o downloadOptions) <-chan received {
	t.Helper()
	out := make(chan received, 1)

	req, err := http.NewRequest("GET", srv.URL+"/d/"+string(id), nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(cookie)

	go func() {
		res, err := hc.Do(req)
		if err != nil {
			out <- received{err: err}
			return
		}
		defer res.Body.Close()

		h := sha256.New()
		var buf bytes.Buffer
		var dst io.Writer = h
		if o.keep {
			dst = io.MultiWriter(h, &buf)
		}

		var n int64
		if o.stopAfter > 0 {
			n, err = io.CopyN(dst, res.Body, o.stopAfter)
			res.Body.Close()
		} else {
			n, err = io.Copy(dst, res.Body)
		}
		out <- received{
			status: res.StatusCode,
			header: res.Header,
			sha:    hex.EncodeToString(h.Sum(nil)),
			bytes:  n,
			body:   buf.Bytes(),
			err:    err,
		}
	}()
	return out
}

// uploadOptions controls how the Sender misbehaves.
type uploadOptions struct {
	// killAfter aborts the request body once this many Payload bytes are in.
	killAfter int64
}

func (x *harness) upload(t *testing.T, cookie *http.Cookie, id transfer.ID, parts []part, o uploadOptions) (*http.Response, error) {
	t.Helper()
	srv := x.server(t)
	return upload(t, srv, srv.Client(), cookie, id, parts, o)
}

// upload streams a multipart body through an io.Pipe, so nothing is ever
// buffered — the same shape spud and the browser use.
func upload(t *testing.T, srv *httptest.Server, hc *http.Client, cookie *http.Cookie, id transfer.ID, parts []part, o uploadOptions) (*http.Response, error) {
	t.Helper()

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		var err error
		defer func() { pw.CloseWithError(err) }()

		var sent int64
		for _, p := range parts {
			w, e := mw.CreateFormFile("files", p.name)
			if e != nil {
				err = e
				return
			}
			src := p.data()
			if o.killAfter > 0 {
				remaining := o.killAfter - sent
				if remaining <= 0 {
					err = fmt.Errorf("sender vanished")
					return
				}
				src = io.LimitReader(src, remaining)
			}
			n, e := io.Copy(w, src)
			sent += n
			if e != nil {
				err = e
				return
			}
			if o.killAfter > 0 && sent >= o.killAfter {
				err = fmt.Errorf("sender vanished")
				return
			}
		}
		err = mw.Close()
	}()

	req, err := http.NewRequest("POST", srv.URL+"/d/"+string(id), pr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.AddCookie(cookie)
	return hc.Do(req)
}

// A whole Transfer, end to end, over real sockets: the checksum is the only
// thing that proves the bytes came out the way they went in.
func TestRelayASingleFile(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	parts := []part{pattern("report.bin", 3<<20)}
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", sum(parts), 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	// The Sender waits to be told the Recipient is attached (ADR 0002).
	p.sender.await(t, EventTransferReady)

	res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("POST /d: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("POST /d = %d, want 204", res.StatusCode)
	}

	r := <-got
	if r.err != nil {
		t.Fatalf("download: %v", r.err)
	}
	if r.status != http.StatusOK {
		t.Fatalf("GET /d = %d, want 200", r.status)
	}
	if r.bytes != sum(parts) {
		t.Errorf("received %d bytes, want %d", r.bytes, sum(parts))
	}
	if want := hashOf(parts); r.sha != want {
		t.Errorf("sha256 = %s, want %s", r.sha, want)
	}
	// A single file passes through untouched, so the length is known up front.
	if got := r.header.Get("Content-Length"); got != fmt.Sprint(sum(parts)) {
		t.Errorf("Content-Length = %q, want %d", got, sum(parts))
	}

	for _, s := range []*stream{p.sender, p.recipient} {
		var done struct {
			Bytes      int64 `json:"bytes"`
			DurationMs int64 `json:"durationMs"`
		}
		json.Unmarshal([]byte(s.await(t, EventTransferCompleted).Data), &done)
		if done.Bytes != sum(parts) {
			t.Errorf("transfer.completed says %d bytes, want %d", done.Bytes, sum(parts))
		}
	}
}

// Proves the archive is valid after streaming, not merely that bytes moved.
func TestRelayAFolderAsAZip(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	parts := make([]part, 12)
	for i := range parts {
		parts[i] = fixed(fmt.Sprintf("docs/sub%d/file%d.txt", i%3, i),
			bytes.Repeat([]byte{byte('a' + i)}, 1000+i*97))
	}
	id := p.offerAs(t, x, transfer.KindFolder, "docs", sum(parts), len(parts))
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{keep: true})
	p.sender.await(t, EventTransferReady)

	res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("POST /d: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("POST /d = %d, want 204", res.StatusCode)
	}

	r := <-got
	if r.err != nil {
		t.Fatalf("download: %v", r.err)
	}
	if r.header.Get("Content-Type") != "application/zip" {
		t.Errorf("Content-Type = %q, want application/zip", r.header.Get("Content-Type"))
	}
	// Chunked: a zip's size is not known until it is finished (ADR 0004).
	if cl := r.header.Get("Content-Length"); cl != "" {
		t.Errorf("Content-Length = %q, want none for a folder", cl)
	}

	zr, err := zip.NewReader(bytes.NewReader(r.body), int64(len(r.body)))
	if err != nil {
		t.Fatalf("the archive does not open: %v", err)
	}
	if len(zr.File) != len(parts) {
		t.Fatalf("archive holds %d entries, want %d", len(zr.File), len(parts))
	}
	for i, f := range zr.File {
		if f.Name != parts[i].name {
			t.Errorf("entry %d is %q, want %q — the relative path was lost", i, f.Name, parts[i].name)
		}
		rc, err := f.Open()
		if err != nil {
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
}

// The ResponseWriter-outlives-its-handler contract, which no fake can model:
// the Sender dies mid-stream and both parties still get a precise answer.
func TestSenderKilledMidStream(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	parts := []part{pattern("big.bin", 4<<20)}
	killAt := int64(1600 << 10) // about 40%
	id := p.offerAs(t, x, transfer.KindFile, "big.bin", sum(parts), 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)

	if _, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{killAfter: killAt}); err == nil {
		t.Log("the client reported no error for an aborted body, which is allowed")
	}

	// Both parties are told, with a plausible byte count.
	for _, s := range []*stream{p.sender, p.recipient} {
		var failed struct {
			Reason       string `json:"reason"`
			BytesRelayed int64  `json:"bytesRelayed"`
		}
		json.Unmarshal([]byte(s.await(t, EventTransferFailed).Data), &failed)
		if failed.Reason != transfer.ReasonSenderDisconnected {
			t.Errorf("reason = %q, want %q", failed.Reason, transfer.ReasonSenderDisconnected)
		}
		if failed.BytesRelayed <= 0 || failed.BytesRelayed > sum(parts) {
			t.Errorf("bytesRelayed = %d, want something between 1 and %d", failed.BytesRelayed, sum(parts))
		}
	}

	// The Recipient's response was already committed as 200, so the only honest
	// ending is a truncated body.
	r := <-got
	if r.bytes >= sum(parts) {
		t.Errorf("the Recipient received %d of %d bytes; the failure did not truncate", r.bytes, sum(parts))
	}
}

func TestRecipientDisconnectsMidStream(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	parts := []part{pattern("big.bin", 8<<20)}
	id := p.offerAs(t, x, transfer.KindFile, "big.bin", sum(parts), 1)
	p.accept(t, x, id)

	// Read a little, then walk away.
	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{stopAfter: 64 << 10})
	p.sender.await(t, EventTransferReady)

	uploadDone := make(chan error, 1)
	go func() {
		_, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{})
		uploadDone <- err
	}()
	<-got

	var failed struct {
		Reason string `json:"reason"`
	}
	json.Unmarshal([]byte(p.sender.await(t, EventTransferFailed).Data), &failed)
	if failed.Reason != transfer.ReasonRecipientDisconnected {
		t.Errorf("reason = %q, want %q", failed.Reason, transfer.ReasonRecipientDisconnected)
	}
	<-uploadDone
}

// A Sender who arrives before the Recipient is refused rather than parked, and
// a second Recipient is refused too (ADR 0002).
func TestDataPlaneOrdering(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	parts := []part{fixed("a.bin", []byte("hello"))}
	id := p.offerAs(t, x, transfer.KindFile, "a.bin", sum(parts), 1)
	p.accept(t, x, id)

	// POST before GET.
	res, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("POST /d: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("POST before GET = %d, want 409", res.StatusCode)
	}
	var body errorBody
	json.NewDecoder(res.Body).Decode(&body)
	if body.Error != codeRecipientNotAttached {
		t.Errorf("error = %q, want %q", body.Error, codeRecipientNotAttached)
	}
}

func TestSecondRecipientIsRefused(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	parts := []part{pattern("a.bin", 1<<20)}
	id := p.offerAs(t, x, transfer.KindFile, "a.bin", sum(parts), 1)
	p.accept(t, x, id)

	first := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)

	srv := x.server(t)
	req, _ := http.NewRequest("GET", srv.URL+"/d/"+string(id), nil)
	req.AddCookie(p.recipientCookie)
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("second GET: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Errorf("a second GET = %d, want 409", res.StatusCode)
	}

	// Let the first one finish so the test does not leave a handler parked.
	if _, err := x.upload(t, p.senderCookie, id, parts, uploadOptions{}); err != nil {
		t.Fatalf("POST /d: %v", err)
	}
	<-first
}

func TestOnlyTheRecipientMayDownloadAndOnlyTheSenderMayUpload(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)
	parts := []part{fixed("a.bin", []byte("hello"))}
	id := p.offerAs(t, x, transfer.KindFile, "a.bin", sum(parts), 1)
	p.accept(t, x, id)

	// The Sender trying to download their own Transfer.
	srv := x.server(t)
	req, _ := http.NewRequest("GET", srv.URL+"/d/"+string(id), nil)
	req.AddCookie(p.senderCookie)
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /d: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Errorf("the sender downloading = %d, want 403", res.StatusCode)
	}

	// The Recipient trying to upload.
	up, err := x.upload(t, p.recipientCookie, id, parts, uploadOptions{})
	if err != nil {
		t.Fatalf("POST /d: %v", err)
	}
	up.Body.Close()
	if up.StatusCode != http.StatusForbidden {
		t.Errorf("the recipient uploading = %d, want 403", up.StatusCode)
	}
}

// A Sender who never arrives: the Recipient's response is still uncommitted, so
// it can be answered truthfully with a 504.
func TestRendezvousTimeout(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) { o.rendezvousWait = 100 * time.Millisecond })
	p := newPair(t, x)

	id := p.offerAs(t, x, transfer.KindFile, "a.bin", 10, 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{keep: true})
	r := <-got
	if r.status != http.StatusGatewayTimeout {
		t.Fatalf("GET /d with no sender = %d, want 504", r.status)
	}

	for _, s := range []*stream{p.sender, p.recipient} {
		var failed struct {
			Reason string `json:"reason"`
		}
		json.Unmarshal([]byte(s.await(t, EventTransferFailed).Data), &failed)
		if failed.Reason != transfer.ReasonRendezvousTimeout {
			t.Errorf("reason = %q, want %q", failed.Reason, transfer.ReasonRendezvousTimeout)
		}
	}
}

func TestCancelDuringTheRelay(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	parts := []part{pattern("big.bin", 32<<20)}
	id := p.offerAs(t, x, transfer.KindFile, "big.bin", sum(parts), 1)
	p.accept(t, x, id)

	got := x.startDownload(t, p.recipientCookie, id, downloadOptions{})
	p.sender.await(t, EventTransferReady)

	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		x.upload(t, p.senderCookie, id, parts, uploadOptions{})
	}()

	// Cancel while the bytes are moving.
	rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/cancel", "", p.senderCookie)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("cancel = %d %s, want 204", rec.Code, rec.Body)
	}
	p.recipient.await(t, EventTransferCanceled)

	r := <-got
	if r.bytes >= sum(parts) {
		t.Errorf("the relay delivered %d of %d bytes despite the cancellation", r.bytes, sum(parts))
	}
	<-uploadDone

	// A cancellation is announced once, as a cancellation. The relay's own
	// ending must not add a failure on top.
	p.sender.quiet(t, EventTransferFailed, 200*time.Millisecond)
}

// The one test that actually proves zero storage: a gigabyte through a process
// whose heap does not grow.
func TestOneGigabyteWithAFlatHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the 1 GB relay in -short mode")
	}
	x := newHarness(t)
	p := newPair(t, x)

	parts := []part{pattern("huge.bin", 1<<30)}
	id := p.offerAs(t, x, transfer.KindFile, "huge.bin", sum(parts), 1)
	p.accept(t, x, id)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

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

	r := <-got
	if r.bytes != sum(parts) {
		t.Fatalf("received %d bytes, want %d", r.bytes, sum(parts))
	}
	if want := hashOf(parts); r.sha != want {
		t.Errorf("sha256 = %s, want %s", r.sha, want)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// The relay holds one 64 KiB buffer per direction. A few megabytes of slack
	// covers the test's own client and server; a gigabyte of growth would mean
	// something is accumulating.
	const slack = 32 << 20
	if growth := int64(after.HeapAlloc) - int64(before.HeapAlloc); growth > slack {
		t.Errorf("heap grew by %d bytes across a 1 GB relay, want under %d", growth, slack)
	}
	t.Logf("1 GB relayed; heap %d → %d bytes, %d total allocated",
		before.HeapAlloc, after.HeapAlloc, after.TotalAlloc-before.TotalAlloc)
}

func hashOf(parts []part) string {
	h := sha256.New()
	for _, p := range parts {
		io.Copy(h, p.data())
	}
	return hex.EncodeToString(h.Sum(nil))
}
