package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"hotpotato/internal/transfer"
)

// The whole control plane, unchanged, with the bytes going somewhere else. The
// number that matters is hp_relay_bytes_total: flat at zero means the server saw
// nothing.
func TestWebRTCTransferCompletesWithoutTheServerSeeingABbyte(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	const total = 5 << 20
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", total, 1)
	p.accept(t, x, id)

	// The Recipient signals first, exactly as it attaches first on the HTTP data
	// plane (ADR 0002).
	rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/signal",
		`{"signal":{"type":"answer","sdp":"v=0..."}}`, p.recipientCookie)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("recipient signal = %d %s, want 204", rec.Code, rec.Body)
	}

	// It reaches the Sender, and only the Sender.
	f := p.sender.await(t, EventTransferSignal)
	var relayed struct {
		ID     transfer.ID     `json:"id"`
		From   string          `json:"from"`
		Signal json.RawMessage `json:"signal"`
	}
	if err := json.Unmarshal([]byte(f.Data), &relayed); err != nil {
		t.Fatalf("signal payload %q: %v", f.Data, err)
	}
	if relayed.ID != id || relayed.From != p.recipientID {
		t.Errorf("signal = %+v, want it from the recipient", relayed)
	}
	// Opaque: whatever went in comes out, unparsed.
	if !strings.Contains(string(relayed.Signal), `"sdp":"v=0..."`) {
		t.Errorf("signal body = %s, want it forwarded verbatim", relayed.Signal)
	}

	rec = x.do(t, "POST", "/api/transfers/"+string(id)+"/signal",
		`{"signal":{"type":"offer","sdp":"v=0..."}}`, p.senderCookie)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("sender signal = %d %s, want 204", rec.Code, rec.Body)
	}
	p.recipient.await(t, EventTransferSignal)

	// Both peers have signalled, so the Transfer is streaming — even though
	// nothing is streaming through here.
	live, err := x.transfers.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if live.State != transfer.StateStreaming {
		t.Fatalf("state = %s, want streaming once both peers signalled", live.State)
	}

	// The peers finish and say so.
	rec = x.do(t, "POST", "/api/transfers/"+string(id)+"/signal",
		`{"outcome":{"bytes":5242880}}`, p.senderCookie)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("outcome = %d %s, want 204", rec.Code, rec.Body)
	}

	for _, s := range []*stream{p.sender, p.recipient} {
		var done struct {
			Bytes      int64  `json:"bytes"`
			ReportedBy string `json:"reportedBy"`
		}
		json.Unmarshal([]byte(s.await(t, EventTransferCompleted).Data), &done)
		if done.Bytes != total {
			t.Errorf("completed with %d bytes, want %d", done.Bytes, total)
		}
		if done.ReportedBy != p.senderID {
			t.Errorf("reportedBy = %q, want the sender — this number is self-attested", done.ReportedBy)
		}
	}

	// The measurement the phase exists for.
	body := x.do(t, "GET", "/metrics", "").Body.String()
	if v := metricValue(t, body, "hp_relay_bytes_total"); v != 0 {
		t.Errorf("hp_relay_bytes_total = %v, want 0 — the server relayed nothing", v)
	}
	if !strings.Contains(body, `hp_transfers_total{state="completed"} 1`) {
		t.Error("the completion was not counted")
	}
}

// The declared-total check still runs. Its input is now a peer's word, which is
// the whole cost of this design in one assertion.
func TestAPeerReportedTotalIsStillChecked(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)

	const total = 1 << 20
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", total, 1)
	p.accept(t, x, id)
	x.do(t, "POST", "/api/transfers/"+string(id)+"/signal", `{"signal":{}}`, p.recipientCookie)
	x.do(t, "POST", "/api/transfers/"+string(id)+"/signal", `{"signal":{}}`, p.senderCookie)

	// A peer claiming a total that is not the declared one.
	rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/signal",
		`{"outcome":{"bytes":12}}`, p.senderCookie)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("outcome = %d %s, want 204", rec.Code, rec.Body)
	}

	var failed struct {
		Reason string `json:"reason"`
	}
	json.Unmarshal([]byte(p.recipient.await(t, EventTransferFailed).Data), &failed)
	if failed.Reason != transfer.ReasonPayloadMismatch {
		t.Errorf("reason = %q, want %q", failed.Reason, transfer.ReasonPayloadMismatch)
	}
}

func TestSignalRejectsStrangersAndOversizeBlobs(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", 1024, 1)
	p.accept(t, x, id)

	stranger := x.signup(t, "cara@example.com", "cara")
	if rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/signal",
		`{"signal":{}}`, stranger); rec.Code != http.StatusForbidden {
		t.Errorf("a stranger signalling = %d, want 403", rec.Code)
	}

	// The control plane is not the data plane, however tempting.
	huge := `{"signal":{"sdp":"` + strings.Repeat("v", maxSignal) + `"}}`
	if rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/signal",
		huge, p.senderCookie); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("an oversize signal = %d, want 413", rec.Code)
	}
}

// A peer that reports a failure gets one of DESIGN §7's reasons, not one of its
// own invention.
func TestAPeerCannotInventAFailureReason(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	p := newPair(t, x)
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", 1024, 1)
	p.accept(t, x, id)
	x.do(t, "POST", "/api/transfers/"+string(id)+"/signal", `{"signal":{}}`, p.recipientCookie)

	rec := x.do(t, "POST", "/api/transfers/"+string(id)+"/signal",
		`{"outcome":{"failed":true,"reason":"the moon was in the wrong place"}}`, p.recipientCookie)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("outcome = %d %s, want 204", rec.Code, rec.Body)
	}

	var failed struct {
		Reason string `json:"reason"`
	}
	json.Unmarshal([]byte(p.sender.await(t, EventTransferFailed).Data), &failed)
	if failed.Reason != transfer.ReasonInternal {
		t.Errorf("reason = %q, want it mapped onto %q", failed.Reason, transfer.ReasonInternal)
	}
}

// Signalling is a control-plane message like any other, so it is subject to the
// ownership redirect.
func TestSignalIsRedirectedToTheOwner(t *testing.T) {
	t.Parallel()
	x := newHarness(t)
	ana := x.signup(t, "ana@example.com", "ana")

	req := x.do(t, "POST", "/api/transfers/inst-elsewhere.deadbeef/signal", `{"signal":{}}`, ana)
	if req.Code != http.StatusNotFound {
		t.Fatalf("signalling a transfer owned elsewhere = %d, want 404 (no such instance)", req.Code)
	}
}

func TestSignalRequiresJSON(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(o *harnessOpts) { o.grace = time.Second })
	p := newPair(t, x)
	id := p.offerAs(t, x, transfer.KindFile, "report.bin", 1024, 1)

	req := x.do(t, "POST", "/api/transfers/"+string(id)+"/signal", "", p.senderCookie)
	// An empty body with the JSON content type still has to be JSON.
	if req.Code != http.StatusBadRequest {
		t.Errorf("an empty signal body = %d, want 400", req.Code)
	}
}
