# The bytes bypass the server, and what that costs

Phase 12 replaces the data plane with an `RTCDataChannel` and changes nothing
else. The offer, the accept, the deny, the cancel, presence, snapshots,
reconnection, the whole event catalogue: unchanged. One new endpoint relays
opaque blobs between the two parties.

This lives on the `webrtc` branch. `main` stays SSE-only, because SSE-only is
what the project set out to build and the comparison is worth more when the
thing being compared against is still intact.

---

## What was added

`POST /api/transfers/{id}/signal` — 130 lines, most of them comments.

```json
{"signal": {"description": {"type":"offer","sdp":"v=0..."}}}
{"signal": {"candidate": {...}}}
{"signal": {"ready": true}}
{"outcome": {"bytes": 5242880}}
```

The `signal` is never parsed. It is checked for size, checked that the caller is
a party to the Transfer, and forwarded to the other one over SSE as
`transfer.signal`. SDP and ICE are between the peers.

The `outcome` is the peers reporting how it went, and it is the interesting part.

Two things fell out for free, which is the strongest evidence the control plane
was designed right:

- **The state machine needed no new states.** A peer signalling *is* a peer
  attaching, so `AttachRecipient` and `AttachSender` apply verbatim — including
  ADR 0002's rule that the Recipient goes first, which turns out to be the same
  rule WebRTC wants anyway.
- **No protocol flag says which data plane a Transfer uses.** A Recipient that
  chooses HTTP opens `GET /d/{id}` and the server emits `transfer.ready`; one
  that chooses peer-to-peer signals `ready` and the Sender hears *that*. Each
  Sender answers whichever invitation arrives. The two data planes coexist
  without negotiating.

## What it costs

### 1. The server cannot count, so it cannot check

`hp_relay_bytes_total` stays flat at **zero** — asserted by a test, and the whole
point of the exercise.

The declared-total check still runs. Its input is now `{"outcome":{"bytes":N}}`
from a party with an interest in the answer. A peer that says 5,242,880 when it
received 12 is believed; a peer that says 12 when 5,242,880 was declared fails as
`payload_mismatch`, which is also tested — but only because it happened to be
honest about being wrong.

Everything in DESIGN §9's "trust-boundary rules, never simplified away" that
concerns bytes is now unenforceable:

| Rule | HTTP data plane | Peer data plane |
|---|---|---|
| Declared sizes enforced against bytes delivered | the relay counts them | the peers say so |
| Max Payload 10 GB | enforced at the byte | a declaration nobody checks |
| Filenames sanitized before being written into archive entries | the server builds the zip | there is no server-side archive |

The Recipient reports, not the Sender, on the grounds that the receiving side at
least knows what arrived. It is still self-attestation.

### 2. Progress is a rumour

`transfer.progress` used to be a server-side count of bytes that had genuinely
been written to a socket. Now each peer counts its own and the number is drawn in
its own tab; the counterparty sees whatever the signalling channel is told.

A Sender can show 100% while the Recipient has 40%. On the HTTP data plane that
was impossible — there was one count, taken at the one place the bytes passed.

### 3. Backpressure stops being free

The relay's backpressure needed no code at all: the copy blocked writing to the
Recipient's socket, which stopped draining the Sender's body, which stopped the
browser uploading. TCP did it.

A data channel has none of that. `webrtc.js` carries a manual watermark —
8 MiB high, 1 MiB low, `bufferedamountlow` to resume — and getting it wrong means
the sending tab's memory grows until the tab dies. That is 30 lines of
application code replacing a property of the transport.

### 4. The Recipient holds the whole Payload in memory

The server streamed into the response and never held more than 64 KiB; the
browser wrote it to disk as it arrived, via `Content-Disposition`. A data channel
delivers messages, so the harness accumulates them in an array and builds a
`Blob` at the end. **A 1 GB transfer needs 1 GB in the receiving tab.**

The File System Access API would fix it, and is not available everywhere, and is
a build-a-streaming-writer project of its own. The 1 GB test from phase 5 —
a gigabyte relayed with the heap going 625 KB → 1.2 MB — has no equivalent here.

### 5. NAT traversal, and a relay you now have to run

`ICE_SERVERS` is empty in `webrtc.js`: host candidates only, which works on
localhost and a LAN and nowhere else.

A real deployment needs:

- **STUN**, so a peer can discover its public address. Cheap, and there are
  public ones.
- **TURN**, for the connections STUN cannot rescue — symmetric NAT, and corporate
  networks that block UDP outright. TURN is a byte relay. **You end up running a
  relay after all**, paying for its bandwidth, except now it is a generic one that
  cannot count Payload bytes, cannot enforce a size, cannot report progress and
  cannot resume. Somewhere between 8% and 20% of peer connections need it,
  depending on whose numbers you believe.

And when traversal fails, the server cannot tell you why, because the server is
not involved. `connectionState === "failed"` in a browser tab is the whole
diagnostic.

### 6. Resume goes away

Phase 11's resume rests on the Owner holding a `zip.Writer` and an offset across a
gap. There is no Owner in the byte path here. A dropped data channel means
starting again, or building an application-level chunk protocol over the channel —
which is to say, rebuilding phase 11 in JavaScript, on both sides.

---

## What it buys

The server's bandwidth bill, and its exposure. Nothing else in this design
changes: it still holds no bytes, still writes nothing to disk, still has a flat
heap. Phase 5 already achieved "the bytes are not stored"; phase 12 achieves "the
bytes are not seen", and the difference is worth about six paragraphs of
capability.

WebRTC is the right answer when the Payloads are large, the peers are on good
networks, and the operator wants nothing to do with the contents. It is the wrong
answer when anybody needs to know whether a transfer worked.

## Trying it

```sh
git checkout webrtc
docker compose up -d postgres
go run ./cmd/server
```

Open two browsers, sign in as different users, and tick **receive peer-to-peer
(WebRTC)** in the Recipient's tab before accepting. Then:

```sh
curl -s localhost:8080/metrics | grep hp_relay_bytes_total
# hp_relay_bytes_total 0
```

The transfer completes; the counter does not move.
