# Two planes, and no stored bytes

Payload bytes must never land on the server, and push notifications must use
Server-Sent Events rather than WebSockets — but SSE only carries data from
server to client, so the bytes need a path of their own. We therefore split the
system into a **control plane** (SSE streams plus small JSON endpoints,
carrying presence, proposals, decisions, progress and outcomes) and a **data
plane** (one HTTP POST body spliced into one in-flight HTTP GET response). The
server holds nothing but a copy buffer; nothing is written to disk or object
storage.

## Considered Options

Server-as-relay was chosen over WebRTC data channels, where the Go server would
broker SDP/ICE and never see a byte. WebRTC is a truer reading of "not on the
server", but it moves all the interesting work into browser JavaScript and
leaves the server unable to observe progress. It is kept as a final comparison
phase rather than the primary design.

## Consequences

Memory per Transfer is O(1) regardless of Payload size. The server can never
retry, resume or verify from a copy of its own, because it has none — so the
death of a Transfer's Owner is unrecoverable by design (see ADR 0007, 0008).

This shape is not novel: Magic-Wormhole runs a mailbox/rendezvous server
alongside a separate transit relay that "glues together two inbound
connections", and `patchbay.pub` exposes HTTP channels where a GET and a POST
block until matched and then stream through each other.

---

## Amendment — 2026-07-25, after the implementation

**Status: confirmed, and measured.**

"Memory per Transfer is O(1) regardless of Payload size" was the claim this whole
record rests on, so it was measured rather than assumed:

- **1 GB relayed in 5.3 s**, with the heap going 625 KB → 1.2 MB and **1.4 MB
  allocated in total** across the entire run. Both ends were hashed and compared,
  so it is an integrity check as well as a memory one.
  (`TestOneGigabyteWithAFlatHeap`, `docs/measurements.md`.)

Where the 1.4 MB goes: two 64 KiB copy buffers, `net/http`'s per-connection
buffers, and the test's own client. Nothing in that list grows with the Payload.

Two things worth recording about the shape:

- **Backpressure needed no code at all.** The copy blocks writing to the
  Recipient's socket, which stops draining the Sender's body, which stops the
  browser uploading. It is a property of the transport, and phase 12 later showed
  what it costs to lose it: a WebRTC data channel needs a manual watermark and
  30 lines of application code to get the same effect (`docs/webrtc.md`).
- **"The server holds nothing but a copy buffer" survived resume.** Phase 11 adds
  exactly one thing to what an interrupted Transfer keeps: a `zip.Writer`'s
  position. Bytes, never — which is also why resume cannot survive the Owner
  dying (ADR 0008).
