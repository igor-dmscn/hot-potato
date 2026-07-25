# Terminal failures first, in-order resume later, parallel parts never

A broken Transfer initially fails outright, with a machine-readable reason and
the byte count reached, announced to both parties. In-order chunked resume is
planned for a later phase: the Sender re-sends from the exact offset the server
last relayed, while the Recipient's parked response never notices, and the
Recipient may likewise reconnect with `Range: bytes=N-` and have the Sender
rewound. This stays compatible with storing nothing, because the only state the
server keeps across a reconnection is its `zip.Writer` position — bytes, never.

S3-style parallel or out-of-order multipart upload is rejected permanently, not
deferred. Assembling parts that arrive out of order requires somewhere to park
part five while part four is missing, and that somewhere is the storage this
design exists to avoid. A Recipient is a single sequential `io.Writer`; Chunks
are therefore strictly ordered and non-overlapping.

## Consequences

Resume cannot survive the death of a Transfer's Owner, since the archive
position lives in that process's memory. That limit is inherent to holding no
bytes, and is documented rather than worked around.

Robustness in v1 means something narrower but stronger than resumability: every
failure produces a precise, addressed event, and no connection is ever left
hanging without an explanation.

---

## Amendment — 2026-07-25, after the implementation

**Status: implemented, both directions. Three findings.**

Resume is built and tested: a Sender killed mid-file resumes to a matching
checksum, a Sender killed mid-entry inside a folder resumes to an archive that
opens with correct CRCs, a Recipient reconnects with `Range: bytes=N-`, and a
wrong offset is rejected with the right one. Parallel and out-of-order parts remain
rejected, permanently, for the reason given above.

### 1. The Owner's byte count is not what the Recipient received

This record says the Sender "re-sends from the exact offset the server last
relayed". The server's count is bytes it wrote *into a socket*, which after a
disconnection exceeds what came out of the other end by whatever was in flight —
**about 98 KB on loopback**, and more over a real network.

So the two directions take their offset from different places:

- **Sender-side resume** uses the Owner's position, which is authoritative: the
  Recipient never disconnected, so everything written was received.
- **Recipient-side resume** uses the *Recipient's* count, carried in
  `Range: bytes=N-`, and `relay.Session.SkipTo(N)` rewinds what the Sender is
  expected to send — re-sending bytes the Owner had already relayed once.

Only the Recipient knows what it holds. This is why every resumable download
protocol works this way round, and it is not obvious until you measure the gap.

### 2. The position has to travel on the control plane

A Sender whose upload was interrupted usually **never sees its own response**:
`net/http` reports the broken request body instead of whatever the server said. A
`409`/`502` carrying `X-Expected-Entry` and `X-Expected-Offset` is therefore
necessary but not sufficient.

`transfer.ready` carries `{entry, offset}` on every invitation, including the
first. It arrives at the same moment as the fact that the Recipient is parked
again, which is the only moment at which a position is actionable — and "here is
the current state of your Transfer" is what a control plane is for (ADR 0002).

### 3. An interruption needs a deadline, or nobody owns the wait

An interrupted party detaches rather than failing: `streaming → accepted`, with the
Recipient's response still open and the `zip.Writer` still positioned. That leaves
a Transfer held open by nobody. Without a deadline it stays that way forever — the
offer TTL does not apply, because the offer was answered.

`Transfer.ResumeBy` is set on detach, cleared on attach, and enforced by the same
five-second reaper that expires unanswered offers. `HP_RESUME_WINDOW=0` restores
the terminal-failure behaviour this record describes as v1, and there is a test
that asserts it.

### What resume actually keeps

Exactly what this record promised, and it is worth stating as code:
`relay.Session` — a `zip.Writer`, its half-written entry, an entry index and a byte
offset — lives in the Recipient's handler frame for the whole Transfer, across as
many Sender requests as it takes. Bytes, never.

Which is also the limit: **killing the Owner is still terminal**, by design, and
now demonstrably so, because the state resume needs is a stack frame in that
process.
