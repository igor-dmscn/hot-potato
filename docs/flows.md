# Flows

How a Transfer actually happens, drawn. Names are the ones in
[`glossary.md`](glossary.md); the reasoning behind the choices is in the ADRs.

Every diagram here is checked against the tests that exercise the same path, so
if one disagrees with the code the code is right and the diagram is a bug.

---

## 1. The two planes

The central fact of the system, and the thing to hold onto before anything else:

```mermaid
flowchart LR
    subgraph browsers[" "]
        A["Ana<br/>(Sender)"]
        B["Bea<br/>(Recipient)"]
        B2["Bea's other tab"]
    end

    subgraph server["One instance"]
        CP["Control plane<br/><i>presence, proposals,<br/>decisions, progress, outcomes</i>"]
        DP["Data plane<br/><i>Payload bytes only</i>"]
    end

    A -- "SSE stream" --> CP
    B -- "SSE stream" --> CP
    B2 -- "SSE stream" --> CP
    CP -. "JSON endpoints" .-> A
    CP -. "JSON endpoints" .-> B

    A == "POST /d/{id}<br/>multipart body" ==> DP
    DP == "GET /d/{id}<br/>the response" ==> B

    style DP stroke-width:3px
```

| | Control plane | Data plane |
|---|---|---|
| Carries | presence, proposals, decisions, progress, outcomes | Payload bytes only |
| Transport | one SSE stream per tab, plus small JSON endpoints | one POST body spliced into one in-flight GET response |
| Audience | every interested User, on every tab | exactly the two Users of one Transfer |
| Scales by | **fanout** — every instance hears every event | **affinity** — both halves must meet in one process |

That asymmetry is why everything about a Transfer is answered by one instance and
why events are not (ADR 0001, 0007).

## 2. The states a Transfer passes through

```mermaid
stateDiagram-v2
    [*] --> pending: POST /api/transfers
    pending --> accepted: accept<br/><i>(recipient only, exactly once)</i>
    pending --> denied: deny<br/><i>(recipient only)</i>
    pending --> expired: 60s with no answer
    pending --> canceled: cancel<br/><i>(either party)</i>

    accepted --> streaming: both attached<br/><i>recipient first</i>
    accepted --> canceled: cancel
    accepted --> failed: rendezvous timeout

    streaming --> completed: every declared byte
    streaming --> failed: any error
    streaming --> canceled: cancel
    streaming --> accepted: a party detached<br/><i>(resume window)</i>

    completed --> [*]
    denied --> [*]
    expired --> [*]
    canceled --> [*]
    failed --> [*]
```

The rules, all enforced in `internal/transfer` without a clock, a mutex or an
HTTP request in sight:

- Only the Recipient may accept or deny; either party may cancel.
- Exactly one accept ever succeeds. `transfer.accepted` carries the winning
  `byStream` so the Recipient's other tabs know to clear their prompt.
- Exactly one Sender and one Recipient may attach — and the Recipient goes first.
- Delivered bytes must equal the declared total, or it is `payload_mismatch`.
- The five terminal states accept nothing further.
- `streaming → accepted` is the resume path: an interrupted party detaches, and
  the reaper enforces the deadline if nobody comes back.

## 3. Signing in and opening a Stream

```mermaid
sequenceDiagram
    autonumber
    participant Tab as Browser tab
    participant API as httpapi
    participant PG as Postgres
    participant P as presence
    participant Bus as bus

    Tab->>API: POST /api/login {email, password}
    API->>PG: ByEmail
    Note over API: argon2id verify — and against a<br/>decoy when the email is unknown,<br/>or the clock enumerates accounts
    API->>PG: insert session
    API-->>Tab: 200 + hp_session cookie<br/>(HttpOnly, SameSite=Lax)

    Tab->>API: GET /events
    API->>API: Registry.Open(user) → (stream, first?)
    opt first Stream for this User
        API->>P: Online(user)
        P->>Bus: user.online
    end
    API->>P: List()
    API-->>Tab: retry: 3000
    API-->>Tab: event: snapshot<br/>{self, streamId, instance, users[], transfers[]}
    loop until the tab goes away
        Bus-->>API: an event
        API-->>Tab: event: … (filtered by Audience)
        API-->>Tab: :hb every 15s
    end
```

The Snapshot goes first and carries the whole picture, so a client that
reconnects after a blip recovers by being told everything rather than by replaying
what it missed. `Last-Event-ID` is read by nobody (ADR 0003).

## 4. The happy path

```mermaid
sequenceDiagram
    autonumber
    participant S as Sender
    participant Srv as Server
    participant R as Recipient

    S->>Srv: POST /api/transfers<br/>{to, name, kind, totalBytes, entryCount}
    Note over Srv: validate the declaration,<br/>check the limits inside the lock,<br/>mint inst-a.<16 hex>
    Srv-->>S: 201 {id, expiresAt}
    Srv--)S: transfer.created
    Srv--)R: transfer.offered

    R->>Srv: POST /api/transfers/{id}/accept {streamId}
    Srv--)S: transfer.accepted {byStream}
    Srv--)R: transfer.accepted

    R->>Srv: GET /d/{id}
    Note over Srv: parks. No status line written yet
    Srv--)S: transfer.ready {entry: 0, offset: 0}

    S->>Srv: POST /d/{id} (multipart body)
    Note over Srv: handoff, then 200 + headers to R,<br/>then bytes as they arrive
    Srv-->>R: 200 + Content-Disposition + body…
    loop every 250ms while it changes
        Srv--)S: transfer.progress {bytes, total, bytesPerSec}
        Srv--)R: transfer.progress
    end
    Srv-->>S: 204
    Srv--)S: transfer.completed {bytes, durationMs}
    Srv--)R: transfer.completed
```

Nothing is read from disk until `transfer.ready` arrives. A POST before that is
`409 recipient_not_attached` — the Sender waits rather than guessing (ADR 0002).

## 5. The rendezvous, and who is blocked on what

The part that is easy to get wrong, so it gets its own diagram:

```mermaid
sequenceDiagram
    autonumber
    participant RH as Recipient's handler<br/>(goroutine A)
    participant RV as relay.Rendezvous
    participant SH as Sender's handler<br/>(goroutine B)

    RH->>RV: Park(id, sink)
    RH->>RH: select on sink.Ready()
    Note over RH: blocked. No status,<br/>no headers, nothing written

    SH->>RV: Handoff(id, source)
    RV-->>RH: source
    Note over SH: blocked on source.Wait()

    RH->>RH: Describe(payload).ApplyTo(w) → 200 + headers
    loop 64 KiB at a time
        RH->>RH: read a part, write the response
        Note over RH: this write blocks on R's socket,<br/>which stops draining SH's body.<br/>That is the backpressure — no code
    end
    RH->>SH: source.Report({bytes, err})
    Note over SH: unblocked → 204
    Note over RH: only now does the handler return
```

Three things this diagram is the argument for:

- **The copy runs on the Recipient's goroutine.** A `ResponseWriter` is invalid
  the instant its handler returns, so the only way to write a Payload into that
  response is to still be inside that function.
- **The Sender's handler blocks.** If it returned early, its body would be closed
  out from under the copy.
- **Nothing is written before the handoff.** An HTTP status cannot be retracted, so
  committing `200` on arrival would make a no-show unreportable. Parking first is
  what permits a truthful `504`.

## 6. Where the bytes are

```mermaid
flowchart LR
    F["Sender's disk<br/>or File object"] --> N1(("network"))
    N1 --> RB["r.Body<br/><i>net/http read buffer</i>"]
    RB --> MP["multipart.Reader<br/><i>one part at a time</i>"]
    MP --> BUF["64 KiB copy buffer<br/><b>io.CopyBuffer</b>"]
    BUF --> ZW["zip.Writer<br/><i>folders only, Store</i>"]
    ZW --> DW["deadlineWriter<br/><i>re-arms the write deadline</i>"]
    BUF -- "files" --> DW
    DW --> W["w (ResponseWriter)<br/><i>net/http write buffer</i>"]
    W --> N2(("network"))
    N2 --> D["Recipient's disk"]

    style BUF stroke-width:3px
```

The bold box is the only thing that scales with nothing. Measured: **1 GB
relayed, heap 625 KB → 1.2 MB, 1.4 MB allocated in total**
([`measurements.md`](measurements.md#1-a-gigabyte-through-a-flat-heap)).

A folder is zipped in flight with `Store`: compression would burn CPU per byte on
the one machine here that should stay a dumb pipe, and `archive/zip` can write to
a non-seekable writer because it records each entry's size in a trailing data
descriptor (ADR 0004).

## 7. One event, every instance

```mermaid
flowchart TB
    E["presence or a handler<br/>publishes an Event"] --> BUS{{"Bus<br/><i>one subject / channel / topic</i>"}}

    BUS --> IA["inst-a<br/>one subscription"]
    BUS --> IB["inst-b<br/>one subscription"]
    BUS --> IC["inst-c<br/>one subscription"]

    IA --> RA["sse.Registry.Deliver<br/><i>filter by Audience</i>"]
    IB --> RB["sse.Registry.Deliver"]
    IC --> RC["sse.Registry.Deliver"]

    RA --> S1["ana's tab 1"]
    RA --> S2["ana's tab 2"]
    RB --> S3["bea's tab"]
    RC --> X["nobody addressed —<br/>dropped locally"]
```

Every instance receives every event and discards what is not for its Users.
Addressing is a *field* on the Event, not a subject, because Kafka has no per-key
subscribe and the swap between four implementations has to stay honest
(ADR 0006). A Stream whose buffer overflows is closed rather than blocking this
fanout; the client reconnects into a fresh Snapshot.

## 8. Two instances, one Transfer

```mermaid
sequenceDiagram
    autonumber
    participant S as Sender<br/>(on inst-a)
    participant A as inst-a<br/><b>the Owner</b>
    participant B as inst-b
    participant R as Recipient<br/>(on inst-b)

    S->>A: POST /api/transfers
    A-->>S: 201 {id: "inst-a.7f3a9c2e…"}
    Note over A,B: the event crosses on the bus
    A--)B: transfer.offered
    B--)R: transfer.offered

    R->>B: POST /api/transfers/{id}/accept
    Note over B: OwnerOf(id) = inst-a ≠ me
    B-->>R: 307 → http://inst-a/api/transfers/{id}/accept
    R->>A: the same request, replayed
    A-->>R: 202

    R->>B: GET /d/{id}
    B-->>R: 307 → http://inst-a/d/{id}
    R->>A: GET /d/{id} — parks here
    A--)S: transfer.ready
    S->>A: POST /d/{id}
    A-->>R: the bytes
```

`307`, never `302`: a `302` is allowed to turn a POST into a GET, which would
silently drop an upload's body.

And the trap — a **streaming** upload's `307` is not followed by `net/http`,
because the body cannot be replayed. The client is handed the redirect and has to
repeat the request itself, rebuilding the body from disk. `spud` does exactly
that, and there is a test that fails if it stops.

## 9. Resume — the Sender comes back

```mermaid
sequenceDiagram
    autonumber
    participant S as Sender
    participant Srv as Server<br/>(holds relay.Session)
    participant R as Recipient

    S->>Srv: POST /d/{id} — X-Entry-Index: 0, X-Entry-Offset: 0
    Srv-->>R: 200 + bytes…
    Note over S,Srv: the connection dies at 400 KB
    Note over Srv: Session survives: zip.Writer and its<br/>half-written entry stay live.<br/>DetachSender → accepted, ResumeBy set
    Srv--)S: transfer.ready {resume: true, entry: 0, offset: 399853}
    Note over R: still parked. Never noticed

    S->>Srv: POST /d/{id} — X-Entry-Index: 0, X-Entry-Offset: 399853
    Note over Srv: Continue() checks the claim<br/>against where the relay actually is
    Srv-->>R: …the rest of the bytes
    Srv-->>S: 204
    Srv--)S: transfer.completed
```

If the Sender claims the wrong place it gets `409` plus `X-Expected-Entry` and
`X-Expected-Offset`. That check is the only defence there is: without a manifest,
a bad offset would splice the wrong bytes in and surface as a CRC error when the
Recipient unzips, far too late to do anything about (ADR 0004, 0008).

The position also travels on the control plane, because a Sender whose upload died
usually never sees its own response — `net/http` reports the broken request body
instead.

## 10. Resume — the Recipient comes back

```mermaid
sequenceDiagram
    autonumber
    participant S as Sender
    participant Srv as Server
    participant R as Recipient

    S->>Srv: POST /d/{id}
    Srv-->>R: 200 + bytes…
    Note over Srv,R: the Recipient's socket dies.<br/>293 KB is on its disk;<br/>the Owner had written 300 KB
    Note over Srv: DetachRecipient → accepted, ResumeBy set
    Srv-->>S: 502 + X-Expected-*

    R->>Srv: GET /d/{id} — Range: bytes=300000-
    Note over Srv: SkipTo(300000) rewinds what<br/>the Sender is expected to send
    Srv-->>R: 206 + Content-Range: bytes 300000-899999/900000
    Srv--)S: transfer.ready {entry: 0, offset: 300000}
    S->>Srv: POST /d/{id} — X-Entry-Offset: 300000
    Srv-->>R: the tail
    Srv--)S: transfer.completed
```

`N` comes from the **Recipient**, and it has to. The Owner counts bytes it wrote
*into a socket*, which after a disconnection exceeds what came out of the other
end by whatever was in flight — about 98 KB on loopback. Only the Recipient knows
what it holds, which is why every resumable download protocol works this way
round.

A folder cannot be resumed into a new response: the archive was being built as it
streamed, and that stream went with the socket. The server answers `416`, and it
answers it *before* attaching — an earlier version checked the Range afterwards
and a rejected request burnt the Recipient's one attach.

## 11. When it goes wrong

Every failure produces a precise, addressed event. No connection is ever left
hanging without an explanation.

```mermaid
sequenceDiagram
    autonumber
    participant Srv as Server
    participant R as Recipient
    participant S as Sender

    R->>Srv: GET /d/{id} — parks
    Srv--)S: transfer.ready
    Note over Srv: 30 seconds pass. No Sender
    Srv-->>R: 504 rendezvous_timeout
    Srv--)S: transfer.failed {rendezvous_timeout}
    Srv--)R: transfer.failed {rendezvous_timeout}
```

That `504` is only possible because no status was written when the Recipient
arrived.

| What happened | Detected by | Reason | Status |
|---|---|---|---|
| Sender never arrives | the rendezvous timer | `rendezvous_timeout` | `504` to the Recipient |
| Sender dies mid-stream | a read error on its body | `sender_disconnected` | truncated body |
| Recipient dies mid-stream | a write error on its socket | `recipient_disconnected` | `502` to the Sender |
| Bytes ≠ declared total | `Complete` | `payload_mismatch` | `502` to the Sender |
| Offer unanswered for 60s | the reaper | `offer_expired` | — |
| Interrupted, nobody returns | the reaper, on `ResumeBy` | `sender_disconnected` | — |
| Either party cancels | the cancel endpoint | — | `transfer.canceled`, and the relay is abandoned at the next 64 KiB boundary |
| Instance draining | the drain hook | `instance_draining` | `503` to new requests |

Which *side* of the copy failed is tracked separately, because that is what
decides who gets blamed. `io.ErrShortWrite` counts as a write failure however it
presents itself: only a destination can short-write.

## 12. Shutting down

```mermaid
sequenceDiagram
    autonumber
    participant OS
    participant Main as cmd/server
    participant Reg as sse.Registry
    participant Tabs as 10,000 tabs
    participant Srv as http.Server

    OS->>Main: SIGTERM
    Main->>Main: draining.Store(true)
    Note over Main: /readyz now answers 503.<br/>/healthz still answers 200 —<br/>the process is fine
    Main->>Reg: Drain(ctx)
    Reg--)Tabs: server.draining
    Reg->>Reg: close every Stream
    Note over Tabs: each pump flushes what is queued,<br/>then its handler returns
    Main->>Srv: Shutdown(15s)
    Srv-->>Main: done — 231 ms
```

The order is the whole point. `Server.Shutdown` waits for open connections and an
SSE handler never returns on its own, so a drain hook added *after* the streams
exist means rediscovering Go issue #41344 the hard way. It has been in this
codebase since phase 0, when it was a no-op.

Measured: **231 ms with ten thousand open Streams**
([`load-test.md`](load-test.md#shutting-down-with-all-of-them-open)).

---

## Reading the code along the diagrams

| Flow | Start here |
|---|---|
| Sign in, sessions | `internal/auth/auth.go`, `internal/auth/middleware.go` |
| Opening a Stream, the Snapshot | `internal/httpapi/events.go`, `internal/sse/stream.go` |
| Offer, accept, deny, the reaper | `internal/httpapi/transfers.go` |
| The rendezvous and the relay | `internal/httpapi/data.go`, `internal/relay/rendezvous.go` |
| The byte path | `internal/relay/copy.go` |
| Resume | `internal/relay/resume.go`, `cmd/spud/resume.go` |
| Event fanout | `internal/bus/`, `internal/sse/registry.go` |
| Ownership | `internal/httpapi/ownership.go`, `internal/mirror/instances.go` |
| Drain | `cmd/server/main.go`, `internal/sse/registry.go` |
