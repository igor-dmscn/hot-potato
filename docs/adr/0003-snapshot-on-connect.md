# Snapshot on connect; `Last-Event-ID` deliberately ignored

Every Stream opens with a `snapshot` event carrying the complete picture — who
is online, and every Transfer of this User's that is active or reached a
terminal state in the last 60 seconds — followed only by deltas. Clients that
reconnect after a network blip recover by receiving a new snapshot, not by
replaying missed events. `Last-Event-ID` is read by nobody; `id:` is emitted
for debugging and `retry: 3000` to steer browser backoff.

## Considered Options

The textbook SSE approach is a bounded per-User replay buffer keyed by event
ID. It was rejected for two reasons: it needs a global event ordering that the
event bus deliberately does not provide (ADR 0006), and its own failure path —
a client that reconnects outside the replay window — terminates in a snapshot
anyway, so the snapshot must be built regardless.

## Consequences

Snapshots are idempotent, so a dropped or duplicated broadcast is self-healing
at the next reconnect. That is what allows the bus to promise only at-most-once
delivery with no cross-User ordering, which in turn is what makes three
different broker implementations genuinely interchangeable.

The 60-second terminal window exists so a brief disconnection cannot swallow a
one-shot outcome such as a denial. Without it, a Transfer that was denied while
the Sender was reconnecting would vanish silently.

---

## Amendment — 2026-07-25, after the implementation

**Status: confirmed, and load-bearing in more places than expected.**

Three places where the design would not work without this decision, none of them
the one it was written for:

**1. The drain drops events, by design.** Shutting down with 10,000 open Streams
takes 231 ms, and the client saw between **8,594 and 10,001** of the 10,000
`server.draining` announcements depending on the run: a Stream whose socket closes
before the frame is read simply loses it. That is acceptable only because
`retry: 3000` plus a fresh Snapshot makes a missed announcement cost nothing
(`docs/load-test.md`).

**2. Kafka loses an event published immediately after `Subscribe`**, reliably,
while its consumer group joins — about 1.3 s of it. The conformance suite
republishes on a 250 ms cadence until something lands, and that retry loop exists
solely for Kafka. It is not a bug in either place: it is what at-most-once looks
like at a subscription boundary, and it is survivable for exactly the reason
recorded here (`docs/bus-comparison.md`).

**3. An overflowing Stream is dropped rather than blocking the bus.** A Stream
32 events behind its client is closed; the client reconnects and gets the current
picture instead of a backlog. This is the mechanism that lets one slow tab avoid
stalling the goroutine feeding every other Stream on the instance.

One addition to the client contract: **a Snapshot's `transfers[]` is not
decoration.** `spud recv` checks it for a pending offer before waiting for
`transfer.offered`, because a client that connects *after* the offer was announced
would otherwise never see it. Any client that only listens for deltas has a race
it cannot win.

**4. One Stream per tab has a ceiling, and it is six.** A browser allows six
HTTP/1.1 connections per origin and a Stream holds one for the life of the tab,
so the sixth tab starves the origin: `/healthz` answers in 4 ms with five Streams
up, never with six, and a seventh tab cannot load `/` at all. Reconnect-with-a-
Snapshot is what makes the fix cheap — HTTP/2 multiplexes the Streams onto one
connection (`make run-tls`, flat at 3–5 ms through twelve), and every tab that
was starved simply reconnects into a fresh Snapshot with nothing to replay.
It has to be TLS: no browser speaks cleartext h2c.
