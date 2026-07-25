# Glossary

Every noun this codebase uses on purpose, what it means, and the Go type that
embodies it. The vocabulary was fixed before the code was written; this file
attaches it to the code. The wire contract those nouns travel over is
[`protocol.md`](protocol.md).

Two rules run through all of it. **One word per concept** — the "avoid" lists are
not style preferences, they are there because two words for one thing eventually
become two concepts. And **a type is named for what it is, not for what it does
to it**: `postgres` is named for what it wraps, and the interface it satisfies
lives with its consumer.

---

## People and connections

### User

A durable, registered account. The unit that appears in the online list.

| | |
|---|---|
| `auth.User` | the identity: ID, email, display name, password hash |
| `presence.User` | a deliberately separate two-field view: ID and display name |
| `httpapi.userDTO` | what a User looks like on the wire |

Three types for one noun, and each earns it. `presence` declares its own rather
than importing `auth`, so nothing that needs presence inherits a dependency on
identity. `userDTO` exists so that adding a field to `auth.User` cannot
accidentally publish it — the password hash is `json:"-"` as well, belt and
braces.

*Avoid: peer, client, account.*

### Session

One logged-in browser. `auth.Session` — an unguessable ID, a User, an expiry.

The ID is a bearer token, so it is 128+ bits from `crypto/rand`, and it travels
in a cookie that is `HttpOnly`, `SameSite=Lax`, and `Secure` unless the request
came from localhost.

### Stream

One live Server-Sent Events connection held by one User. A User may hold several
at once — one per browser tab.

| | |
|---|---|
| `sse.Stream` | the connection: an ID, a User, a bounded channel, a close signal |
| `sse.Registry` | every Stream this instance holds, indexed by User |
| `sse.Sink` | what a Stream writes into — `io.Writer` + flush + deadline |
| `sse.Heartbeat` | one clock for every Stream on the instance |

`Registry.Open` returns whether it handed out that User's *first* Stream, because
reading the count outside the lock that inserts announces one User online twice.
A Stream whose 32-event buffer overflows is closed rather than blocking the
goroutine feeding every other Stream; the client reconnects into a fresh
Snapshot, which is what makes that safe.

*Avoid: connection, subscription, channel, SSE channel.*

### Presence

Whether a User currently holds at least one Stream. Losing the last one starts a
short grace window before the User is announced absent, so a refresh does not
remove them from the list.

`presence.Presence`, with two implementations: `Memory` for one instance and
`Redis` for several. The Redis one keys on **(User, instance)** rather than User
alone — with one key per User, a User with tabs on two instances gets one claim
overwritten, and whichever instance loses its last Stream first deletes a claim
that is still true.

*Avoid: online status, availability, heartbeat.*

---

## The exchange

### Transfer

One attempt by a Sender to send a Payload to a Recipient, from the moment it is
proposed until it ends. It carries a single identity for its whole life, whatever
state it reaches.

`transfer.Transfer` — and the package it lives in has no `net/http` import and no
`time.Now()` call, both enforced by tests. `now` is a parameter to every
transition, which is what makes expiry testable without sleeping.

*Avoid: offer, request, share, invitation, job, session.*

### Sender / Recipient

The User who proposes a Transfer and supplies its bytes; the User it is addressed
to, who accepts or denies it and consumes them.

Fields on `transfer.Transfer`, not types. **Recipient**, never "receiver" — that
word means a method's receiver in Go.

*Avoid: source, uploader, owner / receiver, target, destination, peer.*

### Payload

The bytes a Transfer carries, plus the name, size and shape declared for them.
Either a single file or a folder.

`transfer.Payload`: name, `Kind`, total bytes, entry count. Everything in it is
the Sender's *claim* until the relay has counted what actually arrived — which is
why `Complete` fails as `payload_mismatch` when the two disagree.

*Avoid: file, content, data, blob.*

### Kind

Whether a Payload is a `file` or a `folder`. `transfer.Kind`.

It decides whether the relay passes bytes through or zips them, and it comes from
the *declaration*, never from inspecting the parts: `multipart.Reader.NextPart()`
discards the previous part's unread body, so looking ahead to count parts would
destroy the first one (ADR 0004).

### State

Where a Transfer is in its life: `pending`, `accepted`, `streaming`, `completed`,
`denied`, `expired`, `canceled`, `failed`. `transfer.State`, with the five
terminal ones in one unexported set.

### Relay

Passing Payload bytes from the Sender's request to the Recipient's response as
they arrive, holding no more than a small buffer and never writing to storage.

The `relay` package. `net/http` appears in exactly one signature there —
`Response.ApplyTo` — so everything else is testable with a `bytes.Buffer`.

*Avoid: upload, download, proxy, forward, copy.*

### Rendezvous

The point where a waiting Recipient and an arriving Sender are joined so a Relay
can begin. Until it happens, the Recipient waits and no bytes move.

| | |
|---|---|
| `relay.Rendezvous` | the meeting place: Transfer ID → parked Sink |
| `relay.Sink` | what the Recipient's handler registers while it waits |
| `relay.Source` | what the Sender's handler registers: a live multipart body |
| `relay.Result` | what the Recipient's goroutine tells the Sender when it is done |

It holds a live reader and a live writer, and neither can be serialised, shared
or moved. That single fact is why a Transfer belongs to one instance (ADR 0007).

*Avoid: handshake, match, pairing, sync.*

### Chunk

A contiguous run of Payload bytes the Sender delivers in one request. A
Transfer's Chunks are always delivered in order and never overlap.

Not a type — a Chunk *is* one `POST /d/{id}`. Ordinary Transfers have one; a
resumed Transfer has several, and `relay.Session` is what survives between them.

*Avoid: part, segment, block, piece.*

---

## The system's own vocabulary

### Control plane

Everything that describes the system's state — Presence, proposals, decisions,
progress, outcomes. Travels to Users as events on their Streams.

Scales by **fanout**: every instance subscribes to every event and discards the
ones addressed to Users it is not holding Streams for.

*Avoid: signalling, notifications, metadata channel.*

### Data plane

The path Payload bytes travel, and nothing else. Carries no state descriptions
and reaches only the two Users of one Transfer.

Scales by **affinity**: both halves must meet in one process.

*Avoid: transfer channel, byte channel, pipe.*

### Event

One control-plane fact. `bus.Event`: an ID, a name, an `Audience` of User IDs
(empty means everyone), an opaque JSON payload, and a W3C `traceparent`.

Addressing is a *field*, not a subject, because Kafka has no per-key subscribe
and the swap between implementations has to stay honest (ADR 0006).

### Bus

Broadcast, behind two methods: publish an event, subscribe to the firehose.

`bus.Bus`, with `Memory`, `NATS`, `Redis` and `Kafka`. The contract is
deliberately the lowest common denominator of all four: at-most-once delivery, no
ordering across Users, one firehose. Correctness rests on Snapshots, not on the
bus.

### Snapshot

The complete current state a Stream is given the instant it opens: who is online,
and the Transfers that concern this User. Everything after it is a change to that
picture.

`httpapi.snapshotView`. It is what makes a dropped or duplicated broadcast
self-healing, and therefore what lets the bus promise so little (ADR 0003).
`Last-Event-ID` is read by nobody.

*Avoid: initial state, sync message, hydration, catch-up.*

### Owner

The single running instance that holds a Transfer's live state and through which
that Transfer's bytes must pass.

Not a type: it is the prefix of `transfer.ID`, and `transfer.OwnerOf` splits on
the first dot. Any instance can work out which one owns a Transfer without
asking anybody, and answers `307` to the rest.

*Avoid: leader, host, node, shard.*

### Read model

A TTL'd mirror of state that lives authoritatively in some instance's memory,
readable by any of them. Used only to build Snapshots; never the basis of a
decision.

| | |
|---|---|
| `transfer.ReadModel` | the interface, declared with its consumer |
| `transfer.Local` | one instance: the write model is right here, so there is nothing to mirror |
| `mirror.Transfers` | Redis: `SETEX transfer:<id>` plus a sorted set per User |
| `mirror.Instances` | the instance directory every `307` goes through |

### Directory

Instance ID → the base URL that reaches it. `httpapi.Directory`, with
`LocalDirectory` (one instance: itself) and `mirror.Instances` (Redis, self-
registering with a TTL).

---

## Structures worth knowing by name

| Type | Owns |
|---|---|
| `config.Config` | the whole resolved environment, parsed once, passed down explicitly |
| `config.Secret` | a value whose `String`, `LogValue` **and `MarshalJSON`** all return `[redacted]` |
| `auth.Service` | signup, login, logout, and the middleware that puts a User in a context |
| `auth.UserStore` / `SessionStore` | the two interfaces `postgres` satisfies and `auth.Memory` fakes |
| `transfer.Registry` | this instance's write model, and the mutex the state machine does not have |
| `transfer.Limits` | the policy envelope, enforced inside the lock that inserts |
| `relay.Session` | a relay in progress, including a folder's live `zip.Writer` and its half-written entry |
| `relay.Options` | the copy buffer, the progress callback, the cancel signal, the zip timestamp |
| `httpapi.Server` | every handler, the routing, and the reaper loop |
| `httpapi.Check` | one dependency's readiness probe |
| `metrics.Metrics` | the Prometheus surface; every method is nil-safe so tests can pass nil |
| `sse.PumpOptions` | the per-write deadline, the reconnect hint, the shared heartbeat, the Snapshot |

## Invariants

Things that are true everywhere, and expensive to rediscover:

- **`Server.WriteTimeout` is 0.** It is a deadline on a whole response, so any
  non-zero value kills an SSE Stream and a multi-gigabyte relay on a schedule.
  Long-lived writes take per-write deadlines from `http.ResponseController`.
- **`ReadHeaderTimeout`, never `ReadTimeout`**, or a slow upload dies mid-flight.
- **A `ResponseWriter` is invalid the instant its handler returns.** The copy runs
  on the Recipient's goroutine and the Sender's handler blocks until told it is
  done. Everything about the data plane follows from this.
- **Nothing is written before the Sender attaches.** An HTTP status cannot be
  retracted, so committing `200` on arrival would leave no way to report a Sender
  who never shows up.
- **Backpressure needs no code.** The copy blocks on the Recipient's socket, which
  stops draining the Sender's body.
- **`307`, never `302`.** A `302` is allowed to turn a POST into a GET, which
  would silently drop an upload's body.
- **The drain hook runs before `Shutdown`.** Otherwise `Shutdown` waits forever on
  an open SSE handler (Go issue #41344).
- **Only `cmd/` imports `internal/config`.** Every other package declares its own
  `Options` struct, so no package needs the whole application's configuration to
  be readable or testable.
- **Two implementations is the bar for an interface existing.** One is not.
