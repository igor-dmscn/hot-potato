# Hot Potato internals

How this server actually works: the object graph, the goroutine model, where every
piece of state lives, and each significant flow traced through the source.

Source: `hot-potato` @ `6bfdf75`, Go 1.26. Paths are relative to the repository
root. Companion documents: the wire contract in [`protocol.md`](protocol.md), the
vocabulary in [`glossary.md`](glossary.md), the same flows drawn in
[`flows.md`](flows.md), the reasoning in [`adr/`](adr/). This document is the
mechanism: what holds what, on which goroutine, and why it cannot be arranged
otherwise.

---

## 1. Layering

There is no framework. One process, one `http.Server`, one `ServeMux`, and a
handful of long-lived objects that `main` builds and hands to the HTTP surface as
an `Options` struct. Nothing reaches for configuration or a global on its own.

```
os / init system
│
└─ cmd/server.run()                                        the only importer of internal/config
   ├─ config.Load(os.LookupEnv)                            one immutable typed value (config.go:102)
   ├─ signal.NotifyContext(SIGINT, SIGTERM)                main.go:54 — everything below drains against it
   ├─ postgres.Store              pgxpool                  identity; the only durable state
   │    ├─ Users                                           auth.UserStore
   │    └─ Sessions                                        auth.SessionStore — one joined query per request
   ├─ auth.Service                                         argon2id, sessions, in-process login throttle
   ├─ bus.Bus                     memory | nats | redis | kafka
   │    └─ one Subscribe per process → goroutine → streams.Deliver     (main.go:137-145)
   ├─ sse.Registry                                         every live Stream, indexed by User
   │    ├─ sse.Stream                                      one per tab: chan bus.Event + closed chan
   │    └─ sse.Heartbeat                                   one ticker for all of them
   ├─ transfer.Registry                                    the write model: every Transfer this instance owns
   ├─ relay.Rendezvous                                     transfer.ID → *Sink; where two live handlers meet
   ├─ presence.Presence           Memory | Redis (TTL keys)
   ├─ transfer.ReadModel          Local | mirror.Transfers (Redis)
   ├─ httpapi.Directory           LocalDirectory | mirror.Instances (Redis)
   ├─ metrics.Metrics             its own registry, GaugeFuncs reading the objects above
   ├─ obs                         tracer provider + W3C propagator, no-op without an endpoint
   └─ httpapi.Server
        obs.Middleware → http.ServeMux → per-route middleware → handler
        └─ http.Server            WriteTimeout 0, ReadHeaderTimeout 10s, IdleTimeout 120s
```

The split that matters is not vertical but horizontal, between the two planes:

| | Control plane | Data plane |
|---|---|---|
| Handlers | `/events`, `/api/*` | `GET /d/{id}`, `POST /d/{id}` |
| Carries | presence, proposals, decisions, progress, outcomes | Payload bytes, nothing else |
| State it touches | `sse.Registry`, `presence`, `transfer.Registry`, the bus | `relay.Rendezvous`, one `relay.Session` on a stack |
| Scales by | **fanout** — every instance hears every event and filters by `Audience` | **affinity** — a live `io.Reader` and a live `http.ResponseWriter` must meet in one process |
| Cross-instance | the bus | impossible; hence the `307` (§9) |

### Purity fences, enforced rather than hoped for

Four import rules keep the layering from eroding, and three of them are tests.

- `internal/transfer` imports no `net/http` and calls no `time.Now()`. Both are
  asserted by `internal/transfer/purity_test.go`, which parses the package's own
  imports. `now` is a parameter to every transition, which is why the expiry tests
  need no sleep.
- `internal/relay` touches `net/http` in exactly one signature,
  `Response.ApplyTo` (`describe.go:61`). Everything else in the package is
  `io.Reader`/`io.Writer` and testable against a `bytes.Buffer`.
- `internal/sse` knows nothing about HTTP: a Stream writes into a `Sink`
  (`stream.go:19`), which `httpapi` satisfies with an `http.ResponseController`
  (`events.go:115`).
- Only `cmd/` imports `internal/config`. Every other package declares its own
  `Options` struct and `main` fills it in.

### Where state lives

| State | Owner | Scope | Structure |
|---|---|---|---|
| Live Streams | `sse.Registry.byUser` | one process | `map[userID]map[streamID]*Stream`, `RWMutex` (`registry.go:23`) |
| Events queued for one Stream | `Stream.ch` | one Stream | buffered `chan bus.Event`, 32 deep; overflow closes the Stream |
| The heartbeat tick | `sse.Heartbeat.beat` | one process | `chan struct{}`, closed and replaced per beat |
| Transfers this instance owns | `transfer.Registry.byID` | one process | `map[ID]*Transfer` under one `sync.Mutex` |
| Per-Sender offer rate | `transfer.Registry.offers` | one process | `map[sender]*rateWindow`, swept by the reaper |
| Parked Recipients | `relay.Rendezvous.sinks` | one process | `map[transfer.ID]*Sink` |
| **A relay in progress** | `relay.Session`, in `api.pump`'s frame | one Transfer | `zip.Writer`, the open entry, `entry`/`offset`/`total` — a stack frame and nothing else |
| Bytes relayed so far | `atomic.Int64` in `pump` | one relay | written by the copy, read by the progress goroutine |
| Presence, standalone | `presence.Memory.users` | one process | `map[userID]*entry`, each with a grace `*time.Timer` |
| Presence, distributed | Redis | cluster | `presence:<user>:<instance>` → JSON, TTL 30 s, refreshed at 10 s |
| This instance's presence claims | `presence.Redis.local` | one process | `map[userID]*entry` |
| Instance directory | Redis | cluster | `instance:<id>` → external URL, TTL 30 s |
| Transfer read model | Redis | cluster | `transfer:<id>` → JSON, plus `user:<id>:transfers` zset scored by expiry |
| Identity | Postgres | durable | `users`, `sessions` |
| Failed logins | `auth.throttle.seen` | one process | `map[ip\0email]*attempts`, capped at 50,000 keys |
| Draining | `atomic.Bool` | one process | owned by `main`, read by handlers and `/readyz` |

Two consequences worth internalising.

**Nothing about a Transfer is persisted.** The Owner's memory is the write model
(ADR 0007). Redis holds a TTL'd *mirror* used for exactly one job — a snapshot has
to include Transfers owned by other instances — and nothing ever decides anything
from it (`transfer/readmodel.go:13`). An instance that dies takes its live
Transfers with it, and the ownership middleware answers `404` for them
(`ownership.go:57-65`).

**Redis is never cleaned up on failure.** Every key it holds has a TTL and a
refresher; a hard-killed instance needs no cleanup because its keys simply expire
(ADR 0005). A clean shutdown withdraws early — presence deletes its claims
(`presence/redis.go:275-284`) and the instance directory deletes its entry
(`mirror/instances.go:67-72`) — so nobody is redirected to a process that has
gone.

---

## 2. Goroutine model

The discipline is: one goroutine per connection, one shared ticker per periodic
job, and never a goroutine per Transfer that outlives a handler.

| # | Goroutine | Count | Runs |
|---|---|---|---|
| 1 | `main` | 1 | boot, then blocks in `serve`'s select on `ctx.Done()` or a listener error (`main.go:284`) |
| 2 | Accept loop | 1 | `srv.ListenAndServe` (`main.go:279`) |
| 3 | Per-connection | 1 per TCP connection | the handler itself. For a Stream this is the SSE pump; for a download it is **the relay copy** |
| 4 | net/http background read | 1 per connection with a live handler | detects a client that goes away — the mechanism `r.Context().Done()` rests on. This is the second of the two goroutines per idle Stream ([`load-test.md`](load-test.md)) |
| 5 | Bus fanout | 1 | `for e := range events { streams.Deliver(e) }` (`main.go:141`) |
| 6 | Bus reader | 1 per `Subscribe` | the implementation's own decode loop: NATS `ChanSubscribe` pump, Redis `ps.Channel` pump, Kafka `PollFetches`. Memory has a `ctx` waiter instead (`bus/memory.go:74`) |
| 7 | Heartbeat | 1 | one `time.Ticker` for the whole instance (`sse/heartbeat.go:30`) |
| 8 | Reaper | 1 | `api.Reap` on `HP_REAP_INTERVAL` (`main.go:233`, `transfers.go:180`) |
| 9 | Presence refresher | 1, distributed only | pipelined `SET` for every local claim (`presence/redis.go:214`) |
| 10 | Instance keeper | 1, distributed only | re-register, then withdraw on `ctx` (`mirror/instances.go:53`) |
| 11 | Progress reporter | **1 per running relay** | reads the atomic counter, emits `transfer.progress` (`progress.go:30`) |
| 12 | `/readyz` probes | 1 per check per request | all checks share one budget (`readyz.go:41`) |
| 13 | OTLP batcher | 1, only with `HP_OTLP_ENDPOINT` | span export |

Presence grace windows are `time.AfterFunc` timers, not goroutines
(`presence/memory.go:61`, `presence/redis.go:111`), and there is at most one per
departing User.

Consequences worth internalising:

- **Two goroutines and one file descriptor per idle Stream, ~42 KiB of RSS.**
  Measured at ten thousand: `go_goroutines` 14 → 20,014, `process_open_fds` 8 →
  10,019, RSS 24.7 MB → 459.7 MB, three GC collections at a 0.89 ms median pause.
- **One timer per Stream would not have been fine**, which is why heartbeats go
  through the shared `sse.Heartbeat`: ten thousand tickers on a 15-second interval
  is ten thousand heap operations per beat to do the same thing at the same moment.
  The evidence that it worked is `go_threads` at 23, unchanged in shape from
  baseline.
- **The relay copy is not on a background goroutine, and cannot be.** An
  `http.ResponseWriter` is invalid the instant its handler returns, so the only way
  to write a Payload into the Recipient's response is to still be inside the
  Recipient's handler while it happens (`data.go:31-36`). The Sender's handler
  blocks on `src.Wait()` for the same reason in reverse: its request body must
  still be open.
- **The fanout goroutine must never block.** `Registry.Deliver` collects its
  targets under an `RLock`, releases, then calls `Send` — which is non-blocking and
  closes an overflowing Stream rather than waiting for it (`registry.go:85-103`,
  `stream.go:49`). One slow client cannot stall event delivery to every other
  Stream in the process.
- **Announcements outlive their request.** Anything published after a handler is
  finished with its request uses `context.WithoutCancel(r.Context())`
  (`events.go:48`, `data.go:122`, `data.go:145`, `data.go:371`): the request
  context is already cancelled and the outcome still has to reach both parties.

---

## 3. Flow: boot

`run()` (`main.go:40`) is ordered so that every failure is a failure to start
rather than a surprise later.

1. **Config first, before logging is configured.** A broken environment is the one
   error that has to be readable without a log pipeline in front of it, so it goes
   to stderr (`main.go:41-47`). `config.Load` accumulates every problem and reports
   them together — one restart, not three (`config.go:188`).
2. **The resolved config is logged once**, with `config.Secret` fields redacted.
   `Secret` implements `String`, `LogValue` *and* `MarshalJSON`; the JSON one is
   the load-bearing method, because `slog`'s JSON handler marshals struct fields
   with `encoding/json` and consults neither of the others (`config.go:89-96`).
3. **Signals before dependencies** (`main.go:54`), so everything registered after
   drains against the same context.
4. **A 15-second boot budget** (`main.go:59`): a dependency that is not there
   should fail the process, not hang it. Postgres is opened, pinged and migrated
   inside it.
5. **The bus gets its own context**, cancelled by a `defer` rather than by
   SIGTERM, so events still flow while the server is draining (`main.go:81`).
6. **One subscription per process**, feeding every local Stream (`main.go:137`).
7. **The distributed branch is a set of substitutions, not a mode**
   (`main.go:165-191`): with `HP_REDIS_URL` set, presence, the read model and the
   directory become their Redis implementations; without it they are the in-memory
   ones. No handler knows which.
8. **`/readyz` probes the bus by publishing** (`main.go:121-132`) — there is no
   ping, and "a publish works" is the thing that has to work. The probe carries no
   audience, so it reaches nobody: with `Audience` empty meaning *nobody* rather
   than *everyone*, a probe cannot accidentally be delivered
   (`bus/bus.go:14-23`).

Shutdown is the same order backwards, and the order is the whole point
(`main.go:249-254`, `main.go:277-301`):

```
SIGTERM → draining.Store(true)      /readyz answers 503; /healthz still 200
        → streams.Drain(ctx)        server.draining to every Stream, then close them all
        → srv.Shutdown(sctx)        waits for what is left, inside HP_SHUTDOWN_GRACE
```

`Server.Shutdown` waits for open connections and an SSE handler never returns on
its own, so without the drain hook that last step does not take a long time — it
never finishes (Go issue #41344). Measured: **229 ms from SIGTERM to exit with ten
thousand Streams open.** The fresh `context.Background()` at `main.go:295` is
deliberate: `ctx` is already cancelled, and the grace window is the reason for
shutting down deliberately rather than exiting.

---

## 4. Flow: opening a Stream

```
GET /events  →  obs.Middleware  →  auth.Require  →  api.events
```

1. `auth.Require` (`auth/middleware.go:29`) reads the `hp_session` cookie, fetches
   session and User in **one joined query** (`postgres/sessions.go:29`), checks
   expiry against the injected clock, and puts both in the request context. A
   session that is gone or stale also gets the cookie cleared, so a dead ID stops
   costing a round trip per request.
2. `api.events` (`events.go:32`) calls `streams.Open(u.ID)`, which returns the
   Stream **and whether it is that User's first** (`registry.go:47`). The count is
   read under the same lock that inserts, because otherwise two tabs opening at
   once both believe they are first and the User is announced online twice. A
   drained Registry returns `nil`, and the handler answers `503 draining`.
3. If first, `presence.Online` — *before* the snapshot is built, so the snapshot
   includes this User.
4. The snapshot (`events.go:89`) is `presence.List` plus
   `readModel.ForUser`. Through the read model, never the local Registry: some of
   this User's Transfers are owned by other instances (ADR 0007).
5. Headers, then `WriteHeader(200)`, then the pump. `X-Accel-Buffering: no` is not
   decoration — a reverse proxy buffers a response body by default, which turns a
   live stream into a file that arrives when it ends.
6. `Stream.Pump` (`stream.go:87`) writes `retry:`, then the snapshot as the first
   frame, then loops on four cases: a queued event, a heartbeat, its own closure,
   or the client vanishing.

Every write goes through the same closure (`stream.go:88-99`): set a per-write
deadline, write, flush. That deadline is the substitute for
`Server.WriteTimeout`, which has to stay 0 — it is a deadline on an entire
response, and any non-zero value kills every SSE stream and every long relay on a
schedule.

Two details in the loop:

- `o.Beats.Beats()` is fetched **fresh on every pass** (`stream.go:117`), because
  each beat closes the current channel and installs a new one
  (`heartbeat.go:39-44`). Closing is the only way one signal wakes N selects. A
  `nil` `*Heartbeat` returns a `nil` channel, which in a select never fires — which
  is what most tests want.
- On `<-s.closed` the pump does not just return; it flushes what is still queued
  (`stream.go:132`). On a drain the last queued event is `server.draining`, and
  the client needs it in order to know to reconnect somewhere else.

The deferred `streams.Close(stream)` reports whether it was the User's last
Stream, which starts the presence grace window (`events.go:44-52`).

### Client side of the same flow

The browser harness has no reconnect logic, on purpose (`webui/public/js/sse.js`).
`EventSource` reconnects by itself, the server's `retry:` hint sets the backoff,
and every reconnection opens with a fresh snapshot — so there is nothing to replay
and no `Last-Event-ID` to track (ADR 0003). `applySnapshot` replaces the picture
wholesale; everything after it is a patch keyed by ID, and a patch for something
the client has never seen is *dropped* rather than turned into an invented row
(`store.js:63-69`).

On `server.draining` the harness closes the stream, waits 500 ms and reopens
(`app.js:103-110`) — landing on whichever instance the entry point picks next.

`spud`'s parser is the same protocol read literally: split the body on blank
lines, and that is the whole thing (`cmd/spud/client.go:189`).

---

## 5. Flow: an event, from publish to a browser

```
handler or presence
│
├─ api.emit (transfers.go:239)
│    bus.NewEvent(name, audience, payload)    ← JSON marshalled once, here
│    e.Trace = obs.Traceparent(ctx)           ← so a trace can cross instances
│    bus.Publish  → metrics.BusPublish(elapsed)
│      failure is logged and swallowed: at-most-once by contract, and the
│      client's next reconnect brings a fresh snapshot (ADR 0003)
│
├─ the bus, one subject / channel / topic
│    memory → straight into every subscriber channel, dropping the slow ones
│    nats   → Publish, fire and forget, reconnect forever
│    redis  → PUBLISH, waits for the server to acknowledge the command
│    kafka  → Produce, async, one consumer group per subscription
│
└─ every instance, including the publisher
     bus reader goroutine → the firehose channel
       main.go:141  streams.Deliver(e)
         registry.go:85  for every local Stream whose User is in e.Audience
           stream.go:49  Send — non-blocking; a full buffer closes the Stream
             the Stream's own pump writes the frame  (§4)
```

Addressing is a **field on the Event**, not a subject (`bus/bus.go:39`). Kafka has
no per-key subscribe, so narrowing at the transport would make the four
implementations behave differently, and the interface is deliberately pinned to
the lowest common denominator of all of them: at-most-once, no ordering across
users, one firehose (ADR 0006).

`Everyone` is spelled `"*"` rather than being what an empty audience means. With
empty meaning everyone, a new event type published without an audience — a
forgotten argument, a refactor that drops one — would reach every signed-in User,
and neither the compiler nor a test would say so. Empty now reaches nobody, which
is the harmless direction to fail in.

### What each implementation does differently

- **Memory** (`bus/memory.go`) is a fan-out over channels. `Publish` drops to a
  behind subscriber rather than stalling the publisher (`memory.go:50-56`), and
  `Subscribe` starts a goroutine whose only job is to unregister on `ctx.Done` —
  under the same lock `Publish` holds, which is what makes closing the channel
  safe.
- **NATS** (`bus/nats.go`) reconnects forever (`MaxReconnects(-1)`): a bus outage
  should degrade the control plane, not end the process. `Close` **drains** rather
  than closing, so anything already published is flushed.
- **Redis** (`bus/redis.go`) waits for the subscription to be live before
  returning (`redis.go:62`), or an event published immediately afterwards is
  silently lost. Its `Publish` waits for the server's acknowledgement, which is
  worth knowing when comparing its latency to NATS's.
- **Kafka** (`bus/kafka.go`) is here to be compared, not because it fits. Every
  instance needs every event, so every instance needs a group of its own — and a
  group *per subscription*, because members of a group divide the partitions
  between them (`kafka.go:103-113`). The group name carries a random per-process
  suffix: with a stable name, a restarted instance rejoining a group whose previous
  member has not been evicted waits out `session.timeout.ms` — measured at 20
  seconds of no events at all (`kafka.go:68-79`). Offsets are never committed and
  consumption always starts `AtEnd`.

Numbers for all four are in [`bus-comparison.md`](bus-comparison.md): paced p50
memory 6.6 µs, NATS 140 µs, Redis 145 µs, Kafka 840 µs.

---

## 6. Flow: offer, accept, and the reaper

**`POST /api/transfers`** (`transfers.go:35`), the one Transfer route with no
ownership middleware — it is minted here, so it is owned here.

1. `requireJSON` then `decode`: bounded at 64 KiB, strict, unknown fields are a
   `400` (`json.go:42`). Together with `SameSite=Lax` on the session cookie, the
   required `application/json` *is* the CSRF defence — a cross-site form cannot set
   that content type without a preflight the browser refuses (`json.go:15-21`).
2. The Recipient must be online (`transfers.go:51-59`). Offering to somebody who
   is not there creates a Transfer that can only expire.
3. `transfer.NewID(instance)` → `inst-a.<16 hex>`. The instance prefix is the whole
   of ADR 0007: any instance can work out who owns a Transfer without asking
   anybody. `config` rejects an instance ID containing a dot for exactly this
   reason (`config.go:347`).
4. `t.Trace = obs.Traceparent(ctx)`: this Transfer's accept, relay and outcome are
   separate requests on possibly separate instances, and carrying the header is
   what puts them on one trace.
5. `Registry.Create` (`registry.go:110`) validates the declaration, then counts
   outbound and pending-inbound Transfers **inside the same lock that inserts**.
   Counting outside it would let two simultaneous offers both see room for one
   more. The scan is linear and marked `ponytail:` with its ceiling.
6. `transfer.created` to the Sender's tabs, `transfer.offered` to the Recipient's.
   The Sender's other tabs are told too, which is what makes the outbound list the
   same on every one of them.

**Accept, deny, cancel** are all the same shape: `a.mutate(ctx, id, fn)` around a
method on `*transfer.Transfer`, then one event to both parties.

`Registry.Mutate` (`registry.go:174`) is the entire compare-and-swap: it applies
`fn` with the lock held, so two tabs accepting at the same moment are serialised
and exactly one sees a `nil` error. The winning Stream ID travels in
`transfer.accepted` so the losing tabs can clear their prompt — it is a hint, not
an authorization input, and deliberately not checked against the Stream registry,
because in a multi-instance deployment the accept has been redirected to an
instance that has never heard of that Stream (`transfers.go:105-118`).

`api.mutate` (`transfers.go:214`) also mirrors the result into the read model, and
does so **even when the transition was refused**: it costs one round trip and
removes the question of whether this particular failure changed anything —
`Complete` on a short payload does, because it fails the Transfer on the way out
(`transfer.go:225-228`).

`cancelTransfer` additionally calls `rendezvous.Cancel(id)` (`transfers.go:169`),
which closes the Sink's `canceled` channel so a relay already in flight is
abandoned at the next buffer boundary. When nothing is streaming it is a no-op,
which is the common case.

**The reaper** (`transfers.go:180`) ticks every `HP_REAP_INTERVAL` and calls
`Registry.ReapExpired` (`registry.go:223`), which does three different things in
one pass:

- an offer past its TTL → `Expire`
- an interrupted Transfer past its `ResumeBy` → `Fail(sender_disconnected)`
- a terminal Transfer older than `HP_TERMINAL_WINDOW` → forgotten

It returns what it made terminal, so both parties can be told; an unanswered offer
and an abandoned relay are both outcomes somebody is waiting for. There is no
`transfer.expired` event — an expired offer arrives as `transfer.failed` with
reason `offer_expired`, which is what a client that only handles failures needs to
see. The same pass sweeps idle rate-limit entries, or the map grows with every
Sender that ever sent one offer (`registry.go:243`).

Because the reaper is a ticker, an offer can be past its TTL and still pending.
`Accept` therefore re-checks expiry itself and refuses (`transfer.go:121`), while
`Cancel` deliberately tolerates it: accepting a dead offer would start something
nobody expects, whereas cancelling one is what the caller wants regardless
(`transfer.go:144-148`).

---

## 7. Flow: the rendezvous and the relay

The core of the system, and the part that is easy to get wrong. Two handlers on
two goroutines, each blocked on the other.

```
Recipient's goroutine                          Sender's goroutine
─────────────────────                          ──────────────────
GET /d/{id}
 ownership → auth.Require → api.download
 draining? → 503
 Get(id); u.ID == Recipient? → 403             (early, cheap, no timing oracle)
 ParseRangeStart(Range)      → 416 if unservable
 mutate: AttachRecipient     → 409 if taken
 obs.Continue(t.Trace)       → the Transfer's own trace
 Rendezvous.Park(id, sink)
 emit transfer.ready{entry, offset} → Sender
 select {
   sink.Ready()  ────────────────────────────  POST /d/{id}
   r.Context().Done() → finish(WriteError)      multipart + boundary? → 400
   wait.C (HP_RENDEZVOUS_WAIT)                  resumePosition(headers)
     → 504 + finish(rendezvousTimeout)          mutate: AttachSender
 }                                              → 409 recipient_not_attached if early
 ↓ got a Source                                 NewSource(multipart.Reader, ctx)
 Describe(payload).ApplyTo(w)  ← 200/206       Rendezvous.Handoff(id, src) ──┐
 reportProgress goroutine starts                                            │
 NewSession(deadlineWriter, payload)            src.Wait() ← blocked  ───────┘
 loop: relayChunk(session, src)
   session.Continue(claimed position)
   session.Consume(parts)  ← the copy runs HERE, 64 KiB at a time
   src.Report(result)  ──────────────────────→  unblocked → 204, or 409/502 + X-Expected-*
 end(err) → finish → transfer.completed | transfer.failed
 handler returns  ← only now is w invalid
```

Three properties this arrangement exists for:

- **Nothing is written to the Recipient before the Sender attaches.** An HTTP
  status cannot be retracted, so committing `200` on arrival would leave no way to
  report a Sender who never shows up. Parking first is what makes a truthful `504`
  possible (ADR 0002, `data.go:106`).
- **The copy runs on the Recipient's goroutine.** See §2.
- **The Range is settled before anything is claimed** (`data.go:47-73`). A
  Recipient gets one attach, and a request that is going to be refused must not
  spend it — an earlier version checked the Range *after* attaching and left the
  Transfer unusable by the retry that would have worked.

`Handoff` does not block (`rendezvous.go:124`): nobody parked means the Sender is
early, and a Sink already holding a Source means two POSTs are racing (`ErrBusy` →
`409`). If `Handoff` fails because the Recipient's handler returned in the window
between the state transition and the handoff, the Sender's handler is the one that
publishes the outcome (`data.go:368-374`) — nobody is going to read that body.

### The byte path

`relay.Session.relayPart` (`copy.go:126`) is where a part becomes a response.

```
r.Body → multipart.Reader → one *multipart.Part
  → trackedReader        remembers a read failure (the Sender's fault)
  → stopReader           checks the cancel channel between buffers
  → io.CopyBuffer(64 KiB)
  → countingWriter       calls Options.Count, remembers a write failure
  → zip.Writer entry     folders only, Method: Store
  → deadlineWriter       re-arms the write deadline before every Write
  → http.ResponseWriter
```

- **The 64 KiB buffer is the entire memory cost of the data plane.** Neither end
  implements `ReadFrom` or `WriteTo`, so `io.CopyBuffer` really does use this
  buffer and nothing else. Measured: 1 GB relayed with the heap going 625 KB → 1.2
  MB and 1.4 MB allocated in total.
- **Backpressure needs no code.** The write blocks on the Recipient's socket,
  which stops draining the Sender's body, which closes the TCP window on the
  upload (`copy.go:174-176`).
- **Which side failed is tracked separately**, because that is what decides who
  gets blamed in `transfer.failed`. `trackedReader` → `sender_disconnected`,
  `countingWriter` → `recipient_disconnected` (`copy.go:180-194`,
  `data.go:455-470`). `io.ErrShortWrite` is classified as a write failure however
  it presents itself — only a destination can short-write, and left unclassified it
  would be reported as an internal error and treated as unresumable.
- **`stopReader` exists because a copy blocked inside `io.CopyBuffer` cannot
  select on a channel**, so the cancellation check goes on the read path
  (`copy.go:278-292`).
- **Kind comes from the declaration, never from counting parts** (`copy.go:91-97`).
  `NextPart` discards the previous part's unread body, so looking ahead to see
  whether a second part exists would destroy the first (ADR 0004).
- **`zip.Store`, not Deflate.** Compression burns CPU per byte on the one machine
  here that should stay a dumb pipe, and the payoff is a guess about data the
  server never sees. Store also lets `archive/zip` write to a non-seekable writer,
  recording each entry's size in a trailing data descriptor.
- **Entry names come from the raw `Content-Disposition`, not
  `Part.FileName()`** (`copy.go:228-244`). RFC 7578 §4.2 says a filename must not
  be taken as path information, so Go returns `filepath.Base` of it — destroying
  exactly the relative path a folder Payload is made of. Reading the raw header is
  safe only because `SanitizeEntry` (`sanitize.go:24`) runs next: one function
  preserves the path, the next makes sure it cannot escape the archive.
- **The response shape is pure** (`describe.go:30`). Same Payload, same headers, no
  server involved — which is what lets the headers be right the first time, before
  a byte has arrived. A folder gets `Content-Length: -1` (chunked), because a zip
  built as it streams has no known size; a single file passes through untouched and
  keeps its declared length, which is why the browser's own download indicator
  works for files and not for folders.
- **`Close` is the only integrity check there is** (`copy.go:208`). The server
  holds no copy, so the declared total and entry count are all it can enforce; a
  mismatch fails the Transfer as `payload_mismatch` rather than completing it.

### Progress

`reportProgress` (`progress.go:22`) starts one goroutine per relay. The copy owns
the `atomic.Int64` and writes to it; this goroutine only reads — which is why
`relay` knows nothing about events, it is handed a `func(int64)` and never learns
what happens to the number.

Two behaviours worth knowing: a tick where the count has not changed is
**suppressed**, so a stalled relay goes quiet instead of repeating itself four
times a second; and the rate is computed from the delta since the last *published*
tick, floored at 1, so a suppressed gap does not read as a speed-up and a starved
ticker does not report zero bytes per second while bytes are demonstrably moving.

The returned stop function is idempotent and **does not return until the goroutine
has exited** (`progress.go:77-83`). That is what guarantees no progress event can
follow the terminal one, and the `sync.Once` is what keeps the relay's several
endings from closing a closed channel — a panic inside a handler takes the
connection with it.

### One outcome, exactly once

`api.finish` (`data.go:420`) is the only place a data-plane outcome is published,
so there is exactly one per Transfer however the relay ended. Its odd-looking
guard —

```go
if err != nil && final.State != transfer.StateFailed { ...return }
```

— is there because `Complete` on a short payload *fails* the Transfer and returns
an error (`transfer.go:221-233`), so an error with a final state of `failed` is
still a transition that needs announcing. An error with any other terminal state
means a cancellation got there first and has already been announced.

---

## 8. Flow: resume, both directions

Resume needs no new storage, and that is the design: the only state a resumed
Transfer needs is the `relay.Session` sitting in `api.pump`'s stack frame — the
live `zip.Writer` and its half-written entry (`data.go:137-143`). Which is also
exactly why resume cannot survive the Owner's death (ADR 0008).

### The Sender comes back

When `relayChunk` returns a read error and `resumable(err)` is true
(`data.go:280`), `pump` does not fail. It transitions `streaming → accepted` via
`DetachSender`, which sets `ResumeBy` (`transfer.go:195`), and then waits on the
same Sink again with an `HP_RESUME_WINDOW` timer. The Recipient never notices: its
response is still open, and the archive position is still live.

The invitation back carries the position, not just the fact
(`data.go:217-230`). That is not a convenience: a Sender whose upload died usually
never sees its own response at all, because the HTTP client reports the broken
request body instead. Putting the position on the control plane is what makes
resume usable rather than theoretical.

`Session.Continue` (`resume.go:45`) checks the Sender's claim against where the
relay actually is, and it is the only defence there is. Without a manifest, a wrong
offset that got through would splice the wrong bytes in and surface as a CRC error
when the Recipient unzips — far too late to do anything about. A mismatch is
`409` plus `X-Expected-Entry` and `X-Expected-Offset`, the one failure a Sender can
fix by itself.

If nobody comes back, the reaper enforces `ResumeBy` (§6). Without that, a Transfer
whose Sender never returns would be held open by nobody, forever.

### The Recipient comes back

A write failure means the Recipient's socket is gone, so `pump` calls
`recipientLeft` (`data.go:300`). For a single file with resume enabled it
transitions `DetachRecipient`, records the position, and emits a final
`transfer.progress` to both parties so the Recipient knows what to ask for and the
Sender knows to expect a fresh request. For a folder it fails: the archive was
being built as it streamed, and that stream went with the socket.

The returning `GET /d/{id}` carries `Range: bytes=N-`, and `N` comes from the
**Recipient**, not from the server's count (`resume.go:57-71`). The Owner counts
bytes written *into a socket*, which after a disconnection exceeds what came out of
the other end by whatever was in flight — measured at about 98 KB on loopback.
Only the Recipient knows what it holds, which is why every resumable download
protocol works this way round. `Session.SkipTo` then rewinds what the Sender is
expected to send, even past bytes it had already delivered.

`ParseRangeStart` (`describe.go:97`) accepts only the open-ended single-range
form. The general syntax exists so a client can ask for the middle of a file it can
already seek in; here the bytes do not exist yet and arrive once, in order.

### Client side

`spud`'s loop is the mirror image (`cmd/spud/send.go:118`): on failure, wait for
`transfer.ready` on the control plane, take the position from **the invitation**
rather than from the failed response (`cmd/spud/resume.go:55-65`), and rebuild the
multipart body skipping finished entries and seeking into the half-sent one. If the
position has not moved since the last attempt it stops rather than spinning.

Two traps it documents by handling them:

- **A retried upload must reuse its boundary** (`send.go:176-183`). A retry
  declares the `Content-Type` of the attempt before it, so a fresh
  `multipart.Writer` with a fresh random boundary makes the server look for a
  delimiter that is not in the body and read the whole upload as one header —
  surfacing as `bufio: buffer full`, nowhere near the cause.
- **The partial file *is* the resume state** (`recv.go:152-158`), so a download
  appends when resuming and truncates only when starting.

---

## 9. Flow: cross-instance ownership

`a.ownership` (`ownership.go:44`) wraps every route about an existing Transfer —
accept, deny, cancel, and both halves of the data plane. It is the **outermost**
middleware on those routes, before `auth.Require`, because a redirect should not
cost a session lookup (`router.go:160-173`).

```
id → transfer.OwnerOf(id)          split on the first "."
  ""            → 400
  == a.instance → next.ServeHTTP
  otherwise     → directory.Lookup(owner)
                    not found → 404  "the instance holding that transfer is gone"
                    found     → 307 base + r.URL.RequestURI()
```

`LocalDirectory` (`ownership.go:20`) knows about one instance, itself, which is
what makes the middleware safe to install unconditionally in a standalone
deployment. `mirror.Instances.Lookup` is one Redis `GET` — the whole cost of
resolving an owner.

**`307`, never `302`**: a `302` is allowed to turn a POST into a GET, which would
silently drop an upload's body. And the trap that follows from it: `net/http` will
not follow a `307` for a streaming body, because it cannot replay it. The client
has to handle the redirect itself, rebuilding the body from disk — which is why
`spud` sets `CheckRedirect` to `ErrUseLastResponse` and loops at most twice
(`cmd/spud/client.go:44-53`, `client.go:89-126`). One hop is all ownership ever
needs: the redirect target is the Owner, and the Owner does not redirect.

The `404` case is honest rather than defensive: if the Owner is gone, so is the
Transfer — its state was that process's memory.

---

## 10. Presence

Presence answers one question — who is online — and its only subtlety is that
losing a Stream is not the same as leaving. Both implementations start a grace
window instead of announcing immediately, so a browser refresh does not make a
User flicker out of everyone's list.

**Memory** (`presence/memory.go`) is a map plus one `time.AfterFunc` per departing
User. `Online` inside a live grace window cancels it and announces nothing;
`expire` re-checks that the window is still the live one, because `Online` may have
cancelled it between the timer firing and this taking the lock. Announcements
happen **outside** the lock — an announcement reaches the bus, which reaches the
Stream registry, and holding a presence lock through all of that is how deadlocks
are made.

**Redis** (`presence/redis.go`) is one key per **(User, instance)**, not per User.
The original design said one key per User carrying the instance as a value; that
breaks for a User with tabs on two instances, because the second `SET` overwrites
the first and whichever instance loses its last Stream first deletes a claim that
is still true. Deriving a User's presence from whether *any* claim survives is the
same idea with the hole closed (see ADR 0005's amendment).

- `expire` deletes this instance's claim, then scans for others before announcing
  (`redis.go:117-144`). If the scan fails it announces *nothing*: removing a User
  who may still be here is worse than waiting for a TTL that will remove them
  anyway.
- `List` uses `SCAN`, never `KEYS` — `KEYS` is O(n) with the whole keyspace blocked
  for the duration, on the one server every instance shares — and deduplicates
  across instances (`redis.go:164-211`).
- One refresher goroutine renews every local claim in one pipeline
  (`redis.go:214`). Presence survives a Redis restart for free: the next tick
  re-registers everyone.
- Both implementations sort by display name, so a snapshot does not reshuffle the
  list on every reload.

---

## 11. Writes, deadlines and backpressure

Every long-lived write in this server is governed by the same two decisions, and
they are the invariants most likely to look like a network problem when broken.

**`Server.WriteTimeout` stays 0** (`main.go:242`). It is a deadline on an entire
response. Any non-zero value kills SSE streams and multi-gigabyte relays on a
schedule.

**`ReadHeaderTimeout`, never `ReadTimeout`** (`main.go:245`). A slow upload
outlives any whole-request read deadline worth setting.

What replaces them is a per-write deadline, pushed out by progress and left to fire
on silence:

| Path | Mechanism | Config |
|---|---|---|
| SSE | `Stream.Pump`'s write closure sets it before every frame (`stream.go:92`) | `HP_SSE_WRITE_DEADLINE`, 10 s |
| Relay | `relay.NewDeadlineWriter` re-arms before every `Write` (`deadline.go:34`) | `HP_RELAY_WRITE_DEADLINE`, 30 s |

Both go through `http.ResponseController`, and both tolerate a writer that cannot
do deadlines at all: `optional()` swallows `http.ErrNotSupported`
(`events.go:130`), because an `httptest.ResponseRecorder` or a middleware that
wraps without forwarding is a property of the writer, not a failure of the stream.

Backpressure differs by plane, deliberately:

- **Data plane: block.** The copy's write blocks on the Recipient's socket, which
  stops the Sender's body being drained. Nothing buffers, so nothing grows.
- **Control plane: drop.** A Stream has a 32-event buffer; overflowing it closes
  the Stream (`stream.go:61-63`). The client reconnects and gets a fresh snapshot,
  which is cheaper and more correct than an unbounded queue. The bus behaves the
  same way one level up (`bus/memory.go:52`).

The one hard external limit is on the browser side, and it is measured: **a browser
allows six HTTP/1.1 connections per origin, and every tab holds one open forever
for `/events`.** With five Streams up, `/healthz` answers in 4 ms; with six it
never answers at all, and a seventh tab cannot even load `/`. So the harness makes
no periodic requests of any kind, and anything added to it competes with the
streams. Only HTTP/2 lifts the cap, and no browser speaks h2c — so that means TLS.

---

## 12. Configuration surface

`config.Load` (`config.go:102`) parses the environment into one immutable typed
value, and the parsing is opinionated in three ways.

**It accumulates.** Every problem is collected and reported together, so a broken
deployment costs one restart rather than three (`config.go:188`).

**Set-but-empty is a typo, not a policy.** `present` rejects a blank value rather
than falling back to the default (`config.go:214`), so `HP_DATABASE_URL=` fails
instead of quietly meaning something else. `count`, `duration` and `bytes`
likewise reject zero and negative values: a zero limit is a typo.

**Requirements are derived, never profiled.** There is deliberately no
`HP_ENV=prod` switch, because a profile fails open — forget to set it and every
development default is silently reinstated in production. Instead, requirements
follow from values already present (`config.go:157-186`):

| Required | When | Why |
|---|---|---|
| `HP_DATABASE_URL` | always | a default compiles the development password into the binary |
| `HP_INSTANCE_ID` | `HP_REDIS_URL` is set | two instances both answering to `inst-local` break ownership routing |
| `HP_EXTERNAL_URL` | `HP_REDIS_URL` is set | the *client* follows the `307`, so a container-internal default is unreachable |
| `HP_NATS_URL` / `HP_KAFKA_BROKERS` / `HP_REDIS_URL` | that bus is selected | a localhost default for a broker is the same trap as the database one |

Every duration in the policy envelope is a field here rather than a literal
somewhere, which is what lets the presence and reaper tests run in milliseconds
instead of sleeping for ten seconds. The full list with defaults is
[`../.env.example`](../.env.example) and the table in
[`protocol.md`](protocol.md#limits-and-timings).

`Distributed()` is one predicate — `RedisURL != ""` — and it is the only mode
switch in the system (`config.go:82`).

---

## 13. Instrumentation

**Metrics** (`internal/metrics`) has its own registry, and every method is safe to
call on a `nil *Metrics`, so a test can pass `nil` rather than build a registry it
will not read. Live counts are `GaugeFunc`s reading the real objects — the registry
already knows, and two numbers that can disagree are worse than one
(`metrics.go:24-34`). The Go and process collectors are registered because that is
where a load test reads goroutines, file descriptors, RSS and GC pause. Names are
in [`protocol.md`](protocol.md#metrics).

**Tracing** (`internal/obs`) installs the W3C propagator whether or not a
collector is configured, so `Inject` and `Extract` behave the same either way; with
no endpoint, spans are created and immediately dropped (`tracing.go:37-42`).
`obs.Middleware` is the outermost handler, so a redirect or a rejection is on the
trace too.

The interesting part is that a Transfer's trace outlives its request.
`obs.Traceparent` serialises the active span into a header string, which is stored
on the Transfer (`transfers.go:76`) and on every `bus.Event` (`transfers.go:247`);
`obs.Continue` rebuilds a context from it, which is how `relay.recipient` becomes a
child of the span that proposed the Transfer (`data.go:90`) — a different request,
possibly on a different instance.

**Logging** is `slog`'s JSON handler at `HP_LOG_LEVEL`. The convention throughout
is that a failure which cannot be reported to a client is logged at the level that
matches how much anyone can do about it: `slog.Debug` for a client that left
mid-frame, `slog.Error` for a mirror write that failed, and nothing at all for the
ordinary endings.

**Health** is two endpoints because they answer different questions
(`readyz.go:15-20`). `/healthz` says the process is running and a restart would not
help. `/readyz` says it can do its job — every dependency in parallel, one shared
budget, `503` while draining. Draining is the clearest case of the difference: the
process is perfectly healthy and must stop receiving requests.

---

## 14. Deployment shapes

**Standalone.** `HP_REDIS_URL` unset. Presence, the read model and the directory
are in-memory; the bus can still be `memory` or a real broker. Everything about
every Transfer is answered by this process, and the ownership middleware always
takes the `owner == a.instance` branch. Needs Postgres and nothing else.

**Clustered.** `HP_REDIS_URL` set, plus a bus that crosses processes. Redis holds
presence keys, the instance directory and the Transfer read model — all TTL'd, all
mirrors. The control plane scales by fanout: N instances each hold one subscription
to the same subject and filter locally. The data plane does not scale that way at
all; a Transfer is pinned to the instance that minted it, and every other instance
answers `307`. `make up-cluster` runs exactly this: Postgres, Redis, NATS, two
instances and Caddy round-robining between them.

The load balancer needs no session affinity for the control plane — any instance
can serve any User's Stream — and cannot provide it for the data plane, which is
what the `307` is for.

---

## 15. Failure modes, as designed

| Failure | Detected by | Behaviour |
|---|---|---|
| No session, or a stale one | `auth.Require` | `401`, and the cookie is cleared |
| Wrong credentials | `Service.Login` | `401` with one message for both "no such email" and "wrong password"; an unknown email is verified against a decoy hash so the response clock cannot enumerate accounts |
| Too many failed logins | in-process throttle, per IP+email | `429`. Keyed on both so one attacker cannot lock out a victim; the map is capped and fails closed |
| Offering to someone offline | `createTransfer` | `404` — the Transfer would only expire |
| Per-User limits, offer rate | `Registry.Create`, under the insert lock | `429` |
| Declared size over the ceiling | `Payload.Validate` | `413` |
| A second accept from another tab | `Registry.Mutate` serialising | `409 illegal_state`; the winner's `byStream` clears the loser's prompt |
| Offer unanswered for its TTL | the reaper | `transfer.failed{offer_expired}` to both |
| Sender arrives before the Recipient | `AttachSender` | `409 recipient_not_attached` — wait for `transfer.ready` |
| Sender never arrives | the rendezvous timer | `504` to the Recipient, `transfer.failed{rendezvous_timeout}` to both. Only possible because no status was written on arrival |
| Sender dies mid-relay | a read error on its body | `streaming → accepted`, `ResumeBy` set, `transfer.ready{resume:true}` with the position. The reaper fails it if nobody returns |
| Recipient dies mid-relay | a write error on its socket | a single file detaches and waits for a `Range`; a folder fails as `recipient_disconnected`. `502` + `X-Expected-*` to the Sender |
| Sender resumes from the wrong place | `Session.Continue` | `409` + `X-Expected-Entry`/`X-Expected-Offset` — the only failure a Sender can fix itself |
| `Range` on a folder | `download`, before attaching | `416`, and the Recipient's one attach is not spent |
| Delivered bytes ≠ declared | `Session.Close` / `Transfer.Complete` | `payload_mismatch`; the Transfer fails rather than completing |
| Either party cancels | the cancel endpoint | `transfer.canceled`, and a relay in flight is abandoned at the next 64 KiB boundary |
| A Stream falls 32 events behind | `Stream.Send` | the Stream is closed; the client reconnects into a fresh snapshot |
| A bus publish fails | `api.emit` | logged and swallowed — at-most-once by contract, healed by the next snapshot |
| Redis unreachable | each call site | presence announces nothing rather than removing a User; a mirror failure is a worse snapshot, not a broken one; `/readyz` reports it |
| Bus broker unreachable | the implementation | NATS reconnects forever; the control plane degrades and the process stays up |
| The Owner instance is gone | `directory.Lookup` | `404` — its Transfers were its memory, and there is nothing to recover |
| This instance is draining | `draining` + `Registry.drained` | `503` on `/events` and `GET /d/{id}`, `503` on `/readyz`, `server.draining` to every open Stream, `200` on `/healthz` |

The rule underneath the table: **every failure produces one addressed event.** No
connection is ever left hanging without an explanation, which is why
`api.finish` is the single publisher of data-plane outcomes and why the reaper
announces what it expires.

---

## Reading the source along this document

| Section | Start here |
|---|---|
| Wiring, drain, shutdown | `cmd/server/main.go` |
| The rules, with no clock and no HTTP | `internal/transfer/transfer.go`, `registry.go` |
| Streams, snapshot, heartbeat | `internal/httpapi/events.go`, `internal/sse/` |
| Event fanout | `internal/bus/`, `internal/sse/registry.go` |
| Offer, accept, the reaper | `internal/httpapi/transfers.go` |
| The rendezvous and the relay | `internal/httpapi/data.go`, `internal/relay/rendezvous.go`, `copy.go` |
| Resume | `internal/relay/resume.go`, `internal/httpapi/data.go`, `cmd/spud/send.go` |
| Ownership | `internal/httpapi/ownership.go`, `internal/mirror/instances.go` |
| Presence | `internal/presence/redis.go` |
| Identity | `internal/auth/`, `internal/postgres/` |
