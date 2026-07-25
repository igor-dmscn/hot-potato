# CLAUDE.md

A Go backend that streams a file or folder from one signed-in user to another.
Payload bytes pass through the server as a live stream and are never written to
disk; every state change is pushed to browsers over SSE. Orientation in
[`README.md`](README.md), the wire contract in
[`docs/protocol.md`](docs/protocol.md), vocabulary in
[`docs/glossary.md`](docs/glossary.md), flows in
[`docs/flows.md`](docs/flows.md).

`main` is SSE-only. Phase 12 (WebRTC) lives on the `webrtc` branch and must stay
there.

## Commands

```sh
make            # lists every target
make up         # Postgres — enough to run the server
make run        # one instance on :8080
make check      # fmt + vet + test
make test       # everything that needs no broker
make up-brokers && make test-all   # plus Postgres, distributed, all four buses
make demo       # a whole transfer, two spuds, no browser
```

Integration suites skip themselves unless `HP_TEST_DATABASE_URL`,
`HP_TEST_REDIS_URL`, `HP_TEST_NATS_URL`, `HP_TEST_KAFKA_BROKERS` point at
something real. A clean checkout with nothing running is green — do not "fix" a
skip by removing it.

## Architecture in one paragraph

The **control plane** (SSE + small JSON endpoints) carries presence, proposals,
decisions, progress and outcomes, and scales by **fanout**: every instance
subscribes to every event and filters by an `Audience` field. The **data plane**
carries Payload bytes only, and scales by **affinity**: a rendezvous holds a live
`io.Reader` and a live `http.ResponseWriter`, so both halves must meet in one
process. A Transfer's ID embeds its Owner (`inst-a.7f3a9c2e…`) and any other
instance answers `307`.

## Invariants — break these and it looks like a network problem

- **`Server.WriteTimeout` stays 0.** It is a deadline on a whole response; any
  non-zero value kills SSE streams and long relays on a schedule. Long-lived
  writes take per-write deadlines from `http.ResponseController`.
- **`ReadHeaderTimeout`, never `ReadTimeout`.** A slow upload outlives any
  whole-request deadline.
- **A `ResponseWriter` is invalid once its handler returns.** The relay copy runs
  on the *Recipient's* goroutine and the Sender's handler blocks until told it is
  done. Do not move the copy to a background goroutine.
- **Nothing is written to the Recipient before the Sender attaches.** A status
  cannot be retracted, and that is what makes a `504` for a no-show possible.
- **The drain hook runs before `Server.Shutdown`.** Otherwise Shutdown waits
  forever on an open SSE handler (Go issue #41344).
- **`307`, never `302`** for the ownership redirect: `302` may turn a POST into a
  GET and drop the body. Note that `net/http` will not follow a 307 for a
  streaming body — clients must handle it themselves, as `cmd/spud` does.
- **Every SSE frame ends in a blank line.** Forgetting the second `\n` makes the
  browser buffer forever.
- **A browser allows six HTTP/1.1 connections per origin, and every tab holds one
  open forever for `/events`.** Measured: five streams and `/healthz` answers in
  4 ms, six and it never answers at all — nor does a seventh tab load `/`. So the
  harness makes no periodic requests, and anything added to it competes with the
  streams. Only HTTP/2 lifts the cap, and no browser speaks h2c, so that means
  TLS.
- **`internal/transfer` imports no `net/http` and calls no `time.Now()`.** Both
  are enforced by tests in `purity_test.go`. `now` is a parameter to every
  transition.
- **Only `cmd/` imports `internal/config`.** Every other package declares its own
  `Options` struct.
- **Presence keys are per (User, instance)**, not per User: one key per User
  breaks for a User with tabs on two instances.
- **`multipart.Part.FileName()` returns `filepath.Base`** (RFC 7578 §4.2) and so
  destroys a folder's relative paths. Read the raw `Content-Disposition`, then
  `relay.SanitizeEntry`.
- **A retried multipart upload must reuse its boundary.** Generating a new one
  per attempt makes the server read the whole body as one header.

## Conventions

- **Comments say why, not what.** Most non-obvious lines here carry the reason
  they are that way, usually with the ADR number. Keep that up; delete a comment
  only when the reason stops being true.
- **One word per concept.** `docs/glossary.md` lists the words and the words to
  avoid. Recipient, never receiver — that means a method receiver in Go.
- **Two implementations is the bar for an interface existing.** One is not.
- **Tests do not sleep.** They wait on a channel with a deadline. The exception,
  documented where it appears, is polling state that lives in another process
  (Redis TTLs).
- **Every duration is a config field**, never a literal. That is what lets the
  presence and reaper tests run in milliseconds.
- **A `ponytail:` comment marks a deliberate shortcut** and names its ceiling and
  upgrade path. They are load-bearing documentation, not TODOs.
- Errors are wrapped with `%w` and matched with `errors.Is`/`errors.As`; sentinel
  errors are translated at package boundaries (a Postgres `23505` becomes
  `auth.ErrEmailTaken`).
- Secrets use `config.Secret`, whose `String`, `LogValue` **and `MarshalJSON`**
  all return `[redacted]` — the JSON one is the one that matters, because
  `slog`'s JSON handler marshals struct fields with `encoding/json`.

## Where things are

```
cmd/server/         wiring and signal handling — the only importer of config
cmd/spud/           CLI client: watch, send, recv, load, with resume
internal/transfer/  the state machine — pure, no net/http, no clock
internal/relay/     rendezvous, multipart→zip, counting, resume
internal/httpapi/   handlers, routing, middleware, ownership redirect
internal/sse/       frame writing, Stream registry, shared heartbeat
internal/bus/       Bus interface + memory, nats, redis, kafka
internal/presence/  memory + redis TTL implementations
internal/mirror/    Redis read models: instance directory, Transfer metadata
internal/auth/      identity; owns the store interfaces postgres satisfies
internal/postgres/  adapter, named for what it wraps
internal/webui/     the harness, embedded; files in public/
```

## When changing things

- The wire contract is [`docs/protocol.md`](docs/protocol.md): endpoints, events,
  error codes, limits. Code comments cite it by name. Change one without the other
  and the next reader believes the wrong one.
- The decision records are in [`docs/adr/`](docs/adr/), each with an `## Amendment`
  section for what the build found — **read 0004, 0005, 0006 and 0008's amendments
  before touching the relay, presence, the bus or resume.** If a change contradicts
  a record, amend the record in the same commit rather than quietly diverging, and
  do not edit the original text above the amendment line.
- One commit per coherent change, with a message that says *why* and names what
  the tests found. `git log` here is meant to be readable.
- Measurements in `docs/` are real numbers from a real run. If you change
  something they cover, re-run `make measure` and update them, or say they are
  stale.
- The harness in `internal/webui/public/` is deliberately disposable: no build
  step, no framework, vendored Tailwind. Do not add a bundler.
