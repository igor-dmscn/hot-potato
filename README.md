# Hot Potato

A Go backend where signed-in users see who else is online and stream a file or
folder directly to one of them. Payload bytes pass through the server as a live
stream and are never written to disk; every state change is pushed to browsers
over Server-Sent Events.

The name is the invariant: the server cannot hold what it is handed.

Built from a design worked out before any code, one phase per commit. The wire
contract is in [`docs/protocol.md`](docs/protocol.md) and the eight decision
records are in [`docs/adr/`](docs/adr/), each with an `## Amendment` section
recording what the build confirmed, corrected or measured.

## Running it

`make` on its own lists every target.

```sh
make up          # Postgres
make run         # one instance on :8080
# open http://localhost:8080 in two browsers, sign up as two users

make demo        # a whole transfer, two spuds, no browser
```

Past five browser tabs, use HTTPS. Each tab holds one connection open forever for
its Stream and a browser allows six per origin over HTTP/1.1, so the sixth starves
the rest — measured in [`docs/protocol.md`](docs/protocol.md). HTTP/2 multiplexes
them onto one connection, and no browser speaks cleartext h2c:

```sh
make cert        # self-signed localhost, once
make run-tls     # https://localhost:8443, HTTP/2 over ALPN
```

Two instances behind one entry point, which is where the design gets
interesting:

```sh
make up-cluster  # postgres, redis, nats, inst-a, inst-b, caddy
# http://localhost:8080  round-robins between them
# http://localhost:8081  inst-a directly
# http://localhost:8082  inst-b directly
```

A transfer is owned by whichever instance minted it; requests that land on the
other one are answered `307` to the owner.

## Testing it

```sh
make test           # everything that needs no broker
make test-short     # skips the 1 GB relay
make test-race      # under the race detector

make up-brokers     # Postgres, Redis, NATS, Kafka
make test-all       # plus the Postgres, distributed and four-bus suites
```

Every integration suite skips itself when its dependency is not configured, so a
clean checkout is green with nothing running.

## The CLI

`spud` is a client for a terminal: it proves the protocol is a protocol, and it
is the load generator for the measurements. Full reference in
[`docs/cli.md`](docs/cli.md).

```sh
go build -o spud ./cmd/spud

# two terminals, one transfer
mkdir -p inbox
./spud -email bea@example.com -password hunter2hunter2 -name bea -signup recv ./inbox
./spud -email ana@example.com -password hunter2hunter2 -name ana -signup send bea ./some-folder

# watch the control plane without a browser
./spud -email ana@example.com -password hunter2hunter2 watch

# ten thousand idle Streams
./spud -email load@example.com -password hunter2hunter2 -name loadbot -signup -streams 10000 load
```

Resume is on by default, in both directions and up to `-attempts` tries: an
interrupted upload is picked up from the position the relay reports on the control
plane, and an interrupted download reconnects with `Range: bytes=N-` for whatever
is not already on disk. `-attempts 1` makes any interruption terminal.

## Layout

```
cmd/server/            main, wiring, signal handling — the only importer of config
cmd/spud/              CLI client: watch, send, recv, load
internal/config/       env → typed config, imported by cmd/ only
internal/auth/         signup, login, sessions, argon2id, middleware
internal/postgres/     adapter for auth's store interfaces + schema.sql
internal/presence/     memory + redis TTL implementations
internal/bus/          Bus interface + memory, nats, redis, kafka
internal/mirror/       Redis read models: instance directory, Transfer metadata
internal/sse/          frame writing, Stream registry, shared heartbeat
internal/transfer/     Transfer, State, transitions — pure, no net/http
internal/relay/        rendezvous, multipart→zip, counting, resume
internal/httpapi/      handlers, routing, middleware, ownership redirect
internal/metrics/      Prometheus surface
internal/obs/          tracing
internal/webui/        the harness, embedded — files live in public/
```

## Phases

Each commit is one phase of the plan.

| # | Phase | Where to look |
|---|---|---|
| 0 | Skeleton, config, graceful shutdown | `internal/config`, `cmd/server/main.go` |
| 1 | Auth: argon2id, Postgres, sessions | `internal/auth`, `internal/postgres` |
| 2 | SSE control plane, Stream registry, presence | `internal/sse`, `internal/presence` |
| 3 | The Transfer state machine, pure | `internal/transfer/transfer.go` |
| 4 | Offer, accept, deny over HTTP | `internal/httpapi/transfers.go` |
| 5 | **The relay** | `internal/relay`, `internal/httpapi/data.go` |
| 6 | Progress | `internal/httpapi/progress.go` |
| 7 | **Distributed**: NATS, Redis, `307` ownership | `internal/mirror`, `internal/httpapi/ownership.go` |
| 8 | Redis and Kafka buses, measured | [`docs/bus-comparison.md`](docs/bus-comparison.md) |
| 9 | `spud` | `cmd/spud` |
| 10 | Ops: readyz, metrics, tracing, load test | [`docs/load-test.md`](docs/load-test.md) |
| 11 | Resume, both sides | `internal/relay/resume.go`, `cmd/spud/resume.go` |
| 12 | WebRTC finale | branch `webrtc`, [`docs/webrtc.md`](https://github.com/igor-dmscn/hot-potato-claude-impl/blob/webrtc/docs/webrtc.md) |

Phase 12 lives on its own branch. `main` is SSE-only, which is what the project
set out to build.

## Measured

Five things were measured rather than asserted.
[`docs/measurements.md`](docs/measurements.md) has the exact commands for each.

- **1 GB relayed** in 5.3 s with the heap going 625 KB → 1.2 MB and 1.4 MB
  allocated in total. Nothing accumulates.
- **10,000 idle SSE Streams**: 2 goroutines and 1 fd each, ~42 KiB of RSS each,
  3 GC collections at a 0.89 ms median pause.
- **SIGTERM with all 10,000 open**: exits in 229 ms. Without the drain hook that
  number is not "slow", it is "never" (Go issue #41344).
- **Bus latency**, paced p50: memory 6.6 µs, NATS 140 µs, Redis 145 µs, Kafka
  840 µs — reasoning in [`docs/bus-comparison.md`](docs/bus-comparison.md).
- **Cross-instance transfer** through two containers behind Caddy: 5 MB with
  matching checksums, the accept redirected to the owner and its body replayed.

`make measure` runs all of them.

## Documents

| | |
|---|---|
| [`docs/protocol.md`](docs/protocol.md) | the wire contract: endpoints, events, errors, limits |
| [`docs/glossary.md`](docs/glossary.md) | every noun, and the Go type that embodies it |
| [`docs/flows.md`](docs/flows.md) | how a Transfer happens, drawn |
| [`docs/cli.md`](docs/cli.md) | `spud`: every command and flag, with output |
| [`docs/measurements.md`](docs/measurements.md) | all five measurements and how to repeat them |
| [`docs/bus-comparison.md`](docs/bus-comparison.md) | four buses, measured, and three findings |
| [`docs/load-test.md`](docs/load-test.md) | ten thousand Streams, and the drain |
| [`docs/webrtc.md`](https://github.com/igor-dmscn/hot-potato-claude-impl/blob/webrtc/docs/webrtc.md) | what bypassing the server costs — **on the `webrtc` branch** |
| [`docs/adr/`](docs/adr/) | the eight decision records, with what the build found appended |
| [`CLAUDE.md`](CLAUDE.md) | the invariants and conventions, for anyone editing this |

## Configuration

Environment only, every variable prefixed `HP_`. See
[`.env.example`](.env.example) for the full list with defaults; nothing is loaded
from a file by the binary.
