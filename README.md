# Hot Potato

A Go backend where signed-in users see who else is online and stream a file or
folder directly to one of them. Payload bytes pass through the server as a live
stream and are never written to disk; every state change is pushed to browsers
over Server-Sent Events.

The name is the invariant: the server cannot hold what it is handed.

This is an implementation of the design and plan in
[`../learning-sse-file-transfer`](../learning-sse-file-transfer) —
`DESIGN.md` is the spec, `IMPLEMENTATION-PLAN.md` is the build, and `docs/adr/`
there is the reasoning.

## Running it

```sh
docker compose up -d postgres
go run ./cmd/server
# open http://localhost:8080 in two browsers, sign up as two users
```

Two instances behind one entry point, which is where the design gets
interesting:

```sh
docker compose up            # postgres, redis, nats, inst-a, inst-b, caddy
# http://localhost:8080  round-robins between them
# http://localhost:8081  inst-a directly
# http://localhost:8082  inst-b directly
```

A transfer is owned by whichever instance minted it; requests that land on the
other one are answered `307` to the owner.

## Testing it

```sh
go test ./...                # everything that needs no broker
go test -short ./...         # skips the 1 GB relay

docker compose up -d postgres redis nats kafka
export HP_TEST_DATABASE_URL=postgres://hotpotato:hotpotato@localhost:5432/hotpotato
export HP_TEST_REDIS_URL=redis://localhost:6379/1
export HP_TEST_NATS_URL=nats://localhost:4222
export HP_TEST_KAFKA_BROKERS=localhost:29092
go test ./...                # plus Postgres, the distributed suite, all four buses
```

Every integration suite skips itself when its dependency is not configured, so a
clean checkout is green with nothing running.

## The CLI

```sh
go build -o spud ./cmd/spud
./spud -email ana@example.com -password ... -signup -name ana watch
./spud -email ana@example.com -password ... send bea ./some-folder
./spud -email bea@example.com -password ... recv ./inbox
./spud -email load@example.com -password ... -streams 10000 load
```

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
| 11 | Resume, both sides | `internal/relay/resume.go` |
| 12 | WebRTC finale | branch `webrtc`, [`docs/webrtc.md`](docs/webrtc.md) |

Phase 12 lives on its own branch. `main` is SSE-only, which is what the project
set out to build.

## Measured

- **1 GB relayed** in 5.3s with the heap going 625 KB → 1.2 MB and 1.4 MB
  allocated in total. Nothing accumulates.
- **10,000 idle SSE Streams**: 2 goroutines and 1 fd each, ~42 KiB of RSS each,
  3 GC collections at a 0.89 ms median pause.
- **SIGTERM with all 10,000 open**: exits in 229 ms. Without the drain hook that
  number is not "slow", it is "never" (Go issue #41344).
- **Bus latency** paced p50: memory 6.6 µs, NATS 140 µs, Redis 145 µs, Kafka
  840 µs — with the reasoning in `docs/bus-comparison.md`.

## Configuration

Environment only, every variable prefixed `HP_`. See
[`.env.example`](.env.example) for the full list with defaults; nothing is loaded
from a file by the binary.
