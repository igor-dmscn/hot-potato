# Every measurement, and how to repeat it

Five things were measured rather than asserted. This is the index: what each one
shows, the exact commands, and where the analysis lives.

All numbers below are from one machine — **12-core x86-64 Linux, 18 GB RAM,
everything on loopback** — and none of them are capacity claims. They are here to
answer "is the property real", not "how big can it get".

| | Shows | Where |
|---|---|---|
| [1 GB relay](#1-a-gigabyte-through-a-flat-heap) | zero storage is real, not aspirational | `internal/httpapi/data_test.go` |
| [10,000 Streams](#2-ten-thousand-idle-streams) | what being connected costs | [`load-test.md`](load-test.md) |
| [Graceful drain](#3-shutting-down-with-all-of-them-open) | the drain hook is the difference between 229 ms and never | [`load-test.md`](load-test.md) |
| [Bus latency](#4-four-buses) | which broker, and why | [`bus-comparison.md`](bus-comparison.md) |
| [Cross-instance transfer](#5-two-instances-one-transfer) | the `307` carries a streaming body to its Owner | `internal/httpapi/distributed_test.go` |

Everything needed is in the repo. `docker compose up -d postgres redis nats kafka`
covers every dependency.

---

## 1. A gigabyte through a flat heap

The one measurement that proves the design's central claim. Memory per Transfer is
O(1) regardless of Payload size, or it is not zero-storage, it is
storage-you-happened-to-fit.

```sh
go test -run TestOneGigabyteWithAFlatHeap -v -timeout 300s ./internal/httpapi/
```

```
1 GB relayed; heap 625232 → 1246176 bytes, 1445216 total allocated
--- PASS: TestOneGigabyteWithAFlatHeap (5.33s)
```

**1 GB relayed in 5.3 s. Heap 625 KB → 1.2 MB. 1.4 MB allocated in total.**

The test hashes both ends and compares, so it is an integrity check as well as a
memory one, and it reads `runtime.MemStats` after a forced GC on either side. It
is skipped by `-short`.

Where the 1.4 MB goes: two 64 KiB copy buffers, net/http's per-connection
buffers, and the test's own client. Nothing scales with the Payload — which is
what "the server cannot hold what it is handed" has to mean to be worth saying.

## 2. Ten thousand idle Streams

Idle is the interesting case: a Stream that is carrying events is doing work
somebody asked for, while one that is merely *open* is the cost of being
connected, and that is what decides how many users an instance holds.

```sh
docker compose up -d postgres
go build -o server ./cmd/server && go build -o spud ./cmd/spud
HP_ADDR=127.0.0.1:8099 ./server &

./spud -base http://127.0.0.1:8099 -email load@example.com -password hunter2hunter2 \
       -name loadbot -signup -streams 10000 load &

curl -s localhost:8099/metrics | grep -E \
  '^(go_goroutines|process_open_fds|process_resident_memory_bytes|hp_streams_active|go_gc_duration_seconds) '
```

| | Baseline | 10,000 Streams | Per Stream |
|---|---|---|---|
| `go_goroutines` | 14 | 20,014 | **2** |
| `process_open_fds` | 8 | 10,019 | **1** |
| `process_resident_memory_bytes` | 24.7 MB | 459.7 MB | **~42 KiB** |
| `go_threads` | — | 23 | — |

GC across the whole run: **3 collections, 0.89 ms median pause, 1.85 ms max.**

Two goroutines per Stream, not one: net/http gives a connection one, and starts a
second for the background read that makes `r.Context().Done()` work. Full
reasoning in [`load-test.md`](load-test.md), including why heartbeats share one
ticker instead of holding ten thousand timers.

## 3. Shutting down with all of them open

The measurement most worth having, because getting it wrong does not look slow —
it looks like a hang.

```sh
# with the 10,000 streams from above still open
time kill -TERM $(pgrep -f 'server')
```

```
draining, grace=15s
draining streams, streams=10000
SHUTDOWN_MS=229
```

**229 ms** from SIGTERM to exit with ten thousand open SSE streams.

`Server.Shutdown` waits for open connections and an SSE handler never returns on
its own, so without the drain hook this number is not 229 ms, it is never (Go
issue #41344). The hook broadcasts `server.draining` and closes every Stream
*before* `Shutdown` is called.

The client counted 8,594 events across its 10,000 Streams during the drain, not
10,000: a Stream whose socket closes before the frame is read simply loses it,
which is what `retry: 3000` and snapshot-on-connect are for.

## 4. Four buses

```sh
docker compose up -d redis nats kafka

export HP_TEST_REDIS_URL=redis://localhost:6379/2
export HP_TEST_NATS_URL=nats://localhost:4222
export HP_TEST_KAFKA_BROKERS=localhost:29092

go test -v -run TestBusLatency ./internal/bus/
```

Publish→deliver latency, 1000 events, measured two ways because one number
misleads. **Paced** is one event at a time, each waited for: the transport's round
trip. **Burst** is 1000 back to back: mostly queueing.

| Bus | paced p50 | paced p99 | burst p50 | delivered |
|---|---|---|---|---|
| memory | **6.6 µs** | 18 µs | 1.8 ms | 1000/1000 |
| NATS | 140 µs | 280 µs | 780 µs | 1000/1000 |
| Redis | 145 µs | 300 µs | **183 µs** | 1000/1000 |
| Kafka | 840 µs | 1.3 ms | 2.6 ms | 1000/1000 |

Medians of three runs. Redis wins the burst column because its `PUBLISH` waits
for a server reply and therefore paces the publisher — a property of the *call*,
not of the broker.

The same conformance suite runs against all four:

```sh
go test -v -run 'TestBus(Delivers|FansOut|Unsubscribes)' ./internal/bus/
```

[`bus-comparison.md`](bus-comparison.md) has the three findings that came out of
it and the honest case for and against Kafka.

## 5. Two instances, one Transfer

That the `307` is a real mechanism and not a diagram.

**In one process, over real sockets** — two instances sharing a Redis and a NATS:

```sh
docker compose up -d postgres redis nats
export HP_TEST_REDIS_URL=redis://localhost:6379/1
export HP_TEST_NATS_URL=nats://localhost:4222
go test -v -run 'TestCrossInstance|TestWrongInstance|TestAStreamingUpload' ./internal/httpapi/
```

**In containers, through Caddy** — the full topology, with `curl` as the client:

```sh
docker compose up -d                    # postgres, redis, nats, inst-a, inst-b, caddy
# inst-a on :8081, inst-b on :8082, Caddy round-robins on :8080

# ana signs up on inst-a, bea on inst-b; ana offers, bea accepts against inst-b
# (307 → inst-a, body replayed), bea downloads from inst-b (307 → inst-a), ana
# uploads to inst-a.
```

Measured: **5 MB relayed cross-instance with matching SHA-256**, the accept
answered `202 after 1 redirect(s)`, and both parties' Streams — on *different*
instances — receiving the full event sequence over NATS:

```
--- ana's stream (inst-a) ---   --- bea's stream (inst-b) ---
  1 snapshot                      1 snapshot
  1 transfer.created              1 transfer.offered
  1 transfer.accepted             1 transfer.accepted
  1 transfer.ready                1 transfer.completed
  1 transfer.completed            2 user.online
  2 user.online
```

And the trap, pinned by its own test: a **streaming** upload's `307` is *not*
followed by `net/http`, because the body cannot be replayed. The client is handed
the redirect and has to repeat the request itself, which is what
[`spud` does](cli.md#across-two-instances).

---

## Running the whole suite

```sh
docker compose up -d postgres redis nats kafka
export HP_TEST_DATABASE_URL=postgres://hotpotato:hotpotato@localhost:5432/hotpotato
export HP_TEST_REDIS_URL=redis://localhost:6379/1
export HP_TEST_NATS_URL=nats://localhost:4222
export HP_TEST_KAFKA_BROKERS=localhost:29092

go test ./...            # ~40s, including the 1 GB relay
go test -short ./...     # ~20s, skipping it
go test -race ./...      # slower; the concurrency claims are worth it
```

Each integration suite skips itself when its dependency is not configured, so a
clean checkout with nothing running is green — and `-run 'TestBus'` with no
brokers proves the interface is not quietly shaped around one of them.
