# Ten thousand idle Streams

What one instance costs while holding ten thousand open SSE connections, and what
it costs to shut it down while they are open.

Idle is the interesting case. A Stream that is carrying events is doing work
somebody asked for; a Stream that is merely *open* is the cost of being connected
at all, and it is the cost that decides how many users an instance can hold.

## How it was run

```sh
# One instance, standalone: in-memory bus and presence, Postgres for identity.
HP_ADDR=127.0.0.1:8099 HP_SSE_HEARTBEAT=15s ./server &

# One user, ten thousand Streams.
spud -base http://127.0.0.1:8099 -email load@example.com -password ... \
     -streams 10000 load
```

- 12-core x86-64 Linux, 18 GB RAM, everything on loopback.
- `ulimit -n` was already 524288. Raise it before blaming Go: the failure mode of
  a low limit is `accept: too many open files`, which looks like the server
  refusing connections rather than the shell refusing to lend it file
  descriptors.
- Numbers scraped from `/metrics` — the Go and process collectors are there for
  exactly this.

## What it cost

| | Baseline | 10,000 Streams | Per Stream |
|---|---|---|---|
| `hp_streams_active` | 0 | 10,000 | — |
| `go_goroutines` | 14 | 20,014 | **2** |
| `process_open_fds` | 8 | 10,019 | **1** |
| `process_resident_memory_bytes` | 24.7 MB | 459.7 MB | **~42 KiB** |
| `go_memstats_heap_alloc_bytes` | — | 225.8 MB | ~22 KiB |
| `go_memstats_sys_bytes` | — | 447.0 MB | — |
| `go_threads` | — | 23 | — |

GC, across the whole run: **3 collections**, p50 pause **0.89 ms**, max **1.85 ms**.

### Two goroutines per Stream, not one

net/http gives a connection one goroutine, and the handler runs on it. The second
is the server's background read, started so the handler learns about a client
that goes away — which is precisely the mechanism `r.Context().Done()` depends
on. It is not waste; it is how disconnect detection works.

### One timer per Stream would not have been fine

The plan flagged this before any of it was written, and it is why heartbeats go
through `sse.Heartbeat`: one ticker for the whole instance, broadcasting by
closing and replacing a channel. Ten thousand `time.Ticker`s on a 15-second
interval would be ten thousand heap operations per beat, all to do the same thing
at the same moment.

The evidence is in `go_threads`: 23, unchanged in shape from baseline. A timer
storm shows up there first.

### 42 KiB per Stream, and where it goes

Roughly: 8 KiB of goroutine stack ×2, net/http's 4 KiB read and write buffers, and
a 32-event channel. Nothing in that list grows with what the Stream carries, which
is the property that matters — an idle Stream at hour six costs what it did at
second one.

At this rate an instance with 2 GB of RAM holds around 45,000 idle Streams before
memory is the binding constraint, and file descriptors will usually bind first.

## Shutting down with all of them open

This is the measurement worth having, because getting it wrong hangs forever:

```
draining, grace=15s
draining streams, streams=10000
SHUTDOWN_MS=229
```

**229 ms** from SIGTERM to exit, with ten thousand open SSE streams.

`Server.Shutdown` waits for open connections, and an SSE handler does not return
on its own — so without the drain hook this number is not "slow", it is
"never" (Go issue #41344). The hook broadcasts `server.draining` to every Stream
and closes them all *before* `Shutdown` is called; each pump flushes what is
queued and its handler returns.

The client counted **8,594** events across its 10,000 Streams during the drain.
That is not 10,000 because a Stream whose socket closes before the client has
read the frame simply loses it — which is the whole reason `retry: 3000` and
snapshot-on-connect exist. The Streams that missed the announcement reconnect on
the browser's own schedule and get a fresh snapshot; nothing is left waiting for
an explanation it will never receive.

## Reproducing it

```sh
docker compose up -d postgres
go build -o server ./cmd/server && go build -o spud ./cmd/spud
./server &
./spud -base http://localhost:8080 -email load@example.com -password hunter2hunter2 \
       -name loadbot -signup -streams 10000 load

curl -s localhost:8080/metrics | grep -E '^(go_goroutines|process_open_fds|process_resident_memory_bytes|hp_streams_active) '
```

Then `kill -TERM` the server and time it.
