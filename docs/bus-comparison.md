# Four buses behind one two-method interface

`bus.Bus` is `Publish`, `Subscribe`, `Close`. Four implementations satisfy it,
and one conformance suite runs against all of them
(`internal/bus/conformance_test.go`). This is what each one costs.

Three implementations of one interface is normally exactly the speculative
generality worth deleting. It survives here because comparing them is the point
(ADR 0006) — and because the comparison produced three findings that changed the
code.

---

## What was measured

`go test -run TestBusLatency ./internal/bus/`, three runs, medians below.

- **Hardware**: 12-core x86-64 Linux, 18 GB RAM, everything on loopback.
- **Brokers**: `docker compose up redis nats kafka` — Redis 7-alpine, NATS
  2-alpine, Kafka 3.9.0 in KRaft mode, single node, one partition,
  `GROUP_INITIAL_REBALANCE_DELAY_MS=0`.
- **Payload**: an `Event` carrying one integer, JSON-encoded — the shape of a
  `transfer.progress`, which is the highest-volume traffic on this bus at roughly
  four per second per active Transfer.
- **Clock**: publisher and subscriber are in one process, so the send timestamp
  and the receive timestamp come from the same monotonic clock. No cross-host
  clock skew is involved and none is being hidden.
- **Subscriber buffer**: 4096, well above the burst size, so what is measured is
  latency rather than each bus's overflow policy.

Two measurements, because one number would have been misleading:

| | What it is | What it tells you |
|---|---|---|
| **paced** | publish one event, wait for it, repeat ×1000 | the transport's round trip |
| **burst** | publish 1000 back to back, then read | queueing, dominated by whether `Publish` waits for anything |

## The numbers

| Bus | paced p50 | paced p99 | burst p50 | burst p99 | delivered |
|---|---|---|---|---|---|
| memory | **6.6 µs** | 18 µs | 1.8 ms | 3.4 ms | 1000/1000 |
| NATS | 140 µs | 280 µs | 780 µs | 940 µs | 1000/1000 |
| Redis | 145 µs | 300 µs | **183 µs** | 300 µs | 1000/1000 |
| Kafka | 840 µs | 1.3 ms | 2.6 ms | 3.3 ms | 1000/1000 |

Read the paced column for "how quickly does one event arrive" and the burst
column for "what happens under load".

### Why Redis wins the burst column and loses the paced one

Redis's `PUBLISH` is a command that waits for the server's reply, so the
publisher cannot outrun the subscriber: the burst paces itself and every event
is delivered promptly. NATS and Kafka publishes are fire-and-forget, so a
thousand events land in front of the subscriber at once and the measured
"latency" is the depth of that pile, not the transport.

That is a property of the *call*, not of the broker's speed. It flatters Redis in
a burst and would flatter it much less at a rate where the round trip becomes the
bottleneck. Worth knowing before quoting the burst number at anybody.

### Why the in-memory bus is 20× faster and still not the answer

6.6 µs is a channel send. It is also confined to one process, which makes it
correct for exactly one deployment: a single instance. And at the production
buffer of 32–256 it dropped roughly a fifth of a 1000-event burst — at-most-once
working precisely as designed, healed by the next snapshot (ADR 0003), and a
useful reminder that the fastest implementation is the one that discards the most.

---

## Three things the conformance suite found

**1. Kafka consumer groups divide the firehose instead of duplicating it.**

The first version used one group per instance, `hp-<instanceID>`, exactly as
planned. The fanout test failed immediately: two subscriptions in one process
joined the same group, Kafka handed the single partition to one of them, and the
other received nothing. That is what a consumer group is *for* — dividing work
between members — and it is the opposite of what this interface promises.

The fix is a group per subscription. Every group created here is abandoned
seconds later, holding offsets nobody will ever read. NATS, Redis and the
in-memory bus all fan out to every subscriber without being asked.

**2. A stable Kafka group name costs 45 seconds of silence on restart.**

With a fixed group name, the second run of the latency suite received *nothing*
for 20 seconds and gave up. A new member joining a group whose previous member
has not been evicted yet waits out that member's `session.timeout.ms` — 45
seconds by default — before the rebalance completes.

So the group name carries a per-process random suffix. An instance that restarts
must not be recognised as the thing that just left, which means the group cannot
identify the instance, which means it is not identifying anything.

**3. Kafka needs a warm-up the others do not.**

An event published immediately after `Subscribe` returns is reliably lost with
Kafka: joining the group takes a moment. The conformance suite republishes on a
250 ms cadence until something arrives, and that retry loop exists solely for
Kafka. First delivery after subscribing: ~1.3 s for Kafka, ~0 for the rest.

This is not a bug in either place. The interface promises at-most-once with no
replay, and this is what at-most-once looks like at a subscription boundary. It
is also why nothing in this system decides anything from the bus: correctness
rests on snapshots (ADR 0003).

---

## What each one is actually for

**memory** — the default for a single instance, and the only one that needs no
service running. Nothing to operate, nothing to configure, and no path to a
second instance.

**NATS** — the default for more than one. Fire-and-forget pub/sub over subjects
is exactly this problem, the client reconnects on its own, and the operational
surface is one binary with no storage. 140 µs paced, and the burst column is a
queueing artefact of the async publish rather than a limit.

**Redis** — the pragmatic choice, because Redis is *already* a hard dependency
for presence and the Transfer read model (ADR 0005). Choosing it removes a
service rather than adding one. Its pub/sub has no persistence, no replay and no
consumer groups, which is a precise match for a contract that promises none of
those. The self-pacing publish is a genuine advantage under burst.

**Kafka** — the wrong tool, included to demonstrate why, and the demonstration is
concrete rather than rhetorical:

- Every instance needs every event, so every instance needs its own consumer
  group; the group machinery is pure overhead here.
- The group name cannot identify the instance without inheriting a 45-second
  rebalance stall on restart.
- Retained offsets are meaningless for events that are worthless a second after
  they are published. `DisableAutoCommit` is on because there is nothing worth
  committing.
- A rebalance pauses consumption, on a path that must never pause.
- Topic-per-User would be the natural way to narrow delivery, and is an
  anti-pattern at any real number of users.

And the number that flatters it: **840 µs paced, 2.6 ms burst, 1000/1000
delivered every run**. Kafka lost nothing, ever, including in the burst that made
the in-memory bus drop a fifth of its events. For a workload that wanted
durability and replay that would be the whole argument. This workload wants
neither. It is 6× slower than NATS at the thing this system actually does, in
exchange for guarantees the design explicitly declines.

---

## Running it

```sh
docker compose up -d redis nats kafka

export HP_TEST_REDIS_URL=redis://localhost:6379/2
export HP_TEST_NATS_URL=nats://localhost:4222
export HP_TEST_KAFKA_BROKERS=localhost:29092

go test -v -run 'TestBus' ./internal/bus/
```

Each implementation skips itself when its broker is not configured, so the suite
is green with none of them running — and the same suite is what proves the
interface is not quietly shaped around one of them.

Select an implementation at runtime with `HP_BUS=memory|nats|redis|kafka`.
