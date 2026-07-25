# One bus interface, three implementations, on purpose

Broadcast sits behind a two-method interface — publish an event, subscribe to
the firehose — with implementations for in-memory (single instance), NATS
(default), Redis pub/sub and Kafka. Each instance subscribes to every event and
discards those addressed to Users it is not currently holding Streams for.

Three implementations of one interface is ordinarily exactly the speculative
generality worth deleting. It survives here because comparing them is an
explicit deliverable of a learning project, not a hedge against an unknown
future.

## Consequences

The interface is pinned to the lowest common denominator: at-most-once
delivery, no ordering guarantees across Users, and a single firehose rather
than per-User subjects. NATS subject wildcards and Redis `PSUBSCRIBE` patterns
could both narrow delivery server-side, but Kafka has no per-key subscribe, so
narrowing would break the swap. Correctness therefore rests on snapshots (ADR
0003), not on the bus.

Kafka is expected to fit badly, and is included to demonstrate why: every
instance needs every event, so every instance needs its own consumer group;
retained offsets are meaningless for ephemeral presence; and topic-per-User is
an anti-pattern. Progress events — the highest-volume traffic on the bus at
roughly four per second per active Transfer — are where that mismatch will be
felt most.

---

## Amendment — 2026-07-25, after the implementation

**Status: amended. The Kafka prediction was right and the mechanism was wrong.**

Four implementations, one conformance suite, green against all of them. The
prediction that "Kafka is expected to fit badly" held — but the specific mechanism
this record describes is not sufficient, and finding out changed the code twice.

### "Every instance needs its own consumer group" is not enough

The first implementation did exactly what this record says: one group per
instance, `hp-<instanceID>`. The fanout test failed immediately. **Members of a
Kafka consumer group divide the partitions between them**, so two subscriptions in
one process each received part of the firehose and neither received all of it —
which is what a consumer group is *for*, and the opposite of what this interface
promises.

The group has to be per **subscription**, not per instance. Every group created
here is abandoned seconds later holding offsets nobody will ever read. NATS, Redis
and the in-memory bus all fan out to every subscriber without being asked.

### A stable group name costs 45 seconds of silence

With a fixed group name, the second run of the latency suite received *nothing*
for 20 seconds and gave up. A new member joining a group whose previous member has
not been evicted waits out that member's `session.timeout.ms` — 45 s by default —
before the rebalance completes.

So the group name carries a per-process random suffix. **An instance that restarts
must not be recognised as the thing that just left**, which means the group cannot
identify the instance, which means it is not identifying anything. The anti-pattern
this record predicted is worse than predicted.

### Measured

Publish→deliver latency, 1000 events, medians of three runs. Two numbers each,
because one misleads: *paced* is one event at a time, each waited for — the
transport's round trip. *Burst* is 1000 back to back — mostly queueing.

| Bus | paced p50 | paced p99 | burst p50 | delivered |
|---|---|---|---|---|
| memory | 6.6 µs | 18 µs | 1.8 ms | 1000/1000 |
| NATS | 140 µs | 280 µs | 780 µs | 1000/1000 |
| Redis | 145 µs | 300 µs | **183 µs** | 1000/1000 |
| Kafka | 840 µs | 1.3 ms | 2.6 ms | 1000/1000 |

Redis wins the burst column because its `PUBLISH` waits for a reply and therefore
paces the publisher — a property of the *call*, not of the broker, and worth
knowing before quoting it at anybody.

And the number that flatters Kafka: **1000/1000 delivered, every run**, including
bursts where the in-memory bus at its production buffer dropped about a fifth. For
a workload that wanted durability and replay that would be the whole argument.
This one wants neither, and pays 6× NATS's latency to decline them.

### The interface survived unchanged

Nothing above required a change to `Publish` / `Subscribe` / `Close`. The lowest
common denominator this record chose deliberately — at-most-once, no ordering, one
firehose — turned out to be exactly the set of promises all four can keep. Full
write-up in `docs/bus-comparison.md`.
