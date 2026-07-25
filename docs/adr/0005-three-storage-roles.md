# Postgres for identity, Redis for presence, bus for fanout

State is divided by lifetime, not by feature. **Postgres** holds Users and
sessions — the only durable data, and shared by every instance. **Redis** holds
Presence as one key per online User with a 30-second TTL, refreshed while any
of that User's Streams is alive. The **event bus** carries broadcasts and
stores nothing.

## Considered Options

Presence as a Postgres row heartbeated every 15 seconds works and needs no
third service, but it puts a periodic write per online User on the durable
store — the first thing anyone moves to Redis once load arrives.

Deriving Presence purely from bus events, with periodic re-announcement so a
booting instance learns the world and drift heals, needs no Redis at all. It
was rejected because it means hand-rolling gossip and sweep logic that a TTL
provides for free.

## Consequences

A hard-killed instance requires no cleanup: its Presence keys simply expire and
its Users disappear from everyone's list within the TTL window. No sweeper, no
tombstones, no liveness table.

Redis is a hard dependency even when the bus is NATS or Kafka. SQLite, chosen
earlier for identity, was dropped once horizontal scaling entered scope: two
instances with two database files means signing up on one and being a stranger
to the other.

---

## Amendment — 2026-07-25, after the implementation

**Status: amended. Presence is keyed by (User, instance), and Redis has a third
role.**

### One key per User is not enough

This record, and the design it belongs to, describe Presence as one key per
online User with the instance recorded in the value:

```
SET presence:<userID> {name,instance} EX 30
```

That breaks for a User with tabs on two instances, which is the ordinary case the
moment there is more than one instance. Both write the same key, so the second
overwrites the first; then whichever instance loses its last Stream first deletes
a claim that is still true, and the User disappears from everyone's list while
still being connected.

The implementation keys on **both**:

```
SET presence:<userID>:<instance> {id,displayName} EX 30
```

A User's Presence is then derived from whether *any* claim survives. Withdrawing a
claim is followed by a `SCAN presence:<userID>:*` before announcing anybody
offline, and if that scan fails nothing is announced — announcing on an unreadable
store would remove a User who may still be there, and their key expires on its own
if they are not.

Everything this record claims about TTLs still holds, and holds better: a
hard-killed instance's claims expire without cleanup, and a User connected
elsewhere is unaffected by it.

### Redis has a third role: the instance directory

The record lists two Redis roles — Presence and the Transfer read model. ADR 0007's
`307` needs a third: **instance ID → the base URL that reaches it**, so that any
instance can redirect to an Owner it has never spoken to.

```
SET instance:<id> <externalURL> EX 30      # refreshed every 10s, deleted on shutdown
```

Same shape as the others: self-expiring, one `GET` to read, no sweeper, no
tombstones. Withdrawing it on a clean shutdown matters more than it sounds —
without it, requests are redirected to a process that has gone until the TTL runs
out.

All three live in `internal/mirror`, named for what they are: mirrors of state
whose authoritative copy is in some instance's memory, never the basis of a
decision.

### Measured

Redis pub/sub, as a bus, came out at **145 µs paced p50 / 183 µs under burst** —
the fastest of the four over a network, because `PUBLISH` waits for a server reply
and therefore paces the publisher (`docs/bus-comparison.md`). Which makes the
argument for Redis stronger than this record makes it: it is already a hard
dependency here, so choosing it as the bus removes a service rather than adding
one.
