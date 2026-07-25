# Decision records

The decision records this implementation was built against, kept here so the
repo reads on its own.

**The original text of each record is unchanged.** Everything the implementation
found is in an `## Amendment` section at the end, dated, with a status line. That
is deliberate: a decision record is a record of what was decided and why at the
time, and editing one to look prescient afterwards destroys the only thing it is
for. Where the build proved a record wrong, the amendment says so and says what
replaced it.

| # | Decision | Status after the build |
|---|---|---|
| [0001](0001-two-planes-zero-storage.md) | Two planes, and no stored bytes | **confirmed** — 1 GB relayed, 1.4 MB allocated in total |
| [0002](0002-recipient-first-rendezvous.md) | The Recipient waits, and is told when to expect bytes | **confirmed** — and WebRTC wanted the same ordering |
| [0003](0003-snapshot-on-connect.md) | Snapshot on connect; `Last-Event-ID` ignored | **confirmed** — load-bearing in three places it was not written for |
| [0004](0004-server-side-zip-raw-single-file.md) | Folders zipped in flight; single files untouched | **corrected** — Go's `Part.FileName()` discards the relative path |
| [0005](0005-three-storage-roles.md) | Postgres for identity, Redis for presence, bus for fanout | **amended** — presence keys are per (User, instance), and Redis has a third role |
| [0006](0006-swappable-bus-three-implementations.md) | One bus interface, three implementations | **amended** — the Kafka prediction was right, the mechanism was not |
| [0007](0007-transfer-owned-by-one-instance.md) | Each Transfer owned by one instance; everyone else redirects | **confirmed** — verified across two containers |
| [0008](0008-resume-in-order-only.md) | Terminal failures first, in-order resume later, parallel never | **implemented** — both directions, three findings |

## The four that changed code

Worth reading the amendments to these even if you know the design:

- **0004** — `multipart.Part.FileName()` returns `filepath.Base` per RFC 7578 §4.2,
  so a folder's structure is silently lost if you use the obvious API. The whole
  reason server-side zipping was chosen, absent, with a passing smoke test.
- **0005** — one presence key per User breaks for a User with tabs on two
  instances: the second SET overwrites the first, and the first instance to lose
  its last Stream deletes a claim that is still true.
- **0006** — members of a Kafka consumer group *divide* the partitions, so a group
  per instance gives each subscription part of the firehose. It has to be a group
  per subscription, and the group name cannot identify the instance without
  inheriting a 45-second rebalance stall on restart.
- **0008** — the Owner's byte count is bytes written into a socket, ~98 KB more
  than the Recipient received. A Recipient's `Range` must carry its own count.

## Where the evidence is

| | |
|---|---|
| [`../protocol.md`](../protocol.md) | the wire contract these decisions produced |
| [`../measurements.md`](../measurements.md) | all five measurements and how to repeat them |
| [`../bus-comparison.md`](../bus-comparison.md) | four buses, measured, with the reasoning |
| [`../load-test.md`](../load-test.md) | ten thousand Streams, and the drain |
| [`webrtc.md`](https://github.com/igor-dmscn/hot-potato-claude-impl/blob/webrtc/docs/webrtc.md) | what bypassing the server costs — on the `webrtc` branch |
| [`../flows.md`](../flows.md) | the decisions above, drawn |
