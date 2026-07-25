# The protocol

The wire contract, in one place: the endpoints, the events, the error envelope and
the policy envelope. Comments in the code cite the sections here by name.

Vocabulary is in [`glossary.md`](glossary.md), the shapes are drawn in
[`flows.md`](flows.md), and the reasoning behind the choices is in
[`adr/`](adr/).

---

## HTTP surface

### Control plane

| Method | Path | Who | Result |
|---|---|---|---|
| `POST` | `/api/signup` | anon | `201`, session cookie |
| `POST` | `/api/login` | anon | `200`, session cookie |
| `POST` | `/api/logout` | user | `204` |
| `GET` | `/api/me` | user | `200`, self |
| `GET` | `/events` | user | SSE stream, `snapshot` first |
| `POST` | `/api/transfers` | user | `201 {id, expiresAt}` |
| `POST` | `/api/transfers/{id}/accept` | recipient | `202` |
| `POST` | `/api/transfers/{id}/deny` | recipient | `204` |
| `POST` | `/api/transfers/{id}/cancel` | either party | `204` |
| `POST` | `/api/transfers/{id}/signal` | either party | `204` — `webrtc` branch only |
| `GET` | `/healthz` | anon | `200` while the process is alive |
| `GET` | `/readyz` | anon | `200` iff every dependency answers, `503` while draining |
| `GET` | `/metrics` | anon | Prometheus |
| `GET` | `/` | anon | the harness, out of `embed.FS` |

### Data plane

| Method | Path | Who | Result |
|---|---|---|---|
| `GET` | `/d/{id}` | recipient | parks, then `200` + Payload, or `504` |
| `POST` | `/d/{id}` | sender | `204` when the chunk is relayed |

`POST /api/transfers` body:

```json
{"to":"u_7","name":"docs","kind":"folder","totalBytes":24117248,"entryCount":12}
```

Every `/api/transfers/{id}/*` and `/d/{id}` request is subject to the `307`
ownership redirect (ADR 0007).

### Rules that hold across the surface

- **`Content-Type: application/json` is required on every state-changing
  control-plane route**, including the ones with no body. Together with
  `SameSite=Lax` on the session cookie that is the CSRF defence: a cross-site form
  can only send `application/x-www-form-urlencoded`, `multipart/form-data` or
  `text/plain`, and cannot set `application/json` without a preflight the browser
  will refuse.
- The data plane is exempt, because it *is* multipart. `SameSite=Lax` is what keeps
  a cross-site form from carrying an identity there.
- Control-plane bodies are capped at 64 KiB and decoded strictly — an unknown
  field is a `400`, not something to ignore quietly.
- A Transfer ID is **not** an authorization token. The party's identity is verified
  on every endpoint, every time.

## Events

One Stream per tab, and each one costs a connection for as long as the tab is
open. A browser allows six per origin over HTTP/1.1, so the sixth tab starves the
origin: measured, `/healthz` answers in 4 ms with five Streams up and never
answers with six, and a seventh tab cannot load `/` either. The Stream is also
the liveness signal — it heartbeats, and `EventSource` reconnects on its own — so
a client has no reason to poll anything alongside it. Lifting the cap means
HTTP/2, and since no browser speaks h2c, that means TLS.

The wire format. Note the blank line terminating every frame, and the `:hb`
heartbeat comment every 15 s:

```
retry: 3000

id: 1
event: snapshot
data: {"self":{…},"instance":"inst-b","users":[…],"transfers":[…]}

:hb

id: 2
event: user.online
data: {"id":"u_7","displayName":"ana"}

```

| Event | To | Payload |
|---|---|---|
| `snapshot` | the opening Stream | `{self, streamId, instance, users[], transfers[]}` |
| `user.online` / `user.offline` | everyone | `{id, displayName}` |
| `transfer.created` | sender's tabs | the full Transfer |
| `transfer.offered` | recipient's tabs | the full Transfer |
| `transfer.accepted` | both | `{id, at, byStream}` — tabs where `byStream` is not theirs clear the prompt |
| `transfer.denied` | both | `{id, at}` |
| `transfer.ready` | sender's tabs | `{id, entry, offset}`, plus `resume: true` when picking up an interruption |
| `transfer.progress` | both | `{id, bytes, total, bytesPerSec}` |
| `transfer.completed` | both | `{id, bytes, durationMs}` |
| `transfer.failed` | both | `{id, reason, bytesRelayed}` |
| `transfer.canceled` | both | `{id, by}` |
| `transfer.signal` | the counterparty | `{id, from, signal}` — `webrtc` branch only |
| `server.draining` | every Stream | `{}` — reconnect elsewhere |

There is no `transfer.expired`: an offer nobody answered arrives as
`transfer.failed` with reason `offer_expired`.

`transfer.ready` carries a position on **every** invitation, including the first,
where the offset is 0. A Sender has no other way to learn that a Recipient
reconnected with a `Range` and rewound it (ADR 0002, 0008).

### Failure reasons

`sender_disconnected`, `recipient_disconnected`, `rendezvous_timeout`,
`offer_expired`, `payload_mismatch` (declared bytes ≠ delivered bytes),
`instance_draining`, `internal`.

Which *side* of the copy failed decides which of the first two is used, which is
why the relay tracks its read and write errors separately.

## Errors

One envelope, so a client has one shape to parse:

```json
{"error":"recipient_not_attached","message":"recipient has not opened the download yet"}
```

| Code | Typical status | |
|---|---|---|
| `unauthorized` | 401 | no session, or wrong credentials |
| `forbidden` | 403 | a party to something, but not this |
| `not_found` | 404 | no such Transfer, or an Owner that is gone |
| `illegal_state` | 409 | the state machine said no |
| `recipient_not_attached` | 409 | a Sender arrived before the Recipient parked |
| `already_attached` | 409 | a second GET, or a second POST while one is in flight |
| `rendezvous_timeout` | 504 | the Sender never arrived |
| `payload_too_large` | 413 | over the declared-size ceiling |
| `too_many_transfers` | 429 | a per-User limit |
| `rate_limited` | 429 | the offer rate, or failed logins |
| `draining` | 503 | this instance is shutting down |
| `bad_request` | 400 / 415 / 416 | malformed input, wrong content type, unservable range |
| `internal` | 500 / 502 | logged in full, told to the client in outline |

An unknown Transfer is `404` and somebody else's is `403`. The ID is 64 bits of
randomness, so there is nothing to enumerate and no reason to lie about which case
it is.

### Resume headers

| Header | Direction | |
|---|---|---|
| `X-Entry-Index` | request | which entry the Sender is resuming |
| `X-Entry-Offset` | request | how far into it |
| `X-Expected-Entry` | response | where the relay actually is, on a `409` or `502` |
| `X-Expected-Offset` | response | " |
| `Range: bytes=N-` | request | a Recipient picking up an interrupted download |
| `Content-Range` | response | on the `206` that answers it |

Only the open-ended single-range form is accepted, and only for a single file. A
folder's archive was being built as it streamed, so there is nothing to continue
from — that is a `416` (ADR 0004).

## Limits and timings

Every one of these is a config field rather than a literal, which is what lets the
presence and reaper tests run in milliseconds. Names and defaults in
[`../.env.example`](../.env.example).

| Knob | Default | Variable |
|---|---|---|
| Pending offer TTL | 60 s | `HP_OFFER_TTL` |
| Rendezvous wait | 30 s | `HP_RENDEZVOUS_WAIT` |
| Resume window | 30 s (0 disables) | `HP_RESUME_WINDOW` |
| Concurrent outbound Transfers per User | 3 | `HP_MAX_OUTBOUND` |
| Unanswered offers per Recipient | 10 | `HP_MAX_PENDING_INBOUND` |
| Max declared Payload | 10 GB | `HP_MAX_PAYLOAD_BYTES` |
| Max entries in a folder | 10,000 | `HP_MAX_ENTRIES` |
| Offer rate limit | 10 per minute per Sender | `HP_OFFER_RATE` |
| Failed logins | 10 per 15 min per IP+email | `HP_LOGIN_MAX_FAILURES` |
| SSE heartbeat | 15 s | `HP_SSE_HEARTBEAT` |
| Client reconnect hint (`retry:`) | 3000 ms | `HP_SSE_RETRY` |
| Per-write deadline | 10 s SSE / 30 s relay | `HP_SSE_WRITE_DEADLINE`, `HP_RELAY_WRITE_DEADLINE` |
| Progress cadence | 250 ms, suppressed when unchanged | `HP_PROGRESS_INTERVAL` |
| Presence grace before `user.offline` | 10 s | `HP_PRESENCE_GRACE` |
| Presence key TTL / refresh | 30 s / 10 s | `HP_PRESENCE_TTL`, `HP_PRESENCE_REFRESH` |
| Instance key TTL / refresh | 30 s / 10 s | `HP_INSTANCE_TTL`, `HP_INSTANCE_REFRESH` |
| Terminal Transfers in a snapshot | last 60 s | `HP_TERMINAL_WINDOW` |
| Read model TTL | 5 min | `HP_READ_MODEL_TTL` |
| Relay copy buffer | 64 KiB | `HP_RELAY_BUFFER` |
| Per-Stream send buffer | 32 events, then the Stream is closed | `HP_STREAM_BUFFER` |
| Reaper interval | 5 s | `HP_REAP_INTERVAL` |
| `/readyz` budget | 2 s for all checks | `HP_READY_TIMEOUT` |
| Shutdown grace | 15 s | `HP_SHUTDOWN_GRACE` |

### Trust boundaries, never simplified away

- The party's identity is verified on **every** Transfer endpoint.
- Declared filenames and every path component are sanitized before being echoed or
  written into an archive entry: `..`, absolute paths, drive letters, control
  characters and over-long names are all rejected or stripped.
- Declared sizes are enforced against bytes actually delivered. A mismatch fails
  the Transfer as `payload_mismatch` rather than completing it.
- Passwords are argon2id, m=64 MiB t=3 p=4, and an unknown email is verified
  against a decoy so the response clock cannot enumerate accounts.
- A password is bounded at 1024 bytes: argon2id on a 10 MB password is a denial of
  service the attacker pays nothing for.

## Metrics

| | |
|---|---|
| `hp_streams_active` | live SSE Streams on this instance |
| `hp_transfers_active` | Transfers this instance owns |
| `hp_rendezvous_parked` | Recipients waiting for a Sender |
| `hp_transfers_total{state}` | Transfers that reached each state |
| `hp_relay_bytes_total` | Payload bytes relayed — flat at zero means none passed through |
| `hp_relay_throughput_bytes` | bytes per second achieved by a completed relay |
| `hp_bus_publish_seconds` | time spent in `Bus.Publish` |
| `hp_rendezvous_wait_seconds` | how long a parked Recipient waited |

Plus the Go and process collectors, which is where a load test reads goroutines,
file descriptors, RSS and GC pause ([`load-test.md`](load-test.md)).
