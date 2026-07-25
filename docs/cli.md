# `spud`

A Hot Potato client for a terminal. It exists to prove the protocol is a
protocol — that nothing in it depends on being a browser — and to be the load
generator for the phase 10 measurements.

```sh
go build -o spud ./cmd/spud
```

## Commands

| | |
|---|---|
| `spud watch` | print control-plane events as they arrive |
| `spud send <to> <path>` | offer and stream a file or directory |
| `spud recv <dir>` | accept the next offer and write it to `dir` |
| `spud load` | open `-streams` idle Streams and hold them |

`<to>` may be a user ID, a display name or an email; `spud` resolves it from the
online list in its own snapshot, and tells you who *is* online when it cannot.

## Flags

| Flag | Default | |
|---|---|---|
| `-base` | `$SPUD_BASE`, else `http://localhost:8080` | server base URL |
| `-email` | `$SPUD_EMAIL` | required |
| `-password` | `$SPUD_PASSWORD` | required |
| `-signup` | `false` | create the account instead of logging in |
| `-name` | `spud` | display name, when signing up |
| `-attempts` | `3` | how many times to pick up an interrupted transfer; `1` disables resume |
| `-streams` | `100` | idle Streams for `load` |
| `-timeout` | `0` | give up after this long; `0` waits forever |

There is no client-wide HTTP timeout, ever: an SSE stream and a multi-gigabyte
upload both outlive any value worth setting.

## A transfer, end to end

Two terminals, against a server on `localhost:8080`:

```sh
# terminal 1 — the Recipient. Waits for an offer, accepts it, writes it to ./inbox.
mkdir -p inbox
./spud -email bea@example.com -password hunter2hunter2 -name bea -signup recv ./inbox

# terminal 2 — the Sender.
./spud -email ana@example.com -password hunter2hunter2 -name ana -signup send bea ./some-folder
```

```
sending some-folder (folder, 4.8 MiB, 12 file(s)) to bea
offered inst-local.7f3a9c2e51b4d8a6 — waiting for the recipient
done: 4.8 MiB in 41ms (117.3 MiB/s)
```

A folder arrives as `<name>.zip`, with entry names `"<folder>/<path within it>"` —
the same archive a browser produces, because that is exactly what
`webkitRelativePath` gives it.

Ordering is not optional: the Recipient's `GET` parks first and the server then
emits `transfer.ready`. A `POST` before that is answered `409
recipient_not_attached` (ADR 0002), and `spud` waits rather than guessing.

## Watching the control plane

```sh
./spud -email ana@example.com -password hunter2hunter2 watch
```

```
s_FMKRZEECNM5DIINQIOEWQIH27K on inst-local as ana (2 online, 0 transfers)
user.online          {"id":"u_N3NQ...","displayName":"bea"}
transfer.created     {"id":"inst-local.234118cc...","state":"pending",...}
transfer.accepted    {"at":"2026-07-25T00:41:12Z","byStream":"s_E3DK...","id":"inst-local.2341..."}
transfer.ready       {"entry":0,"id":"inst-local.2341...","offset":0}
transfer.progress    {"bytes":3145728,"bytesPerSec":128974848,"id":"...","total":5242880}
transfer.completed   {"bytes":5242880,"durationMs":41,"id":"inst-local.2341..."}
```

Useful for watching what the browser sees without a browser, and for confirming
that the SSE parser is thirty lines: a `bufio.SplitFunc` that splits on the blank
line terminating each frame.

## Resume

Both directions, on by default, up to `-attempts` tries.

**An interrupted upload.** The relay keeps the Recipient's response open and
holds the archive position, then invites the Sender back and says where from:

```
sending report.bin (file, 1.0 MiB, 1 file(s)) to bea
offered inst-local.219b5c6ab1e6af5a — waiting for the recipient
interrupted after entry 0 offset 0: 502: reading from the sender: connection reset by peer
  — waiting for the relay to invite us back
resuming from entry 0 offset 399853 (683.7 KiB to go)
interrupted after entry 0 offset 399853: 502: ... — waiting for the relay to invite us back
resuming from entry 0 offset 799706 (293.3 KiB to go)
done: 1.0 MiB in 12ms (89.1 MiB/s)
```

The position comes from `transfer.ready` on the control plane, not from the
failed response. A Sender whose upload dies usually never sees its own response —
`net/http` reports the broken request body instead — so the control plane is the
only reliable place to learn it. Only entries after the position are re-read, and
a half-delivered one is `Seek`ed into.

**An interrupted download.** The partial file stays on disk and is the resume
state; `spud` reconnects with `Range: bytes=N-` for what it does not have:

```
offered report.bin (file, 900.0 KiB) from u_ROCJ7WBXIUDHZL7PBAKKCLGI2F
download interrupted with 293.0 KiB on disk: relay ended early: unexpected EOF
  — reconnecting from there
wrote ./inbox/report.bin (878.9 KiB) in 9ms (99.6 MiB/s)
```

`N` is what is on disk, not what the server counted as relayed. The Owner counts
bytes it wrote *into a socket*, which after a disconnection exceeds what came out
of the other end by whatever was in flight — about 98 KB on loopback. Only this
end knows what it actually holds, which is why every resumable download protocol
works this way round.

A **folder** cannot be resumed into a new response: the zip was being built as it
streamed, and that stream went with the socket. The server answers `416` and
`spud` says what is on disk and stops (ADR 0004).

`-attempts 1` restores the phase 5 behaviour, where any interruption is terminal:

```
spud: gave up after 1 attempt(s) at entry 0 offset 0: 502: reading from the sender: ...
```

## Across two instances

`spud` follows the `307` ownership redirect itself, and has to: `net/http` will
not replay a streaming body across one, so the client rebuilds the multipart body
from disk and repeats the request against the Owner (ADR 0007).

```sh
docker compose up -d                       # two instances behind Caddy
./spud -base http://localhost:8081 ... recv ./inbox    # on inst-a
./spud -base http://localhost:8082 ... send bea ./file # on inst-b, redirected to inst-a
```

Nothing in the output says a redirect happened. That is the point.

## Load

```sh
./spud -email load@example.com -password hunter2hunter2 -name loadbot -signup \
       -streams 10000 load
```

```
2s  open=10000 failed=0 events=1
4s  open=10000 failed=0 events=1
^C
closed 10000 streams after 21s
```

One User holding ten thousand Streams, which is the shape
[`load-test.md`](load-test.md) measures. A Stream torn down by `spud`'s own
context is not counted as a failure — being told to stop is not a failure, and
counting it as one would make the exit code depend on how many were
mid-handshake.
