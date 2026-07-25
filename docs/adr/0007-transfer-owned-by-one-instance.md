# Each Transfer is owned by one instance; everyone else redirects to it

A rendezvous holds a live `io.Reader` and a live `http.ResponseWriter`. Neither
can be serialised, shared or moved, so a Transfer's bytes must pass through
exactly one process. Its ID therefore embeds its Owner (`inst-b.7f3a9c…`), and
any instance receiving a request for a Transfer it does not own answers `307
Temporary Redirect` to the Owner. `307` specifically, because it preserves the
method and body — `302` would silently turn a POST into a GET.

The result is an asymmetry worth stating plainly: **the control plane scales by
fanout, the data plane scales by affinity.**

## Considered Options

Kubernetes `sessionAffinity: ClientIP` was rejected: it is documented not to
survive an ingress controller, which masks the original client IP, and
anything finer than client IP needs an L7 proxy that understands the protocol.
An application-level redirect keyed on the entity itself needs no cooperation
from the load balancer at all.

Holding Transfer state in Redis so any instance could serve any request was
rejected because it only helps the control plane — the live readers and writers
still cannot move — while adding distributed compare-and-swap to the accept
race that an in-process mutex already handles correctly.

## Consequences

Redirects apply to acceptance and denial too, not just to byte transport,
because those mutate state that lives in the Owner's memory. The accept race
between a User's several tabs stays a single-process compare-and-swap.

Snapshots need Transfers this User is party to, which may be owned by other
instances. The Owner's memory is therefore the write model, and a
TTL'd Redis mirror of Transfer metadata is a read model used only to build
snapshots.

---

## Amendment — 2026-07-25, after the implementation

**Status: confirmed, and verified against two real containers.**

Measured on the compose topology — two instances behind Caddy, round-robin, with
the browser-reachable URLs each instance advertises for itself: **5 MB relayed
cross-instance with matching SHA-256**, the accept answered `202 after 1
redirect(s)`, and both parties' Streams — on *different* instances — receiving the
full event sequence over NATS.

Three things the implementation added or corrected.

### A `307` will not carry a streaming body, so the client must

`net/http` follows a `307` only when it can replay the request body. A streaming
upload has no `GetBody`, so the client is handed the redirect and has to repeat the
request itself. This record says "`307` preserves the method and body", which is
true of the protocol and not of the client library.

`cmd/spud` therefore rebuilds the multipart body from disk and re-issues against
the Owner, with a fixed boundary so the retry declares the Content-Type it is
actually sending. There is a test that fails if that stops working
(`TestAStreamingUploadDoesNotFollowIts307`, `TestHonours307WithAStreamingBody`).

A browser's `fetch` with a `FormData` body *does* replay it, so the harness needs
nothing.

### The accepting `streamId` cannot be verified by the Owner

`transfer.accepted` carries the winning `byStream` so the Recipient's other tabs
can clear their prompt. The first implementation checked that the Stream belonged
to the caller — and the cross-instance test failed immediately.

The Recipient's Stream lives on whichever instance they are attached to, and the
accept has been redirected to the instance that owns the *Transfer* — usually a
different one, which has never heard of that Stream. There is no way for the Owner
to verify it and no need: it is a hint for the caller's own tabs, not an
authorization input, and the worst a wrong value achieves is failing to clear one
of the caller's own prompts. Only its length is bounded, because it is echoed into
an event.

This is the record's "the control plane scales by fanout, the data plane by
affinity" showing up somewhere unexpected: Stream identity is fanout-shaped, and
Transfer identity is affinity-shaped, and an API that mixes them has to pick.

### An unreachable Owner is a `404`

An instance that is gone takes its Transfers with it — this record says so, and the
implementation makes it explicit rather than a timeout: the directory lookup fails,
and the request is answered `404` with "the instance holding that transfer is
gone". A clean shutdown withdraws its directory entry immediately rather than
leaving requests redirected at a dead process for the TTL.
