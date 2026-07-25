# The Recipient waits, and is told when to expect bytes

After an acceptance, the Sender's POST and the Recipient's GET arrive from
different machines in unpredictable order, and one of them must wait. We chose
a **Recipient-first** rendezvous: the Recipient's GET parks in its handler, the
control plane then emits `transfer.ready` to the Sender, and only then does the
Sender POST. A POST that arrives before the Recipient has attached is rejected
with `409 recipient_not_attached` rather than parked.

## Considered Options

A symmetric rendezvous — whichever side arrives first parks until the other
appears — is more forgiving of client ordering and easier to exercise from
tests in either direction. It was rejected because failure attribution gets
muddier when either party may be the one blocked, and because deterministic
ordering makes the whole data plane easier to reason about.

## Consequences

Clients must obey the sequence; the protocol is no longer order-independent,
and the required ordering has to be documented for whoever writes the frontend.

The Recipient's GET must withhold its status line and headers until the Sender
attaches. An HTTP status cannot be retracted, so committing `200` on arrival
would leave no way to report a Sender that never shows up; parking first
permits a truthful `504` instead.

---

## Amendment — 2026-07-25, after the implementation

**Status: confirmed. Two additions.**

The `504` this record exists to make possible is real and tested: a parked
Recipient whose Sender never arrives is answered `504 rendezvous_timeout` and both
parties get `transfer.failed{rendezvous_timeout}` (`TestRendezvousTimeout`). It
works only because nothing is written when the Recipient arrives, exactly as
argued above.

**1. `transfer.ready` carries a position, not just the fact.**

The record describes `transfer.ready` as the signal to start. Resume (ADR 0008)
made that insufficient: a Recipient reconnecting with `Range: bytes=N-` rewinds
where the Sender must start, and the Sender has no other way to learn it. So the
event carries `{id, entry, offset}` — on the *first* invitation as well, where the
offset is simply 0. "Start here" turned out to be a better instruction than
"start", and it costs one field.

**2. A refused request must not spend the Recipient's one attach.**

An early version of `GET /d/{id}` validated the `Range` header *after* calling
`AttachRecipient`. A folder asking for a non-zero range got its `416`, correctly —
and left the Transfer with `RecipientAttached` set and nobody attached, so the
retry that would have worked was answered `409 already_attached`. Anything that
can refuse a request now runs before anything is claimed.

**3. WebRTC wanted the same rule.**

Phase 12 replaced the data plane with an `RTCDataChannel` and reused
`AttachRecipient` and `AttachSender` verbatim, including this record's ordering: a
peer signalling *is* a peer attaching, and the Recipient is the one who signals
first because it is the one answering the offer. A rule adopted for HTTP's benefit
turned out to be the rule the other transport wanted too, which is the best
evidence available that it was the right rule.
