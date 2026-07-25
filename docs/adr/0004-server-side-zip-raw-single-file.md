# Folders are zipped in flight; single files pass through untouched

Senders upload `multipart/form-data`. When the Payload's declared `Kind` is a
single file, the server relays that part's bytes verbatim, with a real
`Content-Length` and the original filename. When it is a folder, the parts are
streamed into a **zip archive using the `Store` method**, chunked, named
`<folder>.zip`.

The decision comes from the *declared* `Kind`, not from inspecting the parts:
`multipart.Reader.NextPart()` discards the previous part's unread body, so
looking ahead to count parts would destroy the first one. The parts are
validated against the declaration instead — a `file` followed by a second part,
or a total that disagrees with `TotalBytes`, fails as `payload_mismatch`.

## Considered Options

`archive/tar` was the first choice and was rejected on a verified fact: tar
demands each entry's byte count *before* its bytes (`WriteHeader(Size: 0)`
followed by a write fails with `archive/tar: write too long`), and
`multipart/form-data` parts carry no per-part length. Tar would therefore have
required the Sender to declare a per-entry manifest as the first field.
`archive/zip` writes to a non-seekable `io.Writer` and records sizes in a data
descriptor after each entry (flag bit `0x0008`), so it needs nothing up front.

`Store` rather than `Deflate`: compression would burn CPU per byte on the one
machine in the system that should stay a dumb pipe, and the payoff is a
guess about data the server never sees.

Keeping the server format-agnostic (client archives, server relays one opaque
stream) is purer and would leave room for end-to-end encryption, but building an
archive in the browser means a JavaScript library, and this project's frontend
is a disposable harness — the folder feature would have been unreachable outside
Go tests. A `webkitdirectory` input plus `FormData` produces multipart with
relative paths natively.

## Consequences

The server understands the Payload format, which forecloses end-to-end
encrypted Payloads: you cannot archive what you cannot read.

Folder responses are chunked with no `Content-Length`, so exact folder progress
is available only from the control plane. Single-file responses carry a real
length, so the browser's own download indicator works for them.

Without a manifest the server cannot validate any individual entry's size — only
the declared total against bytes actually delivered. When resume arrives (ADR
0008), correctness of a resumed entry rests on the Sender supplying the right
offset; a wrong one produces a CRC mismatch the Recipient only discovers when
unzipping.

---

## Amendment — 2026-07-25, after the implementation

**Status: confirmed, with one correction that matters.**

### `webkitRelativePath` survives the browser and not Go

This record says, correctly, that `FormData` with `webkitRelativePath` "produces
multipart with relative paths natively". It does. What it could not have known is
that **Go throws them away on the way in**:

```go
// mime/multipart
func (p *Part) FileName() string {
    ...
    // RFC 7578, Section 4.2 requires that if a filename is provided, the
    // directory path information must not be used.
    return filepath.Base(filename)
}
```

`Part.FileName()` returns `filepath.Base` of what was sent. Using it produced an
archive with every file in the root and no folder structure at all — the feature
this record chose server-side zipping *for*, silently absent, with a passing
smoke test.

The relative path only survives in the raw header, so the relay parses
`Content-Disposition` itself (`relay.partFilename`). Reading it unmodified is safe
precisely because `relay.SanitizeEntry` runs next: one function's job is to
preserve the path, the other's is to make sure it cannot escape the archive.
`TestCopyZipsAFolder` asserts the entry names, which is what caught it.

### The missing manifest, made concrete

The record's last consequence — no manifest, so a wrong resumed offset surfaces as
a CRC error at unzip time — is now enforced rather than merely noted. A resuming
Sender declares `X-Entry-Index` and `X-Entry-Offset`; the Owner compares them with
where the relay actually is and answers `409` plus `X-Expected-Entry` and
`X-Expected-Offset` on a mismatch (`relay.Session.Continue`). It is the only place
a bad offset can be caught, and the reason it must be.

A related consequence discovered while building it: **a Sender that means to
resume has to break the connection, not close the part cleanly.** Multipart has no
way to say "this part is incomplete", so a part that ends normally is an entry that
finished. There is nothing to be done about this, and it is why an interrupted
upload is detected as a *read failure* rather than announced.

### Chunked folders, restated

"Folder responses are chunked with no `Content-Length`" has a sharper consequence
than progress reporting: **a folder cannot be resumed into a new response.** The
`zip.Writer` was building the archive as it streamed, and that stream went with the
socket. A Recipient asking for `Range: bytes=N-` on a folder is answered `416`;
only a single file can be picked up again (ADR 0008).

`Store` rather than `Deflate` was also right for a reason not listed above:
`archive/zip` can write `Store` entries to a non-seekable writer because it records
each size in a trailing data descriptor, which is what makes a live
`ResponseWriter` a valid destination at all.
