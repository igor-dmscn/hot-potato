package relay

import "fmt"

// Headers a resuming Sender sends and the server answers with. They are headers
// rather than a JSON body because the request they belong to *is* the body.
const (
	HeaderEntryIndex     = "X-Entry-Index"
	HeaderEntryOffset    = "X-Entry-Offset"
	HeaderExpectedEntry  = "X-Expected-Entry"
	HeaderExpectedOffset = "X-Expected-Offset"
)

// OffsetMismatchError is a Sender resuming from somewhere other than where the
// relay actually stopped.
//
// It carries the expected position, because the Sender cannot work it out: what
// it managed to write into a socket is not what the Recipient managed to read
// out of one, and only the Owner knows the difference.
type OffsetMismatchError struct {
	ExpectedEntry  int
	ExpectedOffset int64
	GotEntry       int
	GotOffset      int64
}

func (e OffsetMismatchError) Error() string {
	return fmt.Sprintf("resume from entry %d offset %d, not entry %d offset %d",
		e.ExpectedEntry, e.ExpectedOffset, e.GotEntry, e.GotOffset)
}

// Position is where the next byte must come from: which entry, and how far into
// it. For a single file the entry is always 0 and the offset is the byte count.
func (s *Session) Position() (entry int, offset int64) {
	return s.entry, s.offset
}

// Continue checks a Sender's claimed position against where the relay actually
// is.
//
// There is no manifest to validate an entry against (ADR 0004), so a wrong
// offset that got past this would surface as a CRC error when the Recipient
// unzips — long after anything could be done about it. This is the only place it
// can be caught.
func (s *Session) Continue(entry int, offset int64) error {
	if entry != s.entry || offset != s.offset {
		return OffsetMismatchError{
			ExpectedEntry:  s.entry,
			ExpectedOffset: s.offset,
			GotEntry:       entry,
			GotOffset:      offset,
		}
	}
	return nil
}

// SkipTo positions a fresh Session as though it had already relayed offset bytes
// of the first entry.
//
// This is what a Recipient's `Range: bytes=N-` amounts to: the response starts at
// N, so the Sender must resume at N — even if the previous attempt had already
// relayed more than that.
//
// N comes from the Recipient, and it has to: the Owner's count is bytes *written
// into a socket*, which after a disconnection is larger than what came out of the
// other end by however much was in flight. Measured on loopback, that gap was
// about 98 KB. Only the Recipient knows what it actually holds, which is why
// every resumable download protocol works this way round.
//
// It applies to a single file only — a zip is built as it streams, so a folder
// cannot be resumed into a new response, and asking for one is a 416.
func (s *Session) SkipTo(offset int64) error {
	if s.zw != nil {
		return fmt.Errorf("%w: a folder cannot be resumed into a new response", ErrPayloadMismatch)
	}
	if offset < 0 || offset > s.p.TotalBytes {
		return fmt.Errorf("%w: offset %d is outside a %d byte payload",
			ErrPayloadMismatch, offset, s.p.TotalBytes)
	}
	s.offset = offset
	s.total = offset
	return nil
}

// Complete reports whether every declared byte has arrived.
func (s *Session) Complete() bool {
	if s.total != s.p.TotalBytes {
		return false
	}
	// A folder is only complete once the last entry has been closed off, or the
	// archive would be missing its central directory.
	if s.zw != nil {
		return s.cur == nil && s.entry == s.p.EntryCount
	}
	return s.cur == nil
}
