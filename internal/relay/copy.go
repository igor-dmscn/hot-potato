package relay

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"time"

	"hotpotato/internal/transfer"
)

// DefaultBuffer is DESIGN §9's relay copy buffer. It is, per Transfer, the
// entire memory cost of the data plane.
const DefaultBuffer = 64 << 10

var (
	// ErrPayloadMismatch is bytes or parts disagreeing with the declaration.
	ErrPayloadMismatch = errors.New("payload does not match its declaration")
	// ErrCanceled is a relay abandoned because somebody cancelled it.
	ErrCanceled = errors.New("relay canceled")
)

// ReadError is a failure pulling bytes from the Sender.
type ReadError struct{ Err error }

func (e ReadError) Error() string { return "reading from the sender: " + e.Err.Error() }
func (e ReadError) Unwrap() error { return e.Err }

// WriteError is a failure pushing bytes to the Recipient.
type WriteError struct{ Err error }

func (e WriteError) Error() string { return "writing to the recipient: " + e.Err.Error() }
func (e WriteError) Unwrap() error { return e.Err }

// Options is everything Copy needs besides the bytes themselves.
//
// The plan wrote this as a positional `count func(int64)`; it became a struct
// once cancellation and the zip timestamp needed to arrive the same way.
type Options struct {
	// Count is called with the length of every chunk of Payload — never with
	// zip framing. The caller owns the counter and the progress ticker, so Copy
	// knows nothing about events.
	Count func(int64)
	// Stop abandons the relay at the next buffer boundary when closed.
	Stop <-chan struct{}
	// Buffer defaults to DefaultBuffer.
	Buffer int
	// Modified is the timestamp recorded on zip entries.
	Modified time.Time
}

// Copy relays parts into dst and validates them against the declaration.
func Copy(dst io.Writer, parts *multipart.Reader, p transfer.Payload, o Options) (int64, error) {
	s := NewSession(dst, p, o)
	if _, err := s.Consume(parts); err != nil {
		return s.Total(), err
	}
	return s.Total(), s.Close()
}

// Session is a relay in progress. It exists as a type, rather than living
// inside Copy, because a resumed Transfer needs the zip.Writer and its entry
// position to survive between the Sender's requests (phase 11).
type Session struct {
	dst io.Writer
	p   transfer.Payload
	o   Options
	buf []byte

	zw *zip.Writer
	// cur is the entry being written. It stays non-nil between chunks when a
	// Sender stopped part way through one: for a folder that is a live
	// zip.Writer entry, and keeping it is what lets a resumed entry finish with
	// the right CRC (ADR 0008).
	cur io.Writer
	// entry is how many entries are finished, which is also the index of the one
	// in progress. offset is how far into it we are.
	entry  int
	offset int64
	total  int64
}

func NewSession(dst io.Writer, p transfer.Payload, o Options) *Session {
	if o.Buffer <= 0 {
		o.Buffer = DefaultBuffer
	}
	s := &Session{dst: dst, p: p, o: o, buf: make([]byte, o.Buffer)}
	// The decision comes from the declared Kind, never from counting parts:
	// NextPart discards the previous part's unread body, so looking ahead to
	// see whether a second part exists would destroy the first (ADR 0004).
	if p.Kind == transfer.KindFolder {
		s.zw = zip.NewWriter(dst)
	}
	return s
}

// Total is the Payload bytes relayed so far, excluding any zip framing.
func (s *Session) Total() int64 { return s.total }

// Entries is how many entries are finished.
func (s *Session) Entries() int { return s.entry }

// Consume relays every part in parts, and returns how many bytes this call
// moved.
func (s *Session) Consume(parts *multipart.Reader) (int64, error) {
	before := s.total
	for {
		part, err := parts.NextPart()
		switch {
		case errors.Is(err, io.EOF):
			return s.total - before, nil
		case err != nil:
			return s.total - before, ReadError{err}
		}
		err = s.relayPart(part)
		part.Close()
		if err != nil {
			return s.total - before, err
		}
	}
}

func (s *Session) relayPart(part *multipart.Part) error {
	// A part that continues an interrupted entry writes into the writer that was
	// left open, rather than starting a new one.
	resuming := s.cur != nil
	if !resuming {
		switch {
		case s.p.Kind == transfer.KindFile && s.entry >= 1:
			return fmt.Errorf("%w: declared one file, but a second part arrived", ErrPayloadMismatch)
		case s.p.Kind == transfer.KindFolder && s.p.EntryCount > 0 && s.entry >= s.p.EntryCount:
			return fmt.Errorf("%w: declared %d entries, part %d arrived",
				ErrPayloadMismatch, s.p.EntryCount, s.entry+1)
		}
	}

	dst := s.cur
	if dst == nil {
		dst = s.dst
		if s.zw != nil {
			w, err := s.zw.CreateHeader(&zip.FileHeader{
				Name: SanitizeEntry(partFilename(part), s.entry+1),
				// Store, not Deflate: compression burns CPU per byte on the one
				// machine here that should stay a dumb pipe, and the payoff is a
				// guess about data the server never sees (ADR 0004). Store also
				// means archive/zip can write to a non-seekable writer, recording
				// each entry's size in a trailing data descriptor.
				Method:   zip.Store,
				Modified: s.o.Modified,
			})
			if err != nil {
				return WriteError{err}
			}
			dst = w
		}
		s.cur = dst
	}

	// Which side failed decides which party gets blamed in transfer.failed, so
	// both ends of the copy are tracked separately.
	tracked := &trackedReader{r: part}
	src := io.Reader(tracked)
	if s.o.Stop != nil {
		// Outermost, so its ErrCanceled is not mistaken for the Sender hanging
		// up.
		src = &stopReader{r: src, stop: s.o.Stop}
	}
	counted := &countingWriter{w: dst, add: s.o.Count}

	// Neither end implements ReadFrom or WriteTo, so CopyBuffer really does use
	// this buffer and nothing else. Backpressure needs no code: this write
	// blocks on the Recipient's socket, which stops draining the Sender's body.
	n, err := io.CopyBuffer(counted, src, s.buf)
	s.total += n
	s.offset += n

	switch {
	case counted.err != nil:
		return WriteError{counted.err}
	case tracked.err != nil:
		// The Sender stopped part way. The entry stays open, holding its
		// position, so a resumed chunk can carry on writing into it.
		return ReadError{tracked.err}
	case err != nil:
		return err
	}

	// The part ended cleanly, so the entry is finished. There is no way for a
	// Sender to say "this part is incomplete" in multipart — a Sender that means
	// to resume has to break the connection, not close the part (ADR 0004).
	s.cur = nil
	s.entry++
	s.offset = 0
	return nil
}

// Close finishes the archive, if there is one, and checks what arrived against
// what was declared. The server holds no copy, so the declaration is the only
// thing it can check against.
func (s *Session) Close() error {
	if s.zw != nil {
		if err := s.zw.Close(); err != nil {
			return WriteError{err}
		}
	}
	if s.total != s.p.TotalBytes {
		return fmt.Errorf("%w: declared %d bytes, relayed %d",
			ErrPayloadMismatch, s.p.TotalBytes, s.total)
	}
	switch {
	case s.p.Kind == transfer.KindFile && s.entry != 1:
		return fmt.Errorf("%w: declared one file, %d parts arrived", ErrPayloadMismatch, s.entry)
	case s.p.Kind == transfer.KindFolder && s.p.EntryCount > 0 && s.entry != s.p.EntryCount:
		return fmt.Errorf("%w: declared %d entries, %d arrived",
			ErrPayloadMismatch, s.p.EntryCount, s.entry)
	}
	return nil
}

// partFilename returns the filename as the client actually sent it.
//
// multipart.Part.FileName() cannot be used here: RFC 7578 §4.2 says a filename
// must not be taken as directory path information, so Go's implementation
// returns filepath.Base of it — which discards exactly the relative path a
// folder Payload is made of. The raw header is the only place the path survives.
//
// Reading it unmodified is safe precisely because SanitizeEntry runs next: this
// function's job is to preserve the path, and that one's job is to make sure it
// cannot escape the archive.
func partFilename(part *multipart.Part) string {
	_, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
	if err != nil {
		return ""
	}
	return params["filename"]
}

// countingWriter reports progress and remembers a write failure.
type countingWriter struct {
	w   io.Writer
	add func(int64)
	err error
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 && c.add != nil {
		c.add(int64(n))
	}
	if err != nil {
		c.err = err
	}
	return n, err
}

// trackedReader remembers a read failure. io.EOF is not one.
type trackedReader struct {
	r   io.Reader
	err error
}

func (t *trackedReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		t.err = err
	}
	return n, err
}

// stopReader abandons a relay between buffers. A copy blocked inside
// io.CopyBuffer cannot select on a channel, so the check goes on the read path.
type stopReader struct {
	r    io.Reader
	stop <-chan struct{}
}

func (s *stopReader) Read(p []byte) (int, error) {
	select {
	case <-s.stop:
		return 0, ErrCanceled
	default:
		return s.r.Read(p)
	}
}
