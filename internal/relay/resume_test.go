package relay

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"mime/multipart"
	"testing"

	"hotpotato/internal/transfer"
)

// chunk builds a multipart body containing a slice of one entry's bytes, which
// is what a resuming Sender actually sends.
func chunk(t *testing.T, name string, data []byte) *multipart.Reader {
	t.Helper()
	parts, _ := body(t, entry{name, data})
	return parts
}

func TestContinueRejectsTheWrongOffset(t *testing.T) {
	t.Parallel()
	p := transfer.Payload{Name: "a.bin", Kind: transfer.KindFile, TotalBytes: 10}
	s := NewSession(io.Discard, p, Options{})

	// A fresh session expects the very beginning.
	if err := s.Continue(0, 0); err != nil {
		t.Fatalf("Continue(0, 0) on a fresh session: %v", err)
	}

	var mismatch OffsetMismatchError
	err := s.Continue(0, 4)
	if !errors.As(err, &mismatch) {
		t.Fatalf("Continue(0, 4) = %v, want an OffsetMismatchError", err)
	}
	if mismatch.ExpectedOffset != 0 || mismatch.GotOffset != 4 {
		t.Errorf("mismatch = %+v, want expected 0 and got 4", mismatch)
	}
	// The Sender cannot work the answer out for itself, so the error carries it.
	if !bytes.Contains([]byte(mismatch.Error()), []byte("entry 0 offset 0")) {
		t.Errorf("error = %q, want it to name the expected position", mismatch)
	}
}

// A single file, interrupted and resumed. The relayed bytes must be the original
// bytes, in order, with nothing repeated and nothing missing.
func TestResumeASingleFile(t *testing.T) {
	t.Parallel()
	want := bytes.Repeat([]byte("0123456789"), 500) // 5000 bytes
	p := transfer.Payload{Name: "a.bin", Kind: transfer.KindFile, TotalBytes: int64(len(want))}

	var got bytes.Buffer
	s := NewSession(&got, p, Options{})

	// First attempt stops after 2000 bytes: the Sender's body dies mid-part.
	interrupted := multipart.NewReader(
		io.MultiReader(
			bytes.NewReader(partHeader("a.bin")),
			bytes.NewReader(want[:2000]),
			errReader{},
		), "b")
	if _, err := s.Consume(interrupted); err == nil {
		t.Fatal("a truncated body was accepted as complete")
	}

	entryIndex, offset := s.Position()
	if entryIndex != 0 || offset != 2000 {
		t.Fatalf("position = entry %d offset %d, want entry 0 offset 2000", entryIndex, offset)
	}
	if s.Complete() {
		t.Fatal("an interrupted session reports itself complete")
	}

	// The Sender comes back with exactly the missing tail.
	if err := s.Continue(entryIndex, offset); err != nil {
		t.Fatalf("Continue at the reported position: %v", err)
	}
	if _, err := s.Consume(chunk(t, "a.bin", want[2000:])); err != nil {
		t.Fatalf("resumed chunk: %v", err)
	}
	if !s.Complete() {
		t.Fatal("the session is not complete after the tail arrived")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Errorf("relayed %d bytes, want %d, and they differ", got.Len(), len(want))
	}
}

// The case worth reading twice: a folder interrupted *inside* an entry.
// The zip.Writer and its open entry stay live, so the resumed entry finishes with
// the right CRC and the archive opens.
func TestResumeMidEntryInsideAFolder(t *testing.T) {
	t.Parallel()
	first := bytes.Repeat([]byte{1}, 3000)
	second := bytes.Repeat([]byte{2}, 4000)
	p := transfer.Payload{
		Name: "docs", Kind: transfer.KindFolder,
		TotalBytes: int64(len(first) + len(second)), EntryCount: 2,
	}

	var got bytes.Buffer
	s := NewSession(&got, p, Options{})

	// Entry one arrives whole; entry two is cut off after 1500 bytes.
	interrupted := multipart.NewReader(
		io.MultiReader(
			bytes.NewReader(partHeader("docs/one.bin")),
			bytes.NewReader(first),
			bytes.NewReader([]byte("\r\n--b\r\n")),
			bytes.NewReader(partHeaderBody("docs/two.bin")),
			bytes.NewReader(second[:1500]),
			errReader{},
		), "b")
	if _, err := s.Consume(interrupted); err == nil {
		t.Fatal("a truncated folder body was accepted")
	}

	entryIndex, offset := s.Position()
	if entryIndex != 1 || offset != 1500 {
		t.Fatalf("position = entry %d offset %d, want entry 1 offset 1500", entryIndex, offset)
	}

	// The tail of entry two, and nothing else.
	if err := s.Continue(entryIndex, offset); err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if _, err := s.Consume(chunk(t, "docs/two.bin", second[1500:])); err != nil {
		t.Fatalf("resumed chunk: %v", err)
	}
	if !s.Complete() {
		t.Fatalf("not complete: %d of %d bytes, %d entries", s.Total(), p.TotalBytes, s.Entries())
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(got.Bytes()), int64(got.Len()))
	if err != nil {
		t.Fatalf("the resumed archive does not open: %v", err)
	}
	if len(zr.File) != 2 {
		t.Fatalf("archive holds %d entries, want 2", len(zr.File))
	}
	for i, want := range [][]byte{first, second} {
		rc, err := zr.File[i].Open()
		if err != nil {
			// A CRC mismatch surfaces here, which is the failure mode the whole
			// live-zip.Writer arrangement exists to avoid.
			t.Fatalf("open %q: %v", zr.File[i].Name, err)
		}
		content, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %q: %v", zr.File[i].Name, err)
		}
		if !bytes.Equal(content, want) {
			t.Errorf("entry %q is %d bytes, want %d", zr.File[i].Name, len(content), len(want))
		}
	}
}

// A Recipient reconnecting with Range: bytes=N- rewinds what the Sender is
// expected to send.
func TestSkipToRewindsForAResumedRecipient(t *testing.T) {
	t.Parallel()
	want := bytes.Repeat([]byte("abcdefghij"), 300) // 3000 bytes
	p := transfer.Payload{Name: "a.bin", Kind: transfer.KindFile, TotalBytes: int64(len(want))}

	var got bytes.Buffer
	s := NewSession(&got, p, Options{})
	if err := s.SkipTo(1200); err != nil {
		t.Fatalf("SkipTo: %v", err)
	}
	if entry, offset := s.Position(); entry != 0 || offset != 1200 {
		t.Fatalf("position = entry %d offset %d, want entry 0 offset 1200", entry, offset)
	}

	if _, err := s.Consume(chunk(t, "a.bin", want[1200:])); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !s.Complete() {
		t.Fatal("not complete after the tail arrived")
	}
	// The new response carries only the tail: the Recipient already has the rest.
	if !bytes.Equal(got.Bytes(), want[1200:]) {
		t.Errorf("relayed %d bytes, want the %d byte tail", got.Len(), len(want)-1200)
	}
}

func TestSkipToRefusesAFolderAndAnImpossibleOffset(t *testing.T) {
	t.Parallel()

	folder := NewSession(io.Discard,
		transfer.Payload{Name: "d", Kind: transfer.KindFolder, TotalBytes: 10, EntryCount: 1}, Options{})
	if err := folder.SkipTo(5); !errors.Is(err, ErrPayloadMismatch) {
		t.Errorf("SkipTo on a folder = %v, want ErrPayloadMismatch", err)
	}

	file := NewSession(io.Discard,
		transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: 10}, Options{})
	if err := file.SkipTo(11); !errors.Is(err, ErrPayloadMismatch) {
		t.Errorf("SkipTo past the end = %v, want ErrPayloadMismatch", err)
	}
}

func TestParseRangeStart(t *testing.T) {
	t.Parallel()

	for header, want := range map[string]struct {
		offset int64
		ok     bool
	}{
		"":            {0, true},
		"bytes=0-":    {0, true},
		"bytes=1200-": {1200, true},
		" bytes=99- ": {99, true},
		"bytes=0-499": {0, false}, // a bounded range needs bytes that already exist
		"bytes=-500":  {0, false}, // a suffix range does too
		"items=1-":    {0, false},
		"bytes=abc-":  {0, false},
		"bytes=-":     {0, false},
		"bytes=1-,3-": {0, false},
	} {
		got, ok := ParseRangeStart(header)
		if ok != want.ok || got != want.offset {
			t.Errorf("ParseRangeStart(%q) = %d, %v; want %d, %v", header, got, ok, want.offset, want.ok)
		}
	}
}

// partHeader is the multipart preamble for one part, boundary included.
func partHeader(name string) []byte {
	return append([]byte("--b\r\n"), partHeaderBody(name)...)
}

func partHeaderBody(name string) []byte {
	return []byte("Content-Disposition: form-data; name=\"files\"; filename=\"" + name + "\"\r\n" +
		"Content-Type: application/octet-stream\r\n\r\n")
}
