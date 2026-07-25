package relay

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"strings"
	"testing"
	"time"

	"hotpotato/internal/transfer"
)

type entry struct {
	name string
	data []byte
}

// body builds a multipart request body the way a browser does, including the
// relative path in each part's filename.
func body(t *testing.T, entries ...entry) (*multipart.Reader, int64) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	var total int64
	for _, e := range entries {
		w, err := mw.CreateFormFile("files", e.name)
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		if _, err := w.Write(e.data); err != nil {
			t.Fatalf("write part: %v", err)
		}
		total += int64(len(e.data))
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}
	return multipart.NewReader(&buf, mw.Boundary()), total
}

func TestCopyPassesASingleFileThrough(t *testing.T) {
	t.Parallel()
	want := []byte("the bytes, exactly as they arrived")
	parts, total := body(t, entry{"report.pdf", want})

	var got bytes.Buffer
	var counted int64
	n, err := Copy(&got, parts,
		transfer.Payload{Name: "report.pdf", Kind: transfer.KindFile, TotalBytes: total},
		Options{Count: func(d int64) { counted += d }})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if n != total || counted != total {
		t.Errorf("relayed %d and counted %d, want %d each", n, counted, total)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Errorf("body = %q, want %q — a file is not repackaged", got.Bytes(), want)
	}
}

func TestCopyZipsAFolder(t *testing.T) {
	t.Parallel()
	entries := []entry{
		{"docs/a.txt", []byte("first")},
		{"docs/nested/b.txt", []byte("second")},
		{"docs/c.bin", bytes.Repeat([]byte{7}, 5000)},
	}
	parts, total := body(t, entries...)

	var got bytes.Buffer
	modified := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	n, err := Copy(&got, parts, transfer.Payload{
		Name: "docs", Kind: transfer.KindFolder, TotalBytes: total, EntryCount: len(entries),
	}, Options{Modified: modified})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	// The count is Payload bytes; the archive is larger because of its framing.
	if n != total {
		t.Errorf("counted %d payload bytes, want %d", n, total)
	}
	if int64(got.Len()) <= total {
		t.Errorf("archive is %d bytes for %d of payload; where is the framing?", got.Len(), total)
	}

	// The point of the test: a real zip reader opens it.
	zr, err := zip.NewReader(bytes.NewReader(got.Bytes()), int64(got.Len()))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}
	if len(zr.File) != len(entries) {
		t.Fatalf("archive holds %d entries, want %d", len(zr.File), len(entries))
	}
	for i, f := range zr.File {
		if f.Name != entries[i].name {
			t.Errorf("entry %d is %q, want %q", i, f.Name, entries[i].name)
		}
		if f.Method != zip.Store {
			t.Errorf("entry %q uses method %d, want Store", f.Name, f.Method)
		}
		if !f.Modified.Equal(modified) {
			t.Errorf("entry %q modified %v, want %v", f.Name, f.Modified, modified)
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open %q: %v", f.Name, err)
		}
		content, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %q: %v", f.Name, err)
		}
		if !bytes.Equal(content, entries[i].data) {
			t.Errorf("entry %q holds %d bytes, want %d", f.Name, len(content), len(entries[i].data))
		}
	}
}

// The server keeps no copy, so the declaration is the only thing it can check
// what arrived against.
func TestCopyEnforcesTheDeclaration(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		payload transfer.Payload
		entries []entry
	}{
		"fewer bytes than declared": {
			transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: 100},
			[]entry{{"a", []byte("short")}},
		},
		"more bytes than declared": {
			transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: 2},
			[]entry{{"a", []byte("longer than two")}},
		},
		"a file with two parts": {
			transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: 4},
			[]entry{{"a", []byte("ab")}, {"b", []byte("cd")}},
		},
		"no parts at all": {
			transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: 4},
			nil,
		},
		"folder with fewer entries than declared": {
			transfer.Payload{Name: "d", Kind: transfer.KindFolder, TotalBytes: 2, EntryCount: 3},
			[]entry{{"d/a", []byte("ab")}},
		},
		"folder with more entries than declared": {
			transfer.Payload{Name: "d", Kind: transfer.KindFolder, TotalBytes: 4, EntryCount: 1},
			[]entry{{"d/a", []byte("ab")}, {"d/b", []byte("cd")}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parts, _ := body(t, tc.entries...)
			_, err := Copy(io.Discard, parts, tc.payload, Options{})
			if !errors.Is(err, ErrPayloadMismatch) {
				t.Fatalf("Copy = %v, want ErrPayloadMismatch", err)
			}
		})
	}
}

func TestCopyBlamesTheRightSide(t *testing.T) {
	t.Parallel()

	t.Run("the recipient hung up", func(t *testing.T) {
		t.Parallel()
		parts, total := body(t, entry{"a", bytes.Repeat([]byte{1}, 200<<10)})
		_, err := Copy(&failingWriter{after: 1 << 10}, parts,
			transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: total}, Options{})

		var writeErr WriteError
		if !errors.As(err, &writeErr) {
			t.Fatalf("Copy = %v, want a WriteError", err)
		}
	})

	t.Run("the sender hung up", func(t *testing.T) {
		t.Parallel()
		// A truncated multipart body: the boundary never closes.
		truncated := "--b\r\nContent-Disposition: form-data; name=\"files\"; filename=\"a\"\r\n\r\nsome bytes"
		parts := multipart.NewReader(
			io.MultiReader(strings.NewReader(truncated), errReader{}), "b")

		_, err := Copy(io.Discard, parts,
			transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: 10}, Options{})

		var readErr ReadError
		if !errors.As(err, &readErr) {
			t.Fatalf("Copy = %v, want a ReadError", err)
		}
	})
}

func TestCopyStopsWhenCancelled(t *testing.T) {
	t.Parallel()
	parts, total := body(t, entry{"a", bytes.Repeat([]byte{1}, 1<<20)})

	// Already closed: the very first read is abandoned.
	stop := make(chan struct{})
	close(stop)

	n, err := Copy(io.Discard, parts,
		transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: total},
		Options{Stop: stop, Buffer: 4096})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("Copy = %v, want ErrCanceled", err)
	}
	if n >= total {
		t.Errorf("relayed %d of %d bytes; the cancellation did nothing", n, total)
	}
}

// The counter is what the progress ticker reads, so it has to see the bytes as
// they move rather than in one lump at the end.
func TestCountIsCalledPerChunk(t *testing.T) {
	t.Parallel()
	const size = 100 << 10
	parts, total := body(t, entry{"a", bytes.Repeat([]byte{2}, size)})

	var calls int
	var sum int64
	if _, err := Copy(io.Discard, parts,
		transfer.Payload{Name: "a", Kind: transfer.KindFile, TotalBytes: total},
		Options{Count: func(d int64) { calls++; sum += d }, Buffer: 8 << 10}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if calls < 2 {
		t.Errorf("Count was called %d times for %d bytes in 8 KiB buffers", calls, size)
	}
	if sum != total {
		t.Errorf("Count summed to %d, want %d", sum, total)
	}
}

// failingWriter accepts `after` bytes and then refuses, like a socket whose peer
// has gone.
type failingWriter struct {
	after   int
	written int
}

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.written >= f.after {
		return 0, fmt.Errorf("connection reset by peer")
	}
	f.written += len(p)
	return len(p), nil
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, fmt.Errorf("connection reset by peer") }
