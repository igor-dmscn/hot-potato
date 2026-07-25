package relay

import (
	"mime"
	"net/http/httptest"
	"testing"

	"hotpotato/internal/transfer"
)

func TestDescribe(t *testing.T) {
	t.Parallel()

	file := Describe(transfer.Payload{Name: "report.pdf", Kind: transfer.KindFile, TotalBytes: 4096})
	if file.Filename != "report.pdf" || file.ContentType != "application/pdf" {
		t.Errorf("file = %+v", file)
	}
	// A real length is what makes the browser's own download indicator work.
	if file.Length != 4096 {
		t.Errorf("file length = %d, want 4096", file.Length)
	}

	unknown := Describe(transfer.Payload{Name: "blob.wat", Kind: transfer.KindFile, TotalBytes: 1})
	if unknown.ContentType != "application/octet-stream" {
		t.Errorf("unknown extension = %q, want application/octet-stream", unknown.ContentType)
	}

	// A zip is built as it streams, so its size is not knowable in advance.
	folder := Describe(transfer.Payload{Name: "docs", Kind: transfer.KindFolder, TotalBytes: 99, EntryCount: 3})
	if folder.Filename != "docs.zip" || folder.ContentType != "application/zip" {
		t.Errorf("folder = %+v", folder)
	}
	if folder.Length != -1 {
		t.Errorf("folder length = %d, want -1 for chunked", folder.Length)
	}
}

func TestApplyTo(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	Describe(transfer.Payload{Name: "report.pdf", Kind: transfer.KindFile, TotalBytes: 4096}).ApplyTo(rec)

	res := rec.Result()
	if res.StatusCode != 200 {
		t.Errorf("status = %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Content-Length"); got != "4096" {
		t.Errorf("Content-Length = %q, want 4096", got)
	}
	if got := res.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no — a buffering proxy defeats the whole design", got)
	}
	if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}

	_, params, err := mime.ParseMediaType(res.Header.Get("Content-Disposition"))
	if err != nil {
		t.Fatalf("Content-Disposition does not parse: %v", err)
	}
	if params["filename"] != "report.pdf" {
		t.Errorf("filename = %q", params["filename"])
	}

	// No Content-Length for a folder, or the response would be truncated at a
	// length nobody knew yet.
	rec = httptest.NewRecorder()
	Describe(transfer.Payload{Name: "docs", Kind: transfer.KindFolder, TotalBytes: 9, EntryCount: 1}).ApplyTo(rec)
	if got := rec.Result().Header.Get("Content-Length"); got != "" {
		t.Errorf("Content-Length = %q for a folder, want none", got)
	}
}

// A name with a quote or a non-ASCII character must not be able to break the
// header it travels in.
func TestApplyToEncodesAwkwardFilenames(t *testing.T) {
	t.Parallel()

	for _, name := range []string{`quarterly "final".pdf`, "relatório.pdf", "a b c.txt"} {
		rec := httptest.NewRecorder()
		Describe(transfer.Payload{Name: name, Kind: transfer.KindFile, TotalBytes: 1}).ApplyTo(rec)

		_, params, err := mime.ParseMediaType(rec.Result().Header.Get("Content-Disposition"))
		if err != nil {
			t.Fatalf("%q produced an unparseable header: %v", name, err)
		}
		if params["filename"] != name {
			t.Errorf("filename round-tripped as %q, want %q", params["filename"], name)
		}
	}
}
