package relay

import (
	"fmt"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"hotpotato/internal/transfer"
)

// Response is the shape of the Recipient's response. It follows from the
// *declared* Payload, so it can be worked out before a single byte arrives —
// which matters, because the headers have to be right the first time.
type Response struct {
	Filename    string
	ContentType string
	// Length is -1 when the response has to be chunked. A zip is built as it
	// streams, so its size is not known until it is finished (ADR 0004).
	Length int64
	// From is the offset a resumed response starts at, and turns it into a 206.
	From int64
	// Total is the declared Payload size, needed for Content-Range.
	Total int64
}

// Describe is pure: same Payload, same headers, no server involved.
func Describe(p transfer.Payload) Response {
	if p.Kind == transfer.KindFolder {
		return Response{
			Filename:    p.Name + ".zip",
			ContentType: "application/zip",
			Length:      -1,
		}
	}
	ct := mime.TypeByExtension(filepath.Ext(p.Name))
	if ct == "" {
		ct = "application/octet-stream"
	}
	// A single file passes through untouched, so its length is exactly what the
	// Sender declared — which is what makes the browser's own download
	// indicator work for files and not for folders.
	return Response{Filename: p.Name, ContentType: ct, Length: p.TotalBytes, Total: p.TotalBytes}
}

// ResumedFrom turns a description into a partial one. Only a single file can be
// resumed into a new response: a zip is built as it streams, so there is nothing
// to continue from.
func (r Response) ResumedFrom(offset int64) Response {
	r.From = offset
	if r.Length >= 0 {
		r.Length = r.Total - offset
	}
	return r
}

// ApplyTo writes the headers. This is the only place in the package that knows
// what an HTTP response is.
func (r Response) ApplyTo(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", r.ContentType)
	// FormatMediaType quotes and RFC 2231-encodes the filename, so a name with
	// a space, a quote or a non-ASCII character cannot break the header.
	h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{
		"filename": r.Filename,
	}))
	// The bytes are not ours to keep and not ours to let anyone cache.
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	// Proxies buffer a response body by default, which would defeat the whole
	// design: memory flat, bytes moving as they arrive.
	h.Set("X-Accel-Buffering", "no")

	if r.Length >= 0 {
		h.Set("Content-Length", strconv.FormatInt(r.Length, 10))
		// Only a response with a known length can honestly claim to be
		// resumable, which in this system means a single file.
		h.Set("Accept-Ranges", "bytes")
	}

	if r.From > 0 {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", r.From, r.Total-1, r.Total))
		w.WriteHeader(http.StatusPartialContent)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ParseRangeStart reads a `Range: bytes=N-` header.
//
// Only the open-ended single-range form is accepted. The general syntax exists so
// a client can ask for the middle of a file it can already seek in; here the
// bytes do not exist yet and arrive once, in order, so a suffix is the only thing
// that can be served (ADR 0008).
func ParseRangeStart(header string) (offset int64, ok bool) {
	if header == "" {
		return 0, true
	}
	spec, found := strings.CutPrefix(strings.TrimSpace(header), "bytes=")
	if !found {
		return 0, false
	}
	start, end, found := strings.Cut(spec, "-")
	if !found || end != "" || start == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(start, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}
