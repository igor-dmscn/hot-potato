package relay

import (
	"mime"
	"net/http"
	"path/filepath"
	"strconv"

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
	return Response{Filename: p.Name, ContentType: ct, Length: p.TotalBytes}
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
	}
	w.WriteHeader(http.StatusOK)
}
