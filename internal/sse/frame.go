// Package sse writes Server-Sent Events frames and owns the registry of live
// Streams. It knows nothing about net/http: a Stream writes into a Sink, which
// httpapi implements with an http.ResponseController.
package sse

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"hotpotato/internal/bus"
)

// WriteEvent writes one frame:
//
//	id: 12
//	event: user.online
//	data: {"id":"u_7"}
//	<blank line>
//
// The blank line is the whole protocol. Forgetting the second \n is the classic
// SSE bug: the browser buffers the frame forever, waiting for a terminator, and
// everything looks like a network problem.
func WriteEvent(w io.Writer, e bus.Event) error {
	var b bytes.Buffer
	if e.ID != 0 {
		fmt.Fprintf(&b, "id: %d\n", e.ID)
	}
	if e.Name != "" {
		fmt.Fprintf(&b, "event: %s\n", e.Name)
	}
	// json.Marshal never emits a raw newline, but a data: line containing one
	// would silently split the frame, so split deliberately instead.
	for _, line := range strings.Split(string(e.Data), "\n") {
		fmt.Fprintf(&b, "data: %s\n", line)
	}
	b.WriteByte('\n')
	_, err := w.Write(b.Bytes())
	return err
}

// WriteComment writes a comment frame. Its purpose is to keep proxies from
// reaping an idle connection at the 30–60s mark; it carries no data and the
// browser ignores it.
func WriteComment(w io.Writer, text string) error {
	_, err := fmt.Fprintf(w, ":%s\n\n", text)
	return err
}

// WriteRetry tells the browser how long to wait before reconnecting, which is
// the only backoff control the EventSource API offers.
func WriteRetry(w io.Writer, d time.Duration) error {
	_, err := fmt.Fprintf(w, "retry: %d\n\n", d.Milliseconds())
	return err
}
