package relay

import (
	"io"
	"time"
)

// Deadliner is the only thing the byte path needs from an HTTP response: the
// ability to push the write deadline out again. Taking an interface rather than
// an *http.ResponseController is what keeps net/http out of this file.
type Deadliner interface {
	SetWriteDeadline(time.Time) error
}

// NewDeadlineWriter refreshes the write deadline before every Write.
//
// Server.WriteTimeout has to be 0 or it would kill a multi-gigabyte relay on a
// schedule — but that leaves a Recipient who stops reading holding a goroutine
// and a socket indefinitely. A per-write deadline is the replacement: progress
// keeps pushing it out, silence lets it fire.
func NewDeadlineWriter(w io.Writer, d Deadliner, per time.Duration) io.Writer {
	if per <= 0 {
		return w
	}
	return &deadlineWriter{w: w, d: d, per: per}
}

type deadlineWriter struct {
	w   io.Writer
	d   Deadliner
	per time.Duration
}

func (dw *deadlineWriter) Write(p []byte) (int, error) {
	if err := dw.d.SetWriteDeadline(time.Now().Add(dw.per)); err != nil {
		return 0, err
	}
	return dw.w.Write(p)
}
