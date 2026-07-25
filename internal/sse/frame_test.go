package sse

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"hotpotato/internal/bus"
)

// Golden bytes, because the wire format is the contract with the browser and
// the failure mode of getting it wrong — a frame that never arrives — looks
// like a network problem rather than a bug.
func TestWriteEventGolden(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		event bus.Event
		want  string
	}{
		"complete frame": {
			bus.Event{ID: 12, Name: "user.online", Data: []byte(`{"id":"u_7"}`)},
			"id: 12\nevent: user.online\ndata: {\"id\":\"u_7\"}\n\n",
		},
		"no id yet": {
			bus.Event{Name: "snapshot", Data: []byte(`{}`)},
			"event: snapshot\ndata: {}\n\n",
		},
		"multi-line data becomes multiple data lines": {
			bus.Event{ID: 1, Name: "x", Data: []byte("a\nb")},
			"id: 1\nevent: x\ndata: a\ndata: b\n\n",
		},
		"empty data still gets a line": {
			bus.Event{ID: 1, Name: "x"},
			"id: 1\nevent: x\ndata: \n\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := WriteEvent(&b, tc.event); err != nil {
				t.Fatalf("WriteEvent: %v", err)
			}
			if got := b.String(); got != tc.want {
				t.Errorf("frame = %q, want %q", got, tc.want)
			}
			if !strings.HasSuffix(b.String(), "\n\n") {
				t.Error("frame does not end in a blank line")
			}
		})
	}
}

func TestWriteCommentAndRetryGolden(t *testing.T) {
	t.Parallel()

	var b bytes.Buffer
	if err := WriteComment(&b, "hb"); err != nil {
		t.Fatalf("WriteComment: %v", err)
	}
	if got := b.String(); got != ":hb\n\n" {
		t.Errorf("comment = %q, want \":hb\\n\\n\"", got)
	}

	b.Reset()
	if err := WriteRetry(&b, 3*time.Second); err != nil {
		t.Fatalf("WriteRetry: %v", err)
	}
	if got := b.String(); got != "retry: 3000\n\n" {
		t.Errorf("retry = %q, want \"retry: 3000\\n\\n\"", got)
	}
}
