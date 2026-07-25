package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"hotpotato/internal/relay"
)

// payload is what will be sent, resolved from a path on disk.
type payload struct {
	name    string
	kind    string
	total   int64
	entries []entry
}

// entry is one file, with the relative path it should carry.
type entry struct {
	// rel is what goes in the part's filename. For a directory it is
	// "<dirname>/<path within it>", which is exactly what a browser's
	// webkitRelativePath produces — so the archive a folder becomes is the same
	// whichever client sent it.
	rel  string
	path string
	size int64
}

// send offers a Payload and streams it once the Recipient is attached.
func send(ctx context.Context, c *client, to, path string) error {
	p, err := resolve(path)
	if err != nil {
		return err
	}

	s, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer s.close()
	snap, err := s.snapshot(ctx)
	if err != nil {
		return err
	}

	recipient, err := lookup(snap, to)
	if err != nil {
		return err
	}
	fmt.Printf("sending %s (%s, %s, %d file(s)) to %s\n",
		p.name, p.kind, human(p.total), len(p.entries), recipient.DisplayName)

	res, err := c.postJSON(ctx, c.base+"/api/transfers", map[string]any{
		"to":         recipient.ID,
		"name":       p.name,
		"kind":       p.kind,
		"totalBytes": p.total,
		"entryCount": len(p.entries),
	})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusCreated {
		defer res.Body.Close()
		return fmt.Errorf("offer: %s", errorFrom(res))
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		res.Body.Close()
		return err
	}
	res.Body.Close()
	fmt.Println("offered", created.ID, "— waiting for the recipient")

	// Nothing is read from disk until the Recipient has parked and the server
	// says so. A POST before that is answered 409 (ADR 0002).
	if _, err := s.await(ctx, "transfer.ready", "transfer.denied", "transfer.failed"); err != nil {
		return err
	}

	started := time.Now()
	if err := c.deliver(ctx, s, created.ID, p); err != nil {
		return err
	}

	e, err := s.await(ctx, "transfer.completed", "transfer.failed", "transfer.canceled")
	if err != nil {
		return err
	}
	if e.Name != "transfer.completed" {
		return fmt.Errorf("%s: %s", e.Name, e.Data)
	}
	elapsed := time.Since(started)
	fmt.Printf("done: %s in %s (%s)\n", human(p.total), elapsed.Round(time.Millisecond), rate(p.total, elapsed))
	return nil
}

// deliver uploads the Payload, and picks it up again from wherever the relay
// stopped if the connection breaks.
//
// The Recipient never notices: its response stays open across the gap, and the
// Owner holds the archive position — which is also why this cannot survive the
// Owner dying (ADR 0008).
func (c *client) deliver(ctx context.Context, s *stream, id string, p payload) error {
	from := position{}
	for attempt := 1; ; attempt++ {
		err := c.upload(ctx, id, p, from)
		if err == nil {
			return nil
		}
		if attempt >= c.attempts {
			return fmt.Errorf("gave up after %d attempt(s) at %s: %w", attempt, from, err)
		}

		var failed *uploadError
		if errors.As(err, &failed) && failed.mismatch {
			// The one failure a Sender can fix by itself: it asked to start in the
			// wrong place and has been told the right one.
			fmt.Printf("wrong resume position (%s); the relay is at %s\n", from, failed.expected)
			from = failed.expected
			continue
		}
		fmt.Printf("interrupted after %s: %v — waiting for the relay to invite us back\n", from, err)

		next, err := awaitResume(ctx, s, id)
		if err != nil {
			return err
		}
		if next == from {
			// Nothing moved, so retrying will fail the same way. Say so rather
			// than spin.
			return fmt.Errorf("stuck at %s: nothing was relayed on the last attempt", from)
		}
		from = next
		fmt.Printf("resuming from %s (%s to go)\n", from, human(p.remaining(from)))
	}
}

// remaining is how much of the Payload is still to be sent from a position.
func (p payload) remaining(from position) int64 {
	var left int64
	for i, e := range p.entries {
		switch {
		case i < from.entry:
		case i == from.entry:
			left += max(0, e.size-from.offset)
		default:
			left += e.size
		}
	}
	return left
}

// upload streams the Payload as multipart/form-data through an io.Pipe, so no
// part of it is ever held in memory. The body function can be called twice,
// because the request may be redirected to the Transfer's Owner.
//
// from says where to start. Entries the relay already has are skipped entirely,
// and a half-delivered one is seeked into — a file on disk is seekable, so
// resuming costs nothing but the syscall.
func (c *client) upload(ctx context.Context, id string, p payload, from position) error {
	// The boundary is fixed up front, not taken from whichever writer happens to
	// be built first. A retried request declares the Content-Type of the attempt
	// before it, so a fresh multipart.Writer with a fresh random boundary means
	// the server looks for a delimiter that is not in the body and reads the
	// whole upload as one header — which surfaces as "bufio: buffer full",
	// nowhere near the cause.
	boundary := "spudspudspud" + strings.ToLower(rand.Text()[:20])
	contentType := "multipart/form-data; boundary=" + boundary

	body := func() (io.ReadCloser, error) {
		pr, pw := io.Pipe()
		mw := multipart.NewWriter(pw)
		if err := mw.SetBoundary(boundary); err != nil {
			return nil, err
		}

		go func() {
			var err error
			defer func() { pw.CloseWithError(err) }()
			for i, e := range p.entries {
				if i < from.entry {
					continue // the relay already has this one
				}
				var w io.Writer
				if w, err = mw.CreateFormFile("files", e.rel); err != nil {
					return
				}
				var f *os.File
				if f, err = os.Open(e.path); err != nil {
					return
				}
				if i == from.entry && from.offset > 0 {
					if _, err = f.Seek(from.offset, io.SeekStart); err != nil {
						f.Close()
						return
					}
				}
				_, err = io.Copy(w, f)
				f.Close()
				if err != nil {
					return
				}
			}
			err = mw.Close()
		}()
		return pr, nil
	}

	// body is called again if the request is redirected to the Transfer's Owner:
	// a fresh pipe, reading the files off disk a second time. That is the whole
	// reason spud handles 307 itself instead of letting net/http do it.
	res, err := c.do(ctx, "POST", c.base+"/d/"+id, body, contentType, func(r *http.Request) {
		// Always sent, not only when resuming: an explicit 0/0 is clearer than an
		// absent header meaning the same thing.
		r.Header.Set(relay.HeaderEntryIndex, strconv.Itoa(from.entry))
		r.Header.Set(relay.HeaderEntryOffset, strconv.FormatInt(from.offset, 10))
	})
	if err != nil {
		// The request body broke before the server could answer, which is the
		// usual shape of an interrupted upload.
		return &uploadError{cause: err, expected: from}
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusNoContent {
		return &uploadError{
			status:   res.StatusCode,
			message:  errorFrom(res),
			expected: positionFrom(res),
			mismatch: res.StatusCode == http.StatusConflict &&
				res.Header.Get(relay.HeaderExpectedOffset) != "",
		}
	}
	return nil
}

// resolve turns a path into a declared Payload.
func resolve(path string) (payload, error) {
	info, err := os.Stat(path)
	if err != nil {
		return payload{}, err
	}
	if !info.IsDir() {
		return payload{
			name:  info.Name(),
			kind:  "file",
			total: info.Size(),
			entries: []entry{
				{rel: info.Name(), path: path, size: info.Size()},
			},
		}, nil
	}

	root := filepath.Clean(path)
	base := filepath.Base(root)
	p := payload{name: base, kind: "folder"}
	err = filepath.WalkDir(root, func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		p.entries = append(p.entries, entry{
			rel:  filepath.ToSlash(filepath.Join(base, rel)),
			path: name,
			size: info.Size(),
		})
		p.total += info.Size()
		return nil
	})
	if err != nil {
		return payload{}, err
	}
	if len(p.entries) == 0 {
		return payload{}, fmt.Errorf("%s holds no regular files", path)
	}
	return p, nil
}

// lookup resolves a recipient by ID or display name from the online list.
func lookup(snap snapshot, to string) (user, error) {
	for _, u := range snap.Users {
		if u.ID == to || u.DisplayName == to || u.Email == to {
			return u, nil
		}
	}
	names := make([]string, 0, len(snap.Users))
	for _, u := range snap.Users {
		names = append(names, u.DisplayName)
	}
	return user{}, fmt.Errorf("%q is not online; online now: %v", to, names)
}
