package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"time"
)

// client is a session against one entry point.
type client struct {
	base string
	hc   *http.Client
	self user
	// attempts is how many times a broken transfer is picked up again before
	// giving up. One means no resume at all.
	attempts int
}

type user struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"displayName"`
}

// dial logs in (or signs up) and keeps the session cookie.
func dial(ctx context.Context, base, email, password, name string, signup bool, attempts int) (*client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	if attempts < 1 {
		attempts = 1
	}
	c := &client{
		base:     strings.TrimRight(base, "/"),
		attempts: attempts,
		hc: &http.Client{
			Jar: jar,
			// Redirects are handled by hand. net/http will not replay a
			// streaming body across a 307, and the ownership redirect applies to
			// exactly the requests whose bodies stream (ADR 0007).
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			// No client-wide timeout: an SSE stream and a multi-gigabyte upload
			// both outlive any value worth setting.
			Timeout: 0,
		},
	}

	path, body := "/api/login", map[string]string{"email": email, "password": password}
	if signup {
		path = "/api/signup"
		body["displayName"] = name
	}
	res, err := c.postJSON(ctx, c.base+path, body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("%s: %s", path, errorFrom(res))
	}
	if err := json.NewDecoder(res.Body).Decode(&c.self); err != nil {
		return nil, fmt.Errorf("decode self: %w", err)
	}
	return c, nil
}

// postJSON sends one JSON request, following an ownership redirect. The body is
// held in memory, so replaying it is free.
func (c *client) postJSON(ctx context.Context, url string, body any) (*http.Response, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return c.do(ctx, "POST", url, func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(encoded)), nil
	}, "application/json")
}

// do performs a request and follows one 307 by rebuilding the body from
// scratch, which is the only way a streaming upload can survive it.
func (c *client) do(ctx context.Context, method, url string, body func() (io.ReadCloser, error), contentType string, decorate ...func(*http.Request)) (*http.Response, error) {
	// One hop is all ownership ever needs: the redirect target is the Owner, and
	// the Owner does not redirect.
	for range 2 {
		var rc io.ReadCloser
		if body != nil {
			var err error
			if rc, err = body(); err != nil {
				return nil, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, url, rc)
		if err != nil {
			return nil, err
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		for _, fn := range decorate {
			fn(req)
		}

		res, err := c.hc.Do(req)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusTemporaryRedirect {
			return res, nil
		}
		next := res.Header.Get("Location")
		res.Body.Close()
		if next == "" {
			return nil, fmt.Errorf("307 with no Location")
		}
		url = next
	}
	return nil, fmt.Errorf("redirected more than once, which ownership never does")
}

func errorFrom(res *http.Response) string {
	var body struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil || body.Error == "" {
		return res.Status
	}
	return fmt.Sprintf("%s (%s): %s", res.Status, body.Error, body.Message)
}

// event is one parsed SSE frame.
type event struct {
	ID   string
	Name string
	Data json.RawMessage
}

// stream is an open control-plane connection.
type stream struct {
	res    *http.Response
	events chan event
	err    error
}

// open starts GET /events and parses frames in the background.
func (c *client) open(ctx context.Context) (*stream, error) {
	res, err := c.do(ctx, "GET", c.base+"/events", nil, "")
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		defer res.Body.Close()
		return nil, fmt.Errorf("GET /events: %s", errorFrom(res))
	}

	s := &stream{res: res, events: make(chan event, 64)}
	go s.read()
	return s, nil
}

func (s *stream) close() { s.res.Body.Close() }

// read splits the body on blank lines, which is what an SSE frame is terminated
// by, and is the entire parser.
func (s *stream) read() {
	defer close(s.events)

	sc := bufio.NewScanner(s.res.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	sc.Split(splitFrames)

	for sc.Scan() {
		if e, ok := parseFrame(sc.Text()); ok {
			s.events <- e
		}
	}
	s.err = sc.Err()
}

// splitFrames is a bufio.SplitFunc that yields one frame per blank line.
func splitFrames(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if i := bytes.Index(data, []byte("\n\n")); i >= 0 {
		return i + 2, data[:i], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func parseFrame(frame string) (event, bool) {
	var e event
	var data strings.Builder
	for _, line := range strings.Split(frame, "\n") {
		switch {
		case strings.HasPrefix(line, ":"):
			// A heartbeat comment. Nothing to report, but the connection is
			// alive, which is the whole point of it.
		case strings.HasPrefix(line, "id: "):
			e.ID = line[4:]
		case strings.HasPrefix(line, "event: "):
			e.Name = line[7:]
		case strings.HasPrefix(line, "data: "):
			data.WriteString(line[6:])
		}
	}
	if e.Name == "" {
		return event{}, false
	}
	e.Data = json.RawMessage(data.String())
	return e, true
}

// await waits for one of the named events, ignoring the rest.
func (s *stream) await(ctx context.Context, names ...string) (event, error) {
	for {
		select {
		case <-ctx.Done():
			return event{}, ctx.Err()
		case e, ok := <-s.events:
			if !ok {
				if s.err != nil {
					return event{}, s.err
				}
				return event{}, fmt.Errorf("the stream ended while waiting for %s", strings.Join(names, " or "))
			}
			for _, name := range names {
				if e.Name == name {
					return e, nil
				}
			}
		}
	}
}

// snapshot is the first frame on every Stream.
type snapshot struct {
	Self      user           `json:"self"`
	StreamID  string         `json:"streamId"`
	Instance  string         `json:"instance"`
	Users     []user         `json:"users"`
	Transfers []transferView `json:"transfers"`
}

type transferView struct {
	ID        string `json:"id"`
	State     string `json:"state"`
	Sender    string `json:"sender"`
	Recipient string `json:"recipient"`
	Payload   struct {
		Name       string `json:"name"`
		Kind       string `json:"kind"`
		TotalBytes int64  `json:"totalBytes"`
		EntryCount int    `json:"entryCount"`
	} `json:"payload"`
}

func (s *stream) snapshot(ctx context.Context) (snapshot, error) {
	e, err := s.await(ctx, "snapshot")
	if err != nil {
		return snapshot{}, err
	}
	var snap snapshot
	if err := json.Unmarshal(e.Data, &snap); err != nil {
		return snapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return snap, nil
}

func rate(bytes int64, since time.Duration) string {
	if since <= 0 {
		return "—"
	}
	return fmt.Sprintf("%s/s", human(int64(float64(bytes)/since.Seconds())))
}

func human(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value, units := float64(n), []string{"KiB", "MiB", "GiB", "TiB"}
	i := -1
	for value >= unit && i < len(units)-1 {
		value /= unit
		i++
	}
	return fmt.Sprintf("%.1f %s", value, units[i])
}
