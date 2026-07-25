package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// recv waits for an offer, accepts it, and writes the Payload to dir.
func recv(ctx context.Context, c *client, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
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
	fmt.Printf("%s waiting for an offer on %s\n", snap.Self.DisplayName, snap.Instance)

	offered, err := nextOffer(ctx, s, snap)
	if err != nil {
		return err
	}
	fmt.Printf("offered %s (%s, %s) from %s\n",
		offered.Payload.Name, offered.Payload.Kind, human(offered.Payload.TotalBytes), offered.Sender)

	res, err := c.postJSON(ctx, c.base+"/api/transfers/"+offered.ID+"/accept",
		map[string]string{"streamId": snap.StreamID})
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusAccepted {
		defer res.Body.Close()
		return fmt.Errorf("accept: %s", errorFrom(res))
	}
	res.Body.Close()

	// The GET parks until the Sender attaches, which is why nothing is written
	// until the first byte arrives.
	started := time.Now()
	written, path, err := c.fetch(ctx, offered.ID, dir, offered.Payload.Kind)
	if err != nil {
		return err
	}
	elapsed := time.Since(started)
	fmt.Printf("wrote %s (%s) in %s (%s)\n",
		path, human(written), elapsed.Round(time.Millisecond), rate(written, elapsed))

	// The outcome is the control plane's to report, not the download's.
	final, err := s.await(ctx, "transfer.completed", "transfer.failed", "transfer.canceled")
	if err != nil {
		return err
	}
	if final.Name != "transfer.completed" {
		return fmt.Errorf("%s: %s", final.Name, final.Data)
	}
	return nil
}

// nextOffer takes a pending offer from the snapshot if there is one, and waits
// for the next otherwise.
//
// Checking the snapshot first is not an optimisation: a client that connects
// after the offer was announced would otherwise never see it, and this is
// exactly what a snapshot-on-connect design is for (ADR 0003).
func nextOffer(ctx context.Context, s *stream, snap snapshot) (transferView, error) {
	for _, t := range snap.Transfers {
		if t.State == "pending" && t.Recipient == snap.Self.ID {
			return t, nil
		}
	}

	e, err := s.await(ctx, "transfer.offered")
	if err != nil {
		return transferView{}, err
	}
	var offered transferView
	if err := json.Unmarshal(e.Data, &offered); err != nil {
		return transferView{}, err
	}
	return offered, nil
}

// fetch downloads the Payload, reconnecting with Range if the relay breaks.
//
// Only a single file can be resumed this way. A folder arrives as a zip built as
// it streams, so a broken folder download has to start over — the server answers
// 416 to a non-zero Range on one, and there is nothing to be done about it
// (ADR 0004).
func (c *client) fetch(ctx context.Context, id, dir, kind string) (int64, string, error) {
	var have int64
	var path string

	for attempt := 1; ; attempt++ {
		n, p, err := c.download(ctx, id, dir, have)
		have += n
		if p != "" {
			path = p
		}
		if err == nil {
			return have, path, nil
		}
		if kind != "file" || attempt >= c.attempts {
			if have > 0 {
				return have, path, fmt.Errorf("%w — %s of a %s is on disk at %s",
					err, human(have), kind, path)
			}
			return have, path, err
		}
		// The offset comes from what is on disk, not from anything the server
		// said. The Owner counts bytes it wrote into a socket, which after a
		// disconnection is more than came out of it; only this end knows what it
		// actually holds.
		fmt.Printf("download interrupted with %s on disk: %v — reconnecting from there\n",
			human(have), err)
	}
}

// download streams GET /d/{id} straight to disk, starting at from.
func (c *client) download(ctx context.Context, id, dir string, from int64) (int64, string, error) {
	res, err := c.do(ctx, "GET", c.base+"/d/"+id, nil, "", func(r *http.Request) {
		if from > 0 {
			r.Header.Set("Range", fmt.Sprintf("bytes=%d-", from))
		}
	})
	if err != nil {
		return 0, "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusPartialContent {
		return 0, "", fmt.Errorf("download: %s", errorFrom(res))
	}

	name := filenameFrom(res.Header.Get("Content-Disposition"), id)
	// The server sanitises entry names inside an archive; this is the same
	// caution applied to the filename it hands back.
	path := filepath.Join(dir, filepath.Base(filepath.Clean("/"+name)))

	// Append when resuming, truncate when starting: the partial file *is* the
	// resume state, so it must survive a failed attempt.
	flags := os.O_CREATE | os.O_WRONLY
	if from > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return 0, path, err
	}
	defer f.Close()

	written, err := io.Copy(f, res.Body)
	if err != nil {
		return written, path, fmt.Errorf("relay ended early after %s: %w", human(written), err)
	}
	return written, path, nil
}

func filenameFrom(header, fallback string) string {
	if header == "" {
		return fallback
	}
	_, params, err := mime.ParseMediaType(header)
	if err != nil || params["filename"] == "" {
		return fallback
	}
	return params["filename"]
}
