package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// load opens n idle Streams and holds them, reporting how many are up.
//
// This is phase 10's measuring instrument: one goroutine per Stream is fine at
// ten thousand, one *timer* per Stream is not, and the only way to find out which
// the server does is to open ten thousand.
func load(ctx context.Context, c *client, n int) error {
	var (
		open    atomic.Int64
		failed  atomic.Int64
		frames  atomic.Int64
		wg      sync.WaitGroup
		streams = make([]*stream, n)
	)

	started := time.Now()
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Being told to stop is not a failure. Counting it as one makes the
			// exit code depend on how many Streams happened to be mid-handshake
			// when the context ended.
			s, err := c.open(ctx)
			if err != nil {
				if ctx.Err() == nil {
					failed.Add(1)
				}
				return
			}
			streams[i] = s
			if _, err := s.snapshot(ctx); err != nil {
				if ctx.Err() == nil {
					failed.Add(1)
				}
				return
			}
			open.Add(1)

			// Drain, so a Stream that overflows is the server's decision and not
			// this client's fault.
			for range s.events {
				frames.Add(1)
			}
		}()
	}

	// Report until the context ends, then let go of everything.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			for _, s := range streams {
				if s != nil {
					s.close()
				}
			}
			wg.Wait()
			fmt.Printf("closed %d streams after %s\n", open.Load(), time.Since(started).Round(time.Second))
			if f := failed.Load(); f > 0 {
				return fmt.Errorf("%d of %d streams never opened", f, n)
			}
			return nil
		case <-ticker.C:
			fmt.Printf("%s  open=%d failed=%d events=%d\n",
				time.Since(started).Round(time.Second), open.Load(), failed.Load(), frames.Load())
		}
	}
}
