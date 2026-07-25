package main

import (
	"context"
	"fmt"
)

// watch prints every event on this User's Stream.
func watch(ctx context.Context, c *client) error {
	s, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer s.close()

	snap, err := s.snapshot(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%s on %s as %s (%d online, %d transfers)\n",
		snap.StreamID, snap.Instance, snap.Self.DisplayName, len(snap.Users), len(snap.Transfers))

	for {
		select {
		case <-ctx.Done():
			return nil
		case e, ok := <-s.events:
			if !ok {
				return s.err
			}
			fmt.Printf("%-20s %s\n", e.Name, e.Data)
		}
	}
}
