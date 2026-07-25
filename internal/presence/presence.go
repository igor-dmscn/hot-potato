// Package presence tracks which Users currently hold at least one Stream.
//
// Losing the last Stream starts a short grace window rather than announcing an
// absence immediately, so a browser refresh does not make a User flicker out of
// everyone's list.
package presence

import (
	"context"
	"time"
)

// User is presence's own two-field view. It is declared here rather than
// imported from internal/auth: borrowing a type would make every consumer of
// presence depend on the identity package too.
type User struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

// Event names, matching DESIGN §7.
const (
	EventOnline  = "user.online"
	EventOffline = "user.offline"
)

type Presence interface {
	// Online marks u present. Calling it for a User who is already present
	// cancels a pending grace window and announces nothing.
	Online(ctx context.Context, u User) error
	// Offline starts the grace window. The announcement happens when it expires
	// with the User still absent.
	Offline(ctx context.Context, userID string) error
	List(ctx context.Context) ([]User, error)
	Close() error
}

// Options is shared by both implementations.
type Options struct {
	// Grace is how long a User may hold no Streams before being announced
	// offline. DESIGN §9 says 10s; tests use milliseconds.
	Grace time.Duration
	// Announce is called with EventOnline or EventOffline. Presence does not
	// import the bus: whoever wires it decides what an announcement is.
	Announce func(event string, u User)
}
