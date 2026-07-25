package presence

import (
	"context"
	"testing"
	"time"
)

type announcement struct {
	event string
	user  User
}

// recorder collects announcements on a channel, so tests wait on a deadline
// instead of sleeping for a grace period.
func recorder() (Options, chan announcement) {
	ch := make(chan announcement, 16)
	return Options{
		Grace:    20 * time.Millisecond,
		Announce: func(e string, u User) { ch <- announcement{e, u} },
	}, ch
}

func next(t *testing.T, ch chan announcement) announcement {
	t.Helper()
	select {
	case a := <-ch:
		return a
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an announcement")
		return announcement{}
	}
}

func silent(t *testing.T, ch chan announcement, within time.Duration) {
	t.Helper()
	select {
	case a := <-ch:
		t.Fatalf("unexpected %s for %s", a.event, a.user.ID)
	case <-time.After(within):
	}
}

func TestOnlineAnnouncesOnceForManyStreams(t *testing.T) {
	t.Parallel()
	o, ch := recorder()
	p := NewMemory(o)
	defer p.Close()
	ctx := context.Background()
	ana := User{ID: "u_1", DisplayName: "ana"}

	if err := p.Online(ctx, ana); err != nil {
		t.Fatalf("Online: %v", err)
	}
	if a := next(t, ch); a.event != EventOnline || a.user.ID != "u_1" {
		t.Fatalf("first Stream announced %+v, want user.online for u_1", a)
	}

	// A second tab is not a second arrival.
	if err := p.Online(ctx, ana); err != nil {
		t.Fatalf("Online: %v", err)
	}
	silent(t, ch, 50*time.Millisecond)

	users, err := p.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(users) != 1 || users[0].DisplayName != "ana" {
		t.Errorf("List = %+v, want one ana", users)
	}
}

func TestOfflineAnnouncesAfterTheGraceWindow(t *testing.T) {
	t.Parallel()
	o, ch := recorder()
	p := NewMemory(o)
	defer p.Close()
	ctx := context.Background()

	p.Online(ctx, User{ID: "u_1", DisplayName: "ana"})
	next(t, ch) // the online

	if err := p.Offline(ctx, "u_1"); err != nil {
		t.Fatalf("Offline: %v", err)
	}
	if a := next(t, ch); a.event != EventOffline || a.user.DisplayName != "ana" {
		t.Fatalf("announced %+v, want user.offline carrying the display name", a)
	}
	if users, _ := p.List(ctx); len(users) != 0 {
		t.Errorf("List = %+v after the grace window, want empty", users)
	}
}

// The whole point of the grace window: a refresh must not make a User flicker.
func TestReconnectInsideTheGraceWindowAnnouncesNothing(t *testing.T) {
	t.Parallel()
	o, ch := recorder()
	p := NewMemory(o)
	defer p.Close()
	ctx := context.Background()
	ana := User{ID: "u_1", DisplayName: "ana"}

	p.Online(ctx, ana)
	next(t, ch)

	p.Offline(ctx, "u_1")
	p.Online(ctx, ana) // the refreshed tab, well inside 20ms

	silent(t, ch, 3*o.Grace)
	if users, _ := p.List(ctx); len(users) != 1 {
		t.Errorf("List = %+v, want ana still present", users)
	}
}

func TestOfflineForAnAbsentUserIsHarmless(t *testing.T) {
	t.Parallel()
	o, ch := recorder()
	p := NewMemory(o)
	defer p.Close()

	if err := p.Offline(context.Background(), "u_nobody"); err != nil {
		t.Fatalf("Offline: %v", err)
	}
	silent(t, ch, 3*o.Grace)
}

func TestListIsSorted(t *testing.T) {
	t.Parallel()
	o, _ := recorder()
	p := NewMemory(o)
	defer p.Close()
	ctx := context.Background()

	for _, u := range []User{{"u_3", "cara"}, {"u_1", "ana"}, {"u_2", "bea"}} {
		p.Online(ctx, u)
	}
	users, _ := p.List(ctx)
	for i, want := range []string{"ana", "bea", "cara"} {
		if users[i].DisplayName != want {
			t.Fatalf("List = %+v, want ana, bea, cara", users)
		}
	}
}
