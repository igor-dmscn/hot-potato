// Command spud is a Hot Potato client for a terminal: watch the control plane,
// send a file or folder, receive one, or open ten thousand idle Streams.
//
// It exists to prove the protocol is a protocol — that nothing in it depends on
// being a browser — and to be the load generator for phase 10.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func usage() {
	fmt.Fprint(os.Stderr, `spud — a Hot Potato client

usage:
  spud [flags] watch                    print control-plane events as they arrive
  spud [flags] send <to> <path>         offer and stream a file or directory
  spud [flags] recv <dir>               accept the next offer and write it to dir
  spud [flags] load                     open -streams idle Streams and hold them

flags:
`)
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, `
The recipient of `+"`send`"+` may be a user ID or a display name; spud resolves it
from the online list in its own snapshot.
`)
}

func main() {
	base := flag.String("base", envOr("SPUD_BASE", "http://localhost:8080"), "server base URL")
	email := flag.String("email", os.Getenv("SPUD_EMAIL"), "email to log in with")
	password := flag.String("password", os.Getenv("SPUD_PASSWORD"), "password to log in with")
	signup := flag.Bool("signup", false, "create the account instead of logging in")
	name := flag.String("name", "spud", "display name, when signing up")
	streams := flag.Int("streams", 100, "number of idle Streams for `load`")
	timeout := flag.Duration("timeout", 0, "give up after this long (0 waits forever)")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() == 0 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	if *email == "" || *password == "" {
		fail(fmt.Errorf("-email and -password are required (or SPUD_EMAIL / SPUD_PASSWORD)"))
	}
	c, err := dial(ctx, *base, *email, *password, *name, *signup)
	if err != nil {
		fail(err)
	}

	args := flag.Args()
	switch args[0] {
	case "watch":
		err = watch(ctx, c)
	case "send":
		if len(args) != 3 {
			fail(fmt.Errorf("usage: spud send <to> <path>"))
		}
		err = send(ctx, c, args[1], args[2])
	case "recv":
		if len(args) != 2 {
			fail(fmt.Errorf("usage: spud recv <dir>"))
		}
		err = recv(ctx, c, args[1])
	case "load":
		err = load(ctx, c, *streams)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil && ctx.Err() == nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "spud:", err)
	os.Exit(1)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
