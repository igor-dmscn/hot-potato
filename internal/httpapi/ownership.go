package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"hotpotato/internal/transfer"
)

// Directory resolves an instance ID to the base URL that reaches it.
type Directory interface {
	Lookup(ctx context.Context, instance string) (string, error)
}

// LocalDirectory knows about one instance: itself. It is what a single-process
// deployment uses, and what makes the ownership middleware safe to always
// install.
type LocalDirectory struct {
	Instance string
	BaseURL  string
}

var errUnknownInstance = errors.New("no such instance")

func (d LocalDirectory) Lookup(_ context.Context, instance string) (string, error) {
	if instance == d.Instance {
		return d.BaseURL, nil
	}
	return "", errUnknownInstance
}

// ownership sends any request about a Transfer this instance does not own to the
// instance that does.
//
// This is the asymmetry the whole design turns on: the control plane scales by
// fanout, because every instance hears every event; the data plane scales by
// affinity, because a rendezvous holds a live reader and a live writer and
// neither can be serialised, shared or moved (ADR 0007).
//
// It wraps acceptance and denial as well as byte transport, because those mutate
// state that lives in the Owner's memory too.
func (a *api) ownership(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := transfer.ID(r.PathValue("id"))
		owner := transfer.OwnerOf(id)
		switch {
		case owner == "":
			writeError(w, http.StatusBadRequest, codeBadRequest, "that is not a transfer id")
			return
		case owner == a.instance:
			next.ServeHTTP(w, r)
			return
		}

		base, err := a.directory.Lookup(r.Context(), owner)
		if err != nil {
			// The Owner is gone, and so is the Transfer: its state was that
			// process's memory. Nothing to redirect to and nothing to recover.
			slog.Info("owner is unreachable", "transfer", id, "owner", owner, "err", err)
			writeError(w, http.StatusNotFound, codeNotFound,
				"the instance holding that transfer is gone")
			return
		}

		// 307, not 302: 302 is allowed to turn a POST into a GET, which would
		// silently drop the body of an upload. Note that a client following this
		// must be able to replay its body — for a streaming upload that means
		// the client has to handle the redirect itself.
		http.Redirect(w, r, base+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	})
}
