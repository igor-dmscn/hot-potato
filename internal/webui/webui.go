// Package webui serves the harness out of the binary.
//
// The files live in public/ inside this package because //go:embed patterns
// are relative to the package directory and cannot traverse upward —
// all:../../web is a compile error, not a path. The all: prefix matters too:
// without it, files beginning with _ or . are silently skipped.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed all:public
var files embed.FS

// Handler serves the harness. The embedded FS is compiled in, so a failure to
// open the subtree is a build bug, not a runtime condition.
func Handler() http.Handler {
	sub, err := fs.Sub(files, "public")
	if err != nil {
		panic(err)
	}
	return http.FileServerFS(sub)
}
