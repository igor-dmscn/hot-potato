package transfer

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This package's whole value is that the rules can be read and tested without a
// server, so the two properties that guarantee it are asserted rather than
// hoped for.

func TestPackageDoesNotImportNetHTTP(t *testing.T) {
	t.Parallel()

	pkgs, err := parser.ParseDir(token.NewFileSet(), ".",
		func(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") },
		parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parsing the package: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("parsed no packages; this test is not looking where it thinks")
	}

	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			for _, imp := range file.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				if path == "net/http" || strings.HasPrefix(path, "net/http/") {
					t.Errorf("%s imports %s; the state machine must stay usable without a server", name, path)
				}
			}
		}
	}
}

// now is a parameter to every transition. A clock read inside the logic would
// mean testing expiry by sleeping for a minute.
func TestPackageDoesNotReadTheClock(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(src), "time.Now()") {
			t.Errorf("%s calls time.Now(); pass now in instead", name)
		}
	}
}
