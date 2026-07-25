package relay

import (
	"strings"
	"testing"
)

// Every one of these is a real thing a browser or a scripted client can be made
// to send in a part's filename, and every one of them ends up inside an archive
// somebody else unpacks.
func TestSanitizeEntry(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		raw  string
		want string
	}{
		"an ordinary relative path": {"docs/notes.txt", "docs/notes.txt"},
		"deeply nested":             {"a/b/c/d.txt", "a/b/c/d.txt"},
		"dot segments removed":      {"docs/./notes.txt", "docs/notes.txt"},
		"parent escape":             {"../../etc/passwd", "etc/passwd"},
		"parent escape mid-path":    {"docs/../../secrets", "docs/secrets"},
		"absolute":                  {"/etc/passwd", "etc/passwd"},
		"windows separators":        {`docs\sub\a.txt`, "docs/sub/a.txt"},
		"windows escape":            {`..\..\Windows\System32`, "Windows/System32"},
		"drive letter":              {`C:\secrets.txt`, "secrets.txt"},
		"drive relative":            {"C:secrets.txt", "secrets.txt"},
		"control characters":        {"a\x00b\x1fc.txt", "abc.txt"},
		"newline":                   {"a\nb.txt", "ab.txt"},
		"only dots":                 {"../..", "entry-3"},
		"empty":                     {"", "entry-3"},
		"just a slash":              {"/", "entry-3"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := SanitizeEntry(tc.raw, 3)
			if got != tc.want {
				t.Errorf("SanitizeEntry(%q) = %q, want %q", tc.raw, got, tc.want)
			}
			assertContained(t, got)
		})
	}
}

func TestSanitizeEntryBoundsLength(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 500) + "/" + strings.Repeat("b", 500)
	got := SanitizeEntry(long, 1)
	if len(got) > maxPath {
		t.Errorf("name is %d bytes, want at most %d", len(got), maxPath)
	}
	assertContained(t, got)
}

// assertContained is the property that actually matters: whatever comes out,
// unpacking it cannot write outside the destination directory.
func assertContained(t *testing.T, name string) {
	t.Helper()
	switch {
	case name == "":
		t.Error("empty entry name")
	case strings.HasPrefix(name, "/"), strings.HasPrefix(name, `\`):
		t.Errorf("%q is absolute", name)
	case strings.Contains(name, ".."):
		t.Errorf("%q still contains a parent reference", name)
	case strings.Contains(name, `\`):
		t.Errorf("%q still contains a backslash", name)
	case strings.Contains(name, ":"):
		t.Errorf("%q still contains a colon", name)
	}
}
