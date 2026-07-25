package relay

import (
	"fmt"
	"path"
	"strings"
	"unicode"
)

// maxComponent and maxPath bound an entry name. A browser can be made to send
// anything here, and the value ends up inside an archive somebody else will
// unpack.
const (
	maxComponent = 100
	maxPath      = 200
)

// SanitizeEntry turns a browser-supplied relative path into a zip entry name
// that cannot escape the archive when it is unpacked.
//
// FormData.append("files", file, file.webkitRelativePath) is what puts relative
// paths in a part's filename — without that third argument the folder structure
// is lost — so this input is genuinely useful and genuinely untrusted.
func SanitizeEntry(raw string, index int) string {
	// Windows separators first: a "\" is a path separator to the unpacker and an
	// ordinary character to path.Clean.
	raw = strings.ReplaceAll(raw, `\`, "/")

	var parts []string
	for _, component := range strings.Split(raw, "/") {
		component = strings.TrimSpace(cleanComponent(component))
		switch component {
		case "", ".", "..":
			// Drops ../ and leading slashes both: an absolute path arrives as an
			// empty first component.
			continue
		}
		if i := strings.Index(component, ":"); i >= 0 {
			// "c:secret" — a drive-relative path on Windows.
			component = component[i+1:]
			if component == "" {
				continue
			}
		}
		if len(component) > maxComponent {
			component = component[:maxComponent]
		}
		parts = append(parts, component)
	}

	name := path.Join(parts...)
	if len(name) > maxPath {
		name = name[len(name)-maxPath:]
	}
	if name == "" {
		// Something was sent, and it sanitised down to nothing. It still has to
		// land somewhere nameable.
		return fmt.Sprintf("entry-%d", index)
	}
	return name
}

// cleanComponent strips what a filesystem or a terminal would rather not see.
func cleanComponent(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case unicode.IsControl(r), r == 0x7f:
			return -1
		default:
			return r
		}
	}, s)
}
