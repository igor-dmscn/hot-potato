package httpapi

import (
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strings"
)

// maxJSONBody bounds every control-plane request body. The data plane is
// unbounded by design; a JSON envelope is not.
const maxJSONBody = 64 << 10

// requireJSON is half of the CSRF defence, the other half being SameSite=Lax
// on the session cookie: a cross-site HTML form can only send
// application/x-www-form-urlencoded, multipart/form-data or text/plain, and
// cannot set application/json without a preflight the browser will refuse.
//
// It wraps every state-changing control-plane route, including ones with no
// body, because the point is the header, not the payload.
func requireJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct := r.Header.Get("Content-Type")
		if ct == "" {
			writeError(w, http.StatusUnsupportedMediaType, codeBadRequest,
				"Content-Type: application/json is required")
			return
		}
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || !strings.EqualFold(mt, "application/json") {
			writeError(w, http.StatusUnsupportedMediaType, codeBadRequest,
				"Content-Type: application/json is required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// decode reads a bounded, strict JSON body. Unknown fields are an error: a
// client sending "displayname" should be told, not silently ignored.
func decode(w http.ResponseWriter, r *http.Request, into any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxJSONBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, codeBadRequest, "body too large")
			return false
		}
		writeError(w, http.StatusBadRequest, codeBadRequest, "body is not the expected JSON: "+err.Error())
		return false
	}
	return true
}
