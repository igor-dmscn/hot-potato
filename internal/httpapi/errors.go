package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// The error envelope from DESIGN §6. Every non-2xx response in the API is one
// of these, so a client has exactly one shape to parse.
type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// Error codes. DESIGN §6 lists the domain ones; bad_request and internal are
// added here because malformed input and a broken dependency have to say
// something too.
const (
	codeUnauthorized         = "unauthorized"
	codeForbidden            = "forbidden"
	codeNotFound             = "not_found"
	codeIllegalState         = "illegal_state"
	codeRecipientNotAttached = "recipient_not_attached"
	codeAlreadyAttached      = "already_attached"
	codeRendezvousTimeout    = "rendezvous_timeout"
	codePayloadTooLarge      = "payload_too_large"
	codeTooManyTransfers     = "too_many_transfers"
	codeRateLimited          = "rate_limited"
	codeDraining             = "draining"
	codeBadRequest           = "bad_request"
	codeInternal             = "internal"
)

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status and headers are already committed, so there is nowhere to
		// report this but the log.
		slog.Debug("write response", "err", err)
	}
}

// internalError logs the cause and tells the client nothing about it.
func internalError(w http.ResponseWriter, r *http.Request, err error) {
	slog.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, codeInternal, "something went wrong")
}
