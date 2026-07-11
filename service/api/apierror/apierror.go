// Package apierror provides HTTP-aware errors and writes a consistent JSON
// error response for API handlers.
package apierror

import (
	"encoding/json"
	"errors"
	"net/http"
)

const internalServerErrorMessage = "internal server error"

// Error is an error that can be safely returned to an API client.
type Error struct {
	Status  int
	Message string
	cause   error
}

// New creates an API error with an HTTP status and a client-facing message.
func New(status int, message string) *Error {
	return &Error{Status: status, Message: message}
}

// Wrap creates an API error while retaining the original error for errors.Is
// and errors.As checks.
func Wrap(status int, message string, cause error) *Error {
	return &Error{Status: status, Message: message, cause: cause}
}

func (err *Error) Error() string {
	return err.Message
}

func (err *Error) Unwrap() error {
	return err.cause
}

// Write writes err as a JSON response. Errors that are not API errors are
// deliberately returned as a generic 500 response so internal details do not
// leak to clients.
func Write(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}

	status := http.StatusInternalServerError
	message := internalServerErrorMessage
	var apiErr *Error
	if errors.As(err, &apiErr) {
		status = apiErr.Status
		message = apiErr.Message
	}
	if status < 400 || status > 599 {
		status = http.StatusInternalServerError
		message = internalServerErrorMessage
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: message})
}
