// Package apiresponse writes successful API responses as JSON.
package apiresponse

import (
	"encoding/json"
	"net/http"
)

// Write writes value as a JSON response with the supplied HTTP status.
func Write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
