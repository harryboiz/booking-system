package apierror

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteAPIError(t *testing.T) {
	response := httptest.NewRecorder()
	Write(response, New(http.StatusBadRequest, "invalid request"))

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", response.Code)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	var body map[string]string
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["error"] != "invalid request" {
		t.Fatalf("error = %q", body["error"])
	}
}

func TestWriteHidesInternalError(t *testing.T) {
	response := httptest.NewRecorder()
	Write(response, errors.New("database password leaked"))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", response.Code)
	}
	if got := response.Body.String(); got != "{\"error\":\"internal server error\"}\n" {
		t.Fatalf("body = %q", got)
	}
}

func TestWrapRetainsCause(t *testing.T) {
	cause := errors.New("not found")
	err := Wrap(http.StatusNotFound, "event not found", cause)
	if !errors.Is(err, cause) {
		t.Fatal("wrapped cause was not retained")
	}
}
