package validation

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"ticket/service/api/apierror"
	"ticket/service/api/dto"
)

const maxRequestBodyBytes = 1 << 20

func ValidateCreateEvent(r *http.Request) (dto.EventInput, error) {
	return decodeEventInput(r)
}

func ValidateListEvents(r *http.Request) error {
	if len(r.URL.Query()) != 0 {
		return apierror.New(http.StatusBadRequest, "query parameters are not supported")
	}
	return nil
}

func ValidateGetEvent(r *http.Request) (string, error) {
	return validateEventID(r)
}

func ValidateUpdateEvent(r *http.Request) (string, dto.EventInput, error) {
	id, err := validateEventID(r)
	if err != nil {
		return "", dto.EventInput{}, err
	}
	input, err := decodeEventInput(r)
	if err != nil {
		return "", dto.EventInput{}, err
	}
	return id, input, nil
}

func ValidateDeleteEvent(r *http.Request) (string, error) {
	return validateEventID(r)
}

func validateEventID(r *http.Request) (string, error) {
	id := r.PathValue("id")
	value, err := strconv.ParseInt(id, 10, 64)
	if err != nil || value <= 0 {
		return "", apierror.New(http.StatusBadRequest, "event id must be a positive integer")
	}
	return id, nil
}

func decodeEventInput(r *http.Request) (dto.EventInput, error) {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()

	var input dto.EventInput
	if err := decoder.Decode(&input); err != nil {
		return dto.EventInput{}, apierror.New(
			http.StatusBadRequest,
			fmt.Sprintf("invalid JSON body: %v", err),
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return dto.EventInput{}, apierror.New(
			http.StatusBadRequest,
			"request body must contain exactly one JSON object",
		)
	}
	if err := input.Validate(); err != nil {
		return dto.EventInput{}, apierror.New(http.StatusUnprocessableEntity, err.Error())
	}
	return input, nil
}
