package validation

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"ticket/service/api/apierror"
	"ticket/service/api/dto"
)

func ValidateCreatePendingTicket(r *http.Request) (dto.PendingTicketInput, error) {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()

	var input dto.PendingTicketInput
	if err := decoder.Decode(&input); err != nil {
		return dto.PendingTicketInput{}, apierror.New(
			http.StatusBadRequest,
			fmt.Sprintf("invalid JSON body: %v", err),
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return dto.PendingTicketInput{}, apierror.New(
			http.StatusBadRequest,
			"request body must contain exactly one JSON object",
		)
	}
	if err := input.Validate(); err != nil {
		return dto.PendingTicketInput{}, apierror.New(http.StatusUnprocessableEntity, err.Error())
	}
	return input, nil
}

func ValidateConfirmTicket(r *http.Request) (dto.ConfirmTicketInput, error) {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()

	var input dto.ConfirmTicketInput
	if err := decoder.Decode(&input); err != nil {
		return dto.ConfirmTicketInput{}, apierror.New(
			http.StatusBadRequest,
			fmt.Sprintf("invalid JSON body: %v", err),
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return dto.ConfirmTicketInput{}, apierror.New(
			http.StatusBadRequest,
			"request body must contain exactly one JSON object",
		)
	}
	if err := input.Validate(); err != nil {
		return dto.ConfirmTicketInput{}, apierror.New(http.StatusUnprocessableEntity, err.Error())
	}
	return input, nil
}
