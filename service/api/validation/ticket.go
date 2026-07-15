package validation

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"ticket/service/api/apierror"
	"ticket/service/api/dto"
)

func ValidateGetTicket(r *http.Request) (dto.GetTicketInput, error) {
	query := r.URL.Query()
	for key := range query {
		if key != "user_id" && key != "ticket_id" && key != "client_order_id" {
			return dto.GetTicketInput{}, apierror.New(http.StatusBadRequest, "unsupported query parameter: "+key)
		}
		if len(query[key]) != 1 {
			return dto.GetTicketInput{}, apierror.New(http.StatusBadRequest, key+" must be specified once")
		}
	}

	userID, err := strconv.ParseInt(query.Get("user_id"), 10, 64)
	if err != nil || userID <= 0 {
		return dto.GetTicketInput{}, apierror.New(http.StatusBadRequest, "user_id must be a positive integer")
	}

	ticketIDValue := strings.TrimSpace(query.Get("ticket_id"))
	clientOrderID := strings.TrimSpace(query.Get("client_order_id"))
	if (ticketIDValue == "") == (clientOrderID == "") {
		return dto.GetTicketInput{}, apierror.New(
			http.StatusBadRequest,
			"exactly one of ticket_id or client_order_id is required",
		)
	}
	if utf8.RuneCountInString(clientOrderID) > 255 {
		return dto.GetTicketInput{}, apierror.New(
			http.StatusBadRequest,
			"client_order_id must be at most 255 characters",
		)
	}

	input := dto.GetTicketInput{UserID: userID, ClientOrderID: clientOrderID}
	if ticketIDValue != "" {
		input.TicketID, err = uuid.Parse(ticketIDValue)
		if err != nil || input.TicketID == uuid.Nil {
			return dto.GetTicketInput{}, apierror.New(http.StatusBadRequest, "ticket_id must be a valid UUID")
		}
	}
	return input, nil
}

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

func ValidateCreateTicketPayment(r *http.Request) (dto.CreateTicketPaymentInput, error) {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()

	var input dto.CreateTicketPaymentInput
	if err := decoder.Decode(&input); err != nil {
		return dto.CreateTicketPaymentInput{}, apierror.New(
			http.StatusBadRequest,
			fmt.Sprintf("invalid JSON body: %v", err),
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return dto.CreateTicketPaymentInput{}, apierror.New(
			http.StatusBadRequest,
			"request body must contain exactly one JSON object",
		)
	}
	if err := input.Validate(); err != nil {
		return dto.CreateTicketPaymentInput{}, apierror.New(http.StatusUnprocessableEntity, err.Error())
	}
	return input, nil
}
