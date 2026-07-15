package dto

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

type PendingTicketInput struct {
	UserID        int64  `json:"user_id"`
	EventID       int64  `json:"event_id"`
	ClientOrderID string `json:"client_order_id"`
}

func (input PendingTicketInput) Validate() error {
	if input.UserID <= 0 {
		return errors.New("user_id must be a positive integer")
	}
	if input.EventID <= 0 {
		return errors.New("event_id must be a positive integer")
	}
	clientOrderID := strings.TrimSpace(input.ClientOrderID)
	if clientOrderID == "" {
		return errors.New("client_order_id is required")
	}
	if utf8.RuneCountInString(clientOrderID) > 255 {
		return errors.New("client_order_id must be at most 255 characters")
	}
	return nil
}

type PendingTicket struct {
	TicketID uuid.UUID `json:"ticket_id"`
}
