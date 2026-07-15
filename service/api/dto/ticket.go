package dto

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type PendingTicketInput struct {
	UserID        int64  `json:"user_id"`
	EventID       int64  `json:"event_id"`
	ClientOrderID string `json:"client_order_id"`
}

type ConfirmTicketInput struct {
	UserID   int64     `json:"user_id"`
	TicketID uuid.UUID `json:"ticket_id"`
}

type CreateTicketPaymentInput struct {
	UserID   int64     `json:"user_id"`
	TicketID uuid.UUID `json:"ticket_id"`
}

type GetTicketInput struct {
	UserID        int64
	TicketID      uuid.UUID
	ClientOrderID string
}

func (input ConfirmTicketInput) Validate() error {
	return validateTicketOwner(input.UserID, input.TicketID)
}

func (input CreateTicketPaymentInput) Validate() error {
	return validateTicketOwner(input.UserID, input.TicketID)
}

func validateTicketOwner(userID int64, ticketID uuid.UUID) error {
	if userID <= 0 {
		return errors.New("user_id must be a positive integer")
	}
	if ticketID == uuid.Nil {
		return errors.New("ticket_id is required")
	}
	return nil
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

type TicketPayment struct {
	PayPalOrderID string `json:"paypal_order_id"`
	PaymentURL    string `json:"payment_url"`
}

type Ticket struct {
	ID            uuid.UUID `json:"id"`
	EventID       int64     `json:"event_id"`
	UserID        int64     `json:"user_id"`
	ClientOrderID string    `json:"client_order_id"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}
