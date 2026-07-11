package dto

import (
	"errors"
	"math"
	"strings"
	"time"
)

type Event struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	DateTime     time.Time `json:"date_time"`
	TotalTickets int       `json:"total_tickets"`
	TicketPrice  float64   `json:"ticket_price"`
}

type EventInput struct {
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	DateTime     time.Time `json:"date_time"`
	TotalTickets int       `json:"total_tickets"`
	TicketPrice  float64   `json:"ticket_price"`
}

func (in EventInput) Validate() error {
	if strings.TrimSpace(in.Name) == "" {
		return errors.New("name is required")
	}
	if in.DateTime.IsZero() {
		return errors.New("date_time is required and must use RFC3339 format")
	}
	if in.TotalTickets < 0 {
		return errors.New("total_tickets must be greater than or equal to 0")
	}
	if in.TotalTickets > math.MaxInt32 {
		return errors.New("total_tickets must be less than or equal to 2147483647")
	}
	if in.TicketPrice < 0 || math.IsNaN(in.TicketPrice) || math.IsInf(in.TicketPrice, 0) {
		return errors.New("ticket_price must be a finite number greater than or equal to 0")
	}
	if in.TicketPrice > 9999999999.99 {
		return errors.New("ticket_price must be less than or equal to 9999999999.99")
	}
	return nil
}
