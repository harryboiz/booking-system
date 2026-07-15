package dto

import (
	"errors"
	"math"
	"strings"
	"time"
)

type Event struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	StartDate        time.Time `json:"start_date"`
	EndTime          time.Time `json:"end_time"`
	TotalTickets     int       `json:"total_tickets"`
	TicketPrice      float64   `json:"ticket_price"`
	EstimateRevenue  float64   `json:"estimate_revenue"`
	PendingTickets   int64     `json:"pending_tickets"`
	ConfirmTickets   int64     `json:"confirm_tickets"`
	CancelTickets    int64     `json:"cancel_tickets"`
	MaxTicketPerUser int       `json:"max_ticket_per_user"`
}

type EventInput struct {
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	StartDate        time.Time `json:"start_date"`
	EndTime          time.Time `json:"end_time"`
	TotalTickets     int       `json:"total_tickets"`
	TicketPrice      float64   `json:"ticket_price"`
	MaxTicketPerUser int       `json:"max_ticket_per_user"`
}

func (in EventInput) Validate() error {
	if strings.TrimSpace(in.Name) == "" {
		return errors.New("name is required")
	}
	if in.StartDate.IsZero() {
		return errors.New("start_date is required and must use RFC3339 format")
	}
	if in.EndTime.IsZero() {
		return errors.New("end_time is required and must use RFC3339 format")
	}
	if in.EndTime.Before(in.StartDate) {
		return errors.New("end_time must be greater than or equal to start_date")
	}
	if in.TotalTickets < 0 {
		return errors.New("total_tickets must be greater than or equal to 0")
	}
	if in.TotalTickets > math.MaxInt32 {
		return errors.New("total_tickets must be less than or equal to 2147483647")
	}
	if in.MaxTicketPerUser <= 0 {
		return errors.New("max_ticket_per_user must be greater than 0")
	}
	if in.MaxTicketPerUser > math.MaxInt32 {
		return errors.New("max_ticket_per_user must be less than or equal to 2147483647")
	}
	if in.TicketPrice < 0 || math.IsNaN(in.TicketPrice) || math.IsInf(in.TicketPrice, 0) {
		return errors.New("ticket_price must be a finite number greater than or equal to 0")
	}
	if in.TicketPrice > 9999999999.99 {
		return errors.New("ticket_price must be less than or equal to 9999999999.99")
	}
	return nil
}
