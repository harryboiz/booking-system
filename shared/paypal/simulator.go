// Package paypal provides the PayPal integration used by the ticket API.
package paypal

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	OrderStatusCreated     = "CREATED"
	PaymentStatusCompleted = "COMPLETED"
	checkoutBaseURL        = "https://paypal.local/checkoutnow"
)

var (
	ErrInvalidTicketID = errors.New("paypal: ticket id is required")
	ErrInvalidUserID   = errors.New("paypal: user id must be positive")
	ErrPayerMismatch   = errors.New("paypal: payment belongs to another user")
	ErrOrderNotFound   = errors.New("paypal: order not found")
)

// Order is a simulated PayPal order created for a pending ticket.
type Order struct {
	ID          string
	TicketID    uuid.UUID
	UserID      int64
	Status      string
	ApprovalURL string
	CreatedAt   time.Time
}

// Payment is the result returned by a simulated PayPal capture.
type Payment struct {
	ID         uuid.UUID
	TicketID   uuid.UUID
	UserID     int64
	Status     string
	CapturedAt time.Time
}

// Simulator is an in-memory, concurrency-safe PayPal payment simulator.
// Capturing the same ticket more than once is idempotent and returns the
// original payment, mirroring the idempotency needed when the API retries
// after a downstream failure.
type Simulator struct {
	mu       sync.Mutex
	orders   map[uuid.UUID]Order
	payments map[uuid.UUID]Payment
	now      func() time.Time
}

func NewSimulator() *Simulator {
	return &Simulator{
		orders:   make(map[uuid.UUID]Order),
		payments: make(map[uuid.UUID]Payment),
		now:      time.Now,
	}
}

// CreateOrder creates one PayPal order per ticket. Repeated and concurrent
// calls for the same ticket return exactly the same order and approval URL.
func (simulator *Simulator) CreateOrder(
	ctx context.Context,
	ticketID uuid.UUID,
	userID int64,
) (Order, error) {
	if err := validateRequest(ctx, ticketID, userID); err != nil {
		return Order{}, err
	}

	simulator.mu.Lock()
	defer simulator.mu.Unlock()

	if order, ok := simulator.orders[ticketID]; ok {
		if order.UserID != userID {
			return Order{}, ErrPayerMismatch
		}
		return order, nil
	}

	// The ticket ID is also the idempotency key. A deterministic order ID keeps
	// retries stable even if the in-memory simulator is recreated.
	orderID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("ticket-paypal-order:"+ticketID.String())).String()
	approvalURL := checkoutBaseURL + "?" + url.Values{"token": []string{orderID}}.Encode()
	order := Order{
		ID:          orderID,
		TicketID:    ticketID,
		UserID:      userID,
		Status:      OrderStatusCreated,
		ApprovalURL: approvalURL,
		CreatedAt:   simulator.now().UTC(),
	}
	simulator.orders[ticketID] = order
	return order, nil
}

// Capture simulates capturing a PayPal payment for a pending ticket.
func (simulator *Simulator) Capture(
	ctx context.Context,
	ticketID uuid.UUID,
	userID int64,
) (Payment, error) {
	if err := validateRequest(ctx, ticketID, userID); err != nil {
		return Payment{}, err
	}

	simulator.mu.Lock()
	defer simulator.mu.Unlock()

	if payment, ok := simulator.payments[ticketID]; ok {
		if payment.UserID != userID {
			return Payment{}, ErrPayerMismatch
		}
		return payment, nil
	}
	order, ok := simulator.orders[ticketID]
	if !ok {
		return Payment{}, ErrOrderNotFound
	}
	if order.UserID != userID {
		return Payment{}, ErrPayerMismatch
	}

	payment := Payment{
		ID: uuid.NewSHA1(
			uuid.NameSpaceURL,
			[]byte("ticket-paypal-payment:"+ticketID.String()),
		),
		TicketID:   ticketID,
		UserID:     userID,
		Status:     PaymentStatusCompleted,
		CapturedAt: simulator.now().UTC(),
	}
	simulator.payments[ticketID] = payment
	return payment, nil
}

func validateRequest(ctx context.Context, ticketID uuid.UUID, userID int64) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("paypal: request: %w", err)
	}
	if ticketID == uuid.Nil {
		return ErrInvalidTicketID
	}
	if userID <= 0 {
		return ErrInvalidUserID
	}
	return nil
}
