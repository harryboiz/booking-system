package paypal

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestSimulatorCaptureCompletesPayment(t *testing.T) {
	ticketID := uuid.New()
	simulator := NewSimulator()
	if _, err := simulator.CreateOrder(context.Background(), ticketID, 10); err != nil {
		t.Fatal(err)
	}

	payment, err := simulator.Capture(context.Background(), ticketID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if payment.ID == uuid.Nil || payment.TicketID != ticketID || payment.UserID != 10 ||
		payment.Status != PaymentStatusCompleted || payment.CapturedAt.IsZero() {
		t.Fatalf("payment = %+v", payment)
	}
}

func TestSimulatorCreateOrderIsIdempotent(t *testing.T) {
	ticketID := uuid.New()
	simulator := NewSimulator()

	first, err := simulator.CreateOrder(context.Background(), ticketID, 10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := simulator.CreateOrder(context.Background(), ticketID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("second order = %+v, want %+v", second, first)
	}
	if first.ID == "" || first.TicketID != ticketID || first.UserID != 10 ||
		first.Status != OrderStatusCreated || first.ApprovalURL == "" || first.CreatedAt.IsZero() {
		t.Fatalf("order = %+v", first)
	}

	// Recreating the simulator still produces the same PayPal identity and URL.
	recreated, err := NewSimulator().CreateOrder(context.Background(), ticketID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if recreated.ID != first.ID || recreated.ApprovalURL != first.ApprovalURL {
		t.Fatalf("recreated order = %+v, want id/url from %+v", recreated, first)
	}
}

func TestSimulatorCreateOrderIsSafeForConcurrentRetries(t *testing.T) {
	const requests = 20
	ticketID := uuid.New()
	simulator := NewSimulator()
	orders := make(chan Order, requests)
	errors := make(chan error, requests)

	var wait sync.WaitGroup
	for range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			order, err := simulator.CreateOrder(context.Background(), ticketID, 10)
			orders <- order
			errors <- err
		}()
	}
	wait.Wait()
	close(orders)
	close(errors)

	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var expected Order
	for order := range orders {
		if expected.ID == "" {
			expected = order
			continue
		}
		if order != expected {
			t.Fatalf("order = %+v, want %+v", order, expected)
		}
	}
}

func TestSimulatorCaptureIsIdempotent(t *testing.T) {
	ticketID := uuid.New()
	simulator := NewSimulator()
	if _, err := simulator.CreateOrder(context.Background(), ticketID, 10); err != nil {
		t.Fatal(err)
	}

	first, err := simulator.Capture(context.Background(), ticketID, 10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := simulator.Capture(context.Background(), ticketID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("second payment = %+v, want %+v", second, first)
	}
}

func TestSimulatorCaptureRejectsAnotherPayer(t *testing.T) {
	ticketID := uuid.New()
	simulator := NewSimulator()
	if _, err := simulator.CreateOrder(context.Background(), ticketID, 10); err != nil {
		t.Fatal(err)
	}

	_, err := simulator.Capture(context.Background(), ticketID, 11)
	if !errors.Is(err, ErrPayerMismatch) {
		t.Fatalf("error = %v, want %v", err, ErrPayerMismatch)
	}
}

func TestSimulatorCaptureRequiresOrder(t *testing.T) {
	_, err := NewSimulator().Capture(context.Background(), uuid.New(), 10)
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("error = %v, want %v", err, ErrOrderNotFound)
	}
}

func TestSimulatorCaptureHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewSimulator().Capture(ctx, uuid.New(), 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
}

func TestSimulatorCaptureValidatesInput(t *testing.T) {
	simulator := NewSimulator()
	if _, err := simulator.Capture(context.Background(), uuid.Nil, 10); !errors.Is(err, ErrInvalidTicketID) {
		t.Fatalf("ticket error = %v", err)
	}
	if _, err := simulator.Capture(context.Background(), uuid.New(), 0); !errors.Is(err, ErrInvalidUserID) {
		t.Fatalf("user error = %v", err)
	}
}
