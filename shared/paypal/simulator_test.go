package paypal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSimulatorCreateOrderUsesPayPalRequestAndResponseShapes(t *testing.T) {
	ticketID := uuid.New()
	request := createOrderRequest(ticketID, 10)

	encoded, err := json.Marshal(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	wantFields := []string{`"intent":"CAPTURE"`, `"purchase_units"`, `"currency_code":"USD"`, `"value":"49.50"`}
	for _, field := range wantFields {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("request JSON %s does not contain %s", encoded, field)
		}
	}

	simulator := NewSimulator()
	response, err := simulator.CreateOrder(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated || response.Order.ID == "" ||
		response.Order.Status != OrderStatusCreated || response.Order.CreateTime.IsZero() {
		t.Fatalf("response = %+v", response)
	}
	approve, ok := response.Order.Link("approve")
	if !ok || approve.Method != http.MethodGet ||
		approve.Href != "https://www.sandbox.paypal.com/checkoutnow?token="+response.Order.ID {
		t.Fatalf("approve link = %+v", approve)
	}
	if _, ok := response.Order.Link("self"); !ok {
		t.Fatal("self link is missing")
	}
	if _, ok := response.Order.Link("capture"); !ok {
		t.Fatal("capture link is missing")
	}
	encoded, err = json.Marshal(response.Order)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"update_time"`) ||
		strings.Contains(string(encoded), `"purchase_units"`) {
		t.Fatalf("minimal create response contains representation-only fields: %s", encoded)
	}
	request.Prefer = PreferRepresentation
	representation, err := simulator.CreateOrder(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if representation.StatusCode != http.StatusOK || representation.Order.Intent != IntentCapture ||
		len(representation.Order.PurchaseUnits) != 1 {
		t.Fatalf("representation response = %+v", representation)
	}
}

func TestSimulatorCreateOrderIsIdempotent(t *testing.T) {
	ticketID := uuid.New()
	simulator := NewSimulator()
	request := createOrderRequest(ticketID, 10)

	first, err := simulator.CreateOrder(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := simulator.CreateOrder(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.StatusCode != http.StatusCreated || second.StatusCode != http.StatusOK ||
		!reflect.DeepEqual(second.Order, first.Order) {
		t.Fatalf("first = %+v, second = %+v", first, second)
	}

	request.Body.PurchaseUnits[0].Amount.Value = "50.00"
	if _, err := simulator.CreateOrder(context.Background(), request); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want %v", err, ErrIdempotencyConflict)
	}
}

func TestSimulatorCreateOrderIsSafeForConcurrentRetries(t *testing.T) {
	const requests = 20
	simulator := NewSimulator()
	request := createOrderRequest(uuid.New(), 10)
	responses := make(chan CreateOrderResponse, requests)
	errors := make(chan error, requests)

	var wait sync.WaitGroup
	for range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response, err := simulator.CreateOrder(context.Background(), request)
			responses <- response
			errors <- err
		}()
	}
	wait.Wait()
	close(responses)
	close(errors)

	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var expected Order
	createdResponses := 0
	for response := range responses {
		if response.StatusCode == http.StatusCreated {
			createdResponses++
		}
		if expected.ID == "" {
			expected = response.Order
			continue
		}
		if !reflect.DeepEqual(response.Order, expected) {
			t.Fatalf("order = %+v, want %+v", response.Order, expected)
		}
	}
	if createdResponses != 1 {
		t.Fatalf("201 Created responses = %d, want 1", createdResponses)
	}
}

func TestSimulatorCaptureOrderReturnsPayPalCompletedOrder(t *testing.T) {
	ticketID := uuid.New()
	simulator := NewSimulator()
	created, err := simulator.CreateOrder(context.Background(), createOrderRequest(ticketID, 10))
	if err != nil {
		t.Fatal(err)
	}
	request := CaptureOrderRequest{
		OrderID: created.Order.ID, PayPalRequestID: "capture-" + ticketID.String(), Prefer: PreferRepresentation,
	}

	first, err := simulator.CaptureOrder(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := simulator.CaptureOrder(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.StatusCode != http.StatusCreated || second.StatusCode != http.StatusOK ||
		!reflect.DeepEqual(second.Order, first.Order) {
		t.Fatalf("first = %+v, second = %+v", first, second)
	}
	request.PayPalRequestID = "a-different-capture-request"
	if _, err := simulator.CaptureOrder(context.Background(), request); !errors.Is(err, ErrOrderAlreadyCaptured) {
		t.Fatalf("error = %v, want %v", err, ErrOrderAlreadyCaptured)
	}
	if first.Order.ID != created.Order.ID || first.Order.Intent != IntentCapture ||
		first.Order.Status != OrderStatusCompleted || len(first.Order.PurchaseUnits) != 1 {
		t.Fatalf("captured order = %+v", first.Order)
	}
	captures := first.Order.PurchaseUnits[0].Payments.Captures
	if len(captures) != 1 || captures[0].Status != OrderStatusCompleted || !captures[0].FinalCapture ||
		captures[0].Amount.Value != "49.50" || captures[0].Amount.CurrencyCode != "USD" {
		t.Fatalf("captures = %+v", captures)
	}
	if len(captures[0].Links) == 0 {
		t.Fatal("capture HATEOAS links are missing")
	}
}

func TestSimulatorTicketCaptureAdapterChecksPayerAndOrder(t *testing.T) {
	ticketID := uuid.New()
	simulator := NewSimulator()
	if _, err := simulator.CreateOrder(context.Background(), createOrderRequest(ticketID, 10)); err != nil {
		t.Fatal(err)
	}

	if _, err := simulator.Capture(context.Background(), ticketID, 11); !errors.Is(err, ErrPayerMismatch) {
		t.Fatalf("payer error = %v", err)
	}
	if _, err := simulator.Capture(context.Background(), uuid.New(), 10); !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("order error = %v", err)
	}
	if _, err := simulator.Capture(context.Background(), ticketID, 10); err != nil {
		t.Fatal(err)
	}
}

func TestSimulatorValidatesPayPalCreateOrderRequest(t *testing.T) {
	simulator := NewSimulator()
	request := createOrderRequest(uuid.New(), 10)
	request.PayPalRequestID = ""
	if _, err := simulator.CreateOrder(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("request id error = %v", err)
	}

	request = createOrderRequest(uuid.New(), 10)
	request.Body.PurchaseUnits[0].Amount.Value = "0.00"
	if _, err := simulator.CreateOrder(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("amount error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request = createOrderRequest(uuid.New(), 10)
	if _, err := simulator.CreateOrder(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("context error = %v", err)
	}
}

func TestOrderIsExpiredAtPayPalCreatedOrderDeadline(t *testing.T) {
	createdAt := time.Now().UTC()
	order := Order{Status: OrderStatusCreated, CreateTime: createdAt}

	if order.IsExpired(createdAt.Add(OrderExpiresAfter - time.Nanosecond)) {
		t.Fatal("order expired before its deadline")
	}
	if !order.IsExpired(createdAt.Add(OrderExpiresAfter)) {
		t.Fatal("order should expire at its deadline")
	}
	order.Status = OrderStatusCompleted
	if order.IsExpired(createdAt.Add(2 * OrderExpiresAfter)) {
		t.Fatal("completed order must not be treated as expired")
	}
}

func createOrderRequest(ticketID uuid.UUID, userID int64) CreateOrderRequest {
	return CreateOrderRequest{
		PayPalRequestID: ticketID.String(),
		Prefer:          PreferMinimal,
		Body: OrderRequest{
			Intent: IntentCapture,
			PurchaseUnits: []PurchaseUnit{{
				ReferenceID: ticketID.String(),
				CustomID:    strconv.FormatInt(userID, 10),
				Amount:      Money{CurrencyCode: "USD", Value: "49.50"},
			}},
		},
	}
}
