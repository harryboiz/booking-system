// Package paypal provides the PayPal integration used by the ticket API.
package paypal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	sandboxAPIBaseURL  = "https://api-m.sandbox.paypal.com"
	sandboxCheckoutURL = "https://www.sandbox.paypal.com/checkoutnow?token="
)

var (
	ErrInvalidRequest       = errors.New("paypal: invalid request")
	ErrIdempotencyConflict  = errors.New("paypal: idempotency key reused with a different request")
	ErrOrderNotFound        = errors.New("paypal: order not found")
	ErrOrderAlreadyCaptured = errors.New("paypal: order already captured")
	ErrPayerMismatch        = errors.New("paypal: payment belongs to another user")
	moneyPattern            = regexp.MustCompile(`^[0-9]+(?:\.[0-9]{1,2})?$`)
	orderIDPattern          = regexp.MustCompile(`^[A-Z0-9]{1,36}$`)
)

// Simulator is an in-memory, concurrency-safe PayPal Orders v2 simulator.
// Reusing the same PayPal-Request-Id returns the original resource, mirroring
// PayPal's idempotent retry behavior.
type Simulator struct {
	mu                 sync.Mutex
	orders             map[string]*orderState
	orderIDByRequestID map[string]string
	now                func() time.Time
}

type orderState struct {
	request          CreateOrderRequest
	order            Order
	captureRequestID string
	captureResult    *CaptureOrderResponse
}

func NewSimulator() *Simulator {
	return &Simulator{
		orders:             make(map[string]*orderState),
		orderIDByRequestID: make(map[string]string),
		now:                time.Now,
	}
}

// CreateOrder simulates POST /v2/checkout/orders. PayPal-Request-Id provides
// idempotency: the initial response is 201, while a replay returns 200 and the
// same resource.
func (simulator *Simulator) CreateOrder(
	ctx context.Context,
	request CreateOrderRequest,
) (CreateOrderResponse, error) {
	if err := validateCreateOrderRequest(ctx, request); err != nil {
		return CreateOrderResponse{}, err
	}

	simulator.mu.Lock()
	defer simulator.mu.Unlock()

	if orderID, ok := simulator.orderIDByRequestID[request.PayPalRequestID]; ok {
		state := simulator.orders[orderID]
		if !reflect.DeepEqual(state.request.Body, request.Body) {
			return CreateOrderResponse{}, ErrIdempotencyConflict
		}
		return CreateOrderResponse{
			StatusCode: http.StatusOK,
			Order:      createOrderRepresentation(state, request.Prefer),
		}, nil
	}

	orderID := paypalID("order:" + request.PayPalRequestID)
	now := simulator.now().UTC()
	selfURL := sandboxAPIBaseURL + "/v2/checkout/orders/" + orderID
	order := Order{
		ID:         orderID,
		Status:     OrderStatusCreated,
		CreateTime: now,
		Links: []Link{
			{Href: selfURL, Rel: "self", Method: http.MethodGet},
			{Href: sandboxCheckoutURL + orderID, Rel: "approve", Method: http.MethodGet},
			{Href: selfURL, Rel: "update", Method: http.MethodPatch},
			{Href: selfURL + "/capture", Rel: "capture", Method: http.MethodPost},
		},
	}
	state := &orderState{request: cloneCreateOrderRequest(request), order: order}
	simulator.orders[orderID] = state
	simulator.orderIDByRequestID[request.PayPalRequestID] = orderID
	return CreateOrderResponse{
		StatusCode: http.StatusCreated,
		Order:      createOrderRepresentation(state, request.Prefer),
	}, nil
}

// CaptureOrder simulates POST /v2/checkout/orders/{id}/capture. The initial
// capture returns 201; an idempotent replay returns the existing result as 200.
func (simulator *Simulator) CaptureOrder(
	ctx context.Context,
	request CaptureOrderRequest,
) (CaptureOrderResponse, error) {
	if err := ctx.Err(); err != nil {
		return CaptureOrderResponse{}, fmt.Errorf("paypal: capture order: %w", err)
	}
	if !orderIDPattern.MatchString(request.OrderID) || request.PayPalRequestID == "" ||
		len(request.PayPalRequestID) > 108 || !validPrefer(request.Prefer) {
		return CaptureOrderResponse{}, fmt.Errorf("%w: capture order path or headers are invalid", ErrInvalidRequest)
	}

	simulator.mu.Lock()
	defer simulator.mu.Unlock()

	state, ok := simulator.orders[request.OrderID]
	if !ok {
		return CaptureOrderResponse{}, ErrOrderNotFound
	}
	if state.request.Body.Intent != IntentCapture {
		return CaptureOrderResponse{}, fmt.Errorf("%w: AUTHORIZE orders cannot use the capture-order endpoint", ErrInvalidRequest)
	}
	if state.captureResult != nil {
		if state.captureRequestID != request.PayPalRequestID {
			return CaptureOrderResponse{}, ErrOrderAlreadyCaptured
		}
		result := *state.captureResult
		result.StatusCode = http.StatusOK
		result.Order = cloneOrder(result.Order)
		return result, nil
	}

	now := simulator.now().UTC()
	purchaseUnits := clonePurchaseUnits(state.request.Body.PurchaseUnits)
	for index := range purchaseUnits {
		captureID := paypalID("capture:" + request.OrderID + ":" + strconv.Itoa(index))
		captureURL := sandboxAPIBaseURL + "/v2/payments/captures/" + captureID
		purchaseUnits[index].Payments = &Payments{Captures: []Capture{{
			ID:           captureID,
			Status:       OrderStatusCompleted,
			Amount:       purchaseUnits[index].Amount,
			FinalCapture: true,
			SellerProtection: SellerProtection{
				Status:            "ELIGIBLE",
				DisputeCategories: []string{"ITEM_NOT_RECEIVED", "UNAUTHORIZED_TRANSACTION"},
			},
			CreateTime: now,
			UpdateTime: now,
			Links: []Link{
				{Href: captureURL, Rel: "self", Method: http.MethodGet},
				{Href: captureURL + "/refund", Rel: "refund", Method: http.MethodPost},
				{Href: sandboxAPIBaseURL + "/v2/checkout/orders/" + request.OrderID, Rel: "up", Method: http.MethodGet},
			},
		}}}
	}
	order := Order{
		ID:            state.order.ID,
		Intent:        state.request.Body.Intent,
		Status:        OrderStatusCompleted,
		PurchaseUnits: purchaseUnits,
		CreateTime:    state.order.CreateTime,
		UpdateTime:    &now,
		Links: []Link{{
			Href:   sandboxAPIBaseURL + "/v2/checkout/orders/" + request.OrderID,
			Rel:    "self",
			Method: http.MethodGet,
		}},
	}
	result := CaptureOrderResponse{StatusCode: http.StatusCreated, Order: order}
	state.order = order
	state.captureRequestID = request.PayPalRequestID
	state.captureResult = &result
	result.Order = cloneOrder(result.Order)
	return result, nil
}

// Capture keeps the ticket handler adapter small while the simulator itself
// exposes PayPal-shaped create/capture operations. The handler uses ticket ID
// as its PayPal-Request-Id, so the corresponding PayPal order is unambiguous.
func (simulator *Simulator) Capture(
	ctx context.Context,
	ticketID uuid.UUID,
	userID int64,
) (CaptureOrderResponse, error) {
	if ticketID == uuid.Nil || userID <= 0 {
		return CaptureOrderResponse{}, fmt.Errorf("%w: valid ticket and user IDs are required", ErrInvalidRequest)
	}
	requestID := ticketID.String()

	simulator.mu.Lock()
	orderID, ok := simulator.orderIDByRequestID[requestID]
	if ok {
		state := simulator.orders[orderID]
		if len(state.request.Body.PurchaseUnits) == 0 ||
			state.request.Body.PurchaseUnits[0].CustomID != strconv.FormatInt(userID, 10) {
			simulator.mu.Unlock()
			return CaptureOrderResponse{}, ErrPayerMismatch
		}
	}
	simulator.mu.Unlock()
	if !ok {
		return CaptureOrderResponse{}, ErrOrderNotFound
	}
	return simulator.CaptureOrder(ctx, CaptureOrderRequest{
		OrderID:         orderID,
		PayPalRequestID: "capture-" + requestID,
		Prefer:          PreferRepresentation,
	})
}

func validateCreateOrderRequest(ctx context.Context, request CreateOrderRequest) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("paypal: create order: %w", err)
	}
	if request.PayPalRequestID == "" || len(request.PayPalRequestID) > 108 {
		return fmt.Errorf("%w: PayPal-Request-Id must contain 1 to 108 characters", ErrInvalidRequest)
	}
	if !validPrefer(request.Prefer) {
		return fmt.Errorf("%w: Prefer must be return=minimal or return=representation", ErrInvalidRequest)
	}
	if request.Body.Intent != IntentCapture && request.Body.Intent != IntentAuthorize {
		return fmt.Errorf("%w: intent must be CAPTURE or AUTHORIZE", ErrInvalidRequest)
	}
	if len(request.Body.PurchaseUnits) < 1 || len(request.Body.PurchaseUnits) > 10 {
		return fmt.Errorf("%w: purchase_units must contain 1 to 10 items", ErrInvalidRequest)
	}
	for _, unit := range request.Body.PurchaseUnits {
		if len(unit.Amount.CurrencyCode) != 3 || unit.Amount.CurrencyCode != strings.ToUpper(unit.Amount.CurrencyCode) {
			return fmt.Errorf("%w: currency_code must be a three-letter uppercase code", ErrInvalidRequest)
		}
		if !moneyPattern.MatchString(unit.Amount.Value) {
			return fmt.Errorf("%w: amount value is invalid", ErrInvalidRequest)
		}
		value, err := strconv.ParseFloat(unit.Amount.Value, 64)
		if err != nil || value <= 0 {
			return fmt.Errorf("%w: amount value must be positive", ErrInvalidRequest)
		}
	}
	return nil
}

func paypalID(seed string) string {
	id := strings.ToUpper(strings.ReplaceAll(uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)).String(), "-", ""))
	return id[:17]
}

func validPrefer(prefer string) bool {
	return prefer == "" || prefer == PreferMinimal || prefer == PreferRepresentation
}

func createOrderRepresentation(state *orderState, prefer string) Order {
	order := cloneOrder(state.order)
	if prefer == PreferRepresentation && order.Status == OrderStatusCreated {
		order.Intent = state.request.Body.Intent
		order.PurchaseUnits = clonePurchaseUnits(state.request.Body.PurchaseUnits)
	}
	return order
}

func clonePurchaseUnits(units []PurchaseUnit) []PurchaseUnit {
	result := make([]PurchaseUnit, len(units))
	for index := range units {
		result[index] = units[index]
		if units[index].Payments != nil {
			payments := *units[index].Payments
			payments.Captures = append([]Capture(nil), units[index].Payments.Captures...)
			for captureIndex := range payments.Captures {
				capture := &payments.Captures[captureIndex]
				capture.Links = append([]Link(nil), capture.Links...)
				capture.SellerProtection.DisputeCategories = append(
					[]string(nil),
					capture.SellerProtection.DisputeCategories...,
				)
			}
			result[index].Payments = &payments
		}
	}
	return result
}

func cloneCreateOrderRequest(request CreateOrderRequest) CreateOrderRequest {
	result := request
	result.Body.PurchaseUnits = clonePurchaseUnits(request.Body.PurchaseUnits)
	if request.Body.PaymentSource != nil {
		paymentSource := *request.Body.PaymentSource
		if request.Body.PaymentSource.PayPal != nil {
			payPal := *request.Body.PaymentSource.PayPal
			if payPal.ExperienceContext != nil {
				experienceContext := *payPal.ExperienceContext
				payPal.ExperienceContext = &experienceContext
			}
			paymentSource.PayPal = &payPal
		}
		result.Body.PaymentSource = &paymentSource
	}
	return result
}

func cloneOrder(order Order) Order {
	result := order
	result.PurchaseUnits = clonePurchaseUnits(order.PurchaseUnits)
	result.Links = append([]Link(nil), order.Links...)
	if order.UpdateTime != nil {
		updateTime := *order.UpdateTime
		result.UpdateTime = &updateTime
	}
	return result
}
