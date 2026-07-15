package paypal

import "time"

const (
	IntentCapture   = "CAPTURE"
	IntentAuthorize = "AUTHORIZE"

	OrderStatusCreated   = "CREATED"
	OrderStatusCompleted = "COMPLETED"

	PreferMinimal        = "return=minimal"
	PreferRepresentation = "return=representation"

	// OrderExpiresAfter mirrors the default lifetime of a PayPal order in the
	// CREATED state before it must be captured or modified.
	OrderExpiresAfter = 3 * time.Hour
)

// CreateOrderRequest represents the headers and JSON body sent to
// POST /v2/checkout/orders.
type CreateOrderRequest struct {
	PayPalRequestID string       `json:"-"`
	Prefer          string       `json:"-"`
	Body            OrderRequest `json:"-"`
}

type OrderRequest struct {
	Intent        string         `json:"intent"`
	PurchaseUnits []PurchaseUnit `json:"purchase_units"`
	PaymentSource *PaymentSource `json:"payment_source,omitempty"`
}

type PaymentSource struct {
	PayPal *PayPalPaymentSource `json:"paypal,omitempty"`
}

type PayPalPaymentSource struct {
	EmailAddress      string             `json:"email_address,omitempty"`
	ExperienceContext *ExperienceContext `json:"experience_context,omitempty"`
}

type ExperienceContext struct {
	BrandName          string `json:"brand_name,omitempty"`
	Locale             string `json:"locale,omitempty"`
	ShippingPreference string `json:"shipping_preference,omitempty"`
	UserAction         string `json:"user_action,omitempty"`
	ReturnURL          string `json:"return_url,omitempty"`
	CancelURL          string `json:"cancel_url,omitempty"`
}

type Money struct {
	CurrencyCode string `json:"currency_code"`
	Value        string `json:"value"`
}

type PurchaseUnit struct {
	ReferenceID string    `json:"reference_id,omitempty"`
	Description string    `json:"description,omitempty"`
	CustomID    string    `json:"custom_id,omitempty"`
	InvoiceID   string    `json:"invoice_id,omitempty"`
	Amount      Money     `json:"amount"`
	Payments    *Payments `json:"payments,omitempty"`
}

type Payments struct {
	Captures []Capture `json:"captures,omitempty"`
}

type Capture struct {
	ID               string           `json:"id"`
	Status           string           `json:"status"`
	Amount           Money            `json:"amount"`
	FinalCapture     bool             `json:"final_capture"`
	SellerProtection SellerProtection `json:"seller_protection"`
	CreateTime       time.Time        `json:"create_time"`
	UpdateTime       time.Time        `json:"update_time"`
	Links            []Link           `json:"links"`
}

type SellerProtection struct {
	Status            string   `json:"status"`
	DisputeCategories []string `json:"dispute_categories,omitempty"`
}

type Link struct {
	Href   string `json:"href"`
	Rel    string `json:"rel"`
	Method string `json:"method"`
}

// Order matches the core shape returned by PayPal Orders v2 create and
// capture endpoints. Create returns a minimal representation by default;
// capture returns purchase unit and capture details.
type Order struct {
	ID            string         `json:"id"`
	Intent        string         `json:"intent,omitempty"`
	Status        string         `json:"status"`
	PurchaseUnits []PurchaseUnit `json:"purchase_units,omitempty"`
	CreateTime    time.Time      `json:"create_time"`
	UpdateTime    *time.Time     `json:"update_time,omitempty"`
	Links         []Link         `json:"links"`
}

func (order Order) Link(rel string) (Link, bool) {
	for _, link := range order.Links {
		if link.Rel == rel {
			return link, true
		}
	}
	return Link{}, false
}

// IsExpired reports whether a CREATED order has passed PayPal's default
// lifetime. Terminal orders are not treated as expired resources.
func (order Order) IsExpired(now time.Time) bool {
	if order.Status != OrderStatusCreated {
		return false
	}
	if order.CreateTime.IsZero() {
		return true
	}
	return !now.Before(order.CreateTime.Add(OrderExpiresAfter))
}

type CreateOrderResponse struct {
	StatusCode int   `json:"-"`
	Order      Order `json:"-"`
}

// CaptureOrderRequest represents POST /v2/checkout/orders/{id}/capture.
// PayPal accepts an optional payment_source body; the ticket flow uses an
// empty body, so only path/header values are needed here.
type CaptureOrderRequest struct {
	OrderID         string `json:"-"`
	PayPalRequestID string `json:"-"`
	Prefer          string `json:"-"`
}

type CaptureOrderResponse struct {
	StatusCode int   `json:"-"`
	Order      Order `json:"-"`
}
