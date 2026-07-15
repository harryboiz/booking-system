package redis

import (
	"testing"

	"github.com/google/uuid"
)

func TestTicketKeys(t *testing.T) {
	if got := EventKey(42); got != "events:42" {
		t.Fatalf("event key = %q", got)
	}
	if got := ReservedTicketKey(42); got != "tickets:reserved:42" {
		t.Fatalf("reserved ticket key = %q", got)
	}
	if got := ClientOrderIDKey(10, "order-123"); got != "tickets:client-order-id:10:order-123" {
		t.Fatalf("client order id key = %q", got)
	}
	orderID := uuid.MustParse("c7bca801-a080-45c9-972c-860cd4e44ab6")
	if got := OrderKey(orderID); got != "tickets:c7bca801-a080-45c9-972c-860cd4e44ab6" {
		t.Fatalf("order key = %q", got)
	}
}
