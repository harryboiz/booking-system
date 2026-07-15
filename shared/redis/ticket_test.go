package redis

import "testing"

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
}
