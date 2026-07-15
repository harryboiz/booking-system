package redis

import "testing"

func TestUserTicketKey(t *testing.T) {
	if got := UserTicketKey(42, 10); got != "user_ticket:42:10" {
		t.Fatalf("user ticket key = %q", got)
	}
}
