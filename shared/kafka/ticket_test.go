package kafka

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestUpdatedTicketMessageUsesEventIDAsKey(t *testing.T) {
	ticket := UpdatedTicket{
		ID:            uuid.MustParse("8a1d49bf-88df-43a6-933b-89f90f26092d"),
		UserID:        10,
		EventID:       20,
		ClientOrderID: "order-123",
		Status:        "pending",
	}

	message, err := updatedTicketMessage(ticket)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(message.Key); got != "20" {
		t.Fatalf("message key = %q", got)
	}

	var decoded UpdatedTicket
	if err := json.Unmarshal(message.Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != ticket {
		t.Fatalf("decoded ticket = %+v, want %+v", decoded, ticket)
	}
}
