package kafka

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestUpdatedTicketMessageUsesEventShardAsKey(t *testing.T) {
	ticket := UpdatedTicket{
		ID:            uuid.MustParse("8a1d49bf-88df-43a6-933b-89f90f26092d"),
		UserID:        10,
		EventID:       120,
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

func TestMessageKeyBalancerUsesKeyAsPartition(t *testing.T) {
	message, err := updatedTicketMessage(UpdatedTicket{EventID: 199})
	if err != nil {
		t.Fatal(err)
	}
	if got := (MessageKeyBalancer{}).Balance(message, 1, 20, 99); got != 99 {
		t.Fatalf("partition = %d, want 99", got)
	}
}
