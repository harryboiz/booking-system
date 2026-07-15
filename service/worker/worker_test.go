package worker

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	sharedkafka "ticket/shared/kafka"
	"ticket/shared/model/entity"
)

func TestDecodeOnlyAcceptsMessageOnItsConfiguredShard(t *testing.T) {
	processor := &Processor{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ticket := sharedkafka.UpdatedTicket{
		ID: uuid.New(), EventID: 101, UserID: 2, ClientOrderID: "order-1", Status: statusPending,
	}
	payload, err := json.Marshal(ticket)
	if err != nil {
		t.Fatal(err)
	}
	records := []kafkago.Message{
		{Key: []byte("1"), Partition: 1, Value: payload},
		{Key: []byte("1"), Partition: 2, Value: payload},
		{Key: []byte("2"), Partition: 1, Value: payload},
		{Key: []byte("1"), Partition: 1, Value: []byte("not-json")},
	}
	decoded := processor.decode(records)
	if len(decoded) != 1 || decoded[0].ticket != ticket {
		t.Fatalf("decoded = %+v", decoded)
	}
}

func TestCanCancelPendingTicketOnlyAfterTimeout(t *testing.T) {
	now := time.Now().UTC()
	ticket := &entity.Ticket{EventID: 10, Status: statusPending, CreatedAt: now.Add(-16 * time.Minute)}
	if !canCancel(ticket, 10, now, 15*time.Minute) {
		t.Fatal("old pending ticket should be cancellable")
	}
	ticket.CreatedAt = now.Add(-15 * time.Minute)
	if canCancel(ticket, 10, now, 15*time.Minute) {
		t.Fatal("ticket must be older than 15 minutes")
	}
	ticket.CreatedAt = now.Add(-16 * time.Minute)
	if canCancel(ticket, 11, now, 15*time.Minute) {
		t.Fatal("ticket from another event must not be cancelled")
	}
}
