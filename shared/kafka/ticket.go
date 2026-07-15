package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// UpdatedTicket is the message contract published for asynchronous processing.
type UpdatedTicket struct {
	ID            uuid.UUID `json:"id"`
	UserID        int64     `json:"user_id"`
	EventID       int64     `json:"event_id"`
	ClientOrderID string    `json:"client_order_id"`
	Status        string    `json:"status"`
}

type TicketPublisher struct {
	writer *kafkago.Writer
}

func NewTicketPublisher(brokers []string, topic string) *TicketPublisher {
	return &TicketPublisher{writer: &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		Async:        false,
	}}
}

func (publisher *TicketPublisher) Publish(ctx context.Context, ticket UpdatedTicket) error {
	message, err := updatedTicketMessage(ticket)
	if err != nil {
		return err
	}
	if err := publisher.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish updated ticket message: %w", err)
	}
	return nil
}

func updatedTicketMessage(ticket UpdatedTicket) (kafkago.Message, error) {
	payload, err := json.Marshal(ticket)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("encode updated ticket message: %w", err)
	}
	return kafkago.Message{
		Key:   []byte(strconv.FormatInt(ticket.EventID, 10)),
		Value: payload,
		Time:  time.Now().UTC(),
	}, nil
}

func (publisher *TicketPublisher) Close() error {
	return publisher.writer.Close()
}
