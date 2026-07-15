package cronjob

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	sharedkafka "ticket/shared/kafka"
	"ticket/shared/model/entity"
	"ticket/shared/repository"
)

func TestCancelExpiredTicketPollPublishesExpiredTicketsWithoutShardFilter(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	tickets := []entity.Ticket{
		{ID: uuid.New(), EventID: 101, UserID: 10, ClientOrderID: "order-1"},
		{ID: uuid.New(), EventID: 202, UserID: 11, ClientOrderID: "order-2"},
	}
	repository := &ticketRepositoryFake{tickets: tickets}
	publisher := &publisherFake{}
	job := NewCancelExpiredTicket(repository, publisher, 20*time.Minute, time.Minute, 100, nil)
	job.now = func() time.Time { return now }

	published, err := job.poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if published != 2 {
		t.Fatalf("published = %d, want 2", published)
	}
	if !repository.cutoff.Equal(now.Add(-20*time.Minute)) || repository.limit != 100 {
		t.Fatalf("cutoff = %v, limit = %d", repository.cutoff, repository.limit)
	}
	for index, message := range publisher.messages {
		if message.ID != tickets[index].ID || message.EventID != tickets[index].EventID ||
			message.Status != statusCancel {
			t.Fatalf("message %d = %+v", index, message)
		}
	}
}

func TestCancelExpiredTicketPollContinuesAfterPublishError(t *testing.T) {
	repository := &ticketRepositoryFake{tickets: []entity.Ticket{
		{ID: uuid.New(), EventID: 1, UserID: 1, ClientOrderID: "order-1"},
		{ID: uuid.New(), EventID: 2, UserID: 2, ClientOrderID: "order-2"},
	}}
	publishErr := errors.New("kafka unavailable")
	publisher := &publisherFake{errors: []error{publishErr, nil}}
	job := NewCancelExpiredTicket(repository, publisher, 20*time.Minute, time.Minute, 100, nil)

	published, err := job.poll(context.Background())
	if !errors.Is(err, publishErr) {
		t.Fatalf("error = %v", err)
	}
	if published != 1 || len(publisher.messages) != 2 {
		t.Fatalf("published = %d, attempted = %d", published, len(publisher.messages))
	}
}

type ticketRepositoryFake struct {
	repository.TicketRepository
	tickets []entity.Ticket
	cutoff  time.Time
	limit   int
}

func (repository *ticketRepositoryFake) FindExpiredPendingTickets(
	_ context.Context,
	cutoff time.Time,
	limit int,
) ([]entity.Ticket, error) {
	repository.cutoff = cutoff
	repository.limit = limit
	return repository.tickets, nil
}

type publisherFake struct {
	messages []sharedkafka.UpdatedTicket
	errors   []error
}

func (publisher *publisherFake) Publish(
	_ context.Context,
	message sharedkafka.UpdatedTicket,
) error {
	index := len(publisher.messages)
	publisher.messages = append(publisher.messages, message)
	if index < len(publisher.errors) {
		return publisher.errors[index]
	}
	return nil
}
