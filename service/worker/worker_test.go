package worker

import (
	"context"
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

func TestReconcileCachesTicketsForWorkerEvents(t *testing.T) {
	pendingID := uuid.New()
	doneID := uuid.New()
	repository := &reconcileRepository{
		events:  []entity.Event{{ID: 101}},
		pending: []entity.Ticket{{ID: pendingID, EventID: 101, UserID: 10, ClientOrderID: "pending-1"}},
		done:    []entity.TicketDone{{ID: doneID, EventID: 101, UserID: 11, ClientOrderID: "done-1"}},
	}
	cache := &reconcileCache{}
	processor := NewProcessor(repository, cache, 15*time.Minute, nil)

	if err := processor.Reconcile(context.Background(), []int{1}); err != nil {
		t.Fatal(err)
	}
	if len(repository.pendingEventIDs) != 1 || repository.pendingEventIDs[0] != 101 {
		t.Fatalf("pending event ids = %v", repository.pendingEventIDs)
	}
	if len(repository.doneEventIDs) != 1 || repository.doneEventIDs[0] != 101 {
		t.Fatalf("done event ids = %v", repository.doneEventIDs)
	}
	if len(cache.events) != 1 || cache.events[0].ID != 101 {
		t.Fatalf("cached events = %+v", cache.events)
	}
	if len(cache.pending) != 1 || cache.pending[0].ID != pendingID {
		t.Fatalf("cached pending orders = %+v", cache.pending)
	}
	if len(cache.done) != 1 || cache.done[0].ID != doneID {
		t.Fatalf("cached done orders = %+v", cache.done)
	}
}

type reconcileRepository struct {
	events          []entity.Event
	pending         []entity.Ticket
	done            []entity.TicketDone
	pendingEventIDs []int64
	doneEventIDs    []int64
}

func (repository *reconcileRepository) FindEventsByMessageKeys(
	context.Context,
	[]int,
	int,
) ([]entity.Event, error) {
	return repository.events, nil
}

func (repository *reconcileRepository) FindEventsByIDs(
	context.Context,
	[]int64,
) ([]entity.Event, error) {
	return nil, nil
}

func (repository *reconcileRepository) FindPendingTicketsByEventIDs(
	_ context.Context,
	eventIDs []int64,
) ([]entity.Ticket, error) {
	repository.pendingEventIDs = append([]int64(nil), eventIDs...)
	return repository.pending, nil
}

func (repository *reconcileRepository) FindDoneTicketsByEventIDs(
	_ context.Context,
	eventIDs []int64,
) ([]entity.TicketDone, error) {
	repository.doneEventIDs = append([]int64(nil), eventIDs...)
	return repository.done, nil
}

func (repository *reconcileRepository) FindPendingTicketsByIDs(
	context.Context,
	[]uuid.UUID,
) ([]entity.Ticket, error) {
	return nil, nil
}

func (repository *reconcileRepository) FindDoneTicketsByIDs(
	context.Context,
	[]uuid.UUID,
) ([]entity.TicketDone, error) {
	return nil, nil
}

func (repository *reconcileRepository) PersistTicketChanges(
	context.Context,
	[]entity.Ticket,
	[]entity.Ticket,
	[]entity.TicketDone,
	[]entity.Event,
) error {
	return nil
}

type reconcileCache struct {
	events  []entity.Event
	pending []entity.Ticket
	done    []entity.TicketDone
}

func (cache *reconcileCache) GetEvents(
	context.Context,
	[]int64,
) (map[int64]entity.Event, error) {
	return nil, nil
}

func (cache *reconcileCache) SetEvents(_ context.Context, events []entity.Event) error {
	cache.events = events
	return nil
}

func (cache *reconcileCache) SetOrders(
	_ context.Context,
	pending []entity.Ticket,
	done []entity.TicketDone,
) error {
	cache.pending = pending
	cache.done = done
	return nil
}
