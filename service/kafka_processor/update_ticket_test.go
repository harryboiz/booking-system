package kafkaprocessor

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	sharedkafka "ticket/shared/kafka"
	"ticket/shared/model/entity"
)

func TestDecodeOnlyAcceptsMessageOnItsConfiguredShard(t *testing.T) {
	processor := &UpdateTicket{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
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
	ticket := &entity.Ticket{EventID: 10, Status: statusPending, CreatedAt: now.Add(-21 * time.Minute)}
	if !canCancel(ticket, 10, now, 20*time.Minute) {
		t.Fatal("old pending ticket should be cancellable")
	}
	ticket.CreatedAt = now.Add(-20 * time.Minute)
	if canCancel(ticket, 10, now, 20*time.Minute) {
		t.Fatal("ticket must be older than 20 minutes")
	}
	ticket.CreatedAt = now.Add(-21 * time.Minute)
	if canCancel(ticket, 11, now, 20*time.Minute) {
		t.Fatal("ticket from another event must not be cancelled")
	}
}

func TestProcessCreatesPendingTicketAndUpdatesEventStats(t *testing.T) {
	ticketID := uuid.New()
	createdAt := time.Now().UTC().Add(-time.Minute)
	repository := newProcessorRepository([]entity.Event{{ID: 101}})
	cache := &reconcileCache{}
	processor := NewUpdateTicket(repository, repository, cache, cache, cache, 20*time.Minute, nil)
	message := sharedkafka.UpdatedTicket{
		ID: ticketID, EventID: 101, UserID: 10, ClientOrderID: "order-1", Status: statusPending,
	}

	if err := processor.Process(context.Background(), []kafkago.Message{
		ticketRecord(t, message, createdAt),
	}); err != nil {
		t.Fatal(err)
	}

	if len(repository.inserted) != 1 {
		t.Fatalf("inserted tickets = %+v", repository.inserted)
	}
	inserted := repository.inserted[0]
	if inserted.ID != ticketID || inserted.EventID != 101 || inserted.UserID != 10 ||
		inserted.ClientOrderID != "order-1" || inserted.Status != statusPending ||
		!inserted.CreatedAt.Equal(createdAt) || inserted.UpdatedAt.IsZero() {
		t.Fatalf("inserted ticket = %+v", inserted)
	}
	if len(repository.deleted) != 0 || len(repository.completed) != 0 {
		t.Fatalf("deleted = %+v, completed = %+v", repository.deleted, repository.completed)
	}
	if len(repository.updatedEvents) != 1 || repository.updatedEvents[0].PendingTickets != 1 {
		t.Fatalf("updated events = %+v", repository.updatedEvents)
	}
	if len(repository.updatedUserTickets) != 1 || repository.updatedUserTickets[0].TicketCount != 1 {
		t.Fatalf("updated user tickets = %+v", repository.updatedUserTickets)
	}
	if len(cache.pending) != 1 || cache.pending[0].ID != ticketID || len(cache.done) != 0 {
		t.Fatalf("cached pending = %+v, done = %+v", cache.pending, cache.done)
	}
}

func TestProcessEnforcesMaxTicketPerUserWithinBatch(t *testing.T) {
	repository := newProcessorRepository([]entity.Event{{ID: 101, MaxTicketPerUser: 2}})
	repository.userTickets = []entity.UserTicket{{EventID: 101, UserID: 10, TicketCount: 1}}
	cache := &reconcileCache{}
	processor := NewUpdateTicket(repository, repository, cache, cache, cache, 20*time.Minute, nil)
	first := sharedkafka.UpdatedTicket{
		ID: uuid.New(), EventID: 101, UserID: 10, ClientOrderID: "order-1", Status: statusPending,
	}
	second := sharedkafka.UpdatedTicket{
		ID: uuid.New(), EventID: 101, UserID: 10, ClientOrderID: "order-2", Status: statusPending,
	}

	if err := processor.Process(context.Background(), []kafkago.Message{
		ticketRecord(t, first, time.Now().UTC()),
		ticketRecord(t, second, time.Now().UTC()),
	}); err != nil {
		t.Fatal(err)
	}

	if len(repository.inserted) != 1 || repository.inserted[0].ID != first.ID {
		t.Fatalf("inserted tickets = %+v", repository.inserted)
	}
	if len(repository.updatedUserTickets) != 1 || repository.updatedUserTickets[0].TicketCount != 2 {
		t.Fatalf("updated user tickets = %+v", repository.updatedUserTickets)
	}
	if len(cache.userTickets) != 1 || cache.userTickets[0].TicketCount != 2 {
		t.Fatalf("cached user tickets = %+v", cache.userTickets)
	}
}

func TestProcessLoadsRequiredStateInParallel(t *testing.T) {
	base := newProcessorRepository([]entity.Event{{ID: 101, MaxTicketPerUser: 1}})
	repository := &parallelQueryRepository{
		processorRepository: base,
		started:             make(chan string, 4),
		release:             make(chan struct{}),
	}
	cache := &reconcileCache{}
	processor := NewUpdateTicket(repository, repository, cache, cache, cache, 20*time.Minute, nil)
	message := sharedkafka.UpdatedTicket{
		ID: uuid.New(), EventID: 101, UserID: 10, ClientOrderID: "parallel-order", Status: statusPending,
	}
	record := ticketRecord(t, message, time.Now().UTC())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- processor.Process(ctx, []kafkago.Message{record})
	}()

	started := make(map[string]bool, 4)
	for len(started) < 4 {
		select {
		case query := <-repository.started:
			started[query] = true
		case <-time.After(time.Second):
			t.Fatalf("queries did not run in parallel; started = %v", started)
		}
	}
	close(repository.release)

	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("processor did not finish after releasing queries")
	}
}

func TestProcessConfirmIsIdempotentForDuplicateMessages(t *testing.T) {
	ticketID := uuid.New()
	createdAt := time.Now().UTC().Add(-5 * time.Minute)
	active := entity.Ticket{
		ID: ticketID, EventID: 202, UserID: 10, ClientOrderID: "original-order",
		Status: statusPending, CreatedAt: createdAt,
	}
	repository := newProcessorRepository([]entity.Event{{ID: 202, PendingTickets: 1}})
	repository.pending = []entity.Ticket{active}
	cache := &reconcileCache{}
	processor := NewUpdateTicket(repository, repository, cache, cache, cache, 20*time.Minute, nil)
	message := sharedkafka.UpdatedTicket{
		ID: ticketID, EventID: 202, UserID: 99, ClientOrderID: "ignored-message-order", Status: statusConfirm,
	}
	record := ticketRecord(t, message, time.Now().UTC())

	if err := processor.Process(context.Background(), []kafkago.Message{record, record}); err != nil {
		t.Fatal(err)
	}

	if len(repository.deleted) != 1 || repository.deleted[0].ID != ticketID {
		t.Fatalf("deleted tickets = %+v", repository.deleted)
	}
	if len(repository.completed) != 1 {
		t.Fatalf("completed tickets = %+v", repository.completed)
	}
	completed := repository.completed[0]
	if completed.ID != ticketID || completed.Status != statusConfirm || completed.UserID != active.UserID ||
		completed.ClientOrderID != active.ClientOrderID || !completed.CreatedAt.Equal(createdAt) {
		t.Fatalf("completed ticket = %+v", completed)
	}
	if len(repository.updatedEvents) != 1 || repository.updatedEvents[0].PendingTickets != 0 ||
		repository.updatedEvents[0].ConfirmTickets != 1 {
		t.Fatalf("updated events = %+v", repository.updatedEvents)
	}
	if len(cache.pending) != 0 || len(cache.done) != 1 || cache.done[0].ID != ticketID {
		t.Fatalf("cached pending = %+v, done = %+v", cache.pending, cache.done)
	}
}

func TestProcessCancelOnlyExpiresOldPendingTicket(t *testing.T) {
	now := time.Now().UTC()
	oldID := uuid.New()
	youngID := uuid.New()
	repository := newProcessorRepository([]entity.Event{{ID: 303, PendingTickets: 2}})
	repository.pending = []entity.Ticket{
		{ID: oldID, EventID: 303, UserID: 10, ClientOrderID: "old", Status: statusPending, CreatedAt: now.Add(-21 * time.Minute)},
		{ID: youngID, EventID: 303, UserID: 11, ClientOrderID: "young", Status: statusPending, CreatedAt: now.Add(-19 * time.Minute)},
	}
	repository.userTickets = []entity.UserTicket{
		{EventID: 303, UserID: 10, TicketCount: 1},
		{EventID: 303, UserID: 11, TicketCount: 1},
	}
	cache := &reconcileCache{}
	processor := NewUpdateTicket(repository, repository, cache, cache, cache, 20*time.Minute, nil)
	records := []kafkago.Message{
		ticketRecord(t, sharedkafka.UpdatedTicket{
			ID: oldID, EventID: 303, UserID: 10, ClientOrderID: "old", Status: statusCancel,
		}, now),
		ticketRecord(t, sharedkafka.UpdatedTicket{
			ID: youngID, EventID: 303, UserID: 11, ClientOrderID: "young", Status: statusCancel,
		}, now),
	}

	if err := processor.Process(context.Background(), records); err != nil {
		t.Fatal(err)
	}

	if len(repository.deleted) != 1 || repository.deleted[0].ID != oldID {
		t.Fatalf("deleted tickets = %+v", repository.deleted)
	}
	if len(repository.completed) != 1 || repository.completed[0].ID != oldID ||
		repository.completed[0].Status != statusCancelled {
		t.Fatalf("completed tickets = %+v", repository.completed)
	}
	if len(repository.updatedEvents) != 1 || repository.updatedEvents[0].PendingTickets != 1 ||
		repository.updatedEvents[0].CancelTickets != 1 {
		t.Fatalf("updated events = %+v", repository.updatedEvents)
	}
	if len(cache.pending) != 1 || cache.pending[0].ID != youngID ||
		len(cache.done) != 1 || cache.done[0].ID != oldID {
		t.Fatalf("cached pending = %+v, done = %+v", cache.pending, cache.done)
	}
	if len(repository.updatedUserTickets) != 1 || repository.updatedUserTickets[0].UserID != 10 ||
		repository.updatedUserTickets[0].TicketCount != 0 {
		t.Fatalf("updated user tickets = %+v", repository.updatedUserTickets)
	}
}

func ticketRecord(
	t *testing.T,
	ticket sharedkafka.UpdatedTicket,
	createdAt time.Time,
) kafkago.Message {
	t.Helper()
	payload, err := json.Marshal(ticket)
	if err != nil {
		t.Fatal(err)
	}
	key := sharedkafka.MessageKey(ticket.EventID)
	return kafkago.Message{
		Key:       []byte(strconv.FormatInt(key, 10)),
		Partition: int(key),
		Value:     payload,
		Time:      createdAt,
	}
}

func TestReconcileCachesTicketsForWorkerEvents(t *testing.T) {
	pendingID := uuid.New()
	doneID := uuid.New()
	repository := &reconcileRepository{
		events:      []entity.Event{{ID: 101}},
		pending:     []entity.Ticket{{ID: pendingID, EventID: 101, UserID: 10, ClientOrderID: "pending-1"}},
		done:        []entity.TicketDone{{ID: doneID, EventID: 101, UserID: 11, ClientOrderID: "done-1"}},
		userTickets: []entity.UserTicket{{EventID: 101, UserID: 10, TicketCount: 1}},
	}
	cache := &reconcileCache{}
	processor := NewUpdateTicket(repository, repository, cache, cache, cache, 15*time.Minute, nil)

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
	if len(cache.userTickets) != 1 || cache.userTickets[0].TicketCount != 1 {
		t.Fatalf("cached user tickets = %+v", cache.userTickets)
	}
}

type reconcileRepository struct {
	events          []entity.Event
	pending         []entity.Ticket
	done            []entity.TicketDone
	userTickets     []entity.UserTicket
	pendingEventIDs []int64
	doneEventIDs    []int64
}

type processorRepository struct {
	*reconcileRepository
	inserted           []entity.Ticket
	deleted            []entity.Ticket
	completed          []entity.TicketDone
	updatedEvents      []entity.Event
	updatedUserTickets []entity.UserTicket
}

type parallelQueryRepository struct {
	*processorRepository
	started chan string
	release chan struct{}
}

func (repository *parallelQueryRepository) waitForRelease(ctx context.Context, query string) error {
	select {
	case repository.started <- query:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-repository.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (repository *parallelQueryRepository) FindEventsByIDs(
	ctx context.Context,
	_ []int64,
) ([]entity.Event, error) {
	if err := repository.waitForRelease(ctx, "events"); err != nil {
		return nil, err
	}
	return repository.events, nil
}

func (repository *parallelQueryRepository) FindPendingTicketsByIDs(
	ctx context.Context,
	_ []uuid.UUID,
) ([]entity.Ticket, error) {
	if err := repository.waitForRelease(ctx, "pending"); err != nil {
		return nil, err
	}
	return repository.pending, nil
}

func (repository *parallelQueryRepository) FindDoneTicketsByIDs(
	ctx context.Context,
	_ []uuid.UUID,
) ([]entity.TicketDone, error) {
	if err := repository.waitForRelease(ctx, "done"); err != nil {
		return nil, err
	}
	return repository.done, nil
}

func (repository *parallelQueryRepository) FindUserTicketsByEventIDs(
	ctx context.Context,
	_ []int64,
) ([]entity.UserTicket, error) {
	if err := repository.waitForRelease(ctx, "user_ticket"); err != nil {
		return nil, err
	}
	return repository.userTickets, nil
}

func newProcessorRepository(events []entity.Event) *processorRepository {
	return &processorRepository{reconcileRepository: &reconcileRepository{events: events}}
}

func (repository *processorRepository) FindEventsByIDs(
	context.Context,
	[]int64,
) ([]entity.Event, error) {
	return repository.events, nil
}

func (repository *processorRepository) FindPendingTicketsByIDs(
	context.Context,
	[]uuid.UUID,
) ([]entity.Ticket, error) {
	return repository.pending, nil
}

func (repository *processorRepository) FindDoneTicketsByIDs(
	context.Context,
	[]uuid.UUID,
) ([]entity.TicketDone, error) {
	return repository.done, nil
}

func (repository *processorRepository) PersistTicketChanges(
	_ context.Context,
	inserted []entity.Ticket,
	deleted []entity.Ticket,
	completed []entity.TicketDone,
	updatedEvents []entity.Event,
	updatedUserTickets []entity.UserTicket,
) error {
	repository.inserted = append([]entity.Ticket(nil), inserted...)
	repository.deleted = append([]entity.Ticket(nil), deleted...)
	repository.completed = append([]entity.TicketDone(nil), completed...)
	repository.updatedEvents = append([]entity.Event(nil), updatedEvents...)
	repository.updatedUserTickets = append([]entity.UserTicket(nil), updatedUserTickets...)
	return nil
}

func (repository *reconcileRepository) GetDoneTicketByID(
	context.Context,
	int64,
	uuid.UUID,
) (entity.TicketDone, error) {
	return entity.TicketDone{}, nil
}

func (repository *reconcileRepository) GetDoneTicketByClientOrderID(
	context.Context,
	int64,
	string,
) (entity.TicketDone, error) {
	return entity.TicketDone{}, nil
}

func (repository *reconcileRepository) FindExpiredPendingTickets(
	context.Context,
	time.Time,
	int,
) ([]entity.Ticket, error) {
	return nil, nil
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

func (repository *reconcileRepository) Create(
	context.Context,
	entity.Event,
) (entity.Event, error) {
	return entity.Event{}, nil
}

func (repository *reconcileRepository) List(context.Context) ([]entity.Event, error) {
	return nil, nil
}

func (repository *reconcileRepository) Get(context.Context, string) (entity.Event, error) {
	return entity.Event{}, nil
}

func (repository *reconcileRepository) Update(
	context.Context,
	string,
	entity.Event,
) (entity.Event, error) {
	return entity.Event{}, nil
}

func (repository *reconcileRepository) Delete(context.Context, string) error {
	return nil
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

func (repository *reconcileRepository) FindUserTicketsByEventIDs(
	_ context.Context,
	_ []int64,
) ([]entity.UserTicket, error) {
	return repository.userTickets, nil
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
	[]entity.UserTicket,
) error {
	return nil
}

type reconcileCache struct {
	events      []entity.Event
	pending     []entity.Ticket
	done        []entity.TicketDone
	userTickets []entity.UserTicket
}

func (cache *reconcileCache) SetUserTickets(
	_ context.Context,
	userTickets []entity.UserTicket,
) error {
	cache.userTickets = append([]entity.UserTicket(nil), userTickets...)
	return nil
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

func (cache *reconcileCache) SetTicket(
	_ context.Context,
	pending []entity.Ticket,
	done []entity.TicketDone,
) error {
	cache.pending = pending
	cache.done = done
	return nil
}
