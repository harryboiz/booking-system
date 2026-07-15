package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	sharedkafka "ticket/shared/kafka"
	"ticket/shared/model/entity"
	"ticket/shared/repository"
)

const (
	statusPending   = "pending"
	statusConfirm   = "confirm"
	statusCancel    = "cancel"
	statusCancelled = "cancelled"
)

type EventCache interface {
	GetEvents(context.Context, []int64) (map[int64]entity.Event, error)
	SetEvents(context.Context, []entity.Event) error
}

type Processor struct {
	repository  repository.TicketRepository
	cache       EventCache
	cancelAfter time.Duration
	logger      *slog.Logger
}

type decodedMessage struct {
	record kafkago.Message
	ticket sharedkafka.UpdatedTicket
}

func NewProcessor(
	repository repository.TicketRepository,
	cache EventCache,
	cancelAfter time.Duration,
	logger *slog.Logger,
) *Processor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Processor{repository: repository, cache: cache, cancelAfter: cancelAfter, logger: logger}
}

// Reconcile rebuilds PostgreSQL counters and Redis snapshots for this worker's shards.
func (processor *Processor) Reconcile(ctx context.Context, messageKeys []int) error {
	events, err := processor.repository.ReconcileEventStats(ctx, messageKeys)
	if err != nil {
		return err
	}
	processor.updateCache(ctx, events)
	return nil
}

func (processor *Processor) Process(ctx context.Context, records []kafkago.Message) error {
	decoded := processor.decode(records)
	if len(decoded) == 0 {
		return nil
	}
	eventIDs := uniqueEventIDs(decoded)
	availableEvents, err := processor.loadEventSnapshots(ctx, eventIDs)
	if err != nil {
		return err
	}
	filtered := decoded[:0]
	for _, message := range decoded {
		if _, exists := availableEvents[message.ticket.EventID]; exists {
			filtered = append(filtered, message)
		} else {
			processor.logger.Warn("skip ticket for unknown event", "event_id", message.ticket.EventID)
		}
	}
	if len(filtered) == 0 {
		return nil
	}

	pendingTickets, deletePendingTickets, doneTickets, updatedEventStats, err :=
		processor.processTransaction(ctx, filtered)
	if err != nil {
		return err
	}
	if err := processor.repository.PersistTicketChanges(
		ctx, pendingTickets, deletePendingTickets, doneTickets, updatedEventStats,
	); err != nil {
		return err
	}
	processor.updateCache(ctx, updatedEventStats)
	return nil
}

func (processor *Processor) processTransaction(
	ctx context.Context,
	messages []decodedMessage,
) ([]entity.Ticket, []entity.Ticket, []entity.TicketDone, []entity.Event, error) {
	eventIDs := uniqueEventIDs(messages)
	events, err := processor.repository.FindEventsByIDs(ctx, eventIDs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	eventByID := make(map[int64]*entity.Event, len(events))
	for index := range events {
		eventByID[events[index].ID] = &events[index]
	}

	ticketIDs := uniqueTicketIDs(messages)
	activeTickets, err := processor.repository.FindPendingTicketsByIDs(ctx, ticketIDs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	doneTickets, err := processor.repository.FindDoneTicketsByIDs(ctx, ticketIDs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	activeByID := make(map[uuid.UUID]*entity.Ticket, len(activeTickets))
	for index := range activeTickets {
		activeByID[activeTickets[index].ID] = &activeTickets[index]
	}
	doneByID := make(map[uuid.UUID]struct{}, len(doneTickets))
	for _, ticket := range doneTickets {
		doneByID[ticket.ID] = struct{}{}
	}

	now := time.Now().UTC()
	changed := make(map[int64]*entity.Event)
	pendingTickets := make([]entity.Ticket, 0)
	deletePendingTickets := make([]entity.Ticket, 0)
	newDoneTickets := make([]entity.TicketDone, 0)
	for _, message := range messages {
		event := eventByID[message.ticket.EventID]
		if event == nil {
			continue
		}
		switch message.ticket.Status {
		case statusPending:
			if _, exists := activeByID[message.ticket.ID]; exists {
				continue
			}
			if _, exists := doneByID[message.ticket.ID]; exists {
				continue
			}
			createdAt := message.record.Time.UTC()
			if createdAt.IsZero() {
				createdAt = now
			}
			ticket := entity.Ticket{
				ID: message.ticket.ID, EventID: message.ticket.EventID,
				UserID: message.ticket.UserID, ClientOrderID: message.ticket.ClientOrderID,
				Status: statusPending, CreatedAt: createdAt, UpdatedAt: now,
			}
			pendingTickets = append(pendingTickets, ticket)
			activeByID[ticket.ID] = &ticket
			event.PendingTickets++
			changed[event.ID] = event

		case statusConfirm:
			active := activeByID[message.ticket.ID]
			if active == nil || active.Status != statusPending || active.EventID != message.ticket.EventID {
				continue
			}
			deletePendingTickets = append(deletePendingTickets, *active)
			newDoneTickets = append(newDoneTickets, completedTicket(active, statusConfirm, now))
			delete(activeByID, active.ID)
			doneByID[active.ID] = struct{}{}
			decrementPending(event)
			event.ConfirmTickets++
			changed[event.ID] = event

		case statusCancel:
			active := activeByID[message.ticket.ID]
			if !canCancel(active, message.ticket.EventID, now, processor.cancelAfter) {
				continue
			}
			deletePendingTickets = append(deletePendingTickets, *active)
			newDoneTickets = append(newDoneTickets, completedTicket(active, statusCancelled, now))
			delete(activeByID, active.ID)
			doneByID[active.ID] = struct{}{}
			decrementPending(event)
			event.CancelTickets++
			changed[event.ID] = event
		}
	}

	updatedEventStats := make([]entity.Event, 0, len(changed))
	for _, eventID := range sortedEventIDs(changed) {
		event := changed[eventID]
		event.UpdatedAt = now
		updatedEventStats = append(updatedEventStats, *event)
	}
	return pendingTickets, deletePendingTickets, newDoneTickets, updatedEventStats, nil
}

func completedTicket(ticket *entity.Ticket, status string, now time.Time) entity.TicketDone {
	return entity.TicketDone{
		ID: ticket.ID, EventID: ticket.EventID, UserID: ticket.UserID,
		ClientOrderID: ticket.ClientOrderID, Status: status,
		CreatedAt: ticket.CreatedAt, UpdatedAt: now,
	}
}

func (processor *Processor) decode(records []kafkago.Message) []decodedMessage {
	result := make([]decodedMessage, 0, len(records))
	for _, record := range records {
		var ticket sharedkafka.UpdatedTicket
		if err := json.Unmarshal(record.Value, &ticket); err != nil {
			processor.logger.Warn("skip malformed ticket message", "error", err)
			continue
		}
		expectedKey := strconv.FormatInt(sharedkafka.MessageKey(ticket.EventID), 10)
		if string(record.Key) != expectedKey || record.Partition != int(sharedkafka.MessageKey(ticket.EventID)) {
			processor.logger.Warn("skip ticket with mismatched shard", "event_id", ticket.EventID,
				"message_key", string(record.Key), "partition", record.Partition)
			continue
		}
		if ticket.ID == uuid.Nil || ticket.EventID <= 0 || ticket.UserID <= 0 || ticket.ClientOrderID == "" ||
			(ticket.Status != statusPending && ticket.Status != statusConfirm && ticket.Status != statusCancel) {
			processor.logger.Warn("skip invalid ticket message", "ticket_id", ticket.ID)
			continue
		}
		result = append(result, decodedMessage{record: record, ticket: ticket})
	}
	return result
}

func (processor *Processor) loadEventSnapshots(ctx context.Context, eventIDs []int64) (map[int64]entity.Event, error) {
	result, err := processor.cache.GetEvents(ctx, eventIDs)
	if err != nil {
		processor.logger.Warn("redis unavailable; loading events from postgres", "error", err)
		result = make(map[int64]entity.Event, len(eventIDs))
	}
	missing := make([]int64, 0)
	for _, eventID := range eventIDs {
		if _, exists := result[eventID]; !exists {
			missing = append(missing, eventID)
		}
	}
	if len(missing) == 0 {
		return result, nil
	}
	events, err := processor.repository.FindEventsByIDs(ctx, missing)
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		result[event.ID] = event
	}
	return result, nil
}

func (processor *Processor) updateCache(ctx context.Context, events []entity.Event) {
	if err := processor.cache.SetEvents(ctx, events); err != nil {
		// PostgreSQL is the source of truth. A later batch or startup reconciliation repairs Redis.
		processor.logger.Warn("cannot update events in redis", "error", err)
	}
}

func uniqueEventIDs(messages []decodedMessage) []int64 {
	seen := make(map[int64]struct{})
	for _, message := range messages {
		seen[message.ticket.EventID] = struct{}{}
	}
	result := make([]int64, 0, len(seen))
	for eventID := range seen {
		result = append(result, eventID)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func uniqueTicketIDs(messages []decodedMessage) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{})
	for _, message := range messages {
		seen[message.ticket.ID] = struct{}{}
	}
	result := make([]uuid.UUID, 0, len(seen))
	for ticketID := range seen {
		result = append(result, ticketID)
	}
	return result
}

func sortedEventIDs(events map[int64]*entity.Event) []int64 {
	result := make([]int64, 0, len(events))
	for eventID := range events {
		result = append(result, eventID)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func decrementPending(event *entity.Event) {
	if event.PendingTickets > 0 {
		event.PendingTickets--
	}
}

func canCancel(ticket *entity.Ticket, eventID int64, now time.Time, cancelAfter time.Duration) bool {
	return ticket != nil && ticket.Status == statusPending && ticket.EventID == eventID &&
		now.Sub(ticket.CreatedAt) > cancelAfter
}
