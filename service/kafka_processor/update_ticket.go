package kafkaprocessor

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"

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

type TicketCache interface {
	SetTicket(context.Context, []entity.Ticket, []entity.TicketDone) error
}

type UserTicketCache interface {
	SetUserTickets(context.Context, []entity.UserTicket) error
}

type UpdateTicket struct {
	eventRepository  repository.EventRepository
	ticketRepository repository.TicketRepository
	eventCache       EventCache
	ticketCache      TicketCache
	userTicketCache  UserTicketCache
	cancelAfter      time.Duration
	logger           *slog.Logger
}

type decodedMessage struct {
	record kafkago.Message
	ticket sharedkafka.UpdatedTicket
}

func NewUpdateTicket(
	eventRepository repository.EventRepository,
	ticketRepository repository.TicketRepository,
	eventCache EventCache,
	ticketCache TicketCache,
	userTicketCache UserTicketCache,
	cancelAfter time.Duration,
	logger *slog.Logger,
) *UpdateTicket {
	if logger == nil {
		logger = slog.Default()
	}
	return &UpdateTicket{
		eventRepository: eventRepository, ticketRepository: ticketRepository,
		eventCache: eventCache, ticketCache: ticketCache,
		userTicketCache: userTicketCache,
		cancelAfter:     cancelAfter, logger: logger,
	}
}

// Reconcile refreshes Redis snapshots for this worker's shards from PostgreSQL.
func (processor *UpdateTicket) Reconcile(ctx context.Context, messageKeys []int) error {
	events, err := processor.eventRepository.FindEventsByMessageKeys(
		ctx, messageKeys, int(sharedkafka.MessageKeyCount),
	)
	if err != nil {
		return err
	}
	eventIDs := make([]int64, len(events))
	for index := range events {
		eventIDs[index] = events[index].ID
	}
	pendingOrders, err := processor.ticketRepository.FindPendingTicketsByEventIDs(ctx, eventIDs)
	if err != nil {
		return err
	}
	doneOrders, err := processor.ticketRepository.FindDoneTicketsByEventIDs(ctx, eventIDs)
	if err != nil {
		return err
	}
	userTickets, err := processor.ticketRepository.FindUserTicketsByEventIDs(ctx, eventIDs)
	if err != nil {
		return err
	}
	processor.updateEventCache(ctx, events)
	processor.updateTicketCache(ctx, pendingOrders, doneOrders)
	processor.updateUserTicketCache(ctx, userTickets)
	return nil
}

func (processor *UpdateTicket) Process(ctx context.Context, records []kafkago.Message) error {
	decoded := processor.decode(records)
	if len(decoded) == 0 {
		return nil
	}

	eventIDs := uniqueEventIDs(decoded)
	ticketIDs := uniqueTicketIDs(decoded)

	var (
		events        []entity.Event
		activeTickets []entity.Ticket
		doneTickets   []entity.TicketDone
		userTickets   []entity.UserTicket
	)
	queries, queryContext := errgroup.WithContext(ctx)
	queries.Go(func() error {
		var err error
		events, err = processor.eventRepository.FindEventsByIDs(queryContext, eventIDs)
		return err
	})
	queries.Go(func() error {
		var err error
		activeTickets, err = processor.ticketRepository.FindPendingTicketsByIDs(queryContext, ticketIDs)
		return err
	})
	queries.Go(func() error {
		var err error
		doneTickets, err = processor.ticketRepository.FindDoneTicketsByIDs(queryContext, ticketIDs)
		return err
	})
	queries.Go(func() error {
		var err error
		userTickets, err = processor.ticketRepository.FindUserTicketsByEventIDs(queryContext, eventIDs)
		return err
	})
	if err := queries.Wait(); err != nil {
		return err
	}

	eventByID := make(map[int64]*entity.Event, len(events))
	for index := range events {
		eventByID[events[index].ID] = &events[index]
	}
	activeByID := make(map[uuid.UUID]*entity.Ticket, len(activeTickets))
	for index := range activeTickets {
		activeByID[activeTickets[index].ID] = &activeTickets[index]
	}
	doneByID := make(map[uuid.UUID]*entity.TicketDone, len(doneTickets))
	for index := range doneTickets {
		doneByID[doneTickets[index].ID] = &doneTickets[index]
	}
	userTicketByKey := make(map[userTicketKey]*entity.UserTicket, len(userTickets))
	for index := range userTickets {
		key := userTicketKey{eventID: userTickets[index].EventID, userID: userTickets[index].UserID}
		userTicketByKey[key] = &userTickets[index]
	}

	now := time.Now().UTC()
	changed := make(map[int64]*entity.Event)
	changedUserTickets := make(map[userTicketKey]*entity.UserTicket)
	pendingTickets := make([]entity.Ticket, 0)
	deletePendingTickets := make([]entity.Ticket, 0)
	newDoneTickets := make([]entity.TicketDone, 0)
	for _, message := range decoded {
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
			key := userTicketKey{eventID: message.ticket.EventID, userID: message.ticket.UserID}
			userTicket := userTicketByKey[key]
			if userTicket == nil {
				userTickets = append(userTickets, entity.UserTicket{
					EventID: message.ticket.EventID, UserID: message.ticket.UserID,
					CreatedAt: now, UpdatedAt: now,
				})
				userTicket = &userTickets[len(userTickets)-1]
				userTicketByKey[key] = userTicket
			}
			if event.MaxTicketPerUser > 0 && userTicket.TicketCount >= int64(event.MaxTicketPerUser) {
				continue
			}
			createdAt := message.record.Time.UTC()
			if createdAt.IsZero() {
				createdAt = now
			}
			ticket := entity.Ticket{
				ID:            message.ticket.ID,
				EventID:       message.ticket.EventID,
				UserID:        message.ticket.UserID,
				ClientOrderID: message.ticket.ClientOrderID,
				Status:        statusPending,
				CreatedAt:     createdAt,
				UpdatedAt:     now,
			}
			pendingTickets = append(pendingTickets, ticket)
			activeByID[ticket.ID] = &pendingTickets[len(pendingTickets)-1]
			event.PendingTickets++
			changed[event.ID] = event
			userTicket.TicketCount++
			userTicket.UpdatedAt = now
			changedUserTickets[key] = userTicket

		case statusConfirm:
			if _, exists := doneByID[message.ticket.ID]; exists {
				continue
			}
			active := activeByID[message.ticket.ID]
			if active == nil || active.Status != statusPending || active.EventID != message.ticket.EventID {
				continue
			}
			deletePendingTickets = append(deletePendingTickets, *active)
			done := completedTicket(active, statusConfirm, now)
			newDoneTickets = append(newDoneTickets, done)
			delete(activeByID, active.ID)
			doneByID[done.ID] = &newDoneTickets[len(newDoneTickets)-1]
			event.PendingTickets--
			event.ConfirmTickets++
			changed[event.ID] = event

		case statusCancel:
			if _, exists := doneByID[message.ticket.ID]; exists {
				continue
			}
			active := activeByID[message.ticket.ID]
			if !canCancel(active, message.ticket.EventID, now, processor.cancelAfter) {
				continue
			}
			deletePendingTickets = append(deletePendingTickets, *active)
			done := completedTicket(active, statusCancelled, now)
			newDoneTickets = append(newDoneTickets, done)
			delete(activeByID, active.ID)
			doneByID[done.ID] = &newDoneTickets[len(newDoneTickets)-1]
			event.PendingTickets--
			event.CancelTickets++
			changed[event.ID] = event
			key := userTicketKey{eventID: active.EventID, userID: active.UserID}
			if userTicket := userTicketByKey[key]; userTicket != nil && userTicket.TicketCount > 0 {
				userTicket.TicketCount--
				userTicket.UpdatedAt = now
				changedUserTickets[key] = userTicket
			}
		}
	}

	updatedEventStats := make([]entity.Event, 0, len(changed))
	for _, eventID := range sortedEventIDs(changed) {
		event := changed[eventID]
		event.UpdatedAt = now
		updatedEventStats = append(updatedEventStats, *event)
	}
	updatedUserTickets := sortedUserTickets(changedUserTickets)
	if err := processor.ticketRepository.PersistTicketChanges(
		ctx, pendingTickets, deletePendingTickets, newDoneTickets, updatedEventStats, updatedUserTickets,
	); err != nil {
		return err
	}
	processor.updateEventCache(ctx, updatedEventStats)
	processor.updateTicketCache(ctx, pendingOrders(activeByID), completedOrders(doneByID))
	processor.updateUserTicketCache(ctx, updatedUserTickets)
	return nil
}

type userTicketKey struct {
	eventID int64
	userID  int64
}

func sortedUserTickets(userTickets map[userTicketKey]*entity.UserTicket) []entity.UserTicket {
	keys := make([]userTicketKey, 0, len(userTickets))
	for key := range userTickets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].eventID == keys[j].eventID {
			return keys[i].userID < keys[j].userID
		}
		return keys[i].eventID < keys[j].eventID
	})
	result := make([]entity.UserTicket, 0, len(keys))
	for _, key := range keys {
		result = append(result, *userTickets[key])
	}
	return result
}

func completedTicket(ticket *entity.Ticket, status string, now time.Time) entity.TicketDone {
	return entity.TicketDone{
		ID:            ticket.ID,
		EventID:       ticket.EventID,
		UserID:        ticket.UserID,
		ClientOrderID: ticket.ClientOrderID,
		Status:        status,
		CreatedAt:     ticket.CreatedAt,
		UpdatedAt:     now,
	}
}

func (processor *UpdateTicket) decode(records []kafkago.Message) []decodedMessage {
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

func (processor *UpdateTicket) updateEventCache(ctx context.Context, events []entity.Event) {
	if err := processor.eventCache.SetEvents(ctx, events); err != nil {
		// PostgreSQL is the source of truth. A later batch or startup reconciliation repairs Redis.
		processor.logger.Warn("cannot update events in redis", "error", err)
	}
}

func (processor *UpdateTicket) updateTicketCache(
	ctx context.Context,
	pendingOrders []entity.Ticket,
	doneOrders []entity.TicketDone,
) {
	if err := processor.ticketCache.SetTicket(ctx, pendingOrders, doneOrders); err != nil {
		// PostgreSQL is the source of truth. A duplicate message can repair a stale cache later.
		processor.logger.Warn("cannot update orders in redis", "error", err)
	}
}

func (processor *UpdateTicket) updateUserTicketCache(
	ctx context.Context,
	userTickets []entity.UserTicket,
) {
	if err := processor.userTicketCache.SetUserTickets(ctx, userTickets); err != nil {
		processor.logger.Warn("cannot update user ticket counters in redis", "error", err)
	}
}

func pendingOrders(orders map[uuid.UUID]*entity.Ticket) []entity.Ticket {
	result := make([]entity.Ticket, 0, len(orders))
	for _, order := range orders {
		result = append(result, *order)
	}
	return result
}

func completedOrders(orders map[uuid.UUID]*entity.TicketDone) []entity.TicketDone {
	result := make([]entity.TicketDone, 0, len(orders))
	for _, order := range orders {
		result = append(result, *order)
	}
	return result
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

func canCancel(ticket *entity.Ticket, eventID int64, now time.Time, cancelAfter time.Duration) bool {
	return ticket != nil && ticket.Status == statusPending && ticket.EventID == eventID &&
		now.Sub(ticket.CreatedAt) > cancelAfter
}
