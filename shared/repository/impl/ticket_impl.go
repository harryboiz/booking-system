package impl

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"ticket/shared/model/entity"
	"ticket/shared/repository"
)

const (
	workerDatabaseBatchSize = 1000
	pendingTicketStatus     = "pending"
)

type TicketRepositoryImpl struct {
	db *gorm.DB
}

var _ repository.TicketRepository = (*TicketRepositoryImpl)(nil)

func NewTicketRepository(db *gorm.DB) repository.TicketRepository {
	return &TicketRepositoryImpl{db: db}
}

func (impl *TicketRepositoryImpl) ReconcileEventStats(
	ctx context.Context,
	messageKeys []int,
) ([]entity.Event, error) {
	var events []entity.Event
	err := impl.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`
			UPDATE events AS event
			SET pending_tickets = (
					SELECT COUNT(*) FROM tickets
					WHERE tickets.event_id = event.id AND tickets.status = 'pending'
				),
				confirm_tickets = (
					SELECT COUNT(*) FROM ticket_done
					WHERE ticket_done.event_id = event.id AND ticket_done.status = 'confirm'
				),
				cancel_tickets = (
					SELECT COUNT(*) FROM ticket_done
					WHERE ticket_done.event_id = event.id AND ticket_done.status = 'cancelled'
				),
				updated_at = NOW()
			WHERE MOD(event.id, 100) IN ?`, messageKeys).Error; err != nil {
			return fmt.Errorf("reconcile event counters: %w", err)
		}
		if err := tx.Where("MOD(id, 100) IN ?", messageKeys).Order("id").Find(&events).Error; err != nil {
			return fmt.Errorf("load reconciled events: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (impl *TicketRepositoryImpl) FindEventsByIDs(
	ctx context.Context,
	eventIDs []int64,
) ([]entity.Event, error) {
	var events []entity.Event
	if err := impl.db.WithContext(ctx).Where("id IN ?", eventIDs).Order("id").Find(&events).Error; err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}
	return events, nil
}

func (impl *TicketRepositoryImpl) FindPendingTicketsByIDs(
	ctx context.Context,
	ticketIDs []uuid.UUID,
) ([]entity.Ticket, error) {
	var tickets []entity.Ticket
	if err := impl.db.WithContext(ctx).Where("id IN ?", ticketIDs).Find(&tickets).Error; err != nil {
		return nil, fmt.Errorf("load active tickets: %w", err)
	}
	return tickets, nil
}

func (impl *TicketRepositoryImpl) FindDoneTicketsByIDs(
	ctx context.Context,
	ticketIDs []uuid.UUID,
) ([]entity.TicketDone, error) {
	var tickets []entity.TicketDone
	if err := impl.db.WithContext(ctx).Where("id IN ?", ticketIDs).Find(&tickets).Error; err != nil {
		return nil, fmt.Errorf("load completed tickets: %w", err)
	}
	return tickets, nil
}

func (impl *TicketRepositoryImpl) PersistTicketChanges(
	ctx context.Context,
	pendingTickets []entity.Ticket,
	deletePendingTickets []entity.Ticket,
	doneTickets []entity.TicketDone,
	updatedEventStats []entity.Event,
) error {
	return impl.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if len(pendingTickets) > 0 {
			if err := tx.CreateInBatches(&pendingTickets, workerDatabaseBatchSize).Error; err != nil {
				return fmt.Errorf("batch insert pending tickets: %w", err)
			}
		}
		if len(deletePendingTickets) > 0 {
			if err := tx.Where("id IN ? AND status = ?", ticketIDs(deletePendingTickets), pendingTicketStatus).
				Delete(&entity.Ticket{}).Error; err != nil {
				return fmt.Errorf("batch delete pending tickets: %w", err)
			}
		}
		if len(doneTickets) > 0 {
			if err := tx.CreateInBatches(&doneTickets, workerDatabaseBatchSize).Error; err != nil {
				return fmt.Errorf("batch insert completed tickets: %w", err)
			}
		}
		if len(updatedEventStats) > 0 {
			onConflict := clause.OnConflict{
				Columns: []clause.Column{{Name: "id"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"pending_tickets", "confirm_tickets", "cancel_tickets", "updated_at",
				}),
			}
			if err := tx.Clauses(onConflict).
				CreateInBatches(&updatedEventStats, workerDatabaseBatchSize).Error; err != nil {
				return fmt.Errorf("batch update event stats: %w", err)
			}
		}
		return nil
	})
}

func ticketIDs(tickets []entity.Ticket) []uuid.UUID {
	result := make([]uuid.UUID, 0, len(tickets))
	for _, ticket := range tickets {
		result = append(result, ticket.ID)
	}
	return result
}
