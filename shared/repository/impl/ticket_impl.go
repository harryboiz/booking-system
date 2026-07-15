package impl

import (
	"context"
	"errors"
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

func (impl *TicketRepositoryImpl) GetDoneTicketByID(
	ctx context.Context,
	userID int64,
	ticketID uuid.UUID,
) (entity.TicketDone, error) {
	var ticket entity.TicketDone
	err := impl.db.WithContext(ctx).
		Where("user_id = ? AND id = ?", userID, ticketID).
		Order("updated_at DESC").
		First(&ticket).Error
	if err != nil {
		return entity.TicketDone{}, mapTicketQueryError("get completed ticket by id", err)
	}
	return ticket, nil
}

func (impl *TicketRepositoryImpl) GetDoneTicketByClientOrderID(
	ctx context.Context,
	userID int64,
	clientOrderID string,
) (entity.TicketDone, error) {
	var ticket entity.TicketDone
	err := impl.db.WithContext(ctx).
		Where("user_id = ? AND client_order_id = ?", userID, clientOrderID).
		Order("updated_at DESC").
		First(&ticket).Error
	if err != nil {
		return entity.TicketDone{}, mapTicketQueryError("get completed ticket by client order id", err)
	}
	return ticket, nil
}

func mapTicketQueryError(operation string, err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return repository.ErrTicketNotFound
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func (impl *TicketRepositoryImpl) FindPendingTicketsByEventIDs(
	ctx context.Context,
	eventIDs []int64,
) ([]entity.Ticket, error) {
	if len(eventIDs) == 0 {
		return []entity.Ticket{}, nil
	}
	var tickets []entity.Ticket
	if err := impl.db.WithContext(ctx).
		Where("event_id IN ?", eventIDs).
		Order("id").
		Find(&tickets).Error; err != nil {
		return nil, fmt.Errorf("load active tickets by event ids: %w", err)
	}
	return tickets, nil
}

func (impl *TicketRepositoryImpl) FindDoneTicketsByEventIDs(
	ctx context.Context,
	eventIDs []int64,
) ([]entity.TicketDone, error) {
	if len(eventIDs) == 0 {
		return []entity.TicketDone{}, nil
	}
	var tickets []entity.TicketDone
	if err := impl.db.WithContext(ctx).
		Where("event_id IN ?", eventIDs).
		Order("id").
		Find(&tickets).Error; err != nil {
		return nil, fmt.Errorf("load completed tickets by event ids: %w", err)
	}
	return tickets, nil
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
