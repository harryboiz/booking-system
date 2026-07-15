package impl

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"gorm.io/gorm"

	"ticket/shared/model/entity"
	"ticket/shared/repository"
)

type EventRepositoryImpl struct {
	db *gorm.DB
}

var _ repository.EventRepository = (*EventRepositoryImpl)(nil)

func NewEventRepository(db *gorm.DB) repository.EventRepository {
	return &EventRepositoryImpl{db: db}
}

func (impl *EventRepositoryImpl) FindEventsByMessageKeys(
	ctx context.Context,
	messageKeys []int,
	batchMessageKey int,
) ([]entity.Event, error) {
	var events []entity.Event
	if err := impl.db.WithContext(ctx).
		Where("MOD(id, ?) IN ?", batchMessageKey, messageKeys).
		Where("end_time > NOW() + INTERVAL '1 day'").
		Order("id").
		Find(&events).Error; err != nil {
		return nil, fmt.Errorf("load events by message keys: %w", err)
	}
	return events, nil
}

func (impl *EventRepositoryImpl) FindEventsByIDs(
	ctx context.Context,
	eventIDs []int64,
) ([]entity.Event, error) {
	var events []entity.Event
	if err := impl.db.WithContext(ctx).Where("id IN ?", eventIDs).Order("id").Find(&events).Error; err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}
	return events, nil
}

func (impl *EventRepositoryImpl) Create(ctx context.Context, record entity.Event) (entity.Event, error) {
	if err := impl.db.WithContext(ctx).Create(&record).Error; err != nil {
		return entity.Event{}, fmt.Errorf("create event: %w", err)
	}
	return record, nil
}

func (impl *EventRepositoryImpl) List(ctx context.Context) ([]entity.Event, error) {
	var records []entity.Event
	if err := impl.db.WithContext(ctx).Order("id ASC").Find(&records).Error; err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	return records, nil
}

func (impl *EventRepositoryImpl) Get(ctx context.Context, id string) (entity.Event, error) {
	numericID, err := parseEventID(id)
	if err != nil {
		return entity.Event{}, err
	}
	var record entity.Event
	if err := impl.db.WithContext(ctx).First(&record, numericID).Error; err != nil {
		return entity.Event{}, mapGormError("get event", err)
	}
	return record, nil
}

func (impl *EventRepositoryImpl) Update(ctx context.Context, id string, in entity.Event) (entity.Event, error) {
	numericID, err := parseEventID(id)
	if err != nil {
		return entity.Event{}, err
	}
	var record entity.Event
	err = impl.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&entity.Event{}).Where("id = ?", numericID).Updates(map[string]any{
			"name": in.Name, "description": in.Description, "start_date": in.StartDate,
			"end_time":      in.EndTime,
			"total_tickets": in.TotalTickets, "ticket_price": in.TicketPrice,
			"max_ticket_per_user": in.MaxTicketPerUser,
			"updated_at":          time.Now(),
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return repository.ErrEventNotFound
		}
		return tx.First(&record, numericID).Error
	})
	if err != nil {
		return entity.Event{}, mapGormError("update event", err)
	}
	return record, nil
}

func (impl *EventRepositoryImpl) Delete(ctx context.Context, id string) error {
	numericID, err := parseEventID(id)
	if err != nil {
		return err
	}
	result := impl.db.WithContext(ctx).Delete(&entity.Event{}, numericID)
	if result.Error != nil {
		return fmt.Errorf("delete event: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return repository.ErrEventNotFound
	}
	return nil
}

func mapGormError(operation string, err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, repository.ErrEventNotFound) {
		return repository.ErrEventNotFound
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func parseEventID(id string) (int64, error) {
	value, err := strconv.ParseInt(id, 10, 64)
	if err != nil || value <= 0 {
		return 0, repository.ErrEventNotFound
	}
	return value, nil
}
