package repository

import (
	"context"
	"errors"

	"ticket/shared/model/entity"
)

var ErrEventNotFound = errors.New("event not found")

type EventRepository interface {
	FindEventsByMessageKeys(context.Context, []int, int) ([]entity.Event, error)
	FindEventsByIDs(context.Context, []int64) ([]entity.Event, error)
	Create(context.Context, entity.Event) (entity.Event, error)
	List(context.Context) ([]entity.Event, error)
	Get(context.Context, string) (entity.Event, error)
	Update(context.Context, string, entity.Event) (entity.Event, error)
	Delete(context.Context, string) error
}
