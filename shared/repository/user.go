package repository

import (
	"context"
	"errors"

	"ticket/shared/model/entity"
)

var ErrUserNotFound = errors.New("user not found")

type UserRepository interface {
	Create(context.Context, entity.User) (entity.User, error)
	List(context.Context) ([]entity.User, error)
	Get(context.Context, string) (entity.User, error)
	GetByEmail(context.Context, string) (entity.User, error)
	Update(context.Context, string, entity.User) (entity.User, error)
	Delete(context.Context, string) error
}
