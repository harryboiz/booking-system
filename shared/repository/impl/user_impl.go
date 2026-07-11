package impl

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"

	"ticket/shared/model/entity"
	"ticket/shared/repository"
)

type UserRepositoryImpl struct {
	db *gorm.DB
}

var _ repository.UserRepository = (*UserRepositoryImpl)(nil)

func NewUserRepository(db *gorm.DB) repository.UserRepository {
	return &UserRepositoryImpl{db: db}
}

func (impl *UserRepositoryImpl) Create(ctx context.Context, record entity.User) (entity.User, error) {
	record.Email = normalizeEmail(record.Email)
	if err := impl.db.WithContext(ctx).Create(&record).Error; err != nil {
		return entity.User{}, fmt.Errorf("create user: %w", err)
	}
	return record, nil
}

func (impl *UserRepositoryImpl) List(ctx context.Context) ([]entity.User, error) {
	var records []entity.User
	if err := impl.db.WithContext(ctx).Order("id ASC").Find(&records).Error; err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	return records, nil
}

func (impl *UserRepositoryImpl) Get(ctx context.Context, id string) (entity.User, error) {
	numericID, err := parseUserID(id)
	if err != nil {
		return entity.User{}, err
	}

	var record entity.User
	if err := impl.db.WithContext(ctx).First(&record, numericID).Error; err != nil {
		return entity.User{}, mapUserGormError("get user", err)
	}
	return record, nil
}

func (impl *UserRepositoryImpl) GetByEmail(ctx context.Context, email string) (entity.User, error) {
	var record entity.User
	err := impl.db.WithContext(ctx).
		Where("LOWER(email) = ?", normalizeEmail(email)).
		First(&record).Error
	if err != nil {
		return entity.User{}, mapUserGormError("get user by email", err)
	}
	return record, nil
}

func (impl *UserRepositoryImpl) Update(ctx context.Context, id string, in entity.User) (entity.User, error) {
	numericID, err := parseUserID(id)
	if err != nil {
		return entity.User{}, err
	}

	updates := map[string]any{
		"name":       in.Name,
		"email":      normalizeEmail(in.Email),
		"updated_at": time.Now(),
	}
	if in.PasswordHash != "" {
		updates["password_hash"] = in.PasswordHash
	}

	var record entity.User
	err = impl.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&entity.User{}).Where("id = ?", numericID).Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return repository.ErrUserNotFound
		}
		return tx.First(&record, numericID).Error
	})
	if err != nil {
		return entity.User{}, mapUserGormError("update user", err)
	}
	return record, nil
}

func (impl *UserRepositoryImpl) Delete(ctx context.Context, id string) error {
	numericID, err := parseUserID(id)
	if err != nil {
		return err
	}

	result := impl.db.WithContext(ctx).Delete(&entity.User{}, numericID)
	if result.Error != nil {
		return fmt.Errorf("delete user: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return repository.ErrUserNotFound
	}
	return nil
}

func mapUserGormError(operation string, err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, repository.ErrUserNotFound) {
		return repository.ErrUserNotFound
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func parseUserID(id string) (int64, error) {
	value, err := strconv.ParseInt(id, 10, 64)
	if err != nil || value <= 0 {
		return 0, repository.ErrUserNotFound
	}
	return value, nil
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}
