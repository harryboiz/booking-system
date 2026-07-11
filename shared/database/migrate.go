package database

import (
	"context"
	"fmt"

	"github.com/pressly/goose/v3"
	"gorm.io/gorm"
)

func RunMigrations(ctx context.Context, db *gorm.DB, migrationDir string) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get database connection for goose: %w", err)
	}

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, sqlDB, migrationDir); err != nil {
		return fmt.Errorf("run goose migrations: %w", err)
	}
	return nil
}
