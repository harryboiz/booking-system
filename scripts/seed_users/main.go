package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"

	sharedconfig "ticket/shared/config"
	"ticket/shared/database"
	"ticket/shared/model/entity"
)

const (
	defaultUserCount = 100_000
	defaultBatchSize = 1_000
	defaultPassword  = "password123"
)

func main() {
	count := flag.Int("count", defaultUserCount, "number of users to insert")
	batchSize := flag.Int("batch-size", defaultBatchSize, "users inserted per batch")
	flag.Parse()

	if *count <= 0 {
		log.Fatal("count must be greater than zero")
	}
	if *batchSize <= 0 {
		log.Fatal("batch-size must be greater than zero")
	}

	databaseURL, err := loadDatabaseURL()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.OpenPostgres(ctx, databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("access database connection pool: %v", err)
	}
	defer sqlDB.Close()

	if err := database.RunMigrations(ctx, db, "migrations"); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	password := os.Getenv("SEED_USER_PASSWORD")
	if password == "" {
		password = defaultPassword
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		log.Fatalf("hash seed password: %v", err)
	}

	startedAt := time.Now()
	runID := startedAt.UTC().Format("20060102T150405.000000000")
	inserted := 0
	for inserted < *count {
		if err := ctx.Err(); err != nil {
			log.Fatalf("seed interrupted after %d users: %v", inserted, err)
		}

		currentBatchSize := min(*batchSize, *count-inserted)
		users := make([]entity.User, currentBatchSize)
		for i := range users {
			sequence := inserted + i + 1
			users[i] = entity.User{
				Name:         fmt.Sprintf("Seed User %06d", sequence),
				Email:        fmt.Sprintf("seed-%s-%06d@example.com", runID, sequence),
				PasswordHash: string(passwordHash),
			}
		}

		if err := db.WithContext(ctx).Create(&users).Error; err != nil {
			log.Fatalf("insert batch starting at user %d: %v", inserted+1, err)
		}
		inserted += len(users)
		log.Printf("inserted %d/%d users", inserted, *count)
	}

	log.Printf("done: inserted %d users in %s", inserted, time.Since(startedAt).Round(time.Millisecond))
}

func loadDatabaseURL() (string, error) {
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		return databaseURL, nil
	}

	var postgresConfig sharedconfig.Postgres
	if err := sharedconfig.Load(sharedconfig.LocalPostgresPath, &postgresConfig); err != nil {
		return "", fmt.Errorf("load postgres config: %w", err)
	}
	return postgresConfig.Connection.URL(), nil
}
