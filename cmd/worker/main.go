package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ticket/service/worker"
	sharedconfig "ticket/shared/config"
	"ticket/shared/database"
	sharedkafka "ticket/shared/kafka"
	sharedredis "ticket/shared/redis"
	repositoryimpl "ticket/shared/repository/impl"
)

func main() {
	cfg, err := sharedconfig.LoadWorker()
	if err != nil {
		slog.Error("cannot load worker configuration", "error", err)
		os.Exit(1)
	}
	batchWait, _ := cfg.BatchWait()
	cancelAfter, _ := cfg.CancelAfter()

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	db, err := database.OpenPostgres(startupCtx, cfg.DatabaseURL)
	if err != nil {
		startupCancel()
		slog.Error("cannot initialize database", "error", err)
		os.Exit(1)
	}
	startupCancel()

	sqlDB, err := db.DB()
	if err != nil {
		slog.Error("cannot access database connection pool", "error", err)
		os.Exit(1)
	}
	defer func(sqlDB *sql.DB) {
		err := sqlDB.Close()
		if err != nil {

		}
	}(sqlDB)
	cache := sharedredis.NewEventCache(cfg.Redis.Address, cfg.Redis.Password, cfg.Redis.DB)
	defer func(cache *sharedredis.EventCache) {
		err := cache.Close()
		if err != nil {

		}
	}(cache)
	ticketRepository := repositoryimpl.NewTicketRepository(db)
	processor := worker.NewProcessor(ticketRepository, cache, cancelAfter, slog.Default())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := processor.Reconcile(ctx, cfg.Settings.MessageKeys); err != nil {
		slog.Error("cannot reconcile worker events", "error", err)
		os.Exit(1)
	}
	consumer, err := sharedkafka.NewConsumer(ctx, sharedkafka.ConsumerConfig{
		Brokers: cfg.Kafka.Brokers,
		Topic: cfg.Kafka.Topic,
		GroupID: cfg.Settings.GroupID,
		MessageKeys: cfg.Settings.MessageKeys,
		BatchSize: cfg.Settings.BatchSize,
		BatchWait: batchWait,
	}, processor, slog.Default())
	if err != nil {
		slog.Error("cannot initialize kafka consumer", "error", err)
		os.Exit(1)
	}

	slog.Info("ticket worker started", "message_keys", cfg.Settings.MessageKeys,
		"batch_size", cfg.Settings.BatchSize)
	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("ticket worker stopped", "error", err)
		os.Exit(1)
	}
}
