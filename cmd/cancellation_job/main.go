package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ticket/service/cronjob"
	sharedconfig "ticket/shared/config"
	"ticket/shared/database"
	sharedkafka "ticket/shared/kafka"
	"ticket/shared/paypal"
	repositoryimpl "ticket/shared/repository/impl"
)

func main() {
	cfg, err := sharedconfig.LoadCancellation()
	if err != nil {
		slog.Error("cannot load cancellation configuration", "error", err)
		os.Exit(1)
	}
	pollInterval, _ := cfg.PollInterval()
	cancelAfter, _ := cfg.CancelAfter()

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	db, err := database.OpenPostgres(startupCtx, cfg.DatabaseURL)
	startupCancel()
	if err != nil {
		slog.Error("cannot initialize database", "error", err)
		os.Exit(1)
	}
	sqlDB, err := db.DB()
	if err != nil {
		slog.Error("cannot access database connection pool", "error", err)
		os.Exit(1)
	}
	defer closeDatabase(sqlDB)

	publisher := sharedkafka.NewTicketPublisher(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	defer func() {
		if err := publisher.Close(); err != nil {
			slog.Warn("cannot close kafka publisher", "error", err)
		}
	}()

	cancelExpiredTicket := cronjob.NewCancelExpiredTicket(
		repositoryimpl.NewTicketRepository(db), publisher, paypal.NewSimulator(), cancelAfter,
		pollInterval, cfg.Settings.BatchSize, slog.Default(),
	)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("ticket cancellation service started", "batch_size", cfg.Settings.BatchSize,
		"cancel_after", cancelAfter, "poll_interval", pollInterval)
	cancelExpiredTicket.Run(ctx)
}

func closeDatabase(db *sql.DB) {
	if err := db.Close(); err != nil {
		slog.Warn("cannot close database", "error", err)
	}
}
