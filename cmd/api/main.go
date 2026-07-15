package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	api "ticket/service/api"
	"ticket/service/api/handler"
	sharedconfig "ticket/shared/config"
	"ticket/shared/database"
	"ticket/shared/kafka"
	"ticket/shared/redis"
	repositoryimpl "ticket/shared/repository/impl"
)

func main() {
	apiConfig, err := sharedconfig.LoadAPI()
	if err != nil {
		slog.Error("cannot load API configuration", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := database.OpenPostgres(ctx, apiConfig.DatabaseURL)
	if err != nil {
		slog.Error("cannot initialize database", "error", err)
		os.Exit(1)
	}
	sqlDB, err := db.DB()
	if err != nil {
		slog.Error("cannot access database connection pool", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()
	ticketCache := redis.NewTicketCache(
		apiConfig.Redis.Address,
		apiConfig.Redis.Password,
		apiConfig.Redis.DB,
	)
	defer ticketCache.Close()
	if err := ticketCache.Ping(ctx); err != nil {
		slog.Error("cannot initialize redis", "error", err)
		os.Exit(1)
	}
	eventCache := redis.NewEventCache(
		apiConfig.Redis.Address,
		apiConfig.Redis.Password,
		apiConfig.Redis.DB,
	)
	defer eventCache.Close()
	publisher := kafka.NewTicketPublisher(apiConfig.Kafka.Brokers, apiConfig.Kafka.Topic)
	defer publisher.Close()

	eventStore := repositoryimpl.NewEventRepository(db)
	ticketStore := repositoryimpl.NewTicketRepository(db)
	eventHandler := handler.NewEventHandler(eventStore)
	ticketHandler := handler.NewTicketHandler(ticketCache, ticketStore, eventCache, publisher)
	server := &http.Server{
		Addr:              apiConfig.Server.Address,
		Handler:           api.NewHandler(eventHandler, ticketHandler),
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("ticket API is listening", "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
