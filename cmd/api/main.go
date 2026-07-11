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
	if err := database.RunMigrations(ctx, db, "migrations"); err != nil {
		slog.Error("cannot run database migrations", "error", err)
		os.Exit(1)
	}

	store := repositoryimpl.NewEventRepository(db)
	eventHandler := handler.NewEventHandler(store)
	server := &http.Server{
		Addr:              apiConfig.Server.Address,
		Handler:           api.NewHandler(eventHandler),
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("event API is listening", "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
