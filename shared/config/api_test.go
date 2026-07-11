package config

import "testing"

func TestLoadAPI(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	cfg, err := loadAPI("../../config/services/api/config.local.yml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Address != ":8080" {
		t.Fatalf("server address = %q", cfg.Server.Address)
	}
	want := "postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable"
	if got := cfg.DatabaseURL; got != want {
		t.Fatalf("included postgres URL = %q, want %q", got, want)
	}
}

func TestLoadAPIWithDatabaseURLOverride(t *testing.T) {
	want := "postgres://override:secret@database:5432/production?sslmode=require"
	t.Setenv("DATABASE_URL", want)
	cfg, err := loadAPI("../../config/services/api/config.local.yml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != want {
		t.Fatalf("database URL = %q, want %q", cfg.DatabaseURL, want)
	}
}
