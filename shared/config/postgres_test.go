package config

import "testing"

func TestLoadPostgres(t *testing.T) {
	var cfg Postgres
	if err := Load("../../config/shared/postgres/config.local.yml", &cfg); err != nil {
		t.Fatal(err)
	}
	want := "postgres://ticket:ticket@localhost:5432/ticket?sslmode=disable"
	if got := cfg.Connection.URL(); got != want {
		t.Fatalf("postgres URL = %q, want %q", got, want)
	}
}
