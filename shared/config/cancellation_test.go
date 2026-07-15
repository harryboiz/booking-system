package config

import "testing"

func TestLoadCancellation(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	cfg, err := loadCancellation("../../config/services/cancellation/config.local.yml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Settings.BatchSize != 10000 || cfg.Settings.PollInterval != "1m" ||
		cfg.Settings.CancelAfter != "20m" {
		t.Fatalf("settings = %+v", cfg.Settings)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Topic != "ticket" {
		t.Fatalf("kafka = %+v", cfg.Kafka)
	}
	if cfg.DatabaseURL == "" {
		t.Fatal("database URL is empty")
	}
}

func TestCancellationValidation(t *testing.T) {
	valid := Cancellation{
		Kafka: KafkaConnection{Brokers: []string{"localhost:9092"}, Topic: "ticket"},
		Settings: CancellationSettings{
			BatchSize: 10000, PollInterval: "1m", CancelAfter: "20m",
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Cancellation)
	}{
		{"empty kafka", func(cfg *Cancellation) { cfg.Kafka.Brokers = nil }},
		{"invalid batch", func(cfg *Cancellation) { cfg.Settings.BatchSize = 0 }},
		{"invalid poll interval", func(cfg *Cancellation) { cfg.Settings.PollInterval = "0s" }},
		{"invalid cancel timeout", func(cfg *Cancellation) { cfg.Settings.CancelAfter = "soon" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
