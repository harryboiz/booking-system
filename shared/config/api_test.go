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
	if cfg.Redis.Address != "localhost:6379" || cfg.Redis.DB != 0 {
		t.Fatalf("redis config = %+v", cfg.Redis)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "localhost:9092" {
		t.Fatalf("kafka brokers = %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "ticket" {
		t.Fatalf("kafka topic = %q", cfg.Kafka.Topic)
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

func TestLoadAPIWithRedisAndKafkaOverrides(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "2")
	t.Setenv("KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	t.Setenv("KAFKA_TOPIC", "custom-ticket")

	cfg, err := loadAPI("../../config/services/api/config.local.yml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Address != "redis:6379" || cfg.Redis.Password != "secret" || cfg.Redis.DB != 2 {
		t.Fatalf("redis config = %+v", cfg.Redis)
	}
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[1] != "kafka-2:9092" {
		t.Fatalf("kafka brokers = %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "custom-ticket" {
		t.Fatalf("kafka topic = %q", cfg.Kafka.Topic)
	}
}
