package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const LocalCancellationPath = "config/services/cancellation/config.local.yml"

type Cancellation struct {
	Connection  PostgresConnection   `yaml:"postgres"`
	Kafka       KafkaConnection      `yaml:"kafka"`
	Settings    CancellationSettings `yaml:"cancellation"`
	DatabaseURL string               `yaml:"-"`
}

type CancellationSettings struct {
	BatchSize    int    `yaml:"batch_size"`
	PollInterval string `yaml:"poll_interval"`
	CancelAfter  string `yaml:"cancel_after"`
}

func LoadCancellation() (Cancellation, error) {
	return loadCancellation(LocalCancellationPath)
}

func loadCancellation(path string) (Cancellation, error) {
	var cfg Cancellation
	if err := Load(path, &cfg); err != nil {
		return Cancellation{}, err
	}
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = cfg.Connection.URL()
	}
	if value := os.Getenv("KAFKA_BROKERS"); value != "" {
		cfg.Kafka.Brokers = splitNonEmpty(value)
	}
	if value := os.Getenv("KAFKA_TOPIC"); value != "" {
		cfg.Kafka.Topic = value
	}
	if value := os.Getenv("CANCELLATION_BATCH_SIZE"); value != "" {
		if batchSize, err := strconv.Atoi(value); err == nil {
			cfg.Settings.BatchSize = batchSize
		}
	}
	if value := os.Getenv("CANCELLATION_POLL_INTERVAL"); value != "" {
		cfg.Settings.PollInterval = value
	}
	if value := os.Getenv("CANCELLATION_CANCEL_AFTER"); value != "" {
		cfg.Settings.CancelAfter = value
	}
	if err := cfg.Validate(); err != nil {
		return Cancellation{}, err
	}
	return cfg, nil
}

func (cfg Cancellation) Validate() error {
	if len(cfg.Kafka.Brokers) == 0 || strings.TrimSpace(cfg.Kafka.Topic) == "" {
		return fmt.Errorf("kafka brokers and topic are required")
	}
	if cfg.Settings.BatchSize <= 0 || cfg.Settings.BatchSize > 10000 {
		return fmt.Errorf("cancellation batch_size must be between 1 and 10000")
	}
	if _, err := cfg.PollInterval(); err != nil {
		return err
	}
	if _, err := cfg.CancelAfter(); err != nil {
		return err
	}
	return nil
}

func (cfg Cancellation) PollInterval() (time.Duration, error) {
	value, err := time.ParseDuration(cfg.Settings.PollInterval)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("cancellation poll_interval must be a positive duration")
	}
	return value, nil
}

func (cfg Cancellation) CancelAfter() (time.Duration, error) {
	value, err := time.ParseDuration(cfg.Settings.CancelAfter)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("cancellation cancel_after must be a positive duration")
	}
	return value, nil
}
