package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const LocalWorkerPath = "config/services/worker/config.local.yml"

type Worker struct {
	Connection  PostgresConnection `yaml:"postgres"`
	Redis       RedisConnection    `yaml:"redis"`
	Kafka       KafkaConnection    `yaml:"kafka"`
	Settings    WorkerSettings     `yaml:"worker"`
	DatabaseURL string             `yaml:"-"`
}

type WorkerSettings struct {
	GroupID     string `yaml:"group_id"`
	MessageKeys []int  `yaml:"message_keys"`
	BatchSize   int    `yaml:"batch_size"`
	BatchWait   string `yaml:"batch_wait"`
	CancelAfter string `yaml:"cancel_after"`
}

func LoadWorker() (Worker, error) {
	var cfg Worker
	if err := Load(LocalWorkerPath, &cfg); err != nil {
		return Worker{}, err
	}
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = cfg.Connection.URL()
	}
	applyWorkerEnvironment(&cfg)
	if err := cfg.Validate(); err != nil {
		return Worker{}, err
	}
	return cfg, nil
}

func (cfg Worker) Validate() error {
	if len(cfg.Kafka.Brokers) == 0 || strings.TrimSpace(cfg.Kafka.Topic) == "" {
		return fmt.Errorf("kafka brokers and topic are required")
	}
	if strings.TrimSpace(cfg.Settings.GroupID) == "" {
		return fmt.Errorf("worker group_id is required")
	}
	if len(cfg.Settings.MessageKeys) == 0 {
		return fmt.Errorf("worker message_keys cannot be empty")
	}
	seen := make(map[int]struct{}, len(cfg.Settings.MessageKeys))
	for _, key := range cfg.Settings.MessageKeys {
		if key < 0 || key >= 100 {
			return fmt.Errorf("worker message key %d must be between 0 and 99", key)
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("worker message key %d is duplicated", key)
		}
		seen[key] = struct{}{}
	}
	if cfg.Settings.BatchSize <= 0 || cfg.Settings.BatchSize > 10000 {
		return fmt.Errorf("worker batch_size must be between 1 and 10000")
	}
	if _, err := cfg.BatchWait(); err != nil {
		return err
	}
	if _, err := cfg.CancelAfter(); err != nil {
		return err
	}
	return nil
}

func (cfg Worker) BatchWait() (time.Duration, error) {
	value, err := time.ParseDuration(cfg.Settings.BatchWait)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("worker batch_wait must be a positive duration")
	}
	return value, nil
}

func (cfg Worker) CancelAfter() (time.Duration, error) {
	value, err := time.ParseDuration(cfg.Settings.CancelAfter)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("worker cancel_after must be a positive duration")
	}
	return value, nil
}

func applyWorkerEnvironment(cfg *Worker) {
	if value := os.Getenv("REDIS_ADDR"); value != "" {
		cfg.Redis.Address = value
	}
	if value := os.Getenv("REDIS_PASSWORD"); value != "" {
		cfg.Redis.Password = value
	}
	if value := os.Getenv("REDIS_DB"); value != "" {
		if database, err := strconv.Atoi(value); err == nil {
			cfg.Redis.DB = database
		}
	}
	if value := os.Getenv("KAFKA_BROKERS"); value != "" {
		cfg.Kafka.Brokers = splitNonEmpty(value)
	}
	if value := os.Getenv("KAFKA_TOPIC"); value != "" {
		cfg.Kafka.Topic = value
	}
	if value := os.Getenv("WORKER_GROUP_ID"); value != "" {
		cfg.Settings.GroupID = value
	}
	if value := os.Getenv("WORKER_MESSAGE_KEYS"); value != "" {
		cfg.Settings.MessageKeys = parseIntegerList(value)
	}
	if value := os.Getenv("WORKER_BATCH_SIZE"); value != "" {
		if batchSize, err := strconv.Atoi(value); err == nil {
			cfg.Settings.BatchSize = batchSize
		}
	}
	if value := os.Getenv("WORKER_BATCH_WAIT"); value != "" {
		cfg.Settings.BatchWait = value
	}
	if value := os.Getenv("WORKER_CANCEL_AFTER"); value != "" {
		cfg.Settings.CancelAfter = value
	}
}

func parseIntegerList(value string) []int {
	result := make([]int, 0)
	for _, part := range strings.Split(value, ",") {
		parsed, err := strconv.Atoi(strings.TrimSpace(part))
		if err == nil {
			result = append(result, parsed)
		}
	}
	return result
}
