package config

import (
	"os"
	"strconv"
	"strings"
)

const LocalAPIPath = "config/services/api/config.local.yml"

type API struct {
	Server      Server             `yaml:"server"`
	Connection  PostgresConnection `yaml:"postgres"`
	Redis       RedisConnection    `yaml:"redis"`
	Kafka       KafkaConnection    `yaml:"kafka"`
	DatabaseURL string             `yaml:"-"`
}

type Server struct {
	Address string `yaml:"address"`
}

// LoadAPI loads the API configuration, including its shared configuration
// files, from the default local path.
func LoadAPI() (API, error) {
	return loadAPI(LocalAPIPath)
}

func loadAPI(path string) (API, error) {
	var cfg API
	if err := Load(path, &cfg); err != nil {
		return API{}, err
	}
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = cfg.Connection.URL()
	}
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
	return cfg, nil
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}
