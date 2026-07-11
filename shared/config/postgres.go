package config

import (
	"fmt"
	"net/url"
)

const LocalPostgresPath = "config/shared/postgres/config.local.yml"

type Postgres struct {
	Connection PostgresConnection `yaml:"postgres"`
}

type PostgresConnection struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

func (cfg PostgresConnection) URL() string {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:     cfg.Database,
		RawQuery: url.Values{"sslmode": []string{sslMode}}.Encode(),
	}).String()
}
