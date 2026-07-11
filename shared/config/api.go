package config

import "os"

const LocalAPIPath = "config/services/api/config.local.yml"

type API struct {
	Server      Server             `yaml:"server"`
	Connection  PostgresConnection `yaml:"postgres"`
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
	return cfg, nil
}
