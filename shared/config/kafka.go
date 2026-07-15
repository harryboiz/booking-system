package config

type KafkaConnection struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}
