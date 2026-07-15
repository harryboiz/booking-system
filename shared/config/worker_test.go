package config

import "testing"

func TestWorkerValidation(t *testing.T) {
	valid := Worker{
		Kafka: KafkaConnection{Brokers: []string{"localhost:9092"}, Topic: "ticket"},
		Settings: WorkerSettings{
			GroupID: "worker", MessageKeys: []int{1, 2}, BatchSize: 10000,
			BatchWait: "1s", CancelAfter: "15m",
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Worker)
	}{
		{"empty keys", func(cfg *Worker) { cfg.Settings.MessageKeys = nil }},
		{"duplicate keys", func(cfg *Worker) { cfg.Settings.MessageKeys = []int{1, 1} }},
		{"key out of range", func(cfg *Worker) { cfg.Settings.MessageKeys = []int{100} }},
		{"oversized batch", func(cfg *Worker) { cfg.Settings.BatchSize = 10001 }},
		{"invalid wait", func(cfg *Worker) { cfg.Settings.BatchWait = "0s" }},
		{"invalid cancel timeout", func(cfg *Worker) { cfg.Settings.CancelAfter = "soon" }},
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
