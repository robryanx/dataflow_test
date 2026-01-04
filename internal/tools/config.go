package tools

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
)

type Config struct {
	SpannerProjectID     string `json:"spanner_project_id"`
	SpannerInstanceID    string `json:"spanner_instance_id"`
	SpannerDatabaseID    string `json:"spanner_database_id"`
	SpannerChangeStream  string `json:"spanner_change_stream"`
	SpannerEmulatorHost  string `json:"spanner_emulator_host"`
	PubsubProjectID      string `json:"pubsub_project_id"`
	PubsubTopic          string `json:"pubsub_topic"`
	PubsubSubscription   string `json:"pubsub_subscription"`
	PubsubEmulatorHost   string `json:"pubsub_emulator_host"`
	BigQueryProjectID    string `json:"bigquery_project_id"`
	BigQueryEmulatorHost string `json:"bigquery_emulator_host"`
	ProtoMessage         string `json:"proto_message"`
	OutboxEventType      string `json:"outbox_event_type"`
	WriteIntervalMs      int    `json:"write_interval_ms"`
	AccountCount         int    `json:"account_count"`
}

func ConfigPathFromArgs(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--config" || arg == "-config" {
			if i+1 < len(args) {
				return args[i+1]
			}
		}
		if strings.HasPrefix(arg, "--config=") {
			return strings.TrimPrefix(arg, "--config=")
		}
	}
	return "tools/config.json"
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if len(data) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func MustLoadConfig(path string) (Config, error) {
	cfg, err := loadConfig(path)
	if err != nil {
		return cfg, err
	}
	if cfg.SpannerProjectID == "" ||
		cfg.SpannerInstanceID == "" ||
		cfg.SpannerDatabaseID == "" ||
		cfg.SpannerChangeStream == "" ||
		cfg.SpannerEmulatorHost == "" ||
		cfg.PubsubProjectID == "" ||
		cfg.PubsubTopic == "" ||
		cfg.PubsubSubscription == "" ||
		cfg.PubsubEmulatorHost == "" ||
		cfg.BigQueryProjectID == "" ||
		cfg.BigQueryEmulatorHost == "" ||
		cfg.ProtoMessage == "" {
		return cfg, errors.New("config is missing required fields")
	}
	return cfg, nil
}

func ApplyEmulatorEnv(cfg Config) {
	_ = os.Setenv("SPANNER_EMULATOR_HOST", cfg.SpannerEmulatorHost)
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", cfg.PubsubEmulatorHost)
	_ = os.Setenv("BIGQUERY_EMULATOR_HOST", cfg.BigQueryEmulatorHost)
}
