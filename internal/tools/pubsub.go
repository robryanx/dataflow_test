package tools

import (
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

func RunPubsubSetup(args []string) error {
	cfgPath := ConfigPathFromArgs(args)
	cfg, err := MustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ApplyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("pubsub-setup", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return PubsubSetup(context.Background(), cfg)
}

func PubsubSetup(ctx context.Context, cfg Config) error {
	client, err := pubsub.NewClient(ctx, cfg.PubsubProjectID)
	if err != nil {
		return fmt.Errorf("create pubsub client: %w", err)
	}
	defer client.Close()

	topic := client.Topic(cfg.PubsubTopic)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return fmt.Errorf("check topic exists: %w", err)
	}
	if !exists {
		topic, err = client.CreateTopic(ctx, cfg.PubsubTopic)
		if err != nil {
			return fmt.Errorf("create topic: %w", err)
		}
		log.Printf("created topic %s", cfg.PubsubTopic)
	}

	sub := client.Subscription(cfg.PubsubSubscription)
	subExists, err := sub.Exists(ctx)
	if err != nil {
		return fmt.Errorf("check subscription exists: %w", err)
	}
	if !subExists {
		if _, err := client.CreateSubscription(ctx, cfg.PubsubSubscription, pubsub.SubscriptionConfig{Topic: topic}); err != nil {
			return fmt.Errorf("create subscription: %w", err)
		}
		log.Printf("created subscription %s", cfg.PubsubSubscription)
	}

	log.Printf("pubsub setup complete: topic=%s subscription=%s", cfg.PubsubTopic, cfg.PubsubSubscription)
	return nil
}
