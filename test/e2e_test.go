package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	outboxv1 "dataflow_test/gen/go/proto/outbox/v1"
	"dataflow_test/internal/tools"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestEndToEndBigQuery(t *testing.T) {
	if os.Getenv("RUN_E2E") != "1" {
		t.Skip("set RUN_E2E=1 to run integration test")
	}

	cfgPath, err := findConfigPath()
	require.NoError(t, err, "locate config")
	cfg, err := tools.MustLoadConfig(cfgPath)
	require.NoError(t, err, "load config")
	tools.ApplyEmulatorEnv(cfg)

	ctx := context.Background()
	require.NoError(t, tools.Bootstrap(ctx, cfg), "bootstrap")
	require.NoError(t, tools.PubsubSetup(ctx, cfg), "pubsub-setup")

	sinkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = tools.BigQuerySinkWithContext(sinkCtx, cfg)
	}()

	time.Sleep(2 * time.Second)

	t.Run("BsbAccount", func(t *testing.T) {
		outboxID := fmt.Sprintf("e2e-bsb-%d", time.Now().UnixNano())
		subName := fmt.Sprintf("e2e-sub-bsb-%d", time.Now().UnixNano())
		sub, err := createTestSubscription(ctx, cfg, subName)
		require.NoError(t, err, "create subscription")
		defer sub.Delete(ctx)
		time.Sleep(1 * time.Second)

		insert := tools.OutboxInsertInput{
			OutboxID:      outboxID,
			AccountID:     "account-e2e-bsb",
			Balance:       42,
			Bsb:           "123456",
			AccountNumber: "987654321",
		}
		require.NoError(t, tools.OutboxInsert(ctx, cfg, insert), "outbox-insert")

		event, err := waitForPubsubEvent(ctx, sub, outboxID, 60*time.Second)
		require.NoError(t, err, "pubsub event")
		assertOutboxEvent(t, event, outboxID, "account-e2e-bsb", 42)
		assertOutboxEventBsb(t, event, "123456", "987654321")

		eventFromBQ, err := tools.WaitForBigQueryOutbox(ctx, cfg, outboxID, 90*time.Second)
		require.NoError(t, err, "wait for bigquery row")
		assertOutboxEvent(t, eventFromBQ, outboxID, "account-e2e-bsb", 42)
		assertOutboxEventBsb(t, eventFromBQ, "123456", "987654321")
	})

	t.Run("PayID", func(t *testing.T) {
		outboxID := fmt.Sprintf("e2e-payid-%d", time.Now().UnixNano())
		subName := fmt.Sprintf("e2e-sub-payid-%d", time.Now().UnixNano())
		sub, err := createTestSubscription(ctx, cfg, subName)
		require.NoError(t, err, "create subscription")
		defer sub.Delete(ctx)
		time.Sleep(1 * time.Second)

		insert := tools.OutboxInsertInput{
			OutboxID:  outboxID,
			AccountID: "account-e2e-payid",
			Balance:   7,
			PayID:     "user@example.com",
		}
		require.NoError(t, tools.OutboxInsert(ctx, cfg, insert), "outbox-insert")

		event, err := waitForPubsubEvent(ctx, sub, outboxID, 60*time.Second)
		require.NoError(t, err, "pubsub event")
		assertOutboxEvent(t, event, outboxID, "account-e2e-payid", 7)
		assertOutboxEventPayID(t, event, "user@example.com")

		eventFromBQ, err := tools.WaitForBigQueryOutbox(ctx, cfg, outboxID, 90*time.Second)
		require.NoError(t, err, "wait for bigquery row")
		assertOutboxEvent(t, eventFromBQ, outboxID, "account-e2e-payid", 7)
		assertOutboxEventPayID(t, eventFromBQ, "user@example.com")
	})
}

func findConfigPath() (string, error) {
	start, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := start
	for i := 0; i < 5; i++ {
		path := filepath.Join(dir, "tools", "config.json")
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("tools/config.json not found from %s", start)
}

func createTestSubscription(ctx context.Context, cfg tools.Config, subName string) (*pubsub.Subscription, error) {
	client, err := pubsub.NewClient(ctx, cfg.PubsubProjectID)
	if err != nil {
		return nil, err
	}

	topic := client.Topic(cfg.PubsubTopic)
	sub, err := client.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	sub.ReceiveSettings.MaxOutstandingMessages = 10
	sub.ReceiveSettings.NumGoroutines = 1
	return sub, nil
}

func waitForPubsubEvent(ctx context.Context, sub *pubsub.Subscription, outboxID string, timeout time.Duration) (*outboxv1.OutboxEvent, error) {
	recvCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	eventCh := make(chan *outboxv1.OutboxEvent, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- sub.Receive(recvCtx, func(ctx context.Context, msg *pubsub.Message) {
			event := &outboxv1.OutboxEvent{}
			if err := proto.Unmarshal(msg.Data, event); err != nil {
				msg.Ack()
				return
			}
			if event.GetOutboxId() == outboxID {
				eventCh <- event
				msg.Ack()
				cancel()
				return
			}
			msg.Ack()
		})
	}()

	select {
	case event := <-eventCh:
		return event, nil
	case err := <-errCh:
		if err == nil && recvCtx.Err() != nil {
			return nil, fmt.Errorf("timed out waiting for pubsub event %s", outboxID)
		}
		return nil, err
	case <-recvCtx.Done():
		return nil, fmt.Errorf("timed out waiting for pubsub event %s", outboxID)
	}
}

func assertOutboxEvent(t *testing.T, event *outboxv1.OutboxEvent, outboxID, accountID string, balance int64) {
	t.Helper()
	require.Equal(t, outboxID, event.GetOutboxId(), "outbox_id mismatch")
	require.Equal(t, accountID, event.GetAccountId(), "account_id mismatch")
	require.Equal(t, balance, event.GetBalance(), "balance mismatch")
}

func assertOutboxEventBsb(t *testing.T, event *outboxv1.OutboxEvent, bsb, accountNumber string) {
	t.Helper()
	details := event.GetAccountDetails()
	require.NotNil(t, details, "account_details missing")
	bsbAccount := details.GetBsbAccount()
	require.NotNil(t, bsbAccount, "bsb_account missing")
	require.Equal(t, bsb, bsbAccount.GetBsb(), "bsb mismatch")
	require.Equal(t, accountNumber, bsbAccount.GetAccountNumber(), "account_number mismatch")
	require.Equal(t, "", details.GetPayid(), "payid should be empty")
}

func assertOutboxEventPayID(t *testing.T, event *outboxv1.OutboxEvent, payID string) {
	t.Helper()
	details := event.GetAccountDetails()
	require.NotNil(t, details, "account_details missing")
	require.Equal(t, payID, details.GetPayid(), "payid mismatch")
	require.Nil(t, details.GetBsbAccount(), "bsb_account should be nil")
}
