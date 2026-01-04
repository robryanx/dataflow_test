package tools

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	outboxv1 "dataflow_test/gen/go/proto/outbox/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func RunOutboxSubscribe(args []string) error {
	cfgPath := ConfigPathFromArgs(args)
	cfg, err := MustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ApplyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("outbox-subscribe", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, cfg.PubsubProjectID)
	if err != nil {
		return fmt.Errorf("create pubsub client: %w", err)
	}
	defer client.Close()

	sub := client.Subscription(cfg.PubsubSubscription)
	var received int32

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		if atomic.AddInt32(&received, 1) == 1 {
			event := &outboxv1.OutboxEvent{}
			if err := proto.Unmarshal(msg.Data, event); err != nil {
				log.Printf("decode outbox event failed: %v", err)
			} else {
				log.Printf("outbox event: outbox_id=%s event_type=%s account_id=%s balance=%d details=%s", event.GetOutboxId(), event.GetEventType(), event.GetAccountId(), event.GetBalance(), formatAccountDetails(event.GetAccountDetails()))
			}
		}
		msg.Ack()
	})
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("receive: %w", err)
	}

	if atomic.LoadInt32(&received) == 0 {
		log.Printf("no messages received")
	}
	return nil
}

func RunOutboxInsert(args []string) error {
	cfgPath := ConfigPathFromArgs(args)
	cfg, err := MustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ApplyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("outbox-insert", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	outboxID := fs.String("outbox-id", "", "outbox id (default random)")
	eventType := fs.String("event-type", cfg.OutboxEventType, "event type")
	accountID := fs.String("account-id", "", "account id")
	balance := fs.Int64("balance", 0, "balance")
	payID := fs.String("payid", "", "payid identifier (oneof with bsb/account)")
	bsb := fs.String("bsb", "", "bsb identifier")
	accountNumber := fs.String("account-number", "", "account number identifier")
	payloadFile := fs.String("payload-file", "", "payload file path")
	payloadBase64 := fs.String("payload-base64", "", "payload base64")
	payloadText := fs.String("payload-text", "", "payload text")
	if err := fs.Parse(args); err != nil {
		return err
	}

	payload, err := readPayload(*payloadFile, *payloadBase64, *payloadText)
	if err != nil {
		return err
	}

	input := OutboxInsertInput{
		OutboxID:      *outboxID,
		EventType:     *eventType,
		AccountID:     *accountID,
		Balance:       *balance,
		PayID:         *payID,
		Bsb:           *bsb,
		AccountNumber: *accountNumber,
		Payload:       payload,
	}
	return OutboxInsert(context.Background(), cfg, input)
}

type OutboxInsertInput struct {
	OutboxID      string
	EventType     string
	AccountID     string
	Balance       int64
	PayID         string
	Bsb           string
	AccountNumber string
	Payload       []byte
}

func OutboxInsert(ctx context.Context, cfg Config, input OutboxInsertInput) error {
	outboxID := input.OutboxID
	if outboxID == "" {
		outboxID = uuid.NewString()
	}
	eventType := input.EventType
	if eventType == "" {
		eventType = cfg.OutboxEventType
	}

	payload := input.Payload
	if len(payload) == 0 {
		event := &outboxv1.OutboxEvent{
			OutboxId:  outboxID,
			EventType: eventType,
			AccountId: input.AccountID,
			Balance:   input.Balance,
		}
		applyAccountDetails(event, input.PayID, input.Bsb, input.AccountNumber)
		log.Printf("outbox details: %s", formatAccountDetails(event.GetAccountDetails()))
		var err error
		payload, err = proto.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
	}

	dbPath := "projects/" + cfg.SpannerProjectID + "/instances/" + cfg.SpannerInstanceID + "/databases/" + cfg.SpannerDatabaseID
	client, err := spanner.NewClient(ctx, dbPath, SpannerOpts()...)
	if err != nil {
		return fmt.Errorf("create spanner client: %w", err)
	}
	defer client.Close()

	m := spanner.Insert("Outbox", []string{"OutboxId", "EventType", "Payload", "CreatedAt"}, []interface{}{
		outboxID,
		eventType,
		payload,
		spanner.CommitTimestamp,
	})

	if _, err := client.Apply(ctx, []*spanner.Mutation{m}); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	log.Printf("inserted outbox message outboxId=%s bytes=%d", outboxID, len(payload))
	return nil
}

func applyAccountDetails(event *outboxv1.OutboxEvent, payID, bsb, accountNumber string) {
	if payID == "" && bsb == "" && accountNumber == "" {
		return
	}
	details := &outboxv1.AccountDetails{}
	if payID != "" {
		details.Identifier = &outboxv1.AccountDetails_Payid{Payid: payID}
		event.AccountDetails = details
		return
	}
	if bsb != "" || accountNumber != "" {
		details.Identifier = &outboxv1.AccountDetails_BsbAccount{
			BsbAccount: &outboxv1.BsbAccount{
				Bsb:           bsb,
				AccountNumber: accountNumber,
			},
		}
		event.AccountDetails = details
	}
}

func formatAccountDetails(details *outboxv1.AccountDetails) string {
	if details == nil {
		return "none"
	}
	switch v := details.Identifier.(type) {
	case *outboxv1.AccountDetails_Payid:
		return fmt.Sprintf("payid=%s", v.Payid)
	case *outboxv1.AccountDetails_BsbAccount:
		if v.BsbAccount == nil {
			return "bsb_account=<nil>"
		}
		return fmt.Sprintf("bsb=%s account_number=%s", v.BsbAccount.Bsb, v.BsbAccount.AccountNumber)
	default:
		return "unknown"
	}
}

func readPayload(filePath, payloadBase64, payloadText string) ([]byte, error) {
	if filePath != "" {
		return os.ReadFile(filePath)
	}
	if payloadBase64 != "" {
		return base64.StdEncoding.DecodeString(payloadBase64)
	}
	if payloadText != "" {
		return []byte(payloadText), nil
	}
	return []byte(""), nil
}
