package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1"
	outboxv1 "dataflow_test/gen/go/proto/outbox/v1"
	"github.com/google/uuid"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "bootstrap":
		err = runBootstrap(args)
	case "writer":
		err = runWriter(args)
	case "pubsub-setup":
		err = runPubsubSetup(args)
	case "outbox-subscribe":
		err = runOutboxSubscribe(args)
	case "outbox-insert":
		err = runOutboxInsert(args)
	case "bq-sink":
		err = runBigQuerySink(args)
	case "bq-check":
		err = runBigQueryCheck(args)
	case "spanner-check":
		err = runSpannerCheck(args)
	default:
		usage()
		err = fmt.Errorf("unknown command: %s", cmd)
	}

	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func usage() {
	fmt.Println("usage: go run ./tools <command> [args]")
	fmt.Println("commands: bootstrap, writer, pubsub-setup, outbox-subscribe, outbox-insert, bq-sink, bq-check, spanner-check")
}

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

func configPathFromArgs(args []string) string {
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

func mustLoadConfig(path string) (Config, error) {
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

func applyEmulatorEnv(cfg Config) {
	_ = os.Setenv("SPANNER_EMULATOR_HOST", cfg.SpannerEmulatorHost)
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", cfg.PubsubEmulatorHost)
	_ = os.Setenv("BIGQUERY_EMULATOR_HOST", cfg.BigQueryEmulatorHost)
}

func spannerOpts() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		opts = append(opts, option.WithoutAuthentication())
	}
	return opts
}

func bigqueryOpts(emulatorHost string) []option.ClientOption {
	if emulatorHost == "" {
		return nil
	}
	endpoint := emulatorHost
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}
	return []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
	}
}

func runBootstrap(args []string) error {
	cfgPath := configPathFromArgs(args)
	cfg, err := mustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	instanceClient, err := instance.NewInstanceAdminClient(ctx, spannerOpts()...)
	if err != nil {
		return fmt.Errorf("create instance admin client: %w", err)
	}
	defer instanceClient.Close()

	databaseClient, err := database.NewDatabaseAdminClient(ctx, spannerOpts()...)
	if err != nil {
		return fmt.Errorf("create database admin client: %w", err)
	}
	defer databaseClient.Close()

	instanceName := fmt.Sprintf("projects/%s/instances/%s", cfg.SpannerProjectID, cfg.SpannerInstanceID)
	parent := fmt.Sprintf("projects/%s", cfg.SpannerProjectID)

	createInstanceOp, err := instanceClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     parent,
		InstanceId: cfg.SpannerInstanceID,
		Instance: &instancepb.Instance{
			Name:        instanceName,
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", cfg.SpannerProjectID),
			DisplayName: "Emulator Instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create instance: %w", err)
		}
	} else {
		if _, err := createInstanceOp.Wait(ctx); err != nil {
			return fmt.Errorf("wait for instance creation: %w", err)
		}
	}

	ddl := []string{
		"CREATE TABLE Accounts (AccountId STRING(36) NOT NULL, Name STRING(128), Balance INT64) PRIMARY KEY (AccountId)",
		"CREATE TABLE Outbox (OutboxId STRING(36) NOT NULL, EventType STRING(128), Payload BYTES(MAX), CreatedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (OutboxId)",
		fmt.Sprintf("CREATE CHANGE STREAM %s FOR Accounts, Outbox OPTIONS (value_capture_type = 'NEW_VALUES')", cfg.SpannerChangeStream),
	}

	createDbOp, err := databaseClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instanceName,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", cfg.SpannerDatabaseID),
		ExtraStatements: ddl,
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create database: %w", err)
		}
		log.Printf("database already exists; checking schema (Outbox/change stream)")

		dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.SpannerProjectID, cfg.SpannerInstanceID, cfg.SpannerDatabaseID)
		client, err := spanner.NewClient(ctx, dbPath, spannerOpts()...)
		if err != nil {
			return fmt.Errorf("create spanner client: %w", err)
		}
		defer client.Close()

		outboxExists, err := tableExists(ctx, client, "Outbox")
		if err != nil {
			return fmt.Errorf("check Outbox table: %w", err)
		}
		changeStreamExists, err := changeStreamExists(ctx, client, cfg.SpannerChangeStream)
		if err != nil {
			return fmt.Errorf("check change stream: %w", err)
		}

		var statements []string
		if !outboxExists {
			statements = append(statements, "CREATE TABLE Outbox (OutboxId STRING(36) NOT NULL, EventType STRING(128), Payload BYTES(MAX), CreatedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (OutboxId)")
		}
		if !changeStreamExists {
			statements = append(statements, fmt.Sprintf("CREATE CHANGE STREAM %s FOR Accounts, Outbox OPTIONS (value_capture_type = 'NEW_VALUES')", cfg.SpannerChangeStream))
		} else {
			statements = append(statements, fmt.Sprintf("ALTER CHANGE STREAM %s SET FOR Accounts, Outbox", cfg.SpannerChangeStream))
			statements = append(statements, fmt.Sprintf("ALTER CHANGE STREAM %s SET OPTIONS (value_capture_type = 'NEW_VALUES')", cfg.SpannerChangeStream))
		}

		if len(statements) > 0 {
			updateOp, err := databaseClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
				Database:   dbPath,
				Statements: statements,
			})
			if err != nil {
				log.Printf("schema update request failed: %v", err)
			} else if err := updateOp.Wait(ctx); err != nil {
				log.Printf("schema update failed: %v", err)
			}
		} else {
			log.Printf("schema up to date")
		}
	} else {
		if _, err := createDbOp.Wait(ctx); err != nil {
			return fmt.Errorf("wait for database creation: %w", err)
		}
	}

	log.Printf("spanner bootstrap complete: instance=%s database=%s changeStream=%s", cfg.SpannerInstanceID, cfg.SpannerDatabaseID, cfg.SpannerChangeStream)
	return nil
}

func tableExists(ctx context.Context, client *spanner.Client, table string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @table LIMIT 1",
		Params: map[string]interface{}{"table": table},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func changeStreamExists(ctx context.Context, client *spanner.Client, name string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT 1 FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = @name LIMIT 1",
		Params: map[string]interface{}{"name": name},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func runWriter(args []string) error {
	cfgPath := configPathFromArgs(args)
	cfg, err := mustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("writer", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	intervalDefault := cfg.WriteIntervalMs
	if intervalDefault == 0 {
		intervalDefault = 1000
	}
	accountDefault := cfg.AccountCount
	if accountDefault == 0 {
		accountDefault = 5
	}
	intervalMs := fs.Int("interval-ms", intervalDefault, "write interval ms")
	accountCount := fs.Int("account-count", accountDefault, "number of accounts")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	dbPath := "projects/" + cfg.SpannerProjectID + "/instances/" + cfg.SpannerInstanceID + "/databases/" + cfg.SpannerDatabaseID
	client, err := spanner.NewClient(ctx, dbPath, spannerOpts()...)
	if err != nil {
		return fmt.Errorf("create spanner client: %w", err)
	}
	defer client.Close()

	rand.New(rand.NewSource(time.Now().UnixNano()))

	ids := make([]string, *accountCount)
	balances := make([]int64, *accountCount)

	for i := 0; i < *accountCount; i++ {
		ids[i] = uuid.NewString()
		balances[i] = int64(rand.Intn(1000))
		m := spanner.InsertOrUpdate("Accounts", []string{"AccountId", "Name", "Balance"}, []interface{}{ids[i], "Account-" + ids[i][:8], balances[i]})
		if _, err := client.Apply(ctx, []*spanner.Mutation{m}); err != nil {
			return fmt.Errorf("seed account: %w", err)
		}
	}

	ticker := time.NewTicker(time.Duration(*intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		idx := rand.Intn(*accountCount)
		delta := int64(rand.Intn(200) - 100)
		balances[idx] += delta
		m := spanner.InsertOrUpdate("Accounts", []string{"AccountId", "Name", "Balance"}, []interface{}{ids[idx], "Account-" + ids[idx][:8], balances[idx]})
		if _, err := client.Apply(ctx, []*spanner.Mutation{m}); err != nil {
			log.Printf("apply mutation: %v", err)
		} else {
			log.Printf("updated account %s balance=%d", ids[idx], balances[idx])
		}
	}

	return nil
}

func runPubsubSetup(args []string) error {
	cfgPath := configPathFromArgs(args)
	cfg, err := mustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("pubsub-setup", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
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

func runOutboxSubscribe(args []string) error {
	cfgPath := configPathFromArgs(args)
	cfg, err := mustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyEmulatorEnv(cfg)
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

func runBigQuerySink(args []string) error {
	cfgPath := configPathFromArgs(args)
	cfg, err := mustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("bq-sink", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(cfg.ProtoMessage))
	if err != nil {
		return fmt.Errorf("find proto message %s: %w", cfg.ProtoMessage, err)
	}

	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, cfg.BigQueryProjectID, bigqueryOpts(cfg.BigQueryEmulatorHost)...)
	if err != nil {
		return fmt.Errorf("create bigquery client: %w", err)
	}
	defer bqClient.Close()

	schema := buildBigQuerySchema(msgType.Descriptor())
	datasetID := datasetIDFromProto(msgType.Descriptor().FullName())
	tableID := string(msgType.Descriptor().Name())

	if err := ensureDatasetAndTable(ctx, bqClient, datasetID, tableID, schema); err != nil {
		return err
	}

	pubsubClient, err := pubsub.NewClient(ctx, cfg.PubsubProjectID)
	if err != nil {
		return fmt.Errorf("create pubsub client: %w", err)
	}
	defer pubsubClient.Close()

	sub := pubsubClient.Subscription(cfg.PubsubSubscription)
	inserter := bqClient.Dataset(datasetID).Table(tableID).Inserter()

	log.Printf("bigquery sink ready: dataset=%s table=%s proto=%s", datasetID, tableID, cfg.ProtoMessage)
	return sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		event := msgType.New().Interface()
		if err := proto.Unmarshal(msg.Data, event); err != nil {
			log.Printf("decode proto failed: %v", err)
			msg.Nack()
			return
		}

		row, err := protoToBigQueryRow(event.ProtoReflect())
		if err != nil {
			log.Printf("convert proto to row failed: %v", err)
			msg.Nack()
			return
		}

		values := rowValuesFromSchema(schema, row)
		if err := inserter.Put(ctx, []*bigquery.ValuesSaver{{Schema: schema, Row: values}}); err != nil {
			log.Printf("insert row failed: %v", err)
			msg.Nack()
			return
		}

		msg.Ack()
	})
}

func runBigQueryCheck(args []string) error {
	cfgPath := configPathFromArgs(args)
	cfg, err := mustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("bq-check", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	limit := fs.Int("limit", 5, "max rows to read")
	if err := fs.Parse(args); err != nil {
		return err
	}

	msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(cfg.ProtoMessage))
	if err != nil {
		return fmt.Errorf("find proto message %s: %w", cfg.ProtoMessage, err)
	}
	datasetID := datasetIDFromProto(msgType.Descriptor().FullName())
	tableID := string(msgType.Descriptor().Name())

	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, cfg.BigQueryProjectID, bigqueryOpts(cfg.BigQueryEmulatorHost)...)
	if err != nil {
		return fmt.Errorf("create bigquery client: %w", err)
	}
	defer bqClient.Close()

	table := bqClient.Dataset(datasetID).Table(tableID)
	meta, err := table.Metadata(ctx)
	if err != nil {
		return fmt.Errorf("get table metadata: %w", err)
	}

	log.Printf("bigquery table: %s.%s", datasetID, tableID)
	log.Printf("schema: %s", schemaSummary(meta.Schema))

	query := bqClient.Query(fmt.Sprintf("SELECT * FROM `%s.%s` LIMIT %d", datasetID, tableID, *limit))
	it, err := query.Read(ctx)
	if err != nil {
		return fmt.Errorf("read query: %w", err)
	}

	count := 0
	for {
		var row map[string]bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return fmt.Errorf("iterate rows: %w", err)
		}
		log.Printf("row: %v", row)
		count++
	}

	if count == 0 {
		log.Printf("no rows found")
	}
	return nil
}

func runSpannerCheck(args []string) error {
	cfgPath := configPathFromArgs(args)
	cfg, err := mustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("spanner-check", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	dbPath := "projects/" + cfg.SpannerProjectID + "/instances/" + cfg.SpannerInstanceID + "/databases/" + cfg.SpannerDatabaseID
	client, err := spanner.NewClient(ctx, dbPath, spannerOpts()...)
	if err != nil {
		return fmt.Errorf("create spanner client: %w", err)
	}
	defer client.Close()

	if err := logChangeStreams(ctx, client); err != nil {
		return err
	}
	if err := logOutboxLatest(ctx, client); err != nil {
		return err
	}
	return nil
}

func runOutboxInsert(args []string) error {
	cfgPath := configPathFromArgs(args)
	cfg, err := mustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyEmulatorEnv(cfg)
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

	if *outboxID == "" {
		*outboxID = uuid.NewString()
	}

	payload, err := readPayload(*payloadFile, *payloadBase64, *payloadText)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		event := &outboxv1.OutboxEvent{
			OutboxId:  *outboxID,
			EventType: *eventType,
			AccountId: *accountID,
			Balance:   *balance,
		}
		applyAccountDetails(event, *payID, *bsb, *accountNumber)
		payload, err = proto.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
	}

	ctx := context.Background()
	dbPath := "projects/" + cfg.SpannerProjectID + "/instances/" + cfg.SpannerInstanceID + "/databases/" + cfg.SpannerDatabaseID
	client, err := spanner.NewClient(ctx, dbPath, spannerOpts()...)
	if err != nil {
		return fmt.Errorf("create spanner client: %w", err)
	}
	defer client.Close()

	m := spanner.Insert("Outbox", []string{"OutboxId", "EventType", "Payload", "CreatedAt"}, []interface{}{
		*outboxID,
		*eventType,
		payload,
		spanner.CommitTimestamp,
	})

	if _, err := client.Apply(ctx, []*spanner.Mutation{m}); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	log.Printf("inserted outbox message outboxId=%s bytes=%d", *outboxID, len(payload))
	return nil
}

func ensureDatasetAndTable(ctx context.Context, client *bigquery.Client, datasetID, tableID string, schema bigquery.Schema) error {
	dataset := client.Dataset(datasetID)
	if _, err := dataset.Metadata(ctx); err != nil {
		if isNotFound(err) {
			if err := dataset.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
				return fmt.Errorf("create dataset: %w", err)
			}
		} else {
			return fmt.Errorf("get dataset metadata: %w", err)
		}
	}

	table := dataset.Table(tableID)
	if _, err := table.Metadata(ctx); err != nil {
		if isNotFound(err) {
			if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
				return fmt.Errorf("create table: %w", err)
			}
		} else {
			return fmt.Errorf("get table metadata: %w", err)
		}
	}
	return nil
}

func datasetIDFromProto(fullName protoreflect.FullName) string {
	name := strings.ToLower(string(fullName))
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return name
}

func buildBigQuerySchema(desc protoreflect.MessageDescriptor) bigquery.Schema {
	fields := make(bigquery.Schema, 0, desc.Fields().Len())
	for i := 0; i < desc.Fields().Len(); i++ {
		fields = append(fields, schemaFieldFromProto(desc.Fields().Get(i)))
	}
	return fields
}

func schemaFieldFromProto(field protoreflect.FieldDescriptor) *bigquery.FieldSchema {
	schema := &bigquery.FieldSchema{
		Name: string(field.Name()),
	}
	if field.Cardinality() == protoreflect.Repeated {
		schema.Repeated = true
	}

	switch field.Kind() {
	case protoreflect.BoolKind:
		schema.Type = bigquery.BooleanFieldType
	case protoreflect.StringKind:
		schema.Type = bigquery.StringFieldType
	case protoreflect.BytesKind:
		schema.Type = bigquery.BytesFieldType
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind, protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		schema.Type = bigquery.IntegerFieldType
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		schema.Type = bigquery.FloatFieldType
	case protoreflect.EnumKind:
		schema.Type = bigquery.StringFieldType
	case protoreflect.MessageKind:
		if field.Message().FullName() == "google.protobuf.Timestamp" {
			schema.Type = bigquery.TimestampFieldType
		} else {
			schema.Type = bigquery.RecordFieldType
			schema.Schema = buildBigQuerySchema(field.Message())
		}
	default:
		schema.Type = bigquery.StringFieldType
	}

	return schema
}

func protoToBigQueryRow(msg protoreflect.Message) (map[string]bigquery.Value, error) {
	desc := msg.Descriptor()
	row := make(map[string]bigquery.Value, desc.Fields().Len())
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		name := string(field.Name())

		if field.Cardinality() == protoreflect.Repeated {
			list := msg.Get(field).List()
			values := make([]bigquery.Value, 0, list.Len())
			for j := 0; j < list.Len(); j++ {
				value, err := protoValueToBigQuery(field, list.Get(j))
				if err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			row[name] = values
			continue
		}

		if !msg.Has(field) {
			row[name] = nil
			continue
		}

		value, err := protoValueToBigQuery(field, msg.Get(field))
		if err != nil {
			return nil, err
		}
		row[name] = value
	}
	return row, nil
}

func rowValuesFromSchema(schema bigquery.Schema, row map[string]bigquery.Value) []bigquery.Value {
	values := make([]bigquery.Value, 0, len(schema))
	for _, field := range schema {
		values = append(values, row[field.Name])
	}
	return values
}

func protoValueToBigQuery(field protoreflect.FieldDescriptor, value protoreflect.Value) (bigquery.Value, error) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return value.Bool(), nil
	case protoreflect.StringKind:
		return value.String(), nil
	case protoreflect.BytesKind:
		return []byte(value.Bytes()), nil
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return value.Int(), nil
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return int64(value.Uint()), nil
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return value.Float(), nil
	case protoreflect.EnumKind:
		enumValue := field.Enum().Values().ByNumber(value.Enum())
		if enumValue == nil {
			return int64(value.Enum()), nil
		}
		return string(enumValue.Name()), nil
	case protoreflect.MessageKind:
		if field.Message().FullName() == "google.protobuf.Timestamp" {
			ts, ok := value.Message().Interface().(*timestamppb.Timestamp)
			if !ok {
				return nil, fmt.Errorf("unexpected timestamp type %T", value.Message().Interface())
			}
			return ts.AsTime(), nil
		}
		return protoToBigQueryRow(value.Message())
	default:
		return nil, fmt.Errorf("unsupported field kind %s", field.Kind())
	}
}

func isNotFound(err error) bool {
	apiErr, ok := err.(*googleapi.Error)
	return ok && apiErr.Code == 404
}

func schemaSummary(schema bigquery.Schema) string {
	parts := make([]string, 0, len(schema))
	for _, field := range schema {
		parts = append(parts, fieldSummary(field))
	}
	return strings.Join(parts, ", ")
}

func fieldSummary(field *bigquery.FieldSchema) string {
	summary := fmt.Sprintf("%s:%s", field.Name, field.Type)
	if field.Repeated {
		summary = summary + "[]"
	}
	if field.Type == bigquery.RecordFieldType && len(field.Schema) > 0 {
		child := make([]string, 0, len(field.Schema))
		for _, nested := range field.Schema {
			child = append(child, fieldSummary(nested))
		}
		summary = summary + "{" + strings.Join(child, ", ") + "}"
	}
	return summary
}

func logChangeStreams(ctx context.Context, client *spanner.Client) error {
	stmt := spanner.Statement{SQL: "SELECT CHANGE_STREAM_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAMS"}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var streams []string
	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return fmt.Errorf("query change streams: %w", err)
		}
		var name string
		if err := row.ColumnByName("CHANGE_STREAM_NAME", &name); err != nil {
			return fmt.Errorf("read change stream name: %w", err)
		}
		streams = append(streams, name)
	}

	if len(streams) == 0 {
		log.Printf("no change streams found")
		return nil
	}
	log.Printf("change streams: %s", strings.Join(streams, ", "))

	for _, stream := range streams {
		stmt := spanner.Statement{
			SQL: "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES WHERE CHANGE_STREAM_NAME = @name",
			Params: map[string]interface{}{
				"name": stream,
			},
		}
		iter := client.Single().Query(ctx, stmt)
		var tables []string
		for {
			row, err := iter.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				iter.Stop()
				return fmt.Errorf("query change stream tables: %w", err)
			}
			var table string
			if err := row.ColumnByName("TABLE_NAME", &table); err != nil {
				iter.Stop()
				return fmt.Errorf("read change stream table: %w", err)
			}
			tables = append(tables, table)
		}
		iter.Stop()
		log.Printf("change stream %s tables: %s", stream, strings.Join(tables, ", "))
	}
	return nil
}

func logOutboxLatest(ctx context.Context, client *spanner.Client) error {
	stmt := spanner.Statement{
		SQL: "SELECT CURRENT_TIMESTAMP() AS NowTs",
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		if err == iterator.Done {
			return nil
		}
		return fmt.Errorf("query current timestamp: %w", err)
	}
	var now spanner.NullTime
	if err := row.ColumnByName("NowTs", &now); err != nil {
		return fmt.Errorf("read current timestamp: %w", err)
	}
	log.Printf("spanner now: %s", now.Time.Format(time.RFC3339Nano))

	stmt = spanner.Statement{
		SQL: "SELECT OutboxId, CreatedAt FROM Outbox ORDER BY CreatedAt DESC LIMIT 1",
	}
	iter = client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err = iter.Next()
	if err != nil {
		if err == iterator.Done {
			log.Printf("outbox rows: none")
			return nil
		}
		return fmt.Errorf("query outbox latest: %w", err)
	}
	var outboxID string
	var created spanner.NullTime
	if err := row.ColumnByName("OutboxId", &outboxID); err != nil {
		return fmt.Errorf("read outbox id: %w", err)
	}
	if err := row.ColumnByName("CreatedAt", &created); err != nil {
		return fmt.Errorf("read outbox created: %w", err)
	}
	log.Printf("outbox latest: id=%s created_at=%s", outboxID, created.Time.Format(time.RFC3339Nano))
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
