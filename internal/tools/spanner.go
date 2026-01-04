package tools

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func SpannerOpts() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		opts = append(opts, option.WithoutAuthentication())
	}
	return opts
}

func RunBootstrap(args []string) error {
	cfgPath := ConfigPathFromArgs(args)
	cfg, err := MustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ApplyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return Bootstrap(context.Background(), cfg)
}

func Bootstrap(ctx context.Context, cfg Config) error {
	instanceClient, err := instance.NewInstanceAdminClient(ctx, SpannerOpts()...)
	if err != nil {
		return fmt.Errorf("create instance admin client: %w", err)
	}
	defer instanceClient.Close()

	databaseClient, err := database.NewDatabaseAdminClient(ctx, SpannerOpts()...)
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
		client, err := spanner.NewClient(ctx, dbPath, SpannerOpts()...)
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
			op, err := databaseClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
				Database:   dbPath,
				Statements: statements,
			})
			if err != nil {
				return fmt.Errorf("update database ddl: %w", err)
			}
			if err := op.Wait(ctx); err != nil {
				return fmt.Errorf("wait for ddl update: %w", err)
			}
		}
	} else {
		if _, err := createDbOp.Wait(ctx); err != nil {
			return fmt.Errorf("wait for db creation: %w", err)
		}
	}

	log.Printf("spanner bootstrap complete: instance=%s database=%s changeStream=%s", cfg.SpannerInstanceID, cfg.SpannerDatabaseID, cfg.SpannerChangeStream)
	return nil
}

func tableExists(ctx context.Context, client *spanner.Client, table string) (bool, error) {
	stmt := spanner.Statement{
		SQL: "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @table",
		Params: map[string]interface{}{
			"table": table,
		},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func changeStreamExists(ctx context.Context, client *spanner.Client, name string) (bool, error) {
	stmt := spanner.Statement{
		SQL: "SELECT CHANGE_STREAM_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = @name",
		Params: map[string]interface{}{
			"name": name,
		},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func RunWriter(args []string) error {
	cfgPath := ConfigPathFromArgs(args)
	cfg, err := MustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ApplyEmulatorEnv(cfg)
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
	client, err := spanner.NewClient(ctx, dbPath, SpannerOpts()...)
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

func RunSpannerCheck(args []string) error {
	cfgPath := ConfigPathFromArgs(args)
	cfg, err := MustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ApplyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("spanner-check", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	dbPath := "projects/" + cfg.SpannerProjectID + "/instances/" + cfg.SpannerInstanceID + "/databases/" + cfg.SpannerDatabaseID
	client, err := spanner.NewClient(ctx, dbPath, SpannerOpts()...)
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
