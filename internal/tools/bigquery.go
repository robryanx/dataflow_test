package tools

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	outboxv1 "dataflow_test/gen/go/proto/outbox/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func BigQueryOpts(emulatorHost string) []option.ClientOption {
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

func RunBigQuerySink(args []string) error {
	cfgPath := ConfigPathFromArgs(args)
	cfg, err := MustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ApplyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("bq-sink", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return BigQuerySinkWithContext(context.Background(), cfg)
}

func BigQuerySinkWithContext(ctx context.Context, cfg Config) error {
	msgType, err := resolveProtoMessage(cfg.ProtoMessage)
	if err != nil {
		return err
	}

	bqClient, err := bigquery.NewClient(ctx, cfg.BigQueryProjectID, BigQueryOpts(cfg.BigQueryEmulatorHost)...)
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
		logAccountDetails(cfg.ProtoMessage, event, row)

		values := rowValuesFromSchema(schema, row)
		if err := inserter.Put(ctx, []*bigquery.ValuesSaver{{Schema: schema, Row: values}}); err != nil {
			log.Printf("insert row failed: %v", err)
			msg.Nack()
			return
		}

		msg.Ack()
	})
}

func logAccountDetails(protoName string, event proto.Message, row map[string]bigquery.Value) {
	if protoName != "outbox.v1.OutboxEvent" {
		return
	}
	outbox, ok := event.(*outboxv1.OutboxEvent)
	if !ok {
		return
	}
	log.Printf("bq-sink decoded details: %s", formatAccountDetails(outbox.GetAccountDetails()))
	if value, ok := row["account_details"]; ok {
		log.Printf("bq-sink row account_details=%#v", value)
	}
}

func RunBigQueryCheck(args []string) error {
	cfgPath := ConfigPathFromArgs(args)
	cfg, err := MustLoadConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ApplyEmulatorEnv(cfg)
	fs := flag.NewFlagSet("bq-check", flag.ContinueOnError)
	fs.String("config", cfgPath, "config file path")
	limit := fs.Int("limit", 5, "max rows to read")
	if err := fs.Parse(args); err != nil {
		return err
	}

	msgType, err := resolveProtoMessage(cfg.ProtoMessage)
	if err != nil {
		return err
	}
	datasetID := datasetIDFromProto(msgType.Descriptor().FullName())
	tableID := string(msgType.Descriptor().Name())

	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, cfg.BigQueryProjectID, BigQueryOpts(cfg.BigQueryEmulatorHost)...)
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

func WaitForBigQueryOutbox(ctx context.Context, cfg Config, outboxID string, timeout time.Duration) (*outboxv1.OutboxEvent, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		event, found, err := queryBigQueryForOutbox(ctx, cfg, outboxID)
		if err != nil {
			return nil, err
		}
		if found {
			return event, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("timed out waiting for outbox row %s", outboxID)
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

func resolveProtoMessage(name string) (protoreflect.MessageType, error) {
	msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("find proto message %s: %w", name, err)
	}
	return msgType, nil
}

func queryBigQueryForOutbox(ctx context.Context, cfg Config, outboxID string) (*outboxv1.OutboxEvent, bool, error) {
	msgType, err := resolveProtoMessage(cfg.ProtoMessage)
	if err != nil {
		return nil, false, err
	}
	datasetID := datasetIDFromProto(msgType.Descriptor().FullName())
	tableID := string(msgType.Descriptor().Name())

	bqClient, err := bigquery.NewClient(ctx, cfg.BigQueryProjectID, BigQueryOpts(cfg.BigQueryEmulatorHost)...)
	if err != nil {
		return nil, false, fmt.Errorf("create bigquery client: %w", err)
	}
	defer bqClient.Close()

	query := bqClient.Query(fmt.Sprintf("SELECT * FROM `%s.%s` WHERE outbox_id = '%s'", datasetID, tableID, outboxID))
	it, err := query.Read(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("read query: %w", err)
	}

	for {
		var row map[string]bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return nil, false, fmt.Errorf("iterate rows: %w", err)
		}
		if row["account_details"] != nil {
			event, err := rowToOutboxEvent(row)
			if err != nil {
				return nil, false, err
			}
			return event, true, nil
		}
	}
	return nil, false, nil
}

func rowToOutboxEvent(row map[string]bigquery.Value) (*outboxv1.OutboxEvent, error) {
	event := &outboxv1.OutboxEvent{
		OutboxId:  valueAsString(row["outbox_id"]),
		EventType: valueAsString(row["event_type"]),
		AccountId: valueAsString(row["account_id"]),
		Balance:   valueAsInt64(row["balance"]),
	}

	detailsValue := row["account_details"]
	if detailsValue == nil {
		return event, nil
	}
	detailsMap, ok := detailsValue.(map[string]bigquery.Value)
	if !ok {
		return nil, fmt.Errorf("unexpected account_details type %T", detailsValue)
	}
	details := &outboxv1.AccountDetails{}
	if payID := valueAsString(detailsMap["payid"]); payID != "" {
		details.Identifier = &outboxv1.AccountDetails_Payid{Payid: payID}
		event.AccountDetails = details
		return event, nil
	}
	if bsbValue := detailsMap["bsb_account"]; bsbValue != nil {
		bsbMap, ok := bsbValue.(map[string]bigquery.Value)
		if !ok {
			return nil, fmt.Errorf("unexpected bsb_account type %T", bsbValue)
		}
		details.Identifier = &outboxv1.AccountDetails_BsbAccount{
			BsbAccount: &outboxv1.BsbAccount{
				Bsb:           valueAsString(bsbMap["bsb"]),
				AccountNumber: valueAsString(bsbMap["account_number"]),
			},
		}
		event.AccountDetails = details
	}
	return event, nil
}

func valueAsString(value bigquery.Value) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", value)
}

func valueAsInt64(value bigquery.Value) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
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
		values = append(values, normalizeForSchema(field, row[field.Name]))
	}
	return values
}

func normalizeForSchema(field *bigquery.FieldSchema, value bigquery.Value) bigquery.Value {
	if value == nil {
		return nil
	}
	if field.Type != bigquery.RecordFieldType {
		return value
	}
	if field.Repeated {
		list, ok := value.([]bigquery.Value)
		if !ok {
			return value
		}
		normalized := make([]bigquery.Value, 0, len(list))
		for _, item := range list {
			normalized = append(normalized, normalizeRecordValue(field.Schema, item))
		}
		return normalized
	}
	return normalizeRecordValue(field.Schema, value)
}

func normalizeRecordValue(schema bigquery.Schema, value bigquery.Value) bigquery.Value {
	switch v := value.(type) {
	case map[string]bigquery.Value:
		return rowValuesFromSchema(schema, v)
	case []bigquery.Value:
		return v
	default:
		return value
	}
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
