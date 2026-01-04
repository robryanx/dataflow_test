# Local Spanner -> Change Stream -> Beam (DirectRunner) -> Pub/Sub (Deltio)

This repo sets up a local, Docker-based pipeline:
- Spanner Emulator
- Beam Java DirectRunner pipeline
- Deltio Pub/Sub emulator
- BigQuery emulator
- Go tools for bootstrap + data generation + Pub/Sub topic setup + outbox inserts
- Buf + proto schema for outbox payloads
- Shared JSON config for stable settings

## Quick start

1) Start emulators and build the pipeline container:

```bash
docker compose up -d spanner deltio bigquery
```

2) Create Spanner instance/database/change stream and Pub/Sub topic/subscription:

```bash
cp tools/config.example.json tools/config.json

go run ./tools bootstrap
go run ./tools pubsub-setup
```

3) Start the Beam pipeline:

```bash
docker compose up -d beam-runner
```

4) Start the writer to generate changes:

```bash
go run ./tools writer
```

5) Start the BigQuery sink (new terminal):

```bash
go run ./tools bq-sink
```

6) Insert an outbox message (optional):

```bash
go run ./tools outbox-insert \
  --event-type proto.outbox \
  --account-id account-123 \
  --balance 42
```

You can also provide a proto-encoded payload from a file:

```bash
go run ./tools outbox-insert --payload-file ./tmp/outbox.bin
```

7) Verify with any subscriber:

```bash
go run ./tools outbox-subscribe
```

8) Verify BigQuery table + rows:

```bash
go run ./tools bq-check
```

## Configuration

Environment variables (defaults shown in `docker-compose.yml`):
- `START_OFFSET_SECONDS`

## Notes / compatibility

- Spanner emulator support for change streams is required. If it is not supported in your local emulator version, the Beam pipeline will fail at startup.
- If the database already exists, the bootstrap tool will not modify schema. Recreate the emulator data if you need the Outbox table or change stream to include it.
- Deltio image name/port may differ depending on your local setup. Adjust `DELTIO_IMAGE` and `DELTIO_PORT` in your environment or `docker-compose.yml`.
- Beam API surface for change streams has shifted between versions; if you see compile errors, adjust the pipeline code in `beam-pipeline/src/main/java/local/SpannerToPubsub.java` to match your Beam SDK version.

## Proto schema

The outbox payload schema lives at `proto/outbox/v1/outbox.proto`. Regenerate Go types with:

```bash
buf generate
```

If you do not have buf installed, you can run it via Docker:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace bufbuild/buf:1.34.0 generate
```
## Tools config

The tools read settings from `tools/config.json` (or a path passed via `--config`). See `tools/config.example.json`.
All tool commands require the config file and ignore environment variables.
`bq-sink` uses `proto_message` to derive dataset/table names (dataset = lowercased full name with dots replaced by underscores; table = message name).
