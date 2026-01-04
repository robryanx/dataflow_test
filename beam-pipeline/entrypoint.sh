#!/usr/bin/env bash
set -euo pipefail

JAVA_OPTS=${JAVA_OPTS:-""}

: "${SPANNER_PROJECT_ID:?}"
: "${SPANNER_INSTANCE_ID:?}"
: "${SPANNER_DATABASE_ID:?}"
: "${SPANNER_CHANGE_STREAM:?}"
: "${SPANNER_EMULATOR_HOST:?}"
: "${PUBSUB_TOPIC:?}"

EXTRA_ARGS=()
if [[ -n "${START_OFFSET_SECONDS:-}" ]]; then
  EXTRA_ARGS+=( "--startOffsetSeconds=${START_OFFSET_SECONDS}" )
fi

exec java ${JAVA_OPTS} -jar /app/app-all.jar \
  --runner=DirectRunner \
  --projectId="${SPANNER_PROJECT_ID}" \
  --instanceId="${SPANNER_INSTANCE_ID}" \
  --databaseId="${SPANNER_DATABASE_ID}" \
  --changeStreamName="${SPANNER_CHANGE_STREAM}" \
  --pubsubTopic="${PUBSUB_TOPIC}" \
  --spannerEmulatorHost="${SPANNER_EMULATOR_HOST}" \
  "${EXTRA_ARGS[@]}"
