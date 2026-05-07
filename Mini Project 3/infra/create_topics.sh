#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

KT="$KAFKA_DIR/bin/kafka-topics.sh"

echo "[create_topics] interactions (3 partitions)"
"$KT" --bootstrap-server "$KAFKA_BROKER" --create --if-not-exists \
      --topic interactions --partitions 3 --replication-factor 1

echo "[create_topics] recommendations (1 partition)"
"$KT" --bootstrap-server "$KAFKA_BROKER" --create --if-not-exists \
      --topic recommendations --partitions 1 --replication-factor 1

echo "[create_topics] alerts (1 partition)"
"$KT" --bootstrap-server "$KAFKA_BROKER" --create --if-not-exists \
      --topic alerts --partitions 1 --replication-factor 1

echo "[create_topics] current topics:"
"$KT" --bootstrap-server "$KAFKA_BROKER" --list
