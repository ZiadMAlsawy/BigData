#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

echo "[start_kafka] using KAFKA_DIR = $KAFKA_DIR"
echo "[start_kafka] using KAFKA_LOG_DIR = $KAFKA_LOG_DIR"

# Ensure Unix-style paths for shell operations
KAFKA_DIR_UNIX="$(echo "$KAFKA_DIR" | sed 's|\\|/|g')"
KAFKA_LOG_DIR_UNIX="$KAFKA_LOG_DIR"

echo "[start_kafka] resolved KAFKA_DIR = $KAFKA_DIR_UNIX"
echo "[start_kafka] resolved KAFKA_LOG_DIR = $KAFKA_LOG_DIR_UNIX"

# Clean storage
echo "[start_kafka] wiping Kafka storage"
if [ -d "$KAFKA_LOG_DIR_UNIX" ]; then
  chmod -R 777 "$KAFKA_LOG_DIR_UNIX" 2>/dev/null || true
  find "$KAFKA_LOG_DIR_UNIX" -type f -delete 2>/dev/null || true
  find "$KAFKA_LOG_DIR_UNIX" -type d -delete 2>/dev/null || true
fi
rm -rf "$KAFKA_LOG_DIR_UNIX" 2>/dev/null || true
mkdir -p "$KAFKA_LOG_DIR_UNIX"

# Build config
CFG_TEMPLATE="$PROJECT_DIR/infra/kraft-server.properties"
CFG="$PROJECT_DIR/infra/kraft-server.properties.runtime"
sed "s|__KAFKA_LOG_DIR__|$KAFKA_LOG_DIR_UNIX|g" "$CFG_TEMPLATE" > "$CFG"

echo "[start_kafka] generated config: $CFG"

# Format storage
CLUSTER_ID="$("$KAFKA_DIR_UNIX/bin/kafka-storage.sh" random-uuid)"
echo "[start_kafka] formatting storage with cluster.id = $CLUSTER_ID"
"$KAFKA_DIR_UNIX/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CFG"

# Start broker
LOG_FILE="$PROJECT_DIR/infra/kafka.log"
PID_FILE="$PROJECT_DIR/infra/kafka.pid"

echo "[start_kafka] starting broker..."
nohup "$KAFKA_DIR_UNIX/bin/kafka-server-start.sh" "$CFG" > "$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"
echo "[start_kafka] kafka pid = $PID"

# Wait for broker
echo "[start_kafka] waiting for broker to accept connections..."
for i in $(seq 1 40); do
  if "$KAFKA_DIR_UNIX/bin/kafka-broker-api-versions.sh" --bootstrap-server "$KAFKA_BROKER" >/dev/null 2>&1; then
    echo "[start_kafka] broker READY on $KAFKA_BROKER"
    exit 0
  fi
  sleep 1
done
echo "[start_kafka] ERROR: broker did not start"
echo "[start_kafka] check log: $LOG_FILE"
exit 1
