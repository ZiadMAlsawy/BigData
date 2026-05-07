#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

mkdir -p "$KAFKA_LOG_DIR"

CFG_TEMPLATE="$PROJECT_DIR/infra/kraft-server.properties"
CFG="$PROJECT_DIR/infra/kraft-server.properties.runtime"
sed "s|__KAFKA_LOG_DIR__|$KAFKA_LOG_DIR|" "$CFG_TEMPLATE" > "$CFG"

if [ ! -f "$KAFKA_LOG_DIR/meta.properties" ]; then
  echo "[start_kafka] formatting storage in $KAFKA_LOG_DIR"
  CLUSTER_ID="$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_DIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CFG"
else
  echo "[start_kafka] storage already formatted, skipping"
fi

LOG_FILE="$PROJECT_DIR/infra/kafka.log"
echo "[start_kafka] launching broker, log: $LOG_FILE"
nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$CFG" > "$LOG_FILE" 2>&1 &
PID=$!
echo "[start_kafka] kafka pid=$PID"
echo "$PID" > "$PROJECT_DIR/infra/kafka.pid"

# Wait until broker accepts connections
for i in $(seq 1 30); do
  if "$KAFKA_DIR/bin/kafka-broker-api-versions.sh" --bootstrap-server "$KAFKA_BROKER" >/dev/null 2>&1; then
    echo "[start_kafka] broker ready on $KAFKA_BROKER"
    exit 0
  fi
  sleep 1
done
echo "[start_kafka] WARNING: broker did not respond within 30s; check $LOG_FILE"
exit 1
