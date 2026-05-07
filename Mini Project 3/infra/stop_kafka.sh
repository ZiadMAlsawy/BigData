#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

PID_FILE="$PROJECT_DIR/infra/kafka.pid"
if [ -f "$PID_FILE" ]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    echo "[stop_kafka] sending SIGTERM to pid $PID"
    "$KAFKA_DIR/bin/kafka-server-stop.sh" || kill "$PID"
    sleep 2
  fi
  rm -f "$PID_FILE"
fi

if pgrep -f "kafka.Kafka" >/dev/null; then
  echo "[stop_kafka] forcing fallback shutdown"
  "$KAFKA_DIR/bin/kafka-server-stop.sh" || true
fi
echo "[stop_kafka] done"
