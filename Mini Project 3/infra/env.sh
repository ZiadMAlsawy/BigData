#!/usr/bin/env bash
# Sourced by every infra script. Edit KAFKA_DIR if your folder lives elsewhere.

: "${KAFKA_DIR:=$HOME/kafka_2.12-3.7.0}"
: "${KAFKA_BROKER:=localhost:9092}"

# Resolve project root (one level above infra/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${PROJECT_DIR:=$(cd "$SCRIPT_DIR/.." && pwd)}"
: "${KAFKA_LOG_DIR:=$PROJECT_DIR/infra/kafka-data}"

export KAFKA_DIR KAFKA_BROKER PROJECT_DIR KAFKA_LOG_DIR

if [ ! -d "$KAFKA_DIR" ]; then
  echo "ERROR: KAFKA_DIR=$KAFKA_DIR does not exist." >&2
  echo "Edit infra/env.sh or 'export KAFKA_DIR=/path/to/kafka_2.12-3.7.0'." >&2
  return 1 2>/dev/null || exit 1
fi
