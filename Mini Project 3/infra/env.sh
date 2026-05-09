# #!/usr/bin/env bash
# # Sourced by every infra script. Edit KAFKA_DIR if your folder lives elsewhere.

# : "${KAFKA_DIR:=$HOME/kafka_2.12-3.7.0}"
# : "${KAFKA_BROKER:=localhost:9092}"

# # Resolve project root (one level above infra/)
# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# : "${PROJECT_DIR:=$(cd "$SCRIPT_DIR/.." && pwd)}"
# : "${KAFKA_LOG_DIR:=$PROJECT_DIR/infra/kafka-data}"

# export KAFKA_DIR KAFKA_BROKER PROJECT_DIR KAFKA_LOG_DIR

# if [ ! -d "$KAFKA_DIR" ]; then
#   echo "ERROR: KAFKA_DIR=$KAFKA_DIR does not exist." >&2
#   echo "Edit infra/env.sh or 'export KAFKA_DIR=/path/to/kafka_2.12-3.7.0'." >&2
#   return 1 2>/dev/null || exit 1
# fi

#!/usr/bin/env bash
# Clean environment config for Kafka + Spark (Windows safe)

# -----------------------------
# Kafka installation directory
# -----------------------------
: "${KAFKA_DIR:=/c/Users/zyada/kafka_2.13-3.9.1}"
: "${KAFKA_BROKER:=127.0.0.1:9092}"

# -----------------------------
# Resolve project root
# -----------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# -----------------------------
# IMPORTANT FIX: FORCE WINDOWS-FRIENDLY PATH
# -----------------------------
KAFKA_LOG_DIR="$PROJECT_DIR/infra/kafka-data"

# Convert to Windows-safe format for Kafka (Java layer)
KAFKA_LOG_DIR=$(cygpath -m "$KAFKA_LOG_DIR" 2>/dev/null || echo "$KAFKA_LOG_DIR")

export KAFKA_DIR KAFKA_BROKER PROJECT_DIR KAFKA_LOG_DIR

# -----------------------------
# Validation
# -----------------------------
if [ ! -d "$KAFKA_DIR" ]; then
  echo "ERROR: KAFKA_DIR=$KAFKA_DIR does not exist." >&2
  echo "Fix path in infra/env.sh" >&2
  exit 1
fi

echo "[env] PROJECT_DIR = $PROJECT_DIR"
echo "[env] KAFKA_LOG_DIR = $KAFKA_LOG_DIR"
echo "[env] KAFKA_DIR = $KAFKA_DIR"