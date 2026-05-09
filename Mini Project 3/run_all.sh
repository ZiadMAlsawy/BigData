#!/usr/bin/env bash
# End-to-end launcher for MP3. Run inside WSL.
# Each component is started in the background; logs go to $MP3_OUTPUT_DIR/logs/.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# Auto-prefer WSL-native paths if user set them up (avoids /mnt/c 9p I/O bug)
if [ -d "$HOME/mp3-data" ] && [ -z "${MP3_DATA_DIR:-}" ]; then
  export MP3_DATA_DIR="$HOME/mp3-data"
  echo "[run_all] using WSL-native MP3_DATA_DIR=$MP3_DATA_DIR"
fi
: "${MP3_DATA_DIR:=$PROJECT_DIR/data}"
: "${MP3_MODEL_DIR:=$PROJECT_DIR/models}"
# Default output to WSL-native FS to avoid /mnt/c 9p I/O bug
if [ -z "${MP3_OUTPUT_DIR:-}" ]; then
  if [ -d "$HOME" ] && [[ "$PROJECT_DIR" == /mnt/* ]]; then
    MP3_OUTPUT_DIR="$HOME/mp3-output"
    mkdir -p "$MP3_OUTPUT_DIR"
    echo "[run_all] using WSL-native MP3_OUTPUT_DIR=$MP3_OUTPUT_DIR"
  else
    MP3_OUTPUT_DIR="$PROJECT_DIR/output"
  fi
fi
: "${MP3_CHECKPOINT_DIR:=$MP3_OUTPUT_DIR/checkpoints}"
export MP3_DATA_DIR MP3_MODEL_DIR MP3_OUTPUT_DIR MP3_CHECKPOINT_DIR

mkdir -p "$MP3_OUTPUT_DIR/logs"

PYBIN="${PYTHON:-$(command -v python3 || command -v python)}"
if [ -z "$PYBIN" ]; then
  echo "[run_all] ERROR: no python interpreter on PATH" >&2
  exit 1
fi

echo "[run_all] starting Kafka"
bash infra/start_kafka.sh

echo "[run_all] creating topics"
bash infra/create_topics.sh

if [ ! -d "$MP3_MODEL_DIR/als_model" ]; then
  echo "[run_all] training ALS (this can take a few minutes)"
  "$PYBIN" src/train_als.py
else
  echo "[run_all] ALS model already present at $MP3_MODEL_DIR/als_model, skipping"
fi

LOG_DIR="$MP3_OUTPUT_DIR/logs"

echo "[run_all] starting Spark streaming app"
nohup spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
    --master "local[3]" \
    src/streaming_app.py > "$LOG_DIR/streaming.log" 2>&1 &
echo $! > "$LOG_DIR/streaming.pid"
echo "[run_all]   pid=$(cat "$LOG_DIR/streaming.pid") log=$LOG_DIR/streaming.log"

sleep 8

echo "[run_all] starting producer (synthetic mode, 30 ev/s)"
nohup "$PYBIN" src/kafka_producer.py --mode synthetic --rate 30 \
        > "$LOG_DIR/producer.log" 2>&1 &
echo $! > "$LOG_DIR/producer.pid"
echo "[run_all]   pid=$(cat "$LOG_DIR/producer.pid") log=$LOG_DIR/producer.log"

echo "[run_all] starting Streamlit dashboard on http://localhost:8501"
nohup streamlit run src/dashboard.py --server.headless true \
        > "$LOG_DIR/dashboard.log" 2>&1 &
echo $! > "$LOG_DIR/dashboard.pid"

cat <<EOF

=========================================================
MP3 pipeline running. To stop:
  kill \$(cat "$LOG_DIR/producer.pid")
  kill \$(cat "$LOG_DIR/streaming.pid")
  kill \$(cat "$LOG_DIR/dashboard.pid")
  bash infra/stop_kafka.sh

Logs:
  $LOG_DIR/streaming.log
  $LOG_DIR/producer.log
  $LOG_DIR/dashboard.log
  infra/kafka.log
=========================================================
EOF
