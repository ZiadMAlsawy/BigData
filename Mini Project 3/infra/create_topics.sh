# #!/usr/bin/env bash
# set -euo pipefail
# source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

# KT="$KAFKA_DIR/bin/kafka-topics.sh"

# echo "[create_topics] interactions (3 partitions)"
# "$KT" --bootstrap-server "$KAFKA_BROKER" --create --if-not-exists \
#       --topic interactions --partitions 3 --replication-factor 1

# echo "[create_topics] recommendations (1 partition)"
# "$KT" --bootstrap-server "$KAFKA_BROKER" --create --if-not-exists \
#       --topic recommendations --partitions 1 --replication-factor 1

# echo "[create_topics] alerts (1 partition)"
# "$KT" --bootstrap-server "$KAFKA_BROKER" --create --if-not-exists \
#       --topic alerts --partitions 1 --replication-factor 1

# echo "[create_topics] current topics:"
# "$KT" --bootstrap-server "$KAFKA_BROKER" --list

#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

KT="$KAFKA_DIR/bin/kafka-topics.sh"

echo "[create_topics] checking/creating topics on $KAFKA_BROKER"

create_topic () {
  local topic=$1
  local partitions=$2

  # check if exists
  if "$KT" --bootstrap-server "$KAFKA_BROKER" --list | grep -q "^${topic}$"; then
    echo "[create_topics] topic already exists: $topic"
  else
    echo "[create_topics] creating topic: $topic"
    "$KT" --bootstrap-server "$KAFKA_BROKER" \
      --create \
      --topic "$topic" \
      --partitions "$partitions" \
      --replication-factor 1
  fi
}

create_topic "interactions" 3
create_topic "recommendations" 1
create_topic "alerts" 1

echo "[create_topics] current topics:"
"$KT" --bootstrap-server "$KAFKA_BROKER" --list