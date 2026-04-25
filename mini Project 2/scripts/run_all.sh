#!/usr/bin/env bash
# Run every query + optimization script via spark-submit, capturing logs.
# Designed for WSL/Linux. Usage: ./run_all.sh
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # Get absolute path of the script's directory
cd "$SCRIPT_DIR"

LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs}" # Directory for logs
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/results}" # Directory for CSV results (performance metrics)
mkdir -p "$LOG_DIR" "$RESULTS_DIR" # Create directory
export RESULTS_DIR # export so that spark jobs can write to it

# Reset performance log so each full run is independent.
rm -f "$RESULTS_DIR/performance_results.csv"

SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SUBMIT_OPTS=(
    --master "${SPARK_MASTER:-local[4]}"
    --driver-memory "${DRIVER_MEMORY:-4g}"
    --conf "spark.sql.adaptive.enabled=true"
    --conf "spark.sql.shuffle.partitions=${SHUFFLE_PARTITIONS:-200}"
    --py-files "$SCRIPT_DIR/common.py"
)

run() {
    local script="$1"
    local name
    name="$(basename "$script" .py)"
    echo
    echo "=========================================="
    echo "Running $name"
    echo "=========================================="
    "$SPARK_SUBMIT" "${SUBMIT_OPTS[@]}" "$script" 2>&1 | tee "$LOG_DIR/$name.log"
}

QUERIES=(
    q01_filter.py
    q02_aggregate.py
    q03_multi_group.py
    q04_top20_routes.py
    q05_moving_avg.py
    q06_cumulative.py
    q07_rank.py
    q08_subquery.py
    q09_broadcast_join.py
    q10_sortmerge_join.py
    q11_root_cause.py
    q12_anomaly.py
)

for q in "${QUERIES[@]}"; do
    run "$SCRIPT_DIR/$q"
done

# Optimization studies: caching, parquet (writes Parquet), partition pruning, scalability.
run "$SCRIPT_DIR/optimization/caching.py"
run "$SCRIPT_DIR/optimization/parquet_format.py"
run "$SCRIPT_DIR/optimization/partition_pruning.py"
run "$SCRIPT_DIR/optimization/scalability.py"

echo
echo "=========================================="
echo "Aggregating performance results"
echo "=========================================="
python3 "$SCRIPT_DIR/aggregate_results.py" | tee "$LOG_DIR/aggregate_results.log"

echo
echo "All scripts complete. Logs in $LOG_DIR, CSVs in $RESULTS_DIR."
