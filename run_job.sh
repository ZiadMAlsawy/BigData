#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# run_job.sh — Submit the MapReduce job and show timing + sample output
#
# Run this from the mapreduce/ folder AFTER setup_hdfs.sh:
#   bash run_job.sh
#
# Prerequisites:
#   - Cluster is up (setup_hdfs.sh already ran)
#   - Books are on HDFS at /user/student/library
#   - mapper.py, reducer.py, stopwords.txt are in namenode:/tmp/
# ─────────────────────────────────────────────────────────────────

NAMENODE="namenode"
STREAMING_JAR="/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
HDFS_INPUT="/user/student/library"
HDFS_OUTPUT="/user/student/reverse_index_output"
SAMPLE_LINES=20   # how many output lines to preview

# ── Remove previous output (Hadoop refuses to overwrite) ─────────
echo ""
echo ">>> Clearing previous output (if any)..."
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c "hdfs dfs -rm -r -f $HDFS_OUTPUT"

# ── Submit the job and measure wall-clock time ───────────────────
echo ""
echo ">>> Submitting MapReduce job..."
echo "    Input : $HDFS_INPUT"
echo "    Output: $HDFS_OUTPUT"
echo ""

START=$(date +%s)

# Fix Windows line endings in scripts (safe to run even on Linux)
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c \
  "sed -i 's/\r//' /tmp/mapper.py /tmp/reducer.py /tmp/stopwords.txt"

MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c "
hadoop jar $STREAMING_JAR \
  -files /tmp/stopwords.txt,/tmp/mapper.py,/tmp/reducer.py \
  -mapper  'python3 mapper.py' \
  -reducer 'python3 reducer.py' \
  -input   $HDFS_INPUT \
  -output  $HDFS_OUTPUT
" 2>&1 | grep --line-buffered -E "map [0-9]|reduce [0-9]|completed|failed|ERROR|Streaming|Caused"

END=$(date +%s)
ELAPSED=$((END - START))

# ── Result summary ───────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════╗"
echo "  Execution time : ${ELAPSED} seconds"
echo "  Input          : $HDFS_INPUT"
echo "  Output         : $HDFS_OUTPUT"
echo "╚══════════════════════════════════════════╝"

# ── Check if the job succeeded before previewing output ──────────
JOB_STATUS=$(MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c \
  "hdfs dfs -test -e ${HDFS_OUTPUT}/_SUCCESS && echo OK || echo FAILED")

if [ "$JOB_STATUS" != "OK" ]; then
    echo ""
    echo "!!! Job failed. Task-level error output:"
    echo "─────────────────────────────────────────────────────────────"
    MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c \
      "find /tmp/hadoop-root/userlogs -name 'stderr' | xargs cat 2>/dev/null | tail -40"
    echo "─────────────────────────────────────────────────────────────"
    exit 1
fi

# ── Sample output ────────────────────────────────────────────────
echo ""
echo ">>> Sample output (first $SAMPLE_LINES entries):"
echo "─────────────────────────────────────────────────────────────"
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c \
  "hdfs dfs -cat ${HDFS_OUTPUT}/part-00000" 2>/dev/null \
  | grep -v "^[0-9]" \
  | head -$SAMPLE_LINES
echo "─────────────────────────────────────────────────────────────"

# ── Output file size ─────────────────────────────────────────────
echo ""
echo ">>> Output file info:"
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c \
  "hdfs dfs -ls $HDFS_OUTPUT/part-00000"
