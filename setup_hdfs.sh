#!/bin/bash
# ─────────────────────────────────────────────────────────────────
set -e

NAMENODE="namenode"
HDFS_LIB="/user/student/library"

BOOKS_DIR="../BigData/Assignment 1/Books/"
STOPWORDS="../BigData/Assignment 1/stopwords.txt"
MAPPER="../BigData/mapreduce/mapper.py"
REDUCER="../BigData/mapreduce/reducer.py"

# ── Step 1: Start cluster ────────────────────────────────────────
echo ""
echo ">>> [1/5] Starting Hadoop cluster..."
docker-compose up -d
echo "    Waiting 20 seconds for services to initialize..."
sleep 20

# ── Step 2: Install Python3 in the namenode container ─────────
echo ""
echo ">>> [2/5] Ensuring Python3 is installed in namenode..."
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c \
  "which python3 || (apt-get update -qq && apt-get install -y -qq python3)"
echo "    Python3 ready."

# ── Step 3: Copy scripts into namenode ──────────────────────────
echo ""
echo ">>> [3/5] Copying scripts into namenode..."
docker cp "$STOPWORDS" $NAMENODE:/tmp/stopwords.txt
docker cp "$MAPPER"    $NAMENODE:/tmp/mapper.py
docker cp "$REDUCER"   $NAMENODE:/tmp/reducer.py
echo "    Scripts copied."

# ── Step 4: Copy books into namenode ────────────────────────────
# We copy the whole folder at once to avoid Windows path issues
echo ""
echo ">>> [4/5] Copying books into namenode..."
docker cp "$BOOKS_DIR" $NAMENODE:/tmp/books
echo "    Books copied."

# ── Step 5: Upload to HDFS ──────────────────────────────────────
echo ""
echo ">>> [5/5] Uploading books to HDFS at $HDFS_LIB ..."
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c "
    hdfs dfs -mkdir -p $HDFS_LIB
    hdfs dfs -put -f /tmp/books/*.txt $HDFS_LIB/
"

echo ""
echo "    Verifying upload:"
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c "hdfs dfs -ls $HDFS_LIB"

echo ""
echo "=== HDFS setup complete. Ready to run the MapReduce job. ==="
