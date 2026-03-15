#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# setup_hdfs.sh — Upload books and scripts to the Hadoop cluster
#
# Run this from the BigData/ project root:
#   bash mapreduce/setup_hdfs.sh
#
# What it does:
#   1. Starts the Docker cluster
#   2. Copies mapper.py, reducer.py, stopwords.txt into the namenode
#   3. Copies all books into the namenode
#   4. Creates /user/student/library on HDFS
#   5. Uploads all books to HDFS
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
echo ">>> [1/4] Starting Hadoop cluster..."
docker-compose up -d
echo "    Waiting 20 seconds for services to initialize..."
sleep 20

# ── Step 1.5: Install Python3 in the namenode container ─────────
echo ""
echo ">>> [1.5/4] Ensuring Python3 is installed in namenode..."
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c \
  "which python3 || (apt-get update -qq && apt-get install -y -qq python3)"
echo "    Python3 ready."

# ── Step 2: Copy scripts into namenode ──────────────────────────
echo ""
echo ">>> [2/4] Copying scripts into namenode..."
docker cp "$STOPWORDS" $NAMENODE:/tmp/stopwords.txt
docker cp "$MAPPER"    $NAMENODE:/tmp/mapper.py
docker cp "$REDUCER"   $NAMENODE:/tmp/reducer.py
echo "    Scripts copied."

# ── Step 3: Copy books into namenode ────────────────────────────
# We copy the whole folder at once to avoid Windows path issues
echo ""
echo ">>> [3/4] Copying books into namenode..."
docker cp "$BOOKS_DIR" $NAMENODE:/tmp/books
echo "    Books copied."

# ── Step 4: Upload to HDFS ──────────────────────────────────────
# MSYS_NO_PATHCONV=1 prevents Git Bash from converting /user/... to a Windows path
# bash -c "..." is required so the *.txt glob is expanded inside the container (Linux), not by Windows
echo ""
echo ">>> [4/4] Uploading books to HDFS at $HDFS_LIB ..."
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c "
    hdfs dfs -mkdir -p $HDFS_LIB
    hdfs dfs -put -f /tmp/books/*.txt $HDFS_LIB/
"

echo ""
echo "    Verifying upload:"
MSYS_NO_PATHCONV=1 docker exec $NAMENODE bash -c "hdfs dfs -ls $HDFS_LIB"

echo ""
echo "=== HDFS setup complete. Ready to run the MapReduce job. ==="
