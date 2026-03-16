# BigData — Distributed Reverse Index (Mini Project 1)

A MapReduce-based reverse index over 20 Project Gutenberg books, running on a
Dockerized Hadoop 3.2.1 cluster (1 namenode + up to 3 datanodes).

---

## Prerequisites

### 1. Docker Desktop

Must be installed and running.

```bash
docker --version        # should print Docker version
docker-compose --version
```

### 2. Java (for local testing only — not needed inside the cluster)

The Hadoop cluster runs fully inside Docker, so Java is **not required on your
host machine**. If you want to verify the version inside the namenode:

```bash
MSYS_NO_PATHCONV=1 docker exec namenode java -version
# Expected: openjdk version "1.8.x"
```

### 3. Shell

- **Windows**: use **Git Bash** (comes with Git for Windows). Do NOT use
  PowerShell or CMD — the scripts use Unix syntax.
- **Mac / Linux**: any terminal works, remove `MSYS_NO_PATHCONV=1` from
  commands (it is only needed on Windows/Git Bash to prevent path mangling).

---

## Project Structure

```text
BigData/
├── docker-compose.yml          # Hadoop cluster definition (namenode + 3 datanodes)
├── setup_hdfs.sh               # One-time setup: start cluster, upload books to HDFS
├── run_job.sh                  # Submit the MapReduce job (accepts node count as arg)
├── Mini Project 1/
│   ├── Books/                  # 20 Project Gutenberg .txt books
    ├── stopwords.txt           # Stop-words filter list
    ├── mapreduce/
        ├── mapper.py           # Hadoop Streaming mapper
        ├── reducer.py          # Hadoop Streaming reducer
        └── test_local.sh       # Local test (no Hadoop needed)
```

---

## Quick Start (run everything in order)

### Step 1 — Clone / open the repo

```bash
cd BigData/
```

All commands below assume you are in the `BigData/` root directory.

---

### Step 2 — Start the full cluster and upload books to HDFS

> Do this once before running any experiments.

```bash
# Tear down any previous cluster state (important for a clean start)
docker-compose -f docker-compose.yml down

# Start namenode + all 3 datanodes
docker-compose -f docker-compose.yml up -d

# Wait ~25 seconds for HDFS to fully initialize, then verify
sleep 25
MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes"
# Expected: "Live datanodes (3):"
```

Install Python3 inside the namenode container (only needed on first run):

```bash
MSYS_NO_PATHCONV=1 docker exec namenode bash -c "
  cat > /etc/apt/sources.list << 'EOF'
deb http://archive.debian.org/debian stretch main
deb http://archive.debian.org/debian-security stretch/updates main
EOF
  apt-get -o Acquire::Check-Valid-Until=false update -qq && apt-get install -y -qq python3
  python3 --version
"
```

Copy scripts and books into the namenode, then upload to HDFS:

```bash
# Copy Python scripts and stopwords
docker cp "Mini Project 1/stopwords.txt" namenode:/tmp/stopwords.txt
docker cp "Mini Project 1/mapreduce/mapper.py" namenode:/tmp/mapper.py
docker cp "Mini Project 1/mapreduce/reducer.py" namenode:/tmp/reducer.py

# Copy all books as a folder (avoids Windows path-with-spaces issues)
docker cp "Mini Project 1/Books" namenode:/tmp/books

# Fix Windows line endings, create HDFS directory, upload all books
MSYS_NO_PATHCONV=1 docker exec namenode bash -c "
  sed -i 's/\r//' /tmp/mapper.py /tmp/reducer.py /tmp/stopwords.txt
  hdfs dfs -mkdir -p /user/student/library
  hdfs dfs -D dfs.replication=3 -put /tmp/books/*.txt /user/student/library/
"

# Verify upload (should list 20 files)
MSYS_NO_PATHCONV=1 docker exec namenode bash -c "hdfs dfs -ls /user/student/library/ | tail -5"
```

---

## Running the Scalability Experiments

The assignment requires running the job under 3 different cluster sizes and
measuring execution time to calculate Speedup (S = T1 / Tn).

Each experiment requires:

1. A **fresh cluster** with exactly N datanodes (prevents HDFS from contacting stopped nodes)
2. Books re-uploaded with matching replication factor
3. Running `run_job.sh N` where N = number of datanodes

---

### Experiment 1 — T1: 1 Datanode

```bash
# 1. Tear down and restart with only 1 datanode
docker-compose -f docker-compose.yml down
docker-compose -f docker-compose.yml up -d namenode datanode1
sleep 25

# Verify: should show "Live datanodes (1):"
MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes"

# 2. Re-copy files into the fresh namenode container
docker cp "Mini Project 1/stopwords.txt" namenode:/tmp/stopwords.txt
docker cp "Mini Project 1/mapreduce/mapper.py" namenode:/tmp/mapper.py
docker cp "Mini Project 1/mapreduce/reducer.py" namenode:/tmp/reducer.py
docker cp "Mini Project 1/Books" namenode:/tmp/books

# 3. Install Python3 (fresh container has none)
MSYS_NO_PATHCONV=1 docker exec namenode bash -c "
  cat > /etc/apt/sources.list << 'EOF'
deb http://archive.debian.org/debian stretch main
deb http://archive.debian.org/debian-security stretch/updates main
EOF
  apt-get -o Acquire::Check-Valid-Until=false update -qq && apt-get install -y -qq python3
"

# 4. Upload books with replication=1
MSYS_NO_PATHCONV=1 docker exec namenode bash -c "
  sed -i 's/\r//' /tmp/mapper.py /tmp/reducer.py /tmp/stopwords.txt
  hdfs dfs -mkdir -p /user/student/library
  hdfs dfs -D dfs.replication=1 -put /tmp/books/*.txt /user/student/library/
"

# 5. Run the job — note the execution time printed at the end
bash run_job.sh 1
```

Time taken on one datanode: 11 seconds

---

### Experiment 2 — T2: 2 Datanodes

```bash
# 1. Add datanode2 to the running cluster (no need to tear down)
docker-compose -f docker-compose.yml up -d datanode2
sleep 20

# Verify: should show "Live datanodes (2):"
MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes"

# 2. Re-upload books with replication=2
MSYS_NO_PATHCONV=1 docker exec namenode bash -c "
  hdfs dfs -rm -r -f /user/student/library
  hdfs dfs -mkdir -p /user/student/library
  hdfs dfs -D dfs.replication=2 -put /tmp/books/*.txt /user/student/library/
"

# 3. Run the job
bash run_job.sh 2
```

Time taken on two datanodes: 11 seconds.

---

### Experiment 3 — T3: 3 Datanodes

```bash
# 1. Add datanode3 to the running cluster
docker-compose -f docker-compose.yml up -d datanode3
sleep 20

# Verify: should show "Live datanodes (3):"
MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes"

# 2. Re-upload books with replication=3
MSYS_NO_PATHCONV=1 docker exec namenode bash -c "
  hdfs dfs -rm -r -f /user/student/library
  hdfs dfs -mkdir -p /user/student/library
  hdfs dfs -D dfs.replication=3 -put /tmp/books/*.txt /user/student/library/
"

# 3. Run the job
bash run_job.sh 3
```

Time taken on three datanodes: 11 seconds.

---

## Local Test

To verify mapper and reducer logic on a single book before submitting to the cluster:

```bash
cd "Mini Project 1/mapreduce/"
bash test_local.sh
```

This pipes one book through `mapper.py → sort → reducer.py` locally and prints
the first results to the terminal.

---

## Speedup Calculation

After collecting T1, T2, T3:

| Metric            | Formula      | Time Taken      |
| :---------------- | :----------- | :-------------- |
| Speedup (2 nodes) | S2 = T1 / T2 | 10 / 9 = 1.11x  |
| Speedup (3 nodes) | S3 = T1 / T3 | 10 / 9 = 1.11x  |

> **Note on results:** All containers run on the same physical machine, so
> datanodes share CPU/RAM/disk. True linear speedup requires separate physical
> machines. The small differences observed are consistent with Amdahl's Law —
> the sequential portions (JVM startup, single reducer, HDFS commit) dominate
> when hardware parallelism is constrained.

---

## Viewing the Full Output

```bash
# Print the entire reverse index from HDFS to terminal
MSYS_NO_PATHCONV=1 docker exec namenode bash -c \
  "hdfs dfs -cat /user/student/reverse_index_output/part-00000"

# Search for a specific word (e.g. "freedom")
MSYS_NO_PATHCONV=1 docker exec namenode bash -c \
  "hdfs dfs -cat /user/student/reverse_index_output/part-00000" \
  | grep "^freedom "

# Copy output to your local machine
MSYS_NO_PATHCONV=1 docker exec namenode bash -c \
  "hdfs dfs -get /user/student/reverse_index_output/part-00000 /tmp/output.txt"
docker cp namenode:/tmp/output.txt ./reverse_index_output.txt
```

---

## Stopping the Cluster

```bash
docker-compose -f docker-compose.yml down
```
