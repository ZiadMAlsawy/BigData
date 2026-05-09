# Mini Project 3 — Real-Time Recommendation System (Kindle Books)

End-to-end big-data pipeline: **batch ALS training** + **Kafka + Spark Structured Streaming** for live recommendations, windowed analytics, alerts, watermarked late-data handling, and a Streamlit dashboard.

- Domain: Books — Kindle Reviews (~982K rows)
- Focus: Real-Time Intelligence (trending detection, alerts, low latency)
- Bonuses: Streamlit dashboard (+2), end-to-end p95 latency < 5 s (+1)
- Runtime: WSL2 + Kafka 2.12-3.7.0 (KRaft, portable folder) + Spark 4.1.1 (Scala 2.13) + Java 17

## Recorded results

| Metric | Value |
|---|---|
| ALS RMSE | 0.9001 |
| Latency p50 / p95 / p99 | 1202 / 2576 / 3256 ms |
| Recs rows generated | 26,165 |
| Unique users served | 5,034 |
| Distinct items in recs | 947 |
| Alert windows triggered | 39 |
| Sustained producer rate | 30 ev/s synthetic |

## One-time setup (inside WSL)

```bash
# 0. System deps
sudo apt install -y python3-venv python3-full dos2unix

# 1. Point at the Kafka folder
export KAFKA_DIR=~/kafka_2.12-3.7.0

# 2. Strip any CRLF line endings on shell scripts (Windows checkout artifact)
cd "Mini Project 3"
dos2unix infra/*.sh run_all.sh

# 3. Python venv + deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install streamlit-autorefresh   # smoother dashboard updates

# 4. Kaggle creds: drop kaggle.json at ~/.config/kaggle/kaggle.json (chmod 600)
python src/download_dataset.py
```

WSL-native paths are used automatically (avoids `/mnt/c` 9p I/O bug):

- `MP3_DATA_DIR  = ~/mp3-data`         (set automatically if folder exists)
- `MP3_OUTPUT_DIR = ~/mp3-output`      (auto when project sits on /mnt/c)
- `KAFKA_LOG_DIR  = ~/mp3-kafka-data`  (auto when project sits on /mnt/c)

Override any with `export VAR=…` before running.

## Quick path — one command

```bash
bash run_all.sh
```

Starts: Kafka → topics → ALS train (skipped if model exists) → Spark streaming → producer (synthetic) → Streamlit dashboard.

Open dashboard: <http://localhost:8501>

## Manual path — 5 terminals

Each new terminal needs:

```bash
cd "Mini Project 3"
source .venv/bin/activate
export KAFKA_DIR=~/kafka_2.12-3.7.0
export MP3_OUTPUT_DIR=~/mp3-output
export MP3_DATA_DIR=~/mp3-data
```

```bash
# Terminal 1 — broker
bash infra/start_kafka.sh
bash infra/create_topics.sh

# Terminal 2 — batch ALS (run once)
python src/train_als.py

# Terminal 3 — Kafka producer
#   synthetic = bursty traffic with 60-s spikes (recommended for demo)
#   replay    = real Kindle reviews, ~3.4 h at 80 ev/s; cap with --limit
python src/kafka_producer.py --mode synthetic --rate 30
# python src/kafka_producer.py --mode replay --rate 100 --limit 5000

# Terminal 4 — Spark Structured Streaming
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  --master "local[3]" \
  --conf spark.driver.memory=6g \
  src/streaming_app.py

# Terminal 5 — Streamlit dashboard
streamlit run src/dashboard.py --server.address 0.0.0.0 --server.port 8501
```

## Stop

```bash
kill $(cat ~/mp3-output/logs/*.pid) 2>/dev/null
bash infra/stop_kafka.sh
pkill -f kafka.Kafka
```

## Reset (if checkpoints corrupt or topics rebuilt)

```bash
bash infra/stop_kafka.sh
pkill -f kafka.Kafka
rm -rf ~/mp3-output ~/mp3-kafka-data infra/kafka-data
bash run_all.sh
```

## Verification

```bash
# event count in interactions topic
"$KAFKA_DIR/bin/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic interactions

# latency snapshot
python -c "
import pandas as pd, glob, os
files = [f for f in glob.glob(os.path.expanduser('~/mp3-output/latency/**/*.parquet'), recursive=True)
         if '_temporary' not in f and os.path.exists(f)]
df = pd.concat([pd.read_parquet(f) for f in files])
df = df[df.latency_ms.between(0, 600000)]
print(f'samples={len(df)} p50={df.latency_ms.quantile(0.5):.0f}ms p95={df.latency_ms.quantile(0.95):.0f}ms p99={df.latency_ms.quantile(0.99):.0f}ms')
"

# recs summary
python -c "
import pandas as pd, glob, os
df = pd.concat([pd.read_parquet(f) for f in glob.glob(os.path.expanduser('~/mp3-output/recs/**/*.parquet'), recursive=True) if '_temporary' not in f])
print(f'recs_rows={len(df)} unique_users={df.user_id.nunique()} unique_items={df.item_id.nunique()}')
"
```

## Layout

```
infra/        Kafka KRaft scripts + config (env.sh, start_kafka.sh, create_topics.sh)
src/          ALS trainer, Kafka producer, Spark streaming app, Streamlit dashboard
data/         Downloaded CSV (gitignored)
models/       Saved ALS model + factor parquets (gitignored)
~/mp3-output/ Streaming sinks: windows/, recs/, alerts/, latency/, checkpoints/, logs/
~/mp3-kafka-data/  Kafka log dir (KRaft)
Report/       LaTeX report
run_all.sh    One-shot launcher
```

## Differentiation

- Books domain (not default movies)
- Custom trending score `count × avg_rating × exp(-Δt/5)`
- Hybrid recommender: `0.7 × ALS + 0.3 × trending` (cold-start → top-trending)
- Kafka partition key = `user_id % 3` → preserves per-user order across `local[3]` Spark cores
- p95 latency budget < 5 s, measured per record via producer-stamped `event_time_ms`

## Known gotchas

- **`/mnt/c` 9p bug**: Spark + Kafka writing to `/mnt/c` corrupts state stores. Output and Kafka log dirs are forced to WSL native FS (`~/mp3-output`, `~/mp3-kafka-data`).
- **`kafka-python==2.0.2` broken on Python 3.12** (`kafka.vendor.six.moves` import). Use `kafka-python-ng==2.2.3` (drop-in replacement).
- **CRLF line endings**: shell scripts saved on Windows fail with `set: pipefa` — run `dos2unix infra/*.sh`.
- **Stale checkpoints after Kafka reset**: wipe `~/mp3-output/checkpoints` if streaming throws `OffsetOutOfRangeException` or `Option.get()` NPE.
