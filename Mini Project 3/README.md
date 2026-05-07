# Mini Project 3 — Real-Time Recommendation System (Kindle Books)

End-to-end big-data pipeline: **batch ALS training** + **Kafka + Spark Structured Streaming** for live recommendations, windowed analytics, alerts, and watermarked late-data handling.

- Domain: Books — Kindle Reviews (~982K rows)
- Focus: Real-Time Intelligence (trending detection, alerts, low latency)
- Bonus: Streamlit dashboard (+2), <5 s latency (+1)
- Runtime: WSL2 + Kafka 2.12-3.7.0 (KRaft, portable folder) + Spark 4.1.1 (Scala 2.13) + Java 17

## One-time setup

```bash
# 1. Set Kafka folder location (or edit infra/env.sh)
export KAFKA_DIR=~/kafka_2.12-3.7.0

# 2. Install Python deps inside a venv
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Pull dataset (Kaggle creds required, ~/.kaggle/kaggle.json)
python src/download_dataset.py
```

## Running the pipeline

```bash
# Terminal 1 — start Kafka broker (KRaft single-node)
bash infra/start_kafka.sh
bash infra/create_topics.sh

# Terminal 2 — train ALS + save model
python src/train_als.py

# Terminal 3 — produce events from held-out reviews
python src/kafka_producer.py --mode replay --rate 50

# Terminal 4 — run streaming app
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  --master "local[3]" \
  src/streaming_app.py

# Terminal 5 — dashboard
streamlit run src/dashboard.py
```

Or run end-to-end with `bash run_all.sh`.

## Layout

```
infra/        Kafka KRaft scripts + config
src/          ALS trainer, Kafka producer, Spark streaming app, Streamlit dashboard
data/         Downloaded CSV (gitignored)
models/       Saved ALS model (gitignored)
output/       Streaming sinks: windows/, recs/, alerts/, latency/
Report/       LaTeX report
```

## Differentiation

- Books domain (not default movies)
- Custom trending score `count × avg_rating × exp(-Δt/5)`
- Hybrid recommender: `0.7 × ALS + 0.3 × trending`
- Kafka partition key = `user_id % 3` → preserves per-user order across `local[3]` Spark cores
- Cold-start fallback to top-trending for new users
- p95 latency target < 5 s, measured per record

## Stop

```bash
bash infra/stop_kafka.sh
```
