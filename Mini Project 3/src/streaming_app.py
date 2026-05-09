"""Spark Structured Streaming app for MP3.

Pipeline:
  Kafka(interactions)
    -> safe JSON parse + watermark (1 min, late events dropped)
    -> branch A: 30s/10s windowed aggregates (avg rating, user counts, trending score)
    -> branch B: alerts (trending items, user activity spikes)
    -> branch C: hybrid Top-5 recs (ALS factors broadcast + trending state)
    -> sinks: parquet (windows/, recs/, alerts/) + console + Kafka topics

Submit:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
    --master "local[3]" \
    src/streaming_app.py
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)

from common import (
    CHECKPOINT_DIR, EVENT_SCHEMA, KAFKA_BROKER, OUTPUT_DIR,
    TOPIC_ALERTS, TOPIC_INTERACTIONS, TOPIC_RECS, ensure_dirs, make_spark,
)
from recommender import hybrid_top_k, load_factors

WINDOW_SIZE = "30 seconds"
WINDOW_SLIDE = "10 seconds"
WATERMARK = "1 minute"
TRIGGER = "1 second"

ALERT_AVG_RATING = 4.5
ALERT_MIN_COUNT = 10
ALERT_USER_BURST = 20

# Latch for the alert state we read from per-batch (latest trending score per item)
_TRENDING_STATE: dict[int, float] = {}


def build_kafka_source(spark) -> DataFrame:
    raw = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", KAFKA_BROKER)
                 .option("subscribe", TOPIC_INTERACTIONS)
                 .option("startingOffsets", "latest")
                 .option("maxOffsetsPerTrigger", 500)
                 .option("failOnDataLoss", "false")
                 .load())

    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
              .withColumn("evt", F.from_json(F.col("json_str"), EVENT_SCHEMA))
              .withColumn("malformed", F.col("evt").isNull()))

    good = (parsed.where(~F.col("malformed"))
                  .select(
                      F.col("evt.user_id").alias("user_id"),
                      F.col("evt.item_id").alias("item_id"),
                      F.col("evt.rating").alias("rating"),
                      F.col("evt.timestamp").alias("event_time"),
                      F.col("evt.event_time_ms").alias("event_time_ms"),
                      F.col("evt.user_key").alias("user_key"),
                      F.col("evt.item_key").alias("item_key"),
                      F.col("kafka_ts"))
                  .where(F.col("user_id").isNotNull()
                         & F.col("item_id").isNotNull()
                         & F.col("rating").between(1.0, 5.0))
                  .withWatermark("event_time", WATERMARK))
    return good


def build_item_windows(events: DataFrame) -> DataFrame:
    """Per-item per-window aggregates + custom trending score."""
    win = F.window("event_time", WINDOW_SIZE, WINDOW_SLIDE)
    agg = (events.groupBy(win, "item_id")
                 .agg(F.count("*").alias("count"),
                      F.avg("rating").alias("avg_rating"),
                      F.max("event_time").alias("latest"),
                      F.min("event_time").alias("earliest"))
                 .withColumn("decay_minutes",
                             (F.unix_timestamp("window.end") - F.unix_timestamp("latest")) / 60.0)
                 .withColumn("trending_score",
                             F.col("count") * F.col("avg_rating")
                             * F.exp(-F.col("decay_minutes") / F.lit(5.0)))
                 .select(
                     F.col("window.start").alias("window_start"),
                     F.col("window.end").alias("window_end"),
                     "item_id", "count", "avg_rating", "trending_score"))
    return agg


def build_user_windows(events: DataFrame) -> DataFrame:
    """Per-user per-window interaction count."""
    win = F.window("event_time", WINDOW_SIZE, WINDOW_SLIDE)
    return (events.groupBy(win, "user_id")
                  .agg(F.count("*").alias("count"))
                  .select(
                      F.col("window.start").alias("window_start"),
                      F.col("window.end").alias("window_end"),
                      "user_id", "count"))


def build_alerts(item_win: DataFrame, user_win: DataFrame) -> DataFrame:
    item_alerts = (item_win
                   .where((F.col("avg_rating") > ALERT_AVG_RATING)
                          & (F.col("count") >= ALERT_MIN_COUNT))
                   .select(
                       F.lit("trending_item").alias("alert_type"),
                       F.col("item_id").cast("string").alias("subject"),
                       F.col("avg_rating").alias("metric"),
                       F.col("count").alias("magnitude"),
                       "window_start", "window_end"))
    user_alerts = (user_win
                   .where(F.col("count") >= ALERT_USER_BURST)
                   .select(
                       F.lit("user_burst").alias("alert_type"),
                       F.col("user_id").cast("string").alias("subject"),
                       F.col("count").cast("double").alias("metric"),
                       F.col("count").alias("magnitude"),
                       "window_start", "window_end"))
    return item_alerts.unionByName(user_alerts)


def write_kafka(df: DataFrame, topic: str, checkpoint: Path,
                value_cols: list[str], key_col: str | None = None):
    payload = F.to_json(F.struct(*[F.col(c) for c in value_cols]))
    out = df.select(F.col(key_col).cast("string").alias("key") if key_col
                    else F.lit(None).cast("string").alias("key"),
                    payload.alias("value"))
    return (out.writeStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BROKER)
              .option("topic", topic)
              .option("checkpointLocation", str(checkpoint))
              .outputMode("append")
              .start())


def write_parquet(df: DataFrame, path: Path, checkpoint: Path,
                  trigger_seconds: float = 5.0):
    return (df.writeStream
              .format("parquet")
              .option("path", str(path))
              .option("checkpointLocation", str(checkpoint))
              .outputMode("append")
              .trigger(processingTime=f"{trigger_seconds} seconds")
              .start())


def write_console(df: DataFrame, name: str, num_rows: int = 10):
    return (df.writeStream
              .format("console")
              .option("truncate", "false")
              .option("numRows", num_rows)
              .queryName(name)
              .outputMode("append")
              .start())


def latency_sink(events: DataFrame, output_path: Path, checkpoint: Path):
    """foreachBatch sink: compute end-to-end latency per record + maintain trending state."""

    def _process(batch: DataFrame, batch_id: int):
        if batch.rdd.isEmpty():
            return
        # ingest_ms = Kafka append time, with sub-second precision via double cast.
        # process_ms = wall-clock at the moment this batch lands in foreachBatch
        #              (set in driver, applied as a literal to every row)
        process_ms = int(time.time() * 1000)
        with_lat = batch.withColumn(
            "ingest_ms", (F.col("kafka_ts").cast("double") * 1000).cast("long")
        ).withColumn(
            "process_ms", F.lit(process_ms).cast("long")
        ).withColumn(
            "latency_ms", F.col("process_ms") - F.col("event_time_ms")
        ).select("user_id", "item_id", "event_time_ms",
                 "ingest_ms", "process_ms", "latency_ms")
        (with_lat.write.mode("append").parquet(str(output_path)))

    return (events.writeStream
                  .foreachBatch(_process)
                  .option("checkpointLocation", str(checkpoint))
                  .outputMode("append")
                  .trigger(processingTime=f"{TRIGGER}")
                  .start())


def recs_sink(events: DataFrame, item_win: DataFrame, output_path: Path,
              checkpoint: Path, factors_holder: list, trending_log_path: Path):
    """foreachBatch: load factors lazily, refresh trending state, emit Top-5 per user."""

    def _process(batch: DataFrame, batch_id: int):
        if batch.rdd.isEmpty():
            return
        if not factors_holder:
            print("[recs] loading ALS factors into driver memory")
            factors_holder.append(load_factors())
            print(f"[recs] factors loaded: users={len(factors_holder[0].user_pos)} "
                  f"items={len(factors_holder[0].item_pos)} rank={factors_holder[0].rank}")
        bundle = factors_holder[0]
        trending = dict(_TRENDING_STATE)

        # Distinct users seen this micro-batch
        users = [r["user_id"] for r in batch.select("user_id").distinct().collect()]
        if not users:
            return

        ts = int(time.time() * 1000)
        rows = []
        for uid in users:
            top = hybrid_top_k(bundle, uid, trending, k=5)
            for rank, (item_id, score) in enumerate(top, start=1):
                rows.append({"user_id": int(uid), "rank": rank,
                             "item_id": int(item_id), "score": float(score),
                             "generated_at_ms": ts, "batch_id": int(batch_id)})
        if not rows:
            return
        spark = batch.sparkSession
        rec_df = spark.createDataFrame(rows)
        rec_df.write.mode("append").parquet(str(output_path))
        # Push to Kafka recs topic in this batch as well
        (rec_df.selectExpr("CAST(user_id AS STRING) AS key",
                           "to_json(struct(*)) AS value")
               .write.format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_BROKER)
               .option("topic", TOPIC_RECS)
               .save())

    def _trending_update(win_batch: DataFrame, batch_id: int):
        if win_batch.rdd.isEmpty():
            return
        latest = (win_batch.groupBy("item_id")
                          .agg(F.max("trending_score").alias("score"))
                          .collect())
        for r in latest:
            _TRENDING_STATE[int(r["item_id"])] = float(r["score"])
        # decay older keys we haven't seen in this batch (prevent unbounded growth)
        if len(_TRENDING_STATE) > 5000:
            for k in list(_TRENDING_STATE.keys())[:1000]:
                _TRENDING_STATE.pop(k, None)

    q_recs = (events.writeStream
                    .foreachBatch(_process)
                    .option("checkpointLocation", str(checkpoint / "recs"))
                    .outputMode("append")
                    .trigger(processingTime="3 seconds")
                    .start())

    q_trend = (item_win.writeStream
                       .foreachBatch(_trending_update)
                       .option("checkpointLocation", str(checkpoint / "trending"))
                       .outputMode("append")
                       .trigger(processingTime=f"{WINDOW_SLIDE}")
                       .start())

    return q_recs, q_trend


def main():
    ensure_dirs()
    spark = make_spark("MP3-Streaming",
                       extra={"spark.sql.streaming.minBatchesToRetain": 5})

    events = build_kafka_source(spark)
    item_win = build_item_windows(events)
    user_win = build_user_windows(events)
    alerts = build_alerts(item_win, user_win)

    out = Path(OUTPUT_DIR)
    cp = Path(CHECKPOINT_DIR)

    queries = []
    queries.append(write_parquet(item_win, out / "windows" / "items",
                                 cp / "win_items"))
    queries.append(write_parquet(user_win, out / "windows" / "users",
                                 cp / "win_users"))
    queries.append(write_parquet(alerts, out / "alerts",
                                 cp / "alerts"))
    queries.append(write_kafka(alerts, TOPIC_ALERTS, cp / "alerts_kafka",
                               value_cols=["alert_type", "subject", "metric",
                                           "magnitude", "window_start", "window_end"],
                               key_col="alert_type"))

    queries.append(latency_sink(events, out / "latency", cp / "latency"))

    factors_holder: list = []
    q_recs, q_trend = recs_sink(events, item_win, out / "recs", cp,
                                factors_holder, out / "trending_log")
    queries.extend([q_recs, q_trend])

    print(f"[streaming] {len(queries)} queries running. Ctrl-C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
