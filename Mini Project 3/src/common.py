"""Shared SparkSession factory, paths, and schema for MP3."""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, LongType, StringType, TimestampType,
)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("MP3_DATA_DIR", str(ROOT / "data")))
MODEL_DIR = Path(os.environ.get("MP3_MODEL_DIR", str(ROOT / "models")))
OUTPUT_DIR = Path(os.environ.get("MP3_OUTPUT_DIR", str(ROOT / "output")))
CHECKPOINT_DIR = Path(os.environ.get("MP3_CHECKPOINT_DIR", str(ROOT / "output" / "checkpoints")))

CSV_PATH = DATA_DIR / "kindle_reviews.csv"
ALS_MODEL_PATH = MODEL_DIR / "als_model"
USER_FACTORS_PATH = MODEL_DIR / "user_factors.parquet"
ITEM_FACTORS_PATH = MODEL_DIR / "item_factors.parquet"
USER_INDEX_PATH = MODEL_DIR / "user_index.parquet"
ITEM_INDEX_PATH = MODEL_DIR / "item_index.parquet"

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:9092")
TOPIC_INTERACTIONS = os.environ.get("TOPIC_INTERACTIONS", "interactions")
TOPIC_RECS = os.environ.get("TOPIC_RECS", "recommendations")
TOPIC_ALERTS = os.environ.get("TOPIC_ALERTS", "alerts")

# Raw Kindle Reviews schema (only the columns we need)
RAW_SCHEMA = StructType([
    StructField("reviewerID", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("reviewerName", StringType(), True),
    StructField("helpful", StringType(), True),
    StructField("reviewText", StringType(), True),
    StructField("overall", DoubleType(), True),
    StructField("summary", StringType(), True),
    StructField("unixReviewTime", LongType(), True),
    StructField("reviewTime", StringType(), True),
])

# Raw Kindle Reviews CSV schema — real column order in the file.
# First column is an unnamed pandas-style index, hence "_idx".
RAW_SCHEMA = StructType([
    StructField("_idx", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("helpful", StringType(), True),
    StructField("overall", DoubleType(), True),
    StructField("reviewText", StringType(), True),
    StructField("reviewTime", StringType(), True),
    StructField("reviewerID", StringType(), True),
    StructField("reviewerName", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("unixReviewTime", LongType(), True),
])

# Streaming event schema (producer payload)
EVENT_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("event_time_ms", LongType(), True),
    StructField("user_key", StringType(), True),
    StructField("item_key", StringType(), True),
])


def make_spark(app_name: str, *, master: str | None = None, extra: dict | None = None) -> SparkSession:
    """Build a SparkSession tuned for MP3 (Spark 4.1.1 / Scala 2.13).

    extra: dict of additional .config() pairs.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master or os.environ.get("SPARK_MASTER", "local[3]"))
        .config("spark.sql.shuffle.partitions",
                os.environ.get("SHUFFLE_PARTITIONS", "8"))
        .config("spark.driver.memory",
                os.environ.get("DRIVER_MEMORY", "4g"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    for k, v in (extra or {}).items():
        builder = builder.config(k, str(v))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def ensure_dirs() -> None:
    for d in (DATA_DIR, MODEL_DIR, OUTPUT_DIR, CHECKPOINT_DIR):
        d.mkdir(parents=True, exist_ok=True)
