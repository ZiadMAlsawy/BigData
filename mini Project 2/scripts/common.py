"""Shared Spark session, schema, and helpers used by every query script."""
import os
import time
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType,
)

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = os.environ.get("FLIGHTS_CSV", str(ROOT / "flights_sample_3m.csv"))
PARQUET_PATH = os.environ.get("FLIGHTS_PARQUET", str(ROOT / "flights.parquet"))
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", str(Path(__file__).resolve().parent / "results")))
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
PERF_FILE = RESULTS_DIR / "performance_results.csv"

SCHEMA = StructType([
    StructField("FL_DATE", DateType(), True),
    StructField("AIRLINE", StringType(), True),
    StructField("AIRLINE_DOT", StringType(), True),
    StructField("AIRLINE_CODE", StringType(), True),
    StructField("DOT_CODE", IntegerType(), True),
    StructField("FL_NUMBER", IntegerType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("ORIGIN_CITY", StringType(), True),
    StructField("DEST", StringType(), True),
    StructField("DEST_CITY", StringType(), True),
    StructField("CRS_DEP_TIME", IntegerType(), True),
    StructField("DEP_TIME", DoubleType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("TAXI_OUT", DoubleType(), True),
    StructField("WHEELS_OFF", DoubleType(), True),
    StructField("WHEELS_ON", DoubleType(), True),
    StructField("TAXI_IN", DoubleType(), True),
    StructField("CRS_ARR_TIME", IntegerType(), True),
    StructField("ARR_TIME", DoubleType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("CANCELLED", DoubleType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", DoubleType(), True),
    StructField("CRS_ELAPSED_TIME", DoubleType(), True),
    StructField("ELAPSED_TIME", DoubleType(), True),
    StructField("AIR_TIME", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("DELAY_DUE_CARRIER", DoubleType(), True),
    StructField("DELAY_DUE_WEATHER", DoubleType(), True),
    StructField("DELAY_DUE_NAS", DoubleType(), True),
    StructField("DELAY_DUE_SECURITY", DoubleType(), True),
    StructField("DELAY_DUE_LATE_AIRCRAFT", DoubleType(), True),
])


def make_spark(app_name, broadcast_threshold=None):
    if broadcast_threshold is None:
        broadcast_threshold = 10 * 1024 * 1024
    builder = (SparkSession.builder
               .appName(app_name)
               .master(os.environ.get("SPARK_MASTER", "local[4]"))
               .config("spark.sql.shuffle.partitions",
                       os.environ.get("SHUFFLE_PARTITIONS", "200"))
               .config("spark.driver.memory",
                       os.environ.get("DRIVER_MEMORY", "4g"))
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.autoBroadcastJoinThreshold",
                       broadcast_threshold))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def load_flights(spark, register_view=True):
    df = (spark.read
          .option("header", "true")
          .schema(SCHEMA)
          .csv(CSV_PATH))
    df = (df.withColumn("YEAR", F.year("FL_DATE"))
            .withColumn("MONTH", F.month("FL_DATE"))
            .withColumn("DAY_OF_WEEK", F.dayofweek("FL_DATE")))
    if register_view:
        df.createOrReplaceTempView("flights")
    return df


def load_airlines_dim(flights, register_view=True):
    dim = flights.select("AIRLINE_CODE", "AIRLINE", "DOT_CODE").distinct()
    if register_view:
        dim.createOrReplaceTempView("airlines_dim")
    return dim


def load_airport_stats(flights, register_view=True):
    stats = (flights.groupBy("ORIGIN")
             .agg(F.count("*").alias("ORIGIN_FLIGHT_COUNT"),
                  F.first("ORIGIN_CITY").alias("ORIGIN_CITY")))
    if register_view:
        stats.createOrReplaceTempView("airport_stats")
    return stats


def time_it(label, api, fn):
    """Run fn(), measure wall-clock + row count, print and persist record."""
    t0 = time.perf_counter()
    result = fn()
    if isinstance(result, (list, tuple)):
        n = len(result)
    elif hasattr(result, "count") and callable(getattr(result, "count")):
        try:
            n = result.count()
        except TypeError:
            n = len(result)
    else:
        n = int(result)
    elapsed = time.perf_counter() - t0
    print(f"[{label:25s}][{api:10s}] {elapsed:.3f}s  rows={n}")
    _append_perf({"query": label, "api": api,
                  "seconds": round(elapsed, 3), "rows": n})
    return result, elapsed


def _append_perf(record):
    new_file = not PERF_FILE.exists()
    with PERF_FILE.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["query", "api", "seconds", "rows"])
        if new_file:
            w.writeheader()
        w.writerow(record)


def explain_block(label, df):
    print(f"\n=== {label} .explain(True) ===")
    df.explain(True)
    print("=== end of plan ===\n")
