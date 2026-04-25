"""CSV vs Parquet: Q3 grouping benchmark plus Parquet write (partitioned by YEAR)."""
import sys, os, time
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

from pyspark.sql import functions as F
from common import make_spark, load_flights, PARQUET_PATH, _append_perf

spark = make_spark("Optimization_parquet")
flights = load_flights(spark)


def q3_df(df):
    return (df.groupBy("AIRLINE_CODE", "ORIGIN", "MONTH")
            .agg(F.avg("ARR_DELAY").alias("avg_arr_delay")))


if not Path(PARQUET_PATH).exists():
    print(f"Writing Parquet to {PARQUET_PATH} (partitioned by YEAR)...")
    (flights.write
     .mode("overwrite")
     .partitionBy("YEAR")
     .parquet(PARQUET_PATH))
    print("Parquet written.")
else:
    print(f"Parquet already present at {PARQUET_PATH} - reusing.")

flights_parq = spark.read.parquet(PARQUET_PATH)
flights_parq.createOrReplaceTempView("flights_parq")

t0 = time.perf_counter(); q3_df(flights).count(); csv_t = time.perf_counter() - t0
t0 = time.perf_counter(); q3_df(flights_parq).count(); parq_t = time.perf_counter() - t0

print(f"CSV Q3:     {csv_t:.3f}s")
print(f"Parquet Q3: {parq_t:.3f}s   (speedup {csv_t / parq_t:.2f}x)")

_append_perf({"query": "Q3_format_csv", "api": "DataFrame",
              "seconds": round(csv_t, 3), "rows": None})
_append_perf({"query": "Q3_format_parquet", "api": "DataFrame",
              "seconds": round(parq_t, 3), "rows": None})

spark.stop()
