"""Caching impact: cold (uncached) vs warm (cached) runs of Q2 aggregation."""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

from pyspark.sql import functions as F
from common import make_spark, load_flights, _append_perf

spark = make_spark("Optimization_caching")
flights = load_flights(spark)


def q2_df():
    return (flights.groupBy("AIRLINE_CODE")
            .agg(F.count("*").alias("total_flights"),
                 F.sum("CANCELLED").alias("cancellations"),
                 F.avg("ARR_DELAY").alias("avg_arr_delay"),
                 F.max("ARR_DELAY").alias("max_arr_delay"),
                 F.min("ARR_DELAY").alias("min_arr_delay")))


flights.unpersist()
t0 = time.perf_counter()
q2_df().count()
cold = time.perf_counter() - t0

flights.cache()
flights.count()  # materialise the cache

t0 = time.perf_counter()
q2_df().count()
warm = time.perf_counter() - t0

print(f"Cold (uncached): {cold:.3f}s")
print(f"Warm (cached):   {warm:.3f}s")
print(f"Speedup:         {cold / warm:.2f}x")

_append_perf({"query": "Q2_cache_cold", "api": "DataFrame",
              "seconds": round(cold, 3), "rows": None})
_append_perf({"query": "Q2_cache_warm", "api": "DataFrame",
              "seconds": round(warm, 3), "rows": None})

spark.stop()
