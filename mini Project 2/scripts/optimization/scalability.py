"""Scalability test: run Q3 with different shuffle partition counts."""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

from pyspark.sql import functions as F
from common import make_spark, load_flights, _append_perf

spark = make_spark("Optimization_scalability")
flights = load_flights(spark)
flights.cache()
flights.count()


def q3_df():
    return (flights.groupBy("AIRLINE_CODE", "ORIGIN", "MONTH")
            .agg(F.avg("ARR_DELAY").alias("avg_arr_delay")))


results = []
for n_parts in [8, 50, 200, 400]:
    spark.conf.set("spark.sql.shuffle.partitions", n_parts)
    t0 = time.perf_counter()
    q3_df().count()
    dt = time.perf_counter() - t0
    results.append((n_parts, dt))
    print(f"shuffle.partitions={n_parts:>4}  ->  Q3 in {dt:.3f}s")
    _append_perf({"query": f"Q3_shuffle_{n_parts}", "api": "DataFrame",
                  "seconds": round(dt, 3), "rows": None})

spark.conf.set("spark.sql.shuffle.partitions", 200)
spark.stop()
