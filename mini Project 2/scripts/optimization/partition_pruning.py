"""Partition pruning: filter on YEAR=2022 against the YEAR-partitioned Parquet."""
import sys, os, time
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

from common import make_spark, PARQUET_PATH, _append_perf

spark = make_spark("Optimization_partition_pruning")

if not Path(PARQUET_PATH).exists():
    print(f"ERROR: Parquet missing at {PARQUET_PATH}.")
    print("Run optimization/parquet_format.py first.")
    spark.stop()
    sys.exit(1)

flights_parq = spark.read.parquet(PARQUET_PATH)

pruned = flights_parq.filter("YEAR = 2022")
print("=== Partition pruning plan (expect PartitionFilters: [YEAR=2022]) ===")
pruned.explain(True)

t0 = time.perf_counter(); n = pruned.count(); pruned_t = time.perf_counter() - t0
t0 = time.perf_counter(); n_all = flights_parq.count(); all_t = time.perf_counter() - t0

print(f"Pruned (YEAR=2022): {pruned_t:.3f}s,  rows={n}")
print(f"Full scan:          {all_t:.3f}s,    rows={n_all}")

_append_perf({"query": "Partition_pruned", "api": "DataFrame",
              "seconds": round(pruned_t, 3), "rows": n})
_append_perf({"query": "Partition_fullscan", "api": "DataFrame",
              "seconds": round(all_t, 3), "rows": n_all})

spark.stop()
