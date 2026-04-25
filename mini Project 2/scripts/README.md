# Spark Case Study - Terminal Edition

Standalone PySpark scripts that mirror `main.ipynb` and satisfy the Mini
Project 2 rubric (RDD + DataFrame + Spark SQL for every query, full
`explain(True)` output, and optimization studies). Designed to run via
`spark-submit` on **WSL2 (Ubuntu)**.

## 1. Folder layout

```
scripts/
├── README.md                    # this file
├── common.py                    # SparkSession factory, schema, helpers
├── download_dataset.py          # one-time Kaggle pull
├── run_all.sh                   # spark-submit each script + aggregate
├── aggregate_results.py         # collate performance_results.csv
├── q01_filter.py ... q12_anomaly.py
├── optimization/
│   ├── caching.py
│   ├── parquet_format.py
│   ├── partition_pruning.py
│   └── scalability.py
├── logs/                        # populated by run_all.sh
└── results/                     # performance_results.csv + pivot
```

Each `qNN_*.py` runs the same query through three APIs and prints
`.explain(True)` for the DataFrame and SQL versions, so the **logical**,
**optimized logical**, and **physical** plans appear in the captured log.

## 2. WSL setup (one time)

```bash
# 1. Install Java 17 + Python 3 + pip (Ubuntu 22.04+ on WSL2)
sudo apt update
sudo apt install -y openjdk-17-jre-headless python3 python3-pip python3-venv

# 2. Create a virtual env in the project folder and activate it
cd "/mnt/c/Users/midoh/Desktop/UST/Spring/DSAI 427 Big Data/Mini project/Mini project 1/BigData/mini Project 2"
python3 -m venv .venv
source .venv/bin/activate

# 3. Install PySpark + helper libs
pip install --upgrade pip
pip install pyspark==3.5.1 pandas kagglehub

# 4. Confirm Java
java -version
which spark-submit   # should resolve to .venv/bin/spark-submit
```

If `spark-submit` is not on `$PATH`, point at the one PySpark ships with:

```bash
export PATH="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__) + "/bin")'):$PATH"
```

### Dataset

The CSV (`flights_sample_3m.csv`, ~600 MB, 3 M rows) lives at the project
root next to `main.ipynb`. From WSL the path is
`/mnt/c/.../mini Project 2/flights_sample_3m.csv`. If it is missing:

```bash
python3 scripts/download_dataset.py     # needs kagglehub auth
```

## 3. Run a single query

```bash
cd "/mnt/c/Users/midoh/Desktop/UST/Spring/DSAI 427 Big Data/Mini project/Mini project 1/BigData/mini Project 2/scripts"
spark-submit --master "local[4]" --driver-memory 4g --py-files common.py q01_filter.py
```

Output sample (truncated):

```
[Q1_filter                ][RDD       ] 12.412s  rows=78321
[Q1_filter                ][DataFrame ] 1.953s   rows=78321
[Q1_filter                ][SQL       ] 1.871s   rows=78321

=== Q1 DataFrame .explain(True) ===
== Parsed Logical Plan ==
...
== Analyzed Logical Plan ==
...
== Optimized Logical Plan ==
...
== Physical Plan ==
*(1) Filter (((isnotnull(...) AND ...) AND ...) AND ...)
+- FileScan csv ...
```

## 4. Run everything

```bash
./run_all.sh
```

The script:

1. Wipes `results/performance_results.csv`.
2. Runs every `qNN_*.py` in order.
3. Runs the four `optimization/*.py` scripts (caching, CSV vs Parquet,
   partition pruning, shuffle-partition scalability).
4. Calls `aggregate_results.py` to print the final RDD vs DataFrame vs
   SQL pivot and writes `results/performance_pivot.csv`.

Every script's stdout/stderr is teed into `logs/<script>.log`. The
`.explain(True)` blocks in those logs are what the report screenshots
should be taken from.

## 5. Environment overrides

| Variable             | Default                           | Purpose                            |
| -------------------- | --------------------------------- | ---------------------------------- |
| `SPARK_MASTER`       | `local[4]`                        | Spark master URL                   |
| `DRIVER_MEMORY`      | `4g`                              | Driver heap                        |
| `SHUFFLE_PARTITIONS` | `200`                             | `spark.sql.shuffle.partitions`     |
| `FLIGHTS_CSV`        | `<project>/flights_sample_3m.csv` | Source CSV                         |
| `FLIGHTS_PARQUET`    | `<project>/flights.parquet`       | Parquet output / read path         |
| `RESULTS_DIR`        | `scripts/results`                 | Where performance CSVs are written |
| `SPARK_LOG_LEVEL`    | `WARN`                            | Spark log4j level                  |

Example - run with two cores and 100 shuffle partitions:

```bash
SPARK_MASTER='local[2]' SHUFFLE_PARTITIONS=100 ./run_all.sh
```

## 6. What each query covers (rubric mapping)

| Script                              | Rubric requirement                          |
| ----------------------------------- | ------------------------------------------- |
| `q01_filter.py`                     | (1) Filtering with complex conditions       |
| `q02_aggregate.py`                  | (2) Aggregations: SUM/AVG/COUNT/MAX/MIN     |
| `q03_multi_group.py`                | (3) Grouping by multiple attributes         |
| `q04_top20_routes.py`               | (4) Sorting and ranking                     |
| `q05_moving_avg.py`                 | (5) Window: 7-day moving average            |
| `q06_cumulative.py`                 | (5) Window: cumulative sum                  |
| `q07_rank.py`                       | (5) Window: rank()                          |
| `q08_subquery.py`                   | (6) Nested / subquery                       |
| `q09_broadcast_join.py`             | (7) Broadcast join (with `BROADCAST` hint)  |
| `q10_sortmerge_join.py`             | (7) Sort-merge join (broadcast disabled)    |
| `q11_root_cause.py`                 | Complex aggregation (delay attribution)     |
| `q12_anomaly.py`                    | Anomaly detection (window mean +/- 3 sigma) |
| `optimization/caching.py`           | Caching impact (cold vs warm)               |
| `optimization/parquet_format.py`    | CSV vs Parquet                              |
| `optimization/partition_pruning.py` | Partition pruning on YEAR=2022              |
| `optimization/scalability.py`       | Shuffle-partition sweep                     |

## 7. Reading the explain plans

`.explain(True)` prints four sections. For the report, screenshot:

- **Parsed / Analyzed Logical Plan** -> "initial logical plan"
- **Optimized Logical Plan** -> "Catalyst-optimized plan"
- **Physical Plan** -> "execution strategy"

Compare across APIs by diffing the Optimized Logical Plan: the DataFrame
and SQL versions of the same query collapse to the same plan, while the
RDD has no plan.

## 8. Tips

- First run downloads/parses the CSV; later runs are faster once the OS
  cache warms up. The `optimization/parquet_format.py` step writes
  `flights.parquet` once and is reused by `partition_pruning.py`.
- If WSL runs out of memory, lower `DRIVER_MEMORY` or pass
  `SPARK_MASTER='local[2]'`.
- The notebook is **not** required to run; it is preserved as the
  original reference.
