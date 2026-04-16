# Mini Project 2 - Spark Case Study

**Flight Delay & Cancellation Analysis (2019-2023)** - US flights, 3M rows x 32 columns.

## Folder contents

| File | Purpose |
|------|---------|
| `main.ipynb` | End-to-end notebook: setup, 12 queries (RDD / DataFrame / SQL), execution plans, optimization analysis |
| `download_dataset.py` | Pulls the CSV from Kaggle via `kagglehub` |
| `Big Data Anlaytics - Mini Project 2.pdf` | Assignment brief |
| `L08_DSAI 427- Big Data- Spark - SQL & Dataframes.pptx` | Reference slides |
| `flights_sample_3m.csv` | Dataset (not in git - run the downloader) |
| `flights.parquet/` | Written by the notebook's format-comparison cell (partitioned by YEAR) |
| `performance_results.csv` / `performance_pivot.csv` | Generated timing tables |

## How to run

```bash
# 1. Install deps (inside a venv is recommended)
pip install pyspark kagglehub pandas

# 2. Download the dataset (needs Kaggle credentials)
python download_dataset.py

# 3. Launch Jupyter and run main.ipynb top-to-bottom
jupyter notebook main.ipynb
```

## Query map (12 queries, each in RDD / DataFrame / SQL)

| # | Query | Feature exercised |
|---|-------|-------------------|
| Q1  | Complex-filter winter long-haul delayed flights | multi-predicate filter |
| Q2  | Per-airline aggregates (SUM / AVG / COUNT / MAX / MIN) | aggregation |
| Q3  | Group by (airline, origin, month) | multi-attribute grouping |
| Q4  | Top-20 most-delayed routes | sorting + ranking + HAVING |
| Q5  | 7-day moving avg of arrival delay per airline | window (sliding) |
| Q6  | Cumulative cancellations per airline over time | window (unbounded preceding) |
| Q7  | Rank airlines by on-time rate per month | window RANK() |
| Q8  | Airlines above the overall-average delay | correlated subquery |
| Q9  | Broadcast join with airlines_dim | join optimization |
| Q10 | Sort-merge join with airport_stats | join optimization |
| Q11 | Dominant delay-cause per airline | complex aggregate |
| Q12 | >3-sigma delay anomalies by route | window stddev + filter |

## Extra optimization sections

- **Caching cold vs warm** run of Q2
- **CSV vs Parquet** re-run of Q3
- **Partition pruning** on partitioned Parquet
- **Scalability** - vary `spark.sql.shuffle.partitions` across 8 / 50 / 200 / 400

## Adjusting the cluster

The notebook uses `local[4]`. For a larger cluster:
```python
SparkSession.builder.master('spark://<host>:7077')
    .config('spark.executor.instances', 8)
    .config('spark.executor.cores', 4)
```
