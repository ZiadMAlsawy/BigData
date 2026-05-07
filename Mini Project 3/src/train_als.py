"""Batch ALS trainer for Kindle Reviews.

- Cleans + indexes reviewerID/asin to integer keys
- 80/20 random split
- Fits ALS, evaluates RMSE, runs a small grid only if RMSE > 1.5
- Saves model + factors + index lookup tables for the streaming app
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from itertools import product

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from common import (
    ALS_MODEL_PATH, CSV_PATH, ITEM_FACTORS_PATH, ITEM_INDEX_PATH,
    MODEL_DIR, USER_FACTORS_PATH, USER_INDEX_PATH,
    ensure_dirs, make_spark,
)

RMSE_THRESHOLD = 1.5


def load_clean(spark, csv_path, smoke: bool = False):
    # Read by header (CSV has a pandas index column with empty name we ignore).
    # multiLine=true is required because reviewText contains embedded newlines.
    df = (spark.read
                .option("header", "true")
                .option("escape", '"')
                .option("quote", '"')
                .option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .csv(str(csv_path))
                .select(
                    F.col("reviewerID").cast("string"),
                    F.col("asin").cast("string"),
                    F.col("overall").cast("double").alias("overall"),
                    F.col("unixReviewTime").cast("long").alias("unixReviewTime"))
                .where(F.col("reviewerID").isNotNull()
                       & F.col("asin").isNotNull()
                       & F.col("overall").isNotNull()
                       & F.col("overall").between(1.0, 5.0)))
    if smoke:
        df = df.limit(10_000)

    # Keep latest rating per (user, item)
    w = (F.row_number()
           .over(_latest_window()))
    df = (df.withColumn("__rn", w)
            .where(F.col("__rn") == 1)
            .drop("__rn"))
    return df


def _latest_window():
    from pyspark.sql.window import Window
    return (Window.partitionBy("reviewerID", "asin")
                  .orderBy(F.col("unixReviewTime").desc_nulls_last()))


def index_users_items(df):
    """Add integer user_id, item_id columns; return (df_indexed, user_idx, item_idx)."""
    user_indexer = StringIndexer(inputCol="reviewerID", outputCol="user_id_dbl",
                                 handleInvalid="keep").fit(df)
    item_indexer = StringIndexer(inputCol="asin", outputCol="item_id_dbl",
                                 handleInvalid="keep").fit(df)
    indexed = (item_indexer.transform(user_indexer.transform(df))
               .withColumn("user_id", F.col("user_id_dbl").cast("int"))
               .withColumn("item_id", F.col("item_id_dbl").cast("int"))
               .drop("user_id_dbl", "item_id_dbl"))
    user_idx = (indexed.select("user_id", "reviewerID").distinct())
    item_idx = (indexed.select("item_id", "asin").distinct())
    return indexed, user_idx, item_idx


def fit_eval(train, test, *, rank=10, regParam=0.1, maxIter=10, seed=42):
    als = ALS(userCol="user_id", itemCol="item_id", ratingCol="overall",
              rank=rank, regParam=regParam, maxIter=maxIter,
              coldStartStrategy="drop", nonnegative=True,
              implicitPrefs=False, seed=seed)
    t0 = time.perf_counter()
    model = als.fit(train)
    fit_secs = time.perf_counter() - t0

    preds = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse",
                                    labelCol="overall",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(preds)
    print(f"  rank={rank} reg={regParam} iter={maxIter} -> RMSE={rmse:.4f} fit={fit_secs:.1f}s")
    return model, rmse


def tune(train, test):
    grid = list(product([10, 20], [0.05, 0.1], [10, 15]))
    print(f"[tune] running grid of {len(grid)} configs")
    best = (None, float("inf"), None)
    for rank, reg, it in grid:
        model, rmse = fit_eval(train, test, rank=rank, regParam=reg, maxIter=it)
        if rmse < best[1]:
            best = (model, rmse, dict(rank=rank, regParam=reg, maxIter=it))
    print(f"[tune] best params: {best[2]}, RMSE={best[1]:.4f}")
    return best[0], best[1], best[2]


def save_artifacts(model, user_idx, item_idx):
    if ALS_MODEL_PATH.exists():
        shutil.rmtree(ALS_MODEL_PATH)
    model.write().overwrite().save(str(ALS_MODEL_PATH))

    (model.userFactors.write.mode("overwrite").parquet(str(USER_FACTORS_PATH)))
    (model.itemFactors.write.mode("overwrite").parquet(str(ITEM_FACTORS_PATH)))
    user_idx.write.mode("overwrite").parquet(str(USER_INDEX_PATH))
    item_idx.write.mode("overwrite").parquet(str(ITEM_INDEX_PATH))
    print(f"[save] model -> {ALS_MODEL_PATH}")
    print(f"[save] factors -> {USER_FACTORS_PATH}, {ITEM_FACTORS_PATH}")
    print(f"[save] index   -> {USER_INDEX_PATH}, {ITEM_INDEX_PATH}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true",
                    help="train on a 10K-row slice for a fast smoke test")
    ap.add_argument("--csv", default=str(CSV_PATH))
    args = ap.parse_args()

    ensure_dirs()
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    spark = make_spark("MP3-ALS-Train")

    df = load_clean(spark, args.csv, smoke=args.smoke)
    n = df.count()
    print(f"[data] {n:,} unique (user,item) ratings after cleaning")

    indexed, user_idx, item_idx = index_users_items(df)
    indexed.cache()
    train, test = indexed.randomSplit([0.8, 0.2], seed=42)
    train.cache(); test.cache()
    print(f"[split] train={train.count():,}  test={test.count():,}")

    print("[fit] baseline ALS (rank=10, reg=0.1, iter=10)")
    model, rmse = fit_eval(train, test)

    if rmse > RMSE_THRESHOLD and not args.smoke:
        print(f"[tune] baseline RMSE {rmse:.4f} > {RMSE_THRESHOLD}; running grid")
        model, rmse, params = tune(train, test)

    print(f"[final] RMSE = {rmse:.4f}")
    save_artifacts(model, user_idx, item_idx)

    if args.smoke and rmse > 2.0:
        print(f"[smoke] FAIL: smoke RMSE {rmse:.4f} > 2.0", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
