"""Q2 - Aggregations: SUM, AVG, COUNT, MAX, MIN per airline."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q2_aggregate"

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q2_rdd():
    kv = flights_rdd.map(lambda r: (r.AIRLINE_CODE, (
        1,
        float(r.CANCELLED or 0),
        float(r.ARR_DELAY) if r.ARR_DELAY is not None else 0.0,
        1 if r.ARR_DELAY is not None else 0,
        float(r.ARR_DELAY) if r.ARR_DELAY is not None else float("-inf"),
        float(r.ARR_DELAY) if r.ARR_DELAY is not None else float("inf"),
    )))
    reduced = kv.reduceByKey(lambda a, b: (
        a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3],
        max(a[4], b[4]), min(a[5], b[5])))
    return reduced.map(lambda x: (x[0], x[1][0], x[1][1],
                                  x[1][2] / x[1][3] if x[1][3] else None,
                                  x[1][4], x[1][5]))


def q2_df():
    return (flights.groupBy("AIRLINE_CODE")
            .agg(F.count("*").alias("total_flights"),
                 F.sum("CANCELLED").alias("cancellations"),
                 F.avg("ARR_DELAY").alias("avg_arr_delay"),
                 F.max("ARR_DELAY").alias("max_arr_delay"),
                 F.min("ARR_DELAY").alias("min_arr_delay")))


def q2_sql():
    return spark.sql("""
        SELECT AIRLINE_CODE,
               COUNT(*)        AS total_flights,
               SUM(CANCELLED)  AS cancellations,
               AVG(ARR_DELAY)  AS avg_arr_delay,
               MAX(ARR_DELAY)  AS max_arr_delay,
               MIN(ARR_DELAY)  AS min_arr_delay
        FROM flights
        GROUP BY AIRLINE_CODE
    """)


time_it(LABEL, "RDD", lambda: q2_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q2_df)
sql_res, _ = time_it(LABEL, "SQL", q2_sql)

df_res.orderBy(F.desc("total_flights")).show(truncate=False)
explain_block("Q2 DataFrame", df_res)
explain_block("Q2 SQL", sql_res)

spark.stop()
