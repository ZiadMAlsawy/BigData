"""Q10 - Sort-merge join: forced by disabling broadcast threshold."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from common import (make_spark, load_flights, load_airport_stats,
                    time_it, explain_block)

LABEL = "Q10_sortmerge_join"

spark = make_spark(LABEL)
flights = load_flights(spark)
airport_stats = load_airport_stats(flights)
flights_rdd = flights.rdd
flights_rdd.cache()


def with_smj(fn):
    """Run fn() with broadcast disabled so Catalyst picks SortMergeJoin."""
    prev = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    try:
        return fn()
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", prev)


def q10_df():
    f = flights.alias("f")
    a = airport_stats.alias("a")
    return (f.join(a, F.col("f.ORIGIN") == F.col("a.ORIGIN"))
            .groupBy(F.col("a.ORIGIN_CITY"))
            .agg(F.avg(F.col("f.ARR_DELAY")).alias("avg_delay")))


def q10_sql():
    return spark.sql("""
        SELECT /*+ MERGE(f, s) */ s.ORIGIN_CITY, AVG(f.ARR_DELAY) AS avg_delay
        FROM flights f JOIN airport_stats s ON f.ORIGIN = s.ORIGIN
        GROUP BY s.ORIGIN_CITY
    """)


def q10_rdd():
    left = (flights_rdd.filter(lambda r: r.ARR_DELAY is not None)
            .map(lambda r: (r.ORIGIN, float(r.ARR_DELAY))))
    right = airport_stats.rdd.map(lambda r: (r["ORIGIN"], r["ORIGIN_CITY"]))
    joined = left.join(right)
    return (joined.map(lambda x: (x[1][1], (x[1][0], 1)))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .mapValues(lambda v: v[0] / v[1]))


time_it(LABEL, "RDD", lambda: q10_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", lambda: with_smj(q10_df))
sql_res, _ = time_it(LABEL, "SQL", lambda: with_smj(q10_sql))

print("Expect: SortMergeJoin in physical plan.")
prev = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
explain_block("Q10 DataFrame", q10_df())
explain_block("Q10 SQL", q10_sql())
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", prev)

spark.stop()
