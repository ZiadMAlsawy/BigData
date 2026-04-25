"""Q12 - Anomaly detection: flights >3 sigma above route mean delay."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q12_anomaly"

spark = make_spark(LABEL)
sc = spark.sparkContext
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q12_df():
    w = Window.partitionBy("ORIGIN", "DEST")
    return (flights.filter(F.col("ARR_DELAY").isNotNull())
            .withColumn("route_mean", F.avg("ARR_DELAY").over(w))
            .withColumn("route_sd", F.stddev("ARR_DELAY").over(w))
            .filter(F.col("ARR_DELAY") >
                    F.col("route_mean") + 3 * F.col("route_sd")))


def q12_sql():
    return spark.sql("""
        SELECT * FROM (
            SELECT FL_DATE, AIRLINE_CODE, ORIGIN, DEST, ARR_DELAY,
                   AVG(ARR_DELAY)    OVER (PARTITION BY ORIGIN, DEST) AS route_mean,
                   STDDEV(ARR_DELAY) OVER (PARTITION BY ORIGIN, DEST) AS route_sd
            FROM flights WHERE ARR_DELAY IS NOT NULL
        ) t WHERE ARR_DELAY > route_mean + 3 * route_sd
    """)


def q12_rdd():
    pairs = (flights_rdd.filter(lambda r: r.ARR_DELAY is not None)
             .map(lambda r: ((r.ORIGIN, r.DEST), float(r.ARR_DELAY))))
    stats = (pairs.map(lambda x: (x[0], (x[1], x[1] * x[1], 1)))
             .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
             .mapValues(lambda v: (v[0] / v[2],
                                   max(0, v[1] / v[2] - (v[0] / v[2]) ** 2) ** 0.5)))
    stats_map = dict(stats.collect())
    bcast = sc.broadcast(stats_map)
    return pairs.filter(
        lambda x: (x[1] - bcast.value[x[0]][0]) > 3 * bcast.value[x[0]][1])


time_it(LABEL, "RDD", lambda: q12_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q12_df)
sql_res, _ = time_it(LABEL, "SQL", q12_sql)

explain_block("Q12 DataFrame", df_res)
explain_block("Q12 SQL", sql_res)

spark.stop()
