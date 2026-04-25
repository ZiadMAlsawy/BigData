"""Q9 - Broadcast join: flights joined with the small airlines_dim table."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from common import (make_spark, load_flights, load_airlines_dim,
                    time_it, explain_block)

LABEL = "Q9_broadcast_join"

spark = make_spark(LABEL)
sc = spark.sparkContext
flights = load_flights(spark)
airlines_dim = load_airlines_dim(flights)
flights_rdd = flights.rdd
flights_rdd.cache()


def q9_rdd():
    airlines_map = dict((r["AIRLINE_CODE"], r["AIRLINE"])
                        for r in airlines_dim.collect())
    bcast = sc.broadcast(airlines_map)
    return (flights_rdd
            .filter(lambda r: r.ARR_DELAY is not None)
            .map(lambda r: (bcast.value.get(r.AIRLINE_CODE, "UNKNOWN"),
                            (float(r.ARR_DELAY), 1)))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .mapValues(lambda v: v[0] / v[1]))


def q9_df():
    f = flights.alias("f")
    a = airlines_dim.alias("a")
    return (f.join(F.broadcast(a), "AIRLINE_CODE")
            .groupBy(F.col("a.AIRLINE"))
            .agg(F.avg("f.ARR_DELAY").alias("avg_delay")))


def q9_sql():
    return spark.sql("""
        SELECT /*+ BROADCAST(a) */ a.AIRLINE, AVG(f.ARR_DELAY) AS avg_delay
        FROM flights f JOIN airlines_dim a ON f.AIRLINE_CODE = a.AIRLINE_CODE
        GROUP BY a.AIRLINE
    """)


time_it(LABEL, "RDD", lambda: q9_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q9_df)
sql_res, _ = time_it(LABEL, "SQL", q9_sql)

print("Expect: BroadcastHashJoin in physical plan.")
explain_block("Q9 DataFrame", df_res)
explain_block("Q9 SQL", sql_res)

spark.stop()
