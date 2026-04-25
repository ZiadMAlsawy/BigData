"""Q4 - Sorting and ranking: top 20 most-delayed origin->dest routes."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q4_top20_routes"

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q4_rdd():
    kv = (flights_rdd
          .filter(lambda r: r.ARR_DELAY is not None)
          .map(lambda r: ((r.ORIGIN, r.DEST), (float(r.ARR_DELAY), 1))))
    agg = (kv.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
           .mapValues(lambda v: v[0] / v[1]))
    return agg.takeOrdered(20, key=lambda x: -x[1])


def q4_df():
    return (flights.groupBy("ORIGIN", "DEST")
            .agg(F.avg("ARR_DELAY").alias("avg_delay"),
                 F.count("*").alias("flights"))
            .filter("flights > 100")
            .orderBy(F.desc("avg_delay"))
            .limit(20))


def q4_sql():
    return spark.sql("""
        SELECT ORIGIN, DEST,
               AVG(ARR_DELAY) AS avg_delay,
               COUNT(*)       AS flights
        FROM flights
        GROUP BY ORIGIN, DEST
        HAVING COUNT(*) > 100
        ORDER BY avg_delay DESC
        LIMIT 20
    """)


time_it(LABEL, "RDD", lambda: q4_rdd())
df_res, _ = time_it(LABEL, "DataFrame", q4_df)
sql_res, _ = time_it(LABEL, "SQL", q4_sql)

df_res.show(20, truncate=False)
explain_block("Q4 DataFrame", df_res)
explain_block("Q4 SQL", sql_res)

spark.stop()
