"""Q3 - Grouping by multiple attributes (airline, origin, month)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q3_multi_group"

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q3_rdd():
    kv = (flights_rdd
          .filter(lambda r: r.ARR_DELAY is not None)
          .map(lambda r: ((r.AIRLINE_CODE, r.ORIGIN, r.MONTH),
                          (float(r.ARR_DELAY), 1))))
    return (kv.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
              .mapValues(lambda v: v[0] / v[1]))


def q3_df():
    return (flights.groupBy("AIRLINE_CODE", "ORIGIN", "MONTH")
            .agg(F.avg("ARR_DELAY").alias("avg_arr_delay"),
                 F.count("*").alias("n")))


def q3_sql():
    return spark.sql("""
        SELECT AIRLINE_CODE, ORIGIN, MONTH,
               AVG(ARR_DELAY) AS avg_arr_delay,
               COUNT(*)       AS n
        FROM flights
        WHERE ARR_DELAY IS NOT NULL
        GROUP BY AIRLINE_CODE, ORIGIN, MONTH
    """)


time_it(LABEL, "RDD", lambda: q3_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q3_df)
sql_res, _ = time_it(LABEL, "SQL", q3_sql)

explain_block("Q3 DataFrame", df_res)
explain_block("Q3 SQL", sql_res)

spark.stop()
