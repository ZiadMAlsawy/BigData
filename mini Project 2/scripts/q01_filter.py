"""Q1 - Filtering with complex conditions: winter long-haul flights >= 60 min late."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q1_filter"

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q1_rdd():
    return flights_rdd.filter(
        lambda r: r.MONTH in (12, 1, 2)
        and r.ARR_DELAY is not None and r.ARR_DELAY >= 60
        and r.CANCELLED == 0.0
        and r.DISTANCE is not None and r.DISTANCE > 500
    )


def q1_df():
    return (flights
            .filter(F.col("MONTH").isin(12, 1, 2))
            .filter(F.col("ARR_DELAY") >= 60)
            .filter(F.col("CANCELLED") == 0.0)
            .filter(F.col("DISTANCE") > 500))


def q1_sql():
    return spark.sql("""
        SELECT * FROM flights
        WHERE MONTH IN (12, 1, 2)
          AND ARR_DELAY >= 60
          AND CANCELLED = 0.0
          AND DISTANCE > 500
    """)


time_it(LABEL, "RDD", lambda: q1_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q1_df)
sql_res, _ = time_it(LABEL, "SQL", q1_sql)

explain_block("Q1 DataFrame", df_res)
explain_block("Q1 SQL", sql_res)

spark.stop()
