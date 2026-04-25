"""Q6 - Window function: cumulative cancellations per airline over time."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q6_cumulative"

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q6_rdd():
    daily = (flights_rdd
             .map(lambda r: ((r.AIRLINE_CODE, r.FL_DATE), float(r.CANCELLED or 0)))
             .reduceByKey(lambda a, b: a + b)
             .map(lambda x: (x[0][0], (x[0][1], x[1]))))
    grouped = daily.groupByKey().mapValues(lambda it: sorted(it))

    def cumul(pairs):
        total = 0.0
        out = []
        for d, c in pairs:
            total += c
            out.append((d, total))
        return out

    return grouped.flatMapValues(cumul)


def q6_df():
    daily = (flights.groupBy("AIRLINE_CODE", "FL_DATE")
             .agg(F.sum("CANCELLED").alias("daily_cancel")))
    w = (Window.partitionBy("AIRLINE_CODE").orderBy("FL_DATE")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    return daily.withColumn("cum_cancel", F.sum("daily_cancel").over(w))


def q6_sql():
    return spark.sql("""
        WITH daily AS (
          SELECT AIRLINE_CODE, FL_DATE, SUM(CANCELLED) AS daily_cancel
          FROM flights GROUP BY AIRLINE_CODE, FL_DATE
        )
        SELECT AIRLINE_CODE, FL_DATE, daily_cancel,
               SUM(daily_cancel) OVER (PARTITION BY AIRLINE_CODE
                                       ORDER BY FL_DATE
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_cancel
        FROM daily
    """)


time_it(LABEL, "RDD", lambda: q6_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q6_df)
sql_res, _ = time_it(LABEL, "SQL", q6_sql)

explain_block("Q6 DataFrame", df_res)
explain_block("Q6 SQL", sql_res)

spark.stop()
