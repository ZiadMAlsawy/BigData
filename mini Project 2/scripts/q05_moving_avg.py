"""Q5 - Window function: 7-day moving average of arrival delay per airline."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q5_moving_avg"

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q5_rdd():
    daily = (flights_rdd
             .filter(lambda r: r.ARR_DELAY is not None)
             .map(lambda r: ((r.AIRLINE_CODE, r.FL_DATE), (float(r.ARR_DELAY), 1)))
             .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
             .map(lambda x: (x[0][0], (x[0][1], x[1][0] / x[1][1]))))
    grouped = daily.groupByKey().mapValues(lambda it: sorted(it))

    def rolling(pairs):
        out = []
        for i in range(len(pairs)):
            window = pairs[max(0, i - 6):i + 1]
            out.append((pairs[i][0], sum(p[1] for p in window) / len(window)))
        return out

    return grouped.flatMapValues(rolling)


def q5_df():
    daily = (flights.filter(F.col("ARR_DELAY").isNotNull())
             .groupBy("AIRLINE_CODE", "FL_DATE")
             .agg(F.avg("ARR_DELAY").alias("daily_avg")))
    w = Window.partitionBy("AIRLINE_CODE").orderBy("FL_DATE").rowsBetween(-6, 0)
    return daily.withColumn("ma7", F.avg("daily_avg").over(w))


def q5_sql():
    return spark.sql("""
        WITH daily AS (
          SELECT AIRLINE_CODE, FL_DATE, AVG(ARR_DELAY) AS daily_avg
          FROM flights WHERE ARR_DELAY IS NOT NULL
          GROUP BY AIRLINE_CODE, FL_DATE
        )
        SELECT AIRLINE_CODE, FL_DATE, daily_avg,
               AVG(daily_avg) OVER (PARTITION BY AIRLINE_CODE
                                    ORDER BY FL_DATE
                                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7
        FROM daily
    """)


time_it(LABEL, "RDD", lambda: q5_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q5_df)
sql_res, _ = time_it(LABEL, "SQL", q5_sql)

df_res.show(10)
explain_block("Q5 DataFrame", df_res)
explain_block("Q5 SQL", sql_res)

spark.stop()
