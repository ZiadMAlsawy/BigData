"""Q7 - Window function: rank airlines by on-time rate per (year, month)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q7_rank"

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q7_rdd():
    kv = (flights_rdd
          .filter(lambda r: r.ARR_DELAY is not None)
          .map(lambda r: ((r.YEAR, r.MONTH, r.AIRLINE_CODE),
                          (1, 1 if r.ARR_DELAY <= 15 else 0))))
    agg = (kv.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
           .map(lambda x: (x[0][0], x[0][1], x[0][2], x[1][1] / x[1][0])))
    grouped = agg.map(lambda x: ((x[0], x[1]), (x[2], x[3]))).groupByKey()

    def rank(rows):
        s = sorted(rows, key=lambda r: -r[1])
        return [(airline, rate, i + 1) for i, (airline, rate) in enumerate(s)]

    return grouped.flatMapValues(rank)


def q7_df():
    monthly = (flights.filter(F.col("ARR_DELAY").isNotNull())
               .groupBy("YEAR", "MONTH", "AIRLINE_CODE")
               .agg((F.sum((F.col("ARR_DELAY") <= 15).cast("int")) /
                     F.count("*")).alias("on_time_rate")))
    w = Window.partitionBy("YEAR", "MONTH").orderBy(F.desc("on_time_rate"))
    return monthly.withColumn("rnk", F.rank().over(w))


def q7_sql():
    return spark.sql("""
        WITH monthly AS (
          SELECT YEAR, MONTH, AIRLINE_CODE,
                 SUM(CASE WHEN ARR_DELAY <= 15 THEN 1 ELSE 0 END) / COUNT(*) AS on_time_rate
          FROM flights WHERE ARR_DELAY IS NOT NULL
          GROUP BY YEAR, MONTH, AIRLINE_CODE
        )
        SELECT YEAR, MONTH, AIRLINE_CODE, on_time_rate,
               RANK() OVER (PARTITION BY YEAR, MONTH ORDER BY on_time_rate DESC) AS rnk
        FROM monthly
    """)


time_it(LABEL, "RDD", lambda: q7_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q7_df)
sql_res, _ = time_it(LABEL, "SQL", q7_sql)

df_res.orderBy("YEAR", "MONTH", "rnk").show(15)
explain_block("Q7 DataFrame", df_res)
explain_block("Q7 SQL", sql_res)

spark.stop()
