"""Q11 - Complex aggregation: top delay root-cause per airline."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q11_root_cause"
CAUSES = ["DELAY_DUE_CARRIER", "DELAY_DUE_WEATHER", "DELAY_DUE_NAS",
          "DELAY_DUE_SECURITY", "DELAY_DUE_LATE_AIRCRAFT"]

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q11_rdd():
    def row_to_kv(r):
        vals = tuple(float(getattr(r, c) or 0.0) for c in CAUSES)
        return (r.AIRLINE_CODE, vals)
    agg = (flights_rdd.map(row_to_kv)
           .reduceByKey(lambda a, b: tuple(x + y for x, y in zip(a, b))))
    return agg.mapValues(lambda v: CAUSES[v.index(max(v))])


def q11_df():
    sums = flights.groupBy("AIRLINE_CODE").agg(*[F.sum(c).alias(c) for c in CAUSES])
    expr = F.array(*[F.struct(F.col(c).alias("v"), F.lit(c).alias("k")) for c in CAUSES])
    return sums.withColumn("top_cause", F.array_max(expr).getField("k"))


def q11_sql():
    return spark.sql("""
        WITH sums AS (
          SELECT AIRLINE_CODE,
                 SUM(DELAY_DUE_CARRIER)       AS c_carrier,
                 SUM(DELAY_DUE_WEATHER)       AS c_weather,
                 SUM(DELAY_DUE_NAS)           AS c_nas,
                 SUM(DELAY_DUE_SECURITY)      AS c_security,
                 SUM(DELAY_DUE_LATE_AIRCRAFT) AS c_late
          FROM flights GROUP BY AIRLINE_CODE
        )
        SELECT AIRLINE_CODE,
               CASE greatest(c_carrier, c_weather, c_nas, c_security, c_late)
                 WHEN c_carrier  THEN 'CARRIER'
                 WHEN c_weather  THEN 'WEATHER'
                 WHEN c_nas      THEN 'NAS'
                 WHEN c_security THEN 'SECURITY'
                 ELSE 'LATE_AIRCRAFT'
               END AS top_cause
        FROM sums
    """)


time_it(LABEL, "RDD", lambda: q11_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q11_df)
sql_res, _ = time_it(LABEL, "SQL", q11_sql)

df_res.show(truncate=False)
explain_block("Q11 DataFrame", df_res)
explain_block("Q11 SQL", sql_res)

spark.stop()
