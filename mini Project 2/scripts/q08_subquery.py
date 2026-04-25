"""Q8 - Nested/subquery: airlines whose avg arr delay exceeds the global mean."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F
from common import make_spark, load_flights, time_it, explain_block

LABEL = "Q8_subquery"

spark = make_spark(LABEL)
flights = load_flights(spark)
flights_rdd = flights.rdd
flights_rdd.cache()


def q8_rdd():
    non_null = (flights_rdd
                .filter(lambda r: r.ARR_DELAY is not None)
                .map(lambda r: (r.AIRLINE_CODE, float(r.ARR_DELAY))))
    total = (non_null.map(lambda x: (x[1], 1))
             .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1])))
    overall = total[0] / total[1]
    per_airline = (non_null.map(lambda x: (x[0], (x[1], 1)))
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                   .mapValues(lambda v: v[0] / v[1]))
    return per_airline.filter(lambda x: x[1] > overall)


def q8_df():
    overall = flights.agg(F.avg("ARR_DELAY").alias("o")).first()["o"]
    return (flights.groupBy("AIRLINE_CODE")
            .agg(F.avg("ARR_DELAY").alias("avg_delay"))
            .filter(F.col("avg_delay") > F.lit(overall)))


def q8_sql():
    return spark.sql("""
        SELECT AIRLINE_CODE, AVG(ARR_DELAY) AS avg_delay
        FROM flights
        GROUP BY AIRLINE_CODE
        HAVING AVG(ARR_DELAY) > (SELECT AVG(ARR_DELAY) FROM flights)
    """)


time_it(LABEL, "RDD", lambda: q8_rdd().count())
df_res, _ = time_it(LABEL, "DataFrame", q8_df)
sql_res, _ = time_it(LABEL, "SQL", q8_sql)

sql_res.orderBy(F.desc("avg_delay")).show()
explain_block("Q8 DataFrame", df_res)
explain_block("Q8 SQL (with correlated subquery)", sql_res)

spark.stop()
