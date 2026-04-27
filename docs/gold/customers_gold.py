from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, count, avg, sum as spark_sum, min as spark_min, max as spark_max, round as spark_round

@dp.materialized_view(
    name="ddca_catalog.gold.customer_country_summary",
    comment="Customer demographics and risk analysis by country"
)
def customer_country_summary():
    return (
        spark.read
            .table("ddca_catalog.silver.customers_silver")
            .groupBy("country")
            .agg(
                count("*").alias("customer_count"),
                avg("age").alias("avg_age"),
                avg("income").alias("avg_income"),
                avg("risk_score").alias("avg_risk_score"),
                avg("account_age").alias("avg_account_age")
            )
    )
