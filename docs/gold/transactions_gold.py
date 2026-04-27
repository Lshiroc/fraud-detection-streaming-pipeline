from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, count, avg, sum as spark_sum, round as spark_round

@dp.materialized_view(
    name="ddca_catalog.gold.customer_fraud_summary",
    comment="Aggregated fraud analytics by customer"
)
def customer_fraud_summary():
    return (
        spark.read
            .table("ddca_catalog.silver.transactions_silver")
            .groupBy("customer_id")
            .agg(
                count("*").alias("total_transactions"),
                spark_sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
                spark_round(
                    (spark_sum(when(col("is_fraud"), 1).otherwise(0)) / count("*") * 100), 2
                ).alias("fraud_percentage"),
                avg("distance_from_home").alias("avg_distance_from_home"),
                avg("ratio_to_median_purchase_price").alias("avg_purchase_ratio"),
                spark_sum(when(col("is_online_order"), 1).otherwise(0)).alias("online_order_count")
            )
    )
