from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, min as spark_min, max as spark_max, avg, date_trunc, countDistinct

@dp.materialized_view(
    name="ddca_catalog.gold.customer_chat_summary",
    comment="Customer chat activity summary and engagement metrics"
)
def customer_chat_summary():
    return (
        spark.read
            .table("ddca_catalog.silver.chat_logs_silver")
            .groupBy("customer_id")
            .agg(
                count("*").alias("total_messages"),
                spark_min("timestamp").alias("first_message_time"),
                spark_max("timestamp").alias("last_message_time"),
                avg("message_length").alias("avg_message_length")
            )
    )

@dp.materialized_view(
    name="ddca_catalog.gold.daily_chat_activity",
    comment="Daily chat volume and activity patterns"
)
def daily_chat_activity():
    return (
        spark.read
            .table("ddca_catalog.silver.chat_logs_silver")
            .withColumn("date", date_trunc("day", col("timestamp")))
            .groupBy("date")
            .agg(
                count("*").alias("total_messages"),
                countDistinct("customer_id").alias("unique_customers"),
                avg("message_length").alias("avg_message_length")
            )
            .orderBy("date")
    )

@dp.materialized_view(
    name="ddca_catalog.gold.customer_engagement",
    comment="Customer engagement with transactions analysis"
)
def customer_engagement():
    return (
        spark.read.table("ddca_catalog.gold.customer_chat_summary").alias("chat")
            .join(
                spark.read.table("ddca_catalog.gold.customer_fraud_summary").alias("txn"),
                col("chat.customer_id") == col("txn.customer_id"),
                "inner"
            )
            .select(
                col("chat.customer_id"),
                col("chat.total_messages"),
                col("chat.first_message_time"),
                col("chat.last_message_time"),
                col("txn.total_transactions"),
                col("txn.fraud_count"),
                col("txn.fraud_percentage")
            )
    )
