from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, length

@dp.table(
    name="ddca_catalog.silver.chat_logs_silver",
    comment="Cleaned chat logs with data quality rules and message analysis",
    table_properties={"quality": "silver", "primary_key": "id"}
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dp.expect_or_drop("valid_message", "message IS NOT NULL AND length(message) > 0")
def chat_logs_silver():
    return (
        spark.readStream
            .table("chat_logs_bronze")
            .select(
                col("id"),
                col("customer_id").cast("int").alias("customer_id"),
                col("timestamp"),
                trim(col("message")).alias("message"),
                length(trim(col("message"))).alias("message_length"),
                col("_bronze_ingestion_time").alias("ingestion_time")
            )
    )
