from pyspark import pipelines as dp
from pyspark.sql.functions import col, when

@dp.table(
    name="ddca_catalog.silver.transactions_silver",
    comment="Cleaned and deduplicated transaction data with data quality rules",
    table_properties={"quality": "silver", "primary_key": "id"}
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dp.expect("valid_amounts", "ratio_to_median_purchase_price >= 0")
def transactions_silver():
    return (
        spark.readStream
            .table("transactions_stream")
            .select(
                col("id"),
                col("customer_id"),
                col("distance_from_home"),
                col("distance_from_last_transaction"),
                col("ratio_to_median_purchase_price"),
                col("repeat_retailer").cast("boolean").alias("is_repeat_retailer"),
                col("used_chip").cast("boolean").alias("used_chip"),
                col("used_pin_number").cast("boolean").alias("used_pin"),
                col("online_order").cast("boolean").alias("is_online_order"),
                col("fraud").cast("boolean").alias("is_fraud"),
                col("timestamp"),
                col("_bronze_ingestion_time").alias("ingestion_time")
            )
    )
