from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, current_timestamp
from pyspark import pipelines as dp

@dp.table(
    comment="Raw transaction data ingested from CSV files in bronze layer"
)
def transactions_stream():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            # .schema(schema)
            .option(
                "cloudFiles.schemaHints",
                "id STRING, \
                distance_from_home FLOAT, \
                distance_from_last_transaction FLOAT, \
                ratio_to_median_purchase_price FLOAT, \
                repeat_retailer FLOAT, \
                used_chip FLOAT, \
                used_pin_number FLOAT, \
                online_order FLOAT, \
                fraud FLOAT, \
                customer_id INT, \
                timestamp TIMESTAMP"
            )
            .load("/Volumes/ddca_catalog/bronze/raw_files/transactions/")
            .withColumn("_bronze_ingestion_time", current_timestamp())
            .withColumn("_bronze_source_file", col("_metadata.file_path"))
    )
