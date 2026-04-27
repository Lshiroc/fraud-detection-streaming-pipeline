from pyspark.sql.functions import col, current_timestamp
from pyspark import pipelines as dp

@dp.table(
    comment="Raw chat logs ingested from CSV files in bronze layer"
)
def chat_logs_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", "customer_id INT, timestamp TIMESTAMP")
            .load("/Volumes/ddca_catalog/bronze/raw_files/chat_logs/")
            .withColumn("_bronze_ingestion_time", current_timestamp())
            .withColumn("_bronze_source_file", col("_metadata.file_path"))
    )
