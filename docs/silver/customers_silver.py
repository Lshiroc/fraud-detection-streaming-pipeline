from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, upper, trim

@dp.table(
    name="ddca_catalog.silver.customers_silver",
    comment="Cleaned customer data with data quality rules and business logic",
    table_properties={"quality": "silver", "primary_key": "customer_id"}
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_name", "name IS NOT NULL AND length(name) > 0")
@dp.expect_or_drop("valid_age", "age > 0 AND age < 120")
@dp.expect("valid_income", "income >= 0")
@dp.expect("valid_risk_score", "risk_score BETWEEN 0 AND 1")
def customers_silver():
    return (
        spark.readStream
            .table("customers_bronze")
            .select(
                col("customer_id"),
                trim(col("name")).alias("name"),
                col("age"),
                upper(trim(col("country"))).alias("country"),
                col("account_age"),
                col("income"),
                col("risk_score"),
                col("_bronze_ingestion_time").alias("ingestion_time")
            )
    )
