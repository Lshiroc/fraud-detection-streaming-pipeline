from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, coalesce, datediff, current_timestamp, lit

@dp.materialized_view(
    name="ddca_catalog.gold.ml_fraud_features",
    comment="Comprehensive customer features for fraud detection ML model"
)
def ml_fraud_features():
    """
    Combines customer demographics, transaction patterns, and chat behavior
    for fraud detection ML training and inference.
    
    Features include:
    - Customer demographics (age, income, risk_score, country)
    - Transaction patterns (volume, fraud rate, online vs offline)
    - Chat engagement (message frequency, recency)
    - Derived features (engagement ratios, risk indicators)
    """
    
    # Start with customer base data
    customers = spark.read.table("ddca_catalog.silver.customers_silver").alias("cust")
    
    # Join transaction fraud metrics
    fraud_metrics = spark.read.table("ddca_catalog.gold.customer_fraud_summary").alias("fraud")
    
    # Join chat engagement metrics
    chat_metrics = spark.read.table("ddca_catalog.gold.customer_chat_summary").alias("chat")
    
    return (
        customers
            .join(fraud_metrics, col("cust.customer_id") == col("fraud.customer_id"), "left")
            .join(chat_metrics, col("cust.customer_id") == col("chat.customer_id"), "left")
            .select(
                # Primary key
                col("cust.customer_id"),
                
                # Customer demographics
                col("cust.age"),
                col("cust.income"),
                col("cust.country"),
                col("cust.risk_score"),
                col("cust.account_age"),
                
                # Derived demographic features
                when(col("cust.age") < 25, "young")
                    .when(col("cust.age") < 45, "middle_aged")
                    .when(col("cust.age") < 65, "senior")
                    .otherwise("elderly").alias("age_group"),
                when(col("cust.risk_score") > 0.7, "high")
                    .when(col("cust.risk_score") > 0.4, "medium")
                    .otherwise("low").alias("risk_category"),
                
                # Transaction features (handle customers with no transactions)
                coalesce(col("fraud.total_transactions"), lit(0)).alias("total_transactions"),
                coalesce(col("fraud.fraud_count"), lit(0)).alias("fraud_count"),
                coalesce(col("fraud.fraud_percentage"), lit(0.0)).alias("fraud_percentage"),
                coalesce(col("fraud.avg_distance_from_home"), lit(0.0)).alias("avg_distance_from_home"),
                coalesce(col("fraud.avg_purchase_ratio"), lit(0.0)).alias("avg_purchase_ratio"),
                coalesce(col("fraud.online_order_count"), lit(0)).alias("online_order_count"),
                
                # Chat engagement features (handle customers with no chat history)
                coalesce(col("chat.total_messages"), lit(0)).alias("total_messages"),
                col("chat.first_message_time"),
                col("chat.last_message_time"),
                coalesce(col("chat.avg_message_length"), lit(0.0)).alias("avg_message_length"),
                
                # Derived features for ML
                # Days since last chat (null if no chat history)
                when(col("chat.last_message_time").isNotNull(),
                     datediff(current_timestamp(), col("chat.last_message_time"))
                ).alias("days_since_last_chat"),
                
                # Online transaction ratio
                when(col("fraud.total_transactions") > 0,
                     col("fraud.online_order_count") / col("fraud.total_transactions")
                ).otherwise(lit(0.0)).alias("online_transaction_ratio"),
                
                # Engagement flag (has both transactions and chat)
                when((col("fraud.total_transactions") > 0) & (col("chat.total_messages") > 0), lit(1))
                    .otherwise(lit(0)).alias("is_engaged_customer"),
                
                # High risk indicators
                when(col("cust.risk_score") > 0.7, lit(1)).otherwise(lit(0)).alias("is_high_risk"),
                when(col("fraud.fraud_percentage") > 5, lit(1)).otherwise(lit(0)).alias("has_fraud_history"),
                
                # Transaction per account year (activity level)
                when(col("cust.account_age") > 0,
                     col("fraud.total_transactions") / col("cust.account_age")
                ).otherwise(lit(0.0)).alias("transactions_per_year"),
                
                # Feature engineering timestamp
                current_timestamp().alias("feature_timestamp")
            )
    )
