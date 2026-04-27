# Documentation

This folder contains all the main parts of the project:
- pipeline code (bronze, silver, gold)
- ML notebooks
- explanation of how everything works together

---

## Project Flow Overview

The pipeline works in steps:

1. Data is generated using:
   - [`prep.ipynb`](prep.ipynb)

2. Raw files are saved in:
   - `data/` folder (CSV files)

3. Databricks pipeline reads this data and processes it in layers:
   - Bronze - raw data  
   - Silver - cleaned data  
   - Gold - aggregated data + ML features  

4. ML notebooks use gold/silver tables to train models and make predictions  

---

## Data Preparation

- [`prep.ipynb`](prep.ipynb)

This is an important part of the project.

It creates:
- customers  
- transactions  
- chat logs  

Key things:
- Customers have countries and different fraud risks  
- Transactions are linked to customers (not random)  
- Fraud depends on country and behavior  
- Data is split into batches (initial + next batches)  
- Chat logs include fraud complaints and normal messages  

This makes the dataset more realistic and useful for testing the pipeline.

---

## Pipeline Code (Medallion Architecture)

Pipeline code is inside this folder:

- [`bronze/`](bronze/)  
- [`silver/`](silver/)  
- [`gold/`](gold/)  

---

### Bronze Layer

This layer ingests raw data using streaming (Auto Loader).

Files:
- `chat_logs_bronze.py`
- `customers_bronze.py`
- `transactions_bronze.py`

What it does:
- Reads CSV files from `data/` folder  
- Infers schema  
- Adds metadata:
  - ingestion time  
  - source file  

Tables created:
- `chat_logs_bronze`  
- `customers_bronze`  
- `transactions_stream`  

---

### Silver Layer

This layer cleans and validates the data.

Files:
- `chat_logs_silver.py`
- `customers_silver.py`
- `transactions_silver.py`

What it does:
- Selects only needed columns  
- Cleans text (trim, formatting)  
- Converts types (boolean, int, etc.)  
- Applies data quality rules (`expect`)  
- Drops invalid rows  

Tables created:
- `ddca_catalog.silver.chat_logs_silver`  
- `ddca_catalog.silver.customers_silver`  
- `ddca_catalog.silver.transactions_silver`  

---

### Gold Layer

This layer creates final tables for analysis and ML.

Files:
- `chat_logs_gold.py`
- `customers_gold.py`
- `transactions_gold.py`
- `ml_features.py`

---

###  Gold Tables

#### 1. Customer Fraud Summary
- Table: `ddca_catalog.gold.customer_fraud_summary`  
- Shows:
  - total transactions  
  - fraud count  
  - fraud percentage  

---

#### 2. Chat Activity Summary
- Table: `ddca_catalog.gold.customer_chat_summary`  
- Shows:
  - number of messages  
  - first and last message  
  - average message length  

---

#### 3. Daily Chat Activity
- Table: `ddca_catalog.gold.daily_chat_activity`  
- Shows chat activity per day  

---

#### 4. Customer Engagement
- Combines chat + transaction data  
- Helps understand behavior  

---

#### 5. ML Features Table (Very Important)
- Table: `ddca_catalog.gold.ml_fraud_features`

This is the main table for ML.

It combines:
- customer data  
- transaction data  
- chat data  

Also creates new features like:
- risk category  
- age group  
- fraud indicators  
- engagement metrics  

---

## Machine Learning

ML notebooks are in:

- [`ml/customer_fraud_detection.ipynb`](ml/customer_fraud_detection.ipynb)  
- [`ml/transaction_fraud_detection.ipynb`](ml/transaction_fraud_detection.ipynb)  

---

### Customer Fraud Detection

Uses:
- `ddca_catalog.gold.ml_fraud_features`

What it does:
- Prepares data  
- Converts categories to numbers  
- Trains Random Forest  
- Evaluates performance  
- Saves predictions  

Output table:
- `ddca_catalog.gold.customer_fraud_predictions`  

---

### Transaction Fraud Detection

Uses:
- `ddca_catalog.silver.transactions_silver`

What it does:
- Works on transaction-level data  
- Trains model  
- Evaluates results  
- Saves predictions  

Output table:
- `ddca_catalog.gold.transaction_fraud_predictions`  

---

## Summary

- Data is generated and saved as CSV  
- Bronze reads raw data  
- Silver cleans and validates  
- Gold aggregates and builds features  
- ML uses gold data to detect fraud  

This creates a full pipeline from raw data to predictions.
