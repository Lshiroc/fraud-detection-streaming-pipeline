[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/gApaiRiE)
# Fraud Monitoring System Built Using Databricks.

## Author
- Zeynal Mardanli

---

## Project Overview

This project is a fraud monitoring system built using Databricks.

The goal is to simulate a real system where we get data continuously and try to detect fraud from it.  
We work with three types of data:
- transactions  
- customers  
- chat logs  

These are combined together to better understand behavior and detect fraud.

The pipeline works in a batch + streaming style.  
First we load an initial dataset, then new batches arrive and the pipeline processes them automatically.

---

## Dataset

We start with a public dataset:
- https://www.kaggle.com/datasets/dhanushnarayananr/credit-card-fraud  

Then we extend it using:
- [`docs/prep.ipynb`](docs/prep.ipynb)

## Setup Instructions

Before running the project:

1. Download the dataset from Kaggle:
   https://www.kaggle.com/datasets/dhanushnarayananr/credit-card-fraud

2. Place the file in the `data/` folder.

3. Rename the file to:
   `transactions.csv`

This file is used in `prep.ipynb` to generate the rest of the dataset (customers, transactions batches, and chat logs).

### Why this dataset is important

We did not just use the dataset as we got it from the source. We improved it to make it more realistic:

- Customers are generated with different countries  
- Each country has different fraud probability  
- Some customers are very active, others are not  
- Transactions are linked to customers (not random)  
- Data is split into multiple batches (initial + streaming)  
- Chat logs are added (fraud complaints vs normal messages)  

Because of this, the dataset behaves more like real-world financial data.

---

## Pipeline Architecture

We use a **medallion architecture** in Databricks.

Folders:
- [`docs/bronze/`](docs/bronze/)
- [`docs/silver/`](docs/silver/)
- [`docs/gold/`](docs/gold/)

### Bronze Layer
- Reads raw CSV files using streaming (Auto Loader)  
- Keeps data as close to original as possible  
- Adds metadata like ingestion time and source file  

### Silver Layer
- Cleans the data  
- Applies schema validation rules  
- Drops invalid or broken rows  
- Standardizes types (e.g., booleans, strings)  

### Gold Layer
- Creates aggregated tables  
- Builds features for ML  
- Combines different datasets together  

Example:
- customer fraud summary  
- chat activity summary  
- ML feature table  

---

## Machine Learning

ML part is in:
- [`docs/ml/customer_fraud_detection.ipynb`](docs/ml/customer_fraud_detection.ipynb)  
- [`docs/ml/transaction_fraud_detection.ipynb`](docs/ml/transaction_fraud_detection.ipynb)

### Customer Fraud Detection
- Uses aggregated customer features  
- Predicts if a customer has fraud history  
- Uses data from gold layer  

### Transaction Fraud Detection
- Works on each transaction  
- Predicts if the transaction is fraud  
- Uses cleaned transaction data  

### What the ML does

- Converts data to pandas  
- Does simple analysis and visualization  
- Trains a Random Forest model  
- Evaluates using accuracy, precision, recall, F1  
- Saves predictions back to Databricks tables  

---

## Implemented Tasks

### Data Ingestion
- Structured data (CSV)
- Multiple sources (transactions, customers, chat logs)
- Streaming ingestion using Auto Loader

### Data Processing & Cleaning
- ETL pipeline implemented
- Data validation using rules (`expect`)  
- Type casting and cleaning
- Metadata columns added (ingestion time, source)  

### Architecture & Features
- Medallion architecture (Bronze/Silver/Gold) 
- Data transformations and aggregations 
- ML feature engineering
- ML model integrated with pipeline output 

### Visualization
- Basic plots in notebooks  
- Model evaluation results  

---

## Repository Structure

- `data/` - datasets  
- `docs/` - pipeline code, ML notebooks and documentation  
- `example/` - proof that pipeline works (screenshots)  
- `test/` - testing
- `misc/` - extra files  
---

## Examples

See:
- [`example/`](example/)

This folder contains screenshots and outputs showing:
- pipeline running  
- tables created  
- ML results  

---

## Future Improvements

Some ideas to improve the project further:

- Add real-time notifications when fraud or unusual activity is detected  
  (for example alerts or messages when high-risk transactions happen)

- Improve the dashboard  
  - better visualizations  
  - more real-time insights  
  - easier to understand for users  

---

## Notes

- Data is simulated but designed to be realistic  
- Pipeline supports continuous data ingestion  
- Focus is on combining data engineering + ML in one system  
