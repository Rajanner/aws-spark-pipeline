# ☁️ E-Commerce Medallion Data Lake (AWS, Airflow, PySpark, Docker)

An end-to-end, containerized data engineering pipeline that implements the industry-standard **Medallion Architecture** (Bronze, Silver, Gold). This project orchestrates PySpark ETL jobs using Apache Airflow to process real-world e-commerce data and load it into a single-bucket Amazon S3 Data Lakehouse.

## 🏗️ Architecture Overview

1. **Ingestion (Bronze):** Raw e-commerce transaction data (CSV) is manually or systematically dropped into an AWS S3 `raw/` folder.
2. **Orchestration:** An Apache Airflow `S3KeySensor` constantly monitors the S3 bucket. Upon file arrival, it triggers the PySpark ETL job.
3. **Processing (Silver):** PySpark cleanses the data (removes nulls, filters bad records), engineers new features (dates, transaction types), handles returns, and writes partitioned Parquet files to the `silver/` folder.
4. **Aggregation (Gold):** PySpark reads the Silver layer and computes business-level metrics (Monthly Sales Performance, Customer Lifetime Value, and Segmentation) saving the finalized Parquet files to the `gold/` folder, ready for BI tools like Amazon Athena or Tableau.

## 🛠️ Technology Stack

* **Containerization & Environment:** Docker, Docker Compose
* **Orchestration:** Apache Airflow (Python)
* **Data Processing:** Apache Spark (PySpark)
* **Cloud Storage:** Amazon S3
* **Libraries:** `boto3`, `hadoop-aws` (S3A connector)

## 📊 Medallion Layer Details

* **🥉 Bronze (`/raw`)**: The landing zone. Contains the unmodified `online_retail.csv` containing missing values, returns, and unformatted dates.
* **🥈 Silver (`/silver/transactions`)**: The conformed zone.
  * Missing `CustomerID`s and invalid `UnitPrice`s are dropped.
  * Dates are cast to proper Timestamps.
  * Returns (negative quantities) are separated from Purchases.
  * Stored as compressed Parquet files, dynamically partitioned by `InvoiceYear` and `InvoiceMonth`.
* **🥇 Gold (`/gold`)**: The analytics zone.
  * **Monthly Sales (`/monthly_sales`)**: Calculates Gross Revenue, Total Refunds, and Net Revenue per month.
  * **Customer Metrics (`/customer_metrics`)**: Calculates Customer Lifetime Value (CLV) and categorizes users into VIP, Premium, or Standard tiers.

## 🚀 Step-by-Step Setup Guide

### 1. Prerequisites
* **Docker Desktop** installed and running on your local machine.
* An **AWS Account** with an IAM user that has `AmazonS3FullAccess`.
* An **Amazon S3 Bucket** created (e.g., `my-ecom-datalake-2026`).

### 2. Configure Environment Variables
Create a `.env` file in the root of the project to securely store your AWS credentials. (This file is ignored by Git).

```ini
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-s3-bucket-name