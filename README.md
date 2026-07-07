# AWS Data Engineering Lakehouse Pipeline
A containerized data pipeline utilizing **Apache Airflow**, **PySpark**, **Docker**, and **AWS S3** adopting a single-bucket Medallion Architecture.

## Setup Instructions

1. Update the `.env` file with your AWS credentials and target S3 Bucket name.
2. Run `make setup` to ensure directories are ready.
3. Run `make up` to launch the Airflow Docker cluster.
4. Upload `online_retail.csv` into `s3://your-bucket-name/raw/`.
5. Access Airflow at `http://localhost:8080` (admin/admin), configure the AWS connection, and trigger the DAG!
