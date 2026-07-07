import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, sum, countDistinct

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SingleBucketMedallionETL")

def create_spark_session():
    return SparkSession.builder \
        .appName("AWS-SingleBucket-Medallion-Pipeline") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()

def main():
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        logger.error("S3_BUCKET_NAME environment variable is missing!")
        sys.exit(1)
        
    bronze_path = f"s3a://{bucket_name}/raw"
    silver_path = f"s3a://{bucket_name}/silver"
    gold_path   = f"s3a://{bucket_name}/gold"
    
    spark = create_spark_session()
    
    try:
        # 1. BRONZE -> SILVER
        logger.info(f"Extracting source data from Bronze: {bronze_path}/online_retail.csv")
        bronze_df = spark.read.csv(f"{bronze_path}/online_retail.csv", header=True, inferSchema=True)
        
        logger.info("Transforming and filtering data for Silver Layer...")
        silver_df = bronze_df.filter(col("CustomerID").isNotNull()) \
            .filter(col("Quantity") > 0) \
            .withColumn("TotalAmount", col("Quantity") * col("UnitPrice")) \
            .withColumn("Transaction_Tier", 
                        when(col("TotalAmount") >= 500, "High-Value")
                        .when((col("TotalAmount") < 500) & (col("TotalAmount") >= 100), "Mid-Value")
                        .otherwise("Standard")) \
            .withColumn("processed_at", current_timestamp())
        
        silver_df.write.mode("overwrite") \
            .partitionBy("Transaction_Tier") \
            .parquet(f"{silver_path}/silver_transactions/")
            
        # 2. SILVER -> GOLD
        logger.info("Generating analytics business metrics for Gold Layer...")
        gold_df = silver_df.groupBy("Transaction_Tier") \
            .agg(
                sum("TotalAmount").alias("Total_Revenue"),
                countDistinct("CustomerID").alias("Unique_Active_Customers")
            )
        
        gold_df.write.mode("overwrite").parquet(f"{gold_path}/gold_sales_summary/")
        logger.info("Medallion pipeline completed successfully across all folders.")
        
    except Exception as e:
        logger.error(f"Pipeline processing failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
