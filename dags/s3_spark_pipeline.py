from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'single_bucket_medallion_pipeline',
    default_args=default_args,
    description='Orchestrates Bronze, Silver, Gold folders inside a single AWS S3 bucket',
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'pyspark', 's3-folders']
) as dag:

    target_bucket = os.getenv('S3_BUCKET_NAME', '')

    wait_for_raw_file = S3KeySensor(
        task_id='wait_for_raw_file',
        bucket_key='raw/online_retail.csv',
        bucket_name=target_bucket,
        aws_conn_id='aws_default',
        timeout=1800,
        poke_interval=60,
        mode='poke'
    )

    execute_medallion_transform = BashOperator(
        task_id='execute_medallion_transform',
        bash_command='python /opt/airflow/jobs/spark_process.py',
        env={
            'S3_BUCKET_NAME': target_bucket,
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'AWS_DEFAULT_REGION': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
            **dag.environment
        }
    )

    wait_for_raw_file >> execute_medallion_transform
