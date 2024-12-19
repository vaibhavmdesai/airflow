from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'spark_submit_job',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 18),
    catchup=False
)

# SparkSubmitOperator task
spark_job_task = SparkSubmitOperator(
    task_id='run_spark_job',
    conn_id='spark_default',  # Connection ID for your Spark cluster
    application='/home/vd/data-engineering/projects/pyspark/thebigdatashow/main/code/Driver.py',  # Path to your Spark job
    executor_memory='1g',  # Executor memory
    total_executor_cores=2,  # Number of cores for the job
    name='spark_app',  # Job name
    verbose=True,  # Enable verbose logs
    dag=dag
)

spark_job_task
