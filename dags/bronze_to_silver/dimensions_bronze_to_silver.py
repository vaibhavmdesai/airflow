from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime

aws_access_key = Variable.get('AWS_ACCESS_KEY')
aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

# Define the DAG
dag = DAG(
    'Dim_Bronze_To_Silver',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 18),
    catchup=False
)

# SparkSubmitOperator task
spark_job_task = SparkSubmitOperator(
    task_id='run_spark_job',
    conn_id='spark_default',  # Connection ID for your Spark cluster
    application='/home/vd/data-engineering/projects/Telecom/telecom-env/code/spark/dimensions_bronze_to_silver/load_dimensions.py',  # Path to your Spark job
    executor_memory='1g',  # Executor memory
    total_executor_cores=2,  # Number of cores for the job
    name='spark_app',  # Job name
    verbose=True,  # Enable verbose logs
    env_vars={
        "AWS_ACCESS_KEY": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_access_key
    },
    dag=dag
)

spark_job_task