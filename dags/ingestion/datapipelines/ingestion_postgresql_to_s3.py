from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
from ingestion.businesscode.constants import *
from ingestion.businesscode.s3_operations import *


aws_access_key = Variable.get('AWS_ACCESS_KEY')
aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

dag = DAG(
    'Telecom_Ingestion',
    description='Load PostgreSQL table to S3 in Parquet format using SqlToS3Operator',
    schedule='@daily',
    start_date=datetime(2024, 12, 9),
    catchup=False,
)

for table_config in table_list: 

    table = table_config['table_name']
    table_load_type = table_config['load_type']
    # s3_target_path = table_config['table_name']
    data_source = table_config['data_source']
    watermark_column = table_config['watermark_column']
    s3_parquet_key = f'{table}/{table}.parquet' 
    archive_path = f'{table}/archive/{archive_date}/{table}_{archive_suffix}.parquet'
    load_date = str(get_max_loaddate(table, data_source))

    if table_load_type == 'full':
        sql_query = f"SELECT *, '{data_source}' as data_source FROM {table}"
        count_query = f"SELECT count(*) FROM {table}"
    else:
        sql_query = f"SELECT *, '{data_source}' as data_source FROM {table} where {watermark_column} = '{load_date}'" 
        count_query = f"SELECT count(*) FROM {table} where {watermark_column} >= '{load_date}'"


    # Get Count for all the tables
    get_table_count = PythonOperator(
        task_id = f'count_rows_{data_source}_{table}',
        python_callable=get_row_count,
        op_kwargs={
            "table_name": table,
            "count_query": count_query
        },
        dag = dag
    ) 

    # Check if file already exists
    archive_if_file_exists = PythonOperator(
        task_id = f"check_{data_source}_{table}_file_exists",
        python_callable=copy_and_delete_file_in_s3,
        op_kwargs={
            "aws_access_key": aws_access_key,
            "aws_secret_access_key": aws_secret_access_key,
            "bucket_name": s3_bucket, 
            "source_file_name": s3_parquet_key, 
            "destination_file_name": archive_path
        },
        dag=dag
    )

    # Load data from DB to s3_target_pathS3
    extract_to_s3_task = SqlToS3Operator(
        task_id=f'extract_{data_source}_{table}_to_s3_parquet',
        query=sql_query,
        sql_conn_id=postgres_conn_id,   
        aws_conn_id=aws_conn_id,
        s3_bucket=s3_bucket,
        s3_key=s3_parquet_key,
        file_format='parquet',
        replace=True,
        dag=dag,
    )

    load_audit_data = PythonOperator(
        task_id = f'load_audit_table_for_{data_source}_{table}',
        python_callable=insert_into_postgres,
        op_kwargs={
            "tablename": table, 
            "data_source": data_source,
            "watermarkcolumnname": watermark_column
        },
        dag=dag
    )

    archive_if_file_exists >> extract_to_s3_task >> get_table_count >> load_audit_data