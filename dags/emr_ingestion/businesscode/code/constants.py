from datetime import datetime
import csv

postgres_conn_id = 'my_postgres_connection' 
aws_conn_id='aws_default'
s3_bucket = 'oabronze'
config_path = '/home/vd/data-engineering/airflow/airflowenv/airflow-emr-ingestion/dags/emr_ingestion/businesscode/configs/config.csv'
archive_date = datetime.today().strftime('%Y/%m/%d')
archive_suffix = datetime.today().strftime('%Y%m%d%H%M%S')

with open(config_path, newline='') as csvfile:
    csvreader = csv.DictReader(csvfile)
    table_list = [row for row in csvreader]