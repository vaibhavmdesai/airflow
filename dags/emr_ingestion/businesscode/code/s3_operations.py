import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from airflow.providers.postgres.hooks.postgres import PostgresHook
from businesscode.code.constants import *

def file_exists_in_s3(s3_client, bucket_name, file_name):
    
    """Checks if the file is present on s3.
    Args:
        s3_client: Amazon s3 client connection
        bucket_name: Amazon s3 bucket name
        file_name: key to be checked on s3

    Returns:
        Boolean: Returns True if file is present, else returns False
    """

    try:
        # Try to get the object metadata for the file
        s3_client.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except ClientError as e:
        # If the file does not exist, a ClientError will be raised
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # If some other error occurred, raise it
            raise
    except NoCredentialsError:
        print("AWS credentials not found.")
        return False
    

def copy_and_delete_file_in_s3(aws_access_key, aws_secret_access_key, bucket_name, source_file_name, destination_file_name, destination_bucket_name=None):

    if destination_bucket_name is None:
        destination_bucket_name = bucket_name  # Copy to the same bucket if no destination bucket is specified
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key
    )

    # Check if the source file exists
    if file_exists_in_s3(s3_client, bucket_name, source_file_name):
        try:
            # Copy the file from source to destination
            copy_source = {'Bucket': bucket_name, 'Key': source_file_name}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=destination_bucket_name,
                Key=destination_file_name
            )
            print(f"File copied from {source_file_name} to {destination_file_name} in bucket {destination_bucket_name}.")
            
            # Delete the source file after successful copy
            s3_client.delete_object(Bucket=bucket_name, Key=source_file_name)
            print(f"File {source_file_name} deleted from bucket {bucket_name}.")
            
            return True
        except ClientError as e:
            print(f"Failed to copy or delete file: {e}")
            return False
    else:
        print(f"File {source_file_name} does not exist in bucket {bucket_name}.")
        return False


def get_max_loaddate(table_name, data_source):

    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    sql = f"""SELECT COALESCE(CAST(MAX(loaddate) AS DATE), '1900-01-01')
             FROM load_logs
             WHERE tablename = '{table_name}'
             AND data_source = '{data_source}'
          """
    
    result = postgres_hook.get_first(sql)
    max_load_date = result[0] if result else 0
    
    return max_load_date


def get_row_count(count_query):

    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    result = postgres_hook.get_first(count_query)
    row_count = result[0] if result else 0
    
    return row_count


def insert_into_postgres(ti, data_source, tablename, watermarkcolumnname = 'temp_col', loaddate = datetime.now()):

    result = ti.xcom_pull(task_ids=[f'count_rows_{tablename}_{data_source}'])
    numberofrowscopied = result[0]

    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    data_to_insert = [
        (data_source, tablename, numberofrowscopied, watermarkcolumnname, loaddate)
    ]
    
    postgres_hook.insert_rows(
        table='load_logs', 
        rows=data_to_insert, 
        target_fields=['data_source', 'tablename', 'numberofrowscopied', 'watermarkcolumnname', 'loaddate'], 
        commit_every=1000
    )