from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pendulum


def S3_land_data(aws_connection_id: str, bucket_name: str, timestamp: pendulum.datetime, data: str, dataset_name: str) -> str:
    S3_hook = S3Hook(aws_conn_id=aws_connection_id)
    
    if not S3_hook.check_for_bucket(bucket_name):
            S3_hook.create_bucket(bucket_name)
            
    file_name = f"{dataset_name}/{timestamp}.json"

    S3_hook.load_string(
        string_data=data,
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )
