from include.constants import AirflowConnections, AirflowVariables
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from pendulum import datetime, duration
from typing import List, Dict, Any
from include.services.fetch_coins_data import fetch_top_coins_data
from include.services.fetch_news_data import fetch_news_data
from include.services.loaders import S3_land_data
import json

@dag(
    dag_id="crypto_market_ingestion_dag",
    schedule="@hourly",
    start_date=datetime(2026, 3, 10),
    catchup=False,
    description="Phase 1: Ingesting Crypto Market Data",
    tags=['phase_1'],
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=1, seconds=30),
    }

)
def crypto_market_ingestion_dag():

    @task
    def extract_coins() -> List[Dict[str, Any]]:
        return fetch_top_coins_data()

    @task
    def land_coins_to_S3(coins_data_json: List[Dict[str, Any]], logical_date=None) -> str:
        aws_connection_id = AirflowConnections.AWS_CONNECTION
        bucket_name = Variable.get(
            AirflowVariables.RAW_DATA_BUCKET, default_var="bronze")

        S3_land_data(
            aws_connection_id=aws_connection_id,
            bucket_name=bucket_name,
            timestamp=logical_date.strftime("%Y-%m-%d_%H-%M"),
            data=json.dumps(coins_data_json),
            dataset_name="coins_data"
        )

    @task
    def extract_news(coins_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return fetch_news_data(coins_data)

    @task
    def land_news_to_s3(news_data_json: dict, logical_date=None):
        aws_connection_id = AirflowConnections.AWS_CONNECTION
        bucket_name = Variable.get(
            AirflowVariables.RAW_DATA_BUCKET, default_var="bronze")

        for category, articles in news_data_json.items():
            S3_land_data(
                aws_connection_id=aws_connection_id,
                bucket_name=bucket_name,
                timestamp=logical_date.strftime("%Y-%m-%d_%H-%M"),
                data=json.dumps(articles),
                dataset_name=f"news-{category}"
            )

    coins = extract_coins()
    news = extract_news(coins)

    land_coins = land_coins_to_S3(coins)
    land_news = land_news_to_s3(news)


crypto_market_ingestion_dag()
