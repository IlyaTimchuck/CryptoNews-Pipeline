from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime
from typing import List, Dict, Any
import requests
import json
import os


@dag(
    dag_id="crypto_market_ingestion_dag",
    schedule="@hourly",
    start_date=datetime(2026, 3, 10),
    catchup=False,
    description="Phase 1: Ingesting Crypto Market Data",
    tags=['phase_1']
    )
def crypto_market_ingestion_dag():
    @task
    def fetch_top_coins_data() -> List[Dict[str, Any]]:
        limit = int(Variable.get("crypto_fetch_limit", default_var=15))
        url = "https://api.coingecko.com/api/v3/coins/markets"

        headers = {
            "x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY")
        }

        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": limit,
            "page": 1
            }
        
        coins_data = requests.get(url=url, headers=headers, params=params)
        coins_data.raise_for_status()
        return coins_data.json()

    @task
    def land_coins_data(coins_data: List[Dict[str, Any]]) -> str:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        file_path = f"include/raw_coins_data_{timestamp}.json"
        with open(f"include/raw_data/raw_coins_data_{timestamp}.json", "w") as f:
            json.dump(coins_data, f, indent=4)
        return file_path
    
    fetching_coins_data = fetch_top_coins_data()
    landing_coins_data = land_coins_data(fetching_coins_data)
    fetching_coins_data >> landing_coins_data


crypto_market_ingestion_dag()