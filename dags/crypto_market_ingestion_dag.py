from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
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
        base_path = "include/raw_data/raw_coins_data"
        file_path = f"{base_path}/raw_coins_{timestamp}.json"
        
        os.makedirs(base_path, exist_ok=True)

        with open(file_path, "w") as f:
            json.dump(coins_data, f, indent=4)

        return file_path

    @task
    def fetch_news_data(coins_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        url = "https://newsdata.io/api/1/latest"
        key_words = Variable.get("news_key_words", default_var={}, deserialize_json=True)
        key_words["coin_names"] = [coin["name"] for coin in coins_data[:15]]

        params = {
            "apikey": os.getenv("NEWSDATA_API_KEY"),
            "language": "en",
            "size": 10
            }

        news_data = {}
        for category in key_words:
            params["q"] = " OR ".join(key_words[category])
            if len(params["q"]) < 450:
                response = requests.get(url=url, params=params)
                articles = response.json().get("results", [])
                news_data[category] = articles 
            else:
                print(f"Skipping {category}: Query too long")

        if not news_data:
            raise AirflowSkipException("No news found for any category.")
        
        return news_data

    @task
    def land_news_data(news_data: dict):
        if not news_data:
            raise AirflowSkipException("The variable news_data is empty.")

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        base_path = "include/raw_data/raw_news_data"

        os.makedirs(base_path, exist_ok=True)

        for category, articles in news_data.items(): 
            with open(f"{base_path}/raw_news_{category}_{timestamp}.json", "w") as f:
                json.dump(articles, f, indent=4)


    raw_coins = fetch_top_coins_data()
    land_coins = land_coins_data(raw_coins)
    raw_news = fetch_news_data(raw_coins)
    land_news = land_news_data(raw_news)



crypto_market_ingestion_dag()