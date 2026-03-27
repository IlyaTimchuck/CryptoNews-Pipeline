from include.constants import AirflowConnections, AirflowVariables
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
from typing import List, Dict, Any


def fetch_top_coins_data() -> List[Dict[str, Any]]:
    hook_coins = HttpHook(
        method="GET",
        http_conn_id=AirflowConnections.COIN_API
    )

    limit = int(Variable.get(AirflowVariables.CRYPTO_LIMIT, default_var=15))

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": limit,
        "page": 1
    }

    response = hook_coins.run(
        endpoint="coins/markets",
        data=params
    )

    coins_data_json = response.json()

    return coins_data_json

