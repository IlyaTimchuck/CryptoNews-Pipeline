from include.constants import AirflowConnections, AirflowVariables
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
from typing import List, Dict, Any


def check_length_limit(category_words: List[str], max_length: int) -> List:
    chunks = []
    current_terms = []

    for term in category_words:
        candidate = " OR ".join(current_terms + [term])
        if len(candidate) > max_length:
            if current_terms:
                chunks.append(" OR ".join(current_terms))
            current_terms = [term]
        else:
            current_terms.append(term)

    if current_terms:
        chunks.append(" OR ".join(current_terms))

    return chunks
        
def fetch_news_by_category(conn_id: str, endpoint: str, base_params: dict, keywords: list, max_limit: int) -> List:
    hook = HttpHook(
        method="GET",
        http_conn_id=conn_id
    )
     
    query_chunks = check_length_limit(keywords, max_limit)
    news_data = []
    
    for query in query_chunks:
        params = base_params.copy()
        params["q"] = query
    
        response = hook.run(endpoint=endpoint, data=params)
        articles = response.json().get("results", [])
        news_data.extend(articles)
        
    return news_data


def fetch_news_data(coins_data: List[Dict[str, Any]]) -> Dict[str, Dict]:
    conn_id = AirflowConnections.NEWS_API
    max_limit = int(Variable.get(AirflowVariables.MAX_QUERY_LENGTH_NEWSAPI, default_var=100))
    keywords_config = Variable.get(AirflowVariables.NEWS_KEYWORDS, deserialize_json=True)
    
    keywords_config["coin_names"] = [coin["name"] for coin in coins_data[:15]]
    
    hook = HttpHook(http_conn_id=conn_id)
    api_key = hook.get_connection(conn_id).password
    
    base_params = {"apikey": api_key, "language": "en", "size": 10}
    final_news_data = {}

    for category, terms in keywords_config.items():
        final_news_data[category] = fetch_news_by_category(
            conn_id=conn_id,
            endpoint="api/1/latest",
            base_params=base_params,
            keywords=terms,
            max_limit=max_limit
        )
        
    return final_news_data


