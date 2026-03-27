class AirflowConnections:
    COIN_API = "coingecko_api"
    NEWS_API = "newsdata_api"
    AWS_CONNECTION = "minio_s3_conn"

class AirflowVariables:
    CRYPTO_LIMIT = "crypto_fetch_limit"
    NEWS_KEYWORDS = "news_key_words"
    MAX_QUERY_LENGTH_NEWSAPI = "max_query_length_newsapi"
    RAW_DATA_BUCKET = "raw_data_bucket"