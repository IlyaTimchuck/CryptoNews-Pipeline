# 🚀 CryptoNews-Pipeline

An end-to-end **ELT pipeline** designed to correlate real-time cryptocurrency market data with global news sentiment. 

## 🎯 Project Overview
This pipliene collects real-time crypto prices from CoinGecko and new articles from NewsAPI, processes them trhough transformation layers, and loads the results into an analytical warehouse. The core idea is to correlate market price movements with new sentiment.

## 🏗 Tech Stack
- **Ingestion scripts, API clients:** Python
- **Pipeline orchestration & scheduling:** Apache Airflow
- **Transformations across Bronze / Silver / Gold:** dbt
- **Heavy transformations, sentiment processing:** Apache Spark
- **Primary analytical warehouse:** ClickHouse
- **MPP warehouse for complex analytical SQL:** Greenplum
- **Dashboard & data visualization:** Apache Superset
- **Containerization of all services:** Docker
- **Version control:** Git

## 📊 Data Sources
**CoinGecko API** — free tier, no auth required
Endpoint: /coins/markets
Data: price, market cap, volume, 24h change per coin
Schedule: every 15 minutes

**NewsAPI** — free tier, API key required
Endpoint: /everything
Data: article title, description, source, published date
Query: top crypto keywords (bitcoin, ethereum, etc.)
Schedule: every 1 hour


## 🛠 Project Roadmap
- [x] Project Initialization & Infrastructure Setup (Astro CLI + Docker)
- [ ] **Phase 1:** Get data flowing end-to-end.
- [ ] **Phase 2:** Turn raw JSON into ClickHouse
- [ ] **Phase 3:** Replace Python transformation scripts with dbt models.
- [ ] **Phase 4:** Redesign Silver layer using Data Vault methodology.
- [ ] **Phase 5:** Introduce Spark for heavy transformations.
- [ ] **Phase 6:** Add an MPP warehouse Greenplum
