# SciPulse & SciPulse Insights

Plateforme de veille scientifique : ArXiv + Hacker News + OpenAlex → Medallion (MinIO) → Spark → Elasticsearch → Kibana.

> README en cours de rédaction : il sera complété étape par étape (architecture, choix techniques, validation).

## Prérequis
- Docker Desktop (8 Go de RAM alloués minimum)
- `make`
- Dump ArXiv placé dans `data/arxiv-metadata-oai-snapshot.json`

## Démarrage
```bash
make up       # démarre toute la stack
make health   # vérifie que chaque service répond
```
Puis dans Airflow (http://localhost:8080, admin / admin) : lancer le DAG `00_smoke_test`.

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 |
| MinIO | http://localhost:9001 |
| Kibana | http://localhost:5601 |
| Elasticsearch | http://localhost:9200 |
| Spark master | http://localhost:8081 |
