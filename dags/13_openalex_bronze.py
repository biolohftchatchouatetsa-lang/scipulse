"""DAG citations : OpenAlex (par lots de DOI ArXiv) -> MinIO citations-raw/.

Reprend automatiquement la ou une execution precedente s'est arretee.
Pour repartir de zero : airflow dags trigger 13_openalex_bronze --conf '{"fresh": true}'
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scipulse import ingest_openalex


def run(ds, dag_run=None, **_):
    conf = (dag_run.conf if dag_run else None) or {}
    return ingest_openalex.ingest(ds, fresh=bool(conf.get("fresh", False)))


with DAG(
    dag_id="13_openalex_bronze",
    description="Ingestion citations OpenAlex (Bronze)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # a lancer apres 10_arxiv_bronze
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "citations"],
) as dag:
    PythonOperator(task_id="fetch_openalex", python_callable=run)