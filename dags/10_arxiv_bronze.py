"""DAG batch ArXiv : dump local -> MinIO arxiv-raw/ (couche Bronze)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scipulse import ingest_arxiv

with DAG(
    dag_id="10_arxiv_bronze",
    description="Ingestion batch ArXiv (Bronze)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # déclenchement manuel (le dump est statique)
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["bronze", "arxiv"],
) as dag:
    check = PythonOperator(task_id="check_dump", python_callable=ingest_arxiv.check_dump)
    load = PythonOperator(
        task_id="sample_and_upload",
        python_callable=lambda ds, **_: ingest_arxiv.ingest(ds),
    )
    check >> load
