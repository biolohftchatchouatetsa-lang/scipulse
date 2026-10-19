"""DAG micro-batch Hacker News : polling toutes les 5 minutes -> MinIO hn-raw/."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scipulse import ingest_hn

with DAG(
    dag_id="11_hn_bronze",
    description="Micro-batch Hacker News (Bronze), toutes les 5 minutes",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["bronze", "hackernews"],
) as dag:
    PythonOperator(task_id="poll_firebase", python_callable=ingest_hn.ingest_microbatch)
