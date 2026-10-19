"""DAG ponctuel : historique des stories HN pointant vers arxiv.org (Algolia) -> hn-raw/backfill/.

Annee de depart : 2015 par defaut. Pour la changer au declenchement :
    airflow dags trigger 12_hn_backfill --conf '{"start_year": 2018}'
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scipulse import ingest_hn


def run_backfill(dag_run=None, **_):
    conf = (dag_run.conf if dag_run else None) or {}
    return ingest_hn.backfill_arxiv_stories(int(conf.get("start_year", 2015)))


with DAG(
    dag_id="12_hn_backfill",
    description="Backfill historique HN x ArXiv (Bronze)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["bronze", "hackernews"],
) as dag:
    PythonOperator(task_id="algolia_backfill", python_callable=run_backfill)
