"""DAG Module 1 (Gold partiel) : ArXiv Silver -> Elasticsearch `arxiv-papers`, puis metriques de monitoring."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scipulse import es_index

with DAG(
    dag_id="30_index_arxiv_papers",
    description="Silver ArXiv -> Elasticsearch (arxiv-papers) + pipeline-metrics",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["gold", "elasticsearch"],
) as dag:
    papers = PythonOperator(task_id="index_arxiv_papers", python_callable=es_index.index_arxiv_papers)
    metrics = PythonOperator(task_id="index_pipeline_metrics", python_callable=es_index.index_pipeline_metrics)
    papers >> metrics
