"""DAG Gold : job Spark (jointures, window functions, TF-IDF, score d'impact) -> Elasticsearch `arxiv-papers-enriched`."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from scipulse import es_index

with DAG(
    dag_id="31_gold_spark",
    description="Silver -> Gold avec Spark, ecriture dans Elasticsearch",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=2)},
    tags=["gold", "spark"],
) as dag:
    # Spark en mode local[*] (cf. SPARK_MASTER dans .env) : equivalent d'un spark-submit sur le noeud.
    spark_job = BashOperator(task_id="spark_gold_job", bash_command="python /opt/airflow/spark/gold_job.py")
    metrics = PythonOperator(task_id="index_pipeline_metrics", python_callable=es_index.index_pipeline_metrics)
    spark_job >> metrics
