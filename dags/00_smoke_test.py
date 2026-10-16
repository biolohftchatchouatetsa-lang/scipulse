"""DAG de test de fumée : vérifie que Airflow voit MinIO, Elasticsearch et Spark.

À lancer une fois après `make up` pour valider les connexions inter-services.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def check_minio():
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
    buckets = sorted(b["Name"] for b in s3.list_buckets()["Buckets"])
    expected = {"arxiv-raw", "hn-raw", "citations-raw", "arxiv-clean", "hn-clean", "citations-clean"}
    missing = expected - set(buckets)
    assert not missing, f"Buckets manquants : {missing}"
    print(f"MinIO OK, buckets : {buckets}")


def check_elasticsearch():
    from elasticsearch import Elasticsearch
    es = Elasticsearch(os.environ["ES_HOST"])
    health = es.cluster.health()
    print(f"Elasticsearch OK, statut : {health['status']}")
    assert health["status"] in ("green", "yellow")


with DAG(
    dag_id="00_smoke_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infra"],
) as dag:
    t_minio = PythonOperator(task_id="check_minio", python_callable=check_minio)
    t_es = PythonOperator(task_id="check_elasticsearch", python_callable=check_elasticsearch)
    t_spark = BashOperator(
        task_id="check_spark_java",
        bash_command="java -version && python -c \"import pyspark; print('pyspark', pyspark.__version__)\"",
    )
    [t_minio, t_es, t_spark]
