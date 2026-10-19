"""DAG Silver : nettoyage + validation Great Expectations + ecriture Parquet (une tache par source)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scipulse import silver

with DAG(
    dag_id="20_silver",
    description="Bronze -> Silver (nettoyage, Great Expectations, Parquet)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # declenche apres les DAGs Bronze
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["silver"],
) as dag:
    arxiv = PythonOperator(task_id="silver_arxiv", python_callable=silver.run_arxiv)
    hn = PythonOperator(task_id="silver_hn", python_callable=silver.run_hn)
    citations = PythonOperator(task_id="silver_citations", python_callable=silver.run_citations)

    # Sequentiel : les trois taches partagent un meme projet Great Expectations (fichiers de
    # configuration uniques). En parallele, une lecture concurrente d'une ecriture a provoque une
    # InvalidDataContextConfigError. Le cout est d'environ une minute.
    arxiv >> hn >> citations