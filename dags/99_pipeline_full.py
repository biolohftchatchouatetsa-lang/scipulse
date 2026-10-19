"""DAG chef d'orchestre : Bronze -> Silver -> Gold de bout en bout.

Declenche les DAGs du pipeline dans l'ordre et attend la fin de chacun.
Pre-requis : tous les DAGs doivent etre actifs (non mis en pause) dans Airflow.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def step(dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=f"run_{dag_id}",
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )


with DAG(
    dag_id="99_pipeline_full",
    description="Pipeline complet Bronze -> Silver -> Gold",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestration"],
) as dag:
    arxiv, hn_backfill, openalex = step("10_arxiv_bronze"), step("12_hn_backfill"), step("13_openalex_bronze")
    silver, papers, gold = step("20_silver"), step("30_index_arxiv_papers"), step("31_gold_spark")
    arxiv >> openalex
    [openalex, hn_backfill] >> silver >> papers >> gold
