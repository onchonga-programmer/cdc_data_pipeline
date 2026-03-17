"""
CDC Health Pipeline DAG
========================
.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# DEFAULT ARGUMENTS
# Applied to every task in the DAG
# ─────────────────────────────────────────────
default_args = {
    "owner": "brenda",
    "retries": 1,                           # retry once if a task fails
    "retry_delay": timedelta(minutes=5),    # wait 5 mins before retrying
    "email_on_failure": False,
}

# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id="cdc_health_pipeline",
    description="Fetches CDC death data and builds bronze → silver → gold",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",             # runs every day at midnight
    catchup=False,                          # don't backfill past runs
    tags=["cdc", "health", "pipeline"],
) as dag:

    # ─────────────────────────────────────────
    # TASK 1 — Stage 2: Ingest from CDC API
    # ─────────────────────────────────────────
    fetch_task = BashOperator(
        task_id="fetch_cdc_data",
        bash_command="cd /opt/airflow && python ingestion/fetch_cdc_data.py",
    )

    # ─────────────────────────────────────────
    # TASK 2 — Stage 3: Transform to Silver
    # ─────────────────────────────────────────
    transform_task = BashOperator(
        task_id="transform_silver",
        bash_command="cd /opt/airflow && python ingestion/transform_data.py",
    )

    # ─────────────────────────────────────────
    # TASK 3 — Stage 4: Build Gold Layer
    # ─────────────────────────────────────────
    gold_task = BashOperator(
        task_id="build_gold",
        bash_command="cd /opt/airflow && python ingestion/build_data.py",
    )

    # ─────────────────────────────────────────
    # PIPELINE ORDER
    # >> means "then run this next"
    # fetch must succeed before transform runs
    # transform must succeed before gold runs
    # ─────────────────────────────────────────
    fetch_task >> transform_task >> gold_task
