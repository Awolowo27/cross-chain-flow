from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

# Path to the cross_chain_fund project directory
PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/home/freeman/Cross_chain_fund")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 8, 1),
}

with DAG(
    dag_id="cross_chain_pipeline_dag",
    default_args=default_args,
    description="Daily Orchestration Pipeline for Cross-Chain Fund Ledger & Token Enrichment",
    schedule_interval="0 2 * * *",  # Runs daily at 02:00 UTC
    catchup=False,
    tags=["cross-chain", "dbt", "bigquery", "fund-analytics"],
) as dag:

    # STEP 1: Run dbt Pipeline up to unified_bridge_flows (+ selects upstream models)
    dbt_run_base_and_unified = BashOperator(
        task_id="dbt_run_base_and_unified",
        bash_command=f"cd {PROJECT_DIR} && uv run dbt run --select +unified_bridge_flows",
    )

    # STEP 2: Run Data Quality Tests on Stage 1 Models
    dbt_test_stage_1 = BashOperator(
        task_id="dbt_test_stage_1",
        bash_command=f"cd {PROJECT_DIR} && uv run dbt test --select +unified_bridge_flows",
    )

    # STEP 3: Auto-Discover New ERC-20 Tokens & Fetch Metadata
    enrich_tokens_task = BashOperator(
        task_id="enrich_tokens_task",
        bash_command=f"cd {PROJECT_DIR} && uv run python scripts/enrich_tokens.py",
    )

    # STEP 4: Refresh Seed File in BigQuery
    dbt_seed_tokens = BashOperator(
        task_id="dbt_seed_tokens",
        bash_command=f"cd {PROJECT_DIR} && uv run dbt seed",
    )

    # STEP 5: Run Final Enriched Model
    dbt_run_enriched = BashOperator(
        task_id="dbt_run_enriched",
        bash_command=f"cd {PROJECT_DIR} && uv run dbt run --select unified_bridge_flows_enriched",
    )

    # STEP 6: Run Final Data Quality Tests on Enriched Model
    dbt_test_enriched = BashOperator(
        task_id="dbt_test_enriched",
        bash_command=f"cd {PROJECT_DIR} && uv run dbt test --select unified_bridge_flows_enriched",
    )

    # TASK DEPENDENCIES (DAG EXECUTION FLOW)
    (
        dbt_run_base_and_unified
        >> dbt_test_stage_1
        >> enrich_tokens_task
        >> dbt_seed_tokens
        >> dbt_run_enriched
        >> dbt_test_enriched
    )
