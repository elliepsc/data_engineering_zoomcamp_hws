"""
DAG 2: dbt Transformation Orchestration

This DAG demonstrates production-ready dbt integration with Airflow:
- Waits for ingestion DAG to complete (cross-DAG dependency)
- Runs dbt models in layered architecture (staging → core)
- Executes data quality tests
- Generates documentation

Key Patterns:
- ExternalTaskSensor for DAG orchestration
- Modular dbt execution (staging, core models separate)
- Built-in testing via dbt test
- Idempotent transformations

Prerequisites:
- dbt project in /opt/airflow/dbt/taxi_analytics/
- dbt profiles.yml configured for BigQuery
- Ingestion DAG (01_ingest_nyc_taxi_incremental) must run first

Author: Ellie - Data Engineering Zoomcamp 2026
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os
import sys

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.logger import get_logger
from utils.config_loader import get_data_config

# Initialize logger
logger = get_logger(__name__)

# Load configuration
data_config = get_data_config()

# DAG default arguments
default_args = {
    'owner': 'ellie',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Fewer retries for transformation (data quality issues shouldn't retry)
    'retry_delay': timedelta(minutes=3),
}

# dbt project path
DBT_PROJECT_PATH = '/opt/airflow/dbt/taxi_analytics'
DBT_TARGET = 'prod'  # Use production target


# Define DAG
with DAG(
    dag_id='02_transform_dbt_models',
    default_args=default_args,
    description='Run dbt transformations after data ingestion',
    schedule_interval='@monthly',  # Match ingestion schedule
    start_date=datetime.strptime(data_config['start_date'], '%Y-%m-%d'),
    end_date=datetime.strptime(data_config['end_date'], '%Y-%m-%d'),
    catchup=True,  # Enable backfill
    max_active_runs=1,  # Run sequentially to avoid conflicts
    tags=['transformation', 'dbt', 'data-platform'],
    doc_md=__doc__,
) as dag:
    
    # Task 1: Wait for ingestion DAG to complete
    # This creates a dependency between DAGs without direct coupling
    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion',
        external_dag_id='01_ingest_nyc_taxi_incremental',
        external_task_id='load_to_bigquery',  # Wait for BigQuery load to finish
        mode='reschedule',  # Free up worker slot while waiting
        timeout=600,  # Wait max 10 minutes
        poke_interval=60,  # Check every minute
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
    )
    
    # Task 2: Install dbt dependencies
    # Ensures all dbt packages are available
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt deps --target {DBT_TARGET}',
    )
    
    # Task 3: Run dbt staging models
    # Staging layer: Basic cleaning and standardization
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=(
            f'cd {DBT_PROJECT_PATH} && '
            f'dbt run --select staging --target {DBT_TARGET} --vars \'{{"execution_date": "{{{{ ds }}}}"}}\''
        ),
    )
    
    # Task 4: Test staging models
    # Validate staging layer before proceeding to core models
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt test --select staging --target {DBT_TARGET}',
    )
    
    # Task 5: Run dbt core models
    # Core layer: Business logic and aggregations
    dbt_run_core = BashOperator(
        task_id='dbt_run_core',
        bash_command=(
            f'cd {DBT_PROJECT_PATH} && '
            f'dbt run --select core --target {DBT_TARGET} --vars \'{{"execution_date": "{{{{ ds }}}}"}}\''
        ),
    )
    
    # Task 6: Test core models
    # Final validation of all transformations
    dbt_test_core = BashOperator(
        task_id='dbt_test_core',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt test --select core --target {DBT_TARGET}',
    )
    
    # Task 7: Generate dbt documentation (optional, runs on success)
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt docs generate --target {DBT_TARGET}',
        trigger_rule='all_success',
    )
    
    # Define task dependencies - Layered execution flow
    # 
    # Pattern: Wait for data → Install deps → Run staging → Test staging → 
    #          Run core → Test core → Generate docs
    #
    # Benefits:
    # - Fail fast: Test each layer before proceeding
    # - Clear separation of concerns
    # - Easy to debug: Know exactly which layer failed
    
    wait_for_ingestion >> dbt_deps >> dbt_run_staging
    dbt_run_staging >> dbt_test_staging >> dbt_run_core
    dbt_run_core >> dbt_test_core >> dbt_docs_generate


# Additional patterns you could add:
#
# 1. Snapshot models (SCD Type 2):
#    dbt_snapshot = BashOperator(
#        task_id='dbt_snapshot',
#        bash_command=f'cd {DBT_PROJECT_PATH} && dbt snapshot --target {DBT_TARGET}',
#    )
#
# 2. Incremental model refresh:
#    dbt_run_incremental = BashOperator(
#        task_id='dbt_run_incremental',
#        bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --select +fact_trips --target {DBT_TARGET}',
#    )
#
# 3. Full refresh on demand:
#    dbt_full_refresh = BashOperator(
#        task_id='dbt_full_refresh',
#        bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --full-refresh --target {DBT_TARGET}',
#    )
