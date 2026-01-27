# =============================================================================
# Backfill 2020 Data - For Homework Q3 & Q4
# =============================================================================
# Loads ALL 12 months of 2020 for both Yellow and Green taxis
# Catchup=True generates one run per month
# =============================================================================

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_backfill_start(**context):
    execution_date = context['execution_date']
    logger.info(f"🔄 Backfill started for {execution_date.strftime('%Y-%m')}")

def log_backfill_complete(**context):
    execution_date = context['execution_date']
    logger.info(f"✅ Backfill completed for {execution_date.strftime('%Y-%m')}")

with DAG(
    dag_id='taxi_backfill_2020',
    default_args=default_args,
    description='Backfill 2020 data (Jan-Dec) for Yellow and Green taxis',
    schedule_interval='@monthly',
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 31),
    catchup=True,  # CRITICAL: generates 12 runs (one per month)
    is_paused_upon_creation=True,  # Paused by default - activate when ready
    tags=['taxi', 'backfill', '2020'],
    max_active_runs=1,  # Process one month at a time
) as dag:
    
    start_log = PythonOperator(
        task_id='log_backfill_start',
        python_callable=log_backfill_start,
        provide_context=True,
    )
    
    # Trigger Yellow DAG for this month
    trigger_yellow = TriggerDagRunOperator(
        task_id='trigger_yellow_backfill',
        trigger_dag_id='taxi_etl_yellow',
        execution_date='{{ execution_date }}',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # Trigger Green DAG for this month
    trigger_green = TriggerDagRunOperator(
        task_id='trigger_green_backfill',
        trigger_dag_id='taxi_etl_green',
        execution_date='{{ execution_date }}',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    complete_log = PythonOperator(
        task_id='log_backfill_complete',
        python_callable=log_backfill_complete,
        provide_context=True,
    )
    
    # Run Yellow and Green in parallel, then log completion
    start_log >> [trigger_yellow, trigger_green] >> complete_log
