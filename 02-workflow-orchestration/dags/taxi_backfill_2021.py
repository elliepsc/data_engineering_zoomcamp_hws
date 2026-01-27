# =============================================================================
# Backfill 2021 Data - For Homework Q5
# =============================================================================
# Loads Jan-Jul 2021 for both Yellow and Green taxis
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
    dag_id='taxi_backfill_2021',
    default_args=default_args,
    description='Backfill 2021 data (Jan-Jul) for Yellow and Green taxis',
    schedule_interval='@monthly',
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 7, 31),
    catchup=True,  # Generates 7 runs (Jan-Jul)
    is_paused_upon_creation=True,  # Paused by default - activate when ready
    tags=['taxi', 'backfill', '2021'],
    max_active_runs=1,
) as dag:
    
    start_log = PythonOperator(
        task_id='log_backfill_start',
        python_callable=log_backfill_start,
        provide_context=True,
    )
    
    trigger_yellow = TriggerDagRunOperator(
        task_id='trigger_yellow_backfill',
        trigger_dag_id='taxi_etl_yellow',
        execution_date='{{ execution_date }}',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
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
    
    start_log >> [trigger_yellow, trigger_green] >> complete_log
