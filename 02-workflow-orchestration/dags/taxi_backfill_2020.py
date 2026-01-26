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

# =============================================================================
# COMMENT UTILISER CE DAG
# =============================================================================
#
# MÉTHODE 1 - Via Airflow UI (RECOMMANDÉ):
# 1. Aller sur http://localhost:8080
# 2. Trouver le DAG "taxi_backfill_2021"
# 3. Toggle ON (activer le DAG)
# 4. Airflow détecte catchup=True et génère automatiquement les 7 runs
# 5. Attendre que tous les runs finissent (peut prendre 1-2h selon data size)
#
# MÉTHODE 2 - Via CLI:
# docker exec -it airflow-scheduler airflow dags backfill \
#     taxi_backfill_2021 \
#     --start-date 2021-01-01 \
#     --end-date 2021-07-31
#
# MONITORING:
# - Voir la progression dans Airflow UI → DAGs → taxi_backfill_2021
# - Chaque run (Jan, Feb, ..., Jul) apparaît comme une ligne
# - Status: queued → running → success/failed
# - Logs: Cliquer sur une task pour voir les logs détaillés
#
# =============================================================================

# =============================================================================
# NOTES IMPORTANTES
# =============================================================================
#
# 1. CATCHUP MECHANISM:
#    - catchup=True génère des runs historiques automatiquement
#    - Utile pour charger des données passées
#    - ATTENTION: Peut créer beaucoup de runs si longue période
#
# 2. TRIGGER DAG RUN:
#    - TriggerDagRunOperator réutilise les DAGs existants
#    - Meilleure pratique que de dupliquer le code ETL
#    - Permet orchestration complexe (DAG qui appelle d'autres DAGs)
#
# 3. PARALLEL vs SEQUENTIAL:
#    - [trigger_yellow, trigger_green]: Parallèle (plus rapide)
#    - trigger_yellow >> trigger_green: Séquentiel (plus safe)
#    - Trade-off: Performance vs Resource usage
#
# 4. HOMEWORK:
#    - Q5: Yellow March 2021 rows
#    - SOLUTION: Run ce DAG, il va charger Mars 2021
#    - Puis query: SELECT COUNT(*) FROM yellow_taxi_trips WHERE year=2021 AND month=3
#
# 5. IDEMPOTENCE:
#    - if_exists='replace' dans les DAGs Yellow/Green garantit idempotence
#    - On peut re-run ce backfill DAG sans problème
#    - Pas de duplicates
#
# 6. ERROR HANDLING:
#    - Si Yellow fail pour Mars 2021, le run de Mars fail
#    - Les autres mois (Jan, Feb, Apr, etc.) continuent normalement
#    - On peut re-run JUSTE le mois qui a fail dans l'UI
#
# 7. PERFORMANCE:
#    - max_active_runs=1: Traite UN mois à la fois
#    - Pour 7 mois: ~7 * (temps d'un mois) = 1-2h total
#    - Pour aller plus vite: max_active_runs=3 (3 mois en parallèle)
#
# 8. ALTERNATIVES:
#    - Option A: Mettre catchup=True dans taxi_etl_yellow/green directement
#    - Option B: Créer un DAG backfill comme celui-ci (plus de contrôle)
#    - On a choisi B pour séparer les concerns (daily ops vs backfill)
