"""
DAG 3: Data Quality Monitoring

This DAG demonstrates production-ready data quality patterns:
- Uses custom DataQualityOperator for reusable checks
- Validates data freshness, completeness, and accuracy
- Runs after ingestion completes
- Zero retries (quality checks should fail fast)

Key Patterns:
- Custom operators for reusability
- Multiple quality dimensions (freshness, completeness, validity)
- Configurable thresholds
- Clear failure messages for debugging

Quality Checks Performed:
1. Freshness: Data loaded for expected month
2. Completeness: No NULL values in critical columns
3. Validity: Reasonable value ranges (trip distance, fares)
4. Uniqueness: No duplicate records (optional)

Author: Ellie - Data Engineering Zoomcamp 2026
"""

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add plugins and utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../plugins'))

from operators.data_quality_operator import DataQualityOperator
from utils.logger import get_logger
from utils.config_loader import get_gcp_config, get_bigquery_config, get_data_config

# Initialize logger
logger = get_logger(__name__)

# Load configuration
gcp_config = get_gcp_config()
bq_config = get_bigquery_config()
data_config = get_data_config()

# Build full table ID
TAXI_TYPE = data_config.get('taxi_type', 'green')
TABLE_ID = f"{gcp_config['project_id']}.{bq_config['dataset_raw']}.{TAXI_TYPE}_taxi"

# DAG default arguments
default_args = {
    'owner': 'ellie',
    'depends_on_past': False,
    'email_on_failure': True,  # Alert on quality failures
    'email_on_retry': False,
    'retries': 0,  # Quality checks should NOT retry
}


def log_quality_summary(**context):
    """
    Log summary of all quality checks for monitoring.
    
    This function aggregates results from all quality check tasks
    and logs them in structured format for monitoring dashboards.
    
    Args:
        context: Airflow context dictionary
    """
    execution_date = context['execution_date']
    year_month = execution_date.strftime('%Y-%m')
    
    logger.info(
        f"Quality checks completed for {year_month}",
        extra={
            'execution_date': str(execution_date),
            'year_month': year_month,
            'table_id': TABLE_ID,
            'status': 'success'
        }
    )


# Define DAG
with DAG(
    dag_id='03_quality_monitoring',
    default_args=default_args,
    description='Data quality monitoring for NYC taxi data',
    schedule_interval='@monthly',
    start_date=datetime.strptime(data_config['start_date'], '%Y-%m-%d'),
    end_date=datetime.strptime(data_config['end_date'], '%Y-%m-%d'),
    catchup=True,
    max_active_runs=3,
    tags=['quality', 'monitoring', 'data-platform'],
    doc_md=__doc__,
) as dag:
    
    # Task 1: Wait for ingestion to complete
    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion',
        external_dag_id='01_ingest_nyc_taxi_incremental',
        external_task_id='load_to_bigquery',
        mode='reschedule',
        timeout=600,
        poke_interval=60,
    )
    
    # ==========================================
    # QUALITY CHECK 1: Data Freshness
    # ==========================================
    # Verify that data was loaded for the expected execution month
    # Failure indicates ingestion pipeline issue
    
    check_data_freshness = DataQualityOperator(
        task_id='check_data_freshness',
        table_id=TABLE_ID,
        check_type='row_count',
        threshold=0,
        comparison='gt',
        partition_date='{{ ds }}',  # Use execution date for partition filter
    )
    
    # ==========================================
    # QUALITY CHECK 2: No NULL Pickup Datetime
    # ==========================================
    # Critical field - NULL values indicate data corruption
    
    check_no_null_pickup_datetime = DataQualityOperator(
        task_id='check_no_null_pickup_datetime',
        table_id=TABLE_ID,
        check_type='null_check',
        column='lpep_pickup_datetime',  # Green taxi pickup datetime
    )
    
    # ==========================================
    # QUALITY CHECK 3: No NULL Dropoff Datetime
    # ==========================================
    
    check_no_null_dropoff_datetime = DataQualityOperator(
        task_id='check_no_null_dropoff_datetime',
        table_id=TABLE_ID,
        check_type='null_check',
        column='lpep_dropoff_datetime',
    )
    
    # ==========================================
    # QUALITY CHECK 4: Reasonable Trip Distances
    # ==========================================
    # Validate trip distances are within expected range
    # Max threshold: 200 miles (outliers exist but should be rare)
    
    check_trip_distances = DataQualityOperator(
        task_id='check_trip_distances',
        table_id=TABLE_ID,
        check_type='range_check',
        column='trip_distance',
        threshold=200,  # Max reasonable distance
        comparison='lte',
    )
    
    # ==========================================
    # QUALITY CHECK 5: Positive Passenger Counts
    # ==========================================
    # Passenger count must be >= 1
    
    check_passenger_count = DataQualityOperator(
        task_id='check_passenger_count',
        table_id=TABLE_ID,
        check_type='custom_sql',
        custom_sql=f"""
            SELECT 
                COUNT(CASE WHEN passenger_count < 1 THEN 1 END) * 1.0 / COUNT(*) < 0.01 as is_valid
            FROM `{TABLE_ID}`
            WHERE DATE(lpep_pickup_datetime) = '{{{{ ds }}}}'
        """,
    )
    
    # ==========================================
    # QUALITY CHECK 6: Reasonable Fare Amounts
    # ==========================================
    # Total amount should be positive and less than $1000
    # (occasional high fares exist for long trips)
    
    check_fare_amounts = DataQualityOperator(
        task_id='check_fare_amounts',
        table_id=TABLE_ID,
        check_type='custom_sql',
        custom_sql=f"""
            SELECT 
                COUNT(CASE 
                    WHEN total_amount <= 0 OR total_amount > 1000 
                    THEN 1 
                END) * 1.0 / COUNT(*) < 0.01 as is_valid
            FROM `{TABLE_ID}`
            WHERE DATE(lpep_pickup_datetime) = '{{{{ ds }}}}'
        """,
    )
    
    # ==========================================
    # QUALITY CHECK 7: Valid Location IDs
    # ==========================================
    # Ensure pickup and dropoff location IDs are not NULL
    # (NULL indicates GPS/geocoding failure)
    
    check_location_ids = DataQualityOperator(
        task_id='check_location_ids',
        table_id=TABLE_ID,
        check_type='custom_sql',
        custom_sql=f"""
            SELECT 
                COUNT(CASE 
                    WHEN PULocationID IS NULL OR DOLocationID IS NULL 
                    THEN 1 
                END) * 1.0 / COUNT(*) < 0.05 as is_valid
            FROM `{TABLE_ID}`
            WHERE DATE(lpep_pickup_datetime) = '{{{{ ds }}}}'
        """,
    )
    
    # ==========================================
    # TASK: Log Quality Summary
    # ==========================================
    # Aggregate and log results for monitoring
    
    log_summary = PythonOperator(
        task_id='log_quality_summary',
        python_callable=log_quality_summary,
        provide_context=True,
        trigger_rule='all_success',  # Only run if all checks pass
    )
    
    # Define task dependencies
    #
    # Pattern: All quality checks run in parallel after data is loaded
    # This provides fast feedback on multiple quality dimensions
    #
    # wait_for_ingestion → [all quality checks in parallel] → log_summary
    
    wait_for_ingestion >> [
        check_data_freshness,
        check_no_null_pickup_datetime,
        check_no_null_dropoff_datetime,
        check_trip_distances,
        check_passenger_count,
        check_fare_amounts,
        check_location_ids,
    ] >> log_summary


# Production enhancements you could add:
#
# 1. Great Expectations integration:
#    from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
#    
#    run_ge_checkpoint = GreatExpectationsOperator(
#        task_id='run_great_expectations',
#        checkpoint_name='taxi_data_checkpoint',
#        ...
#    )
#
# 2. Anomaly detection:
#    check_anomalies = DataQualityOperator(
#        task_id='detect_anomalies',
#        check_type='custom_sql',
#        custom_sql='''
#            WITH stats AS (
#                SELECT 
#                    AVG(trip_distance) as avg_distance,
#                    STDDEV(trip_distance) as stddev_distance
#                FROM `{TABLE_ID}`
#                WHERE DATE(lpep_pickup_datetime) BETWEEN 
#                    DATE_SUB('{{ ds }}', INTERVAL 30 DAY) AND '{{ ds }}'
#            )
#            SELECT 
#                COUNT(CASE 
#                    WHEN t.trip_distance > s.avg_distance + 3 * s.stddev_distance 
#                    THEN 1 
#                END) * 1.0 / COUNT(*) < 0.01 as is_valid
#            FROM `{TABLE_ID}` t, stats s
#            WHERE DATE(t.lpep_pickup_datetime) = '{{ ds }}'
#        '''
#    )
#
# 3. Slack/Email notifications:
#    from airflow.operators.slack_operator import SlackWebhookOperator
#    
#    notify_quality_failure = SlackWebhookOperator(
#        task_id='notify_quality_failure',
#        http_conn_id='slack_webhook',
#        message='Quality check failed for {{ ds }}',
#        trigger_rule='one_failed',
#    )
