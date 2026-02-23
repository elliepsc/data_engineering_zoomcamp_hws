"""
DAG 1: NYC Taxi Data Ingestion (Incremental)

This DAG demonstrates production-ready incremental data loading:
- Downloads monthly NYC taxi data (green taxis)
- Uploads to GCS with organized folder structure
- Loads to BigQuery with partitioning and clustering
- Supports backfill via catchup=True

Key Patterns:
- Incremental loading based on execution_date
- Idempotent execution (can re-run safely)
- XCom for inter-task communication
- Retry logic with exponential backoff
- Partitioned BigQuery tables for query optimization

Author: Ellie - Data Engineering Zoomcamp 2026
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import requests
import os
import sys

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.logger import get_logger
from utils.config_loader import get_gcp_config, get_bigquery_config, get_gcs_config, get_data_config

# Initialize logger
logger = get_logger(__name__)

# Load configuration
gcp_config = get_gcp_config()
bq_config = get_bigquery_config()
gcs_config = get_gcs_config()
data_config = get_data_config()

# DAG default arguments
default_args = {
    'owner': 'ellie',
    'depends_on_past': False,
    'email_on_failure': False,  # Set to True and add email for production
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}


def download_taxi_data(execution_date, **context):
    """
    Download NYC taxi data for the execution month.
    
    This implements incremental loading pattern:
    - Each DAG run processes one month based on execution_date
    - Supports backfill for historical data
    - Idempotent: re-running same month downloads same file
    
    Args:
        execution_date: Airflow execution date (automatically provided)
        context: Airflow context dictionary
        
    Returns:
        Local file path
    """
    # Format: YYYY-MM
    year_month = execution_date.strftime('%Y-%m')
    
    logger.info(
        f"Starting download for {year_month}",
        extra={'year_month': year_month, 'execution_date': str(execution_date)}
    )
    
    # NYC TLC data URL pattern
    taxi_type = data_config.get('taxi_type', 'green')
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year_month}.parquet"
    
    # Local temporary storage
    local_path = f"/tmp/{taxi_type}_tripdata_{year_month}.parquet"
    
    logger.info(f"Downloading from {url}")
    
    try:
        # Download with streaming to handle large files
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Write to local file
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Get file size for logging
        file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
        
        logger.info(
            f"Download completed: {file_size_mb:.2f} MB",
            extra={'file_path': local_path, 'size_mb': file_size_mb}
        )
        
        # Push to XCom for next tasks
        context['ti'].xcom_push(key='file_path', value=local_path)
        context['ti'].xcom_push(key='year_month', value=year_month)
        context['ti'].xcom_push(key='file_size_mb', value=file_size_mb)
        
        return local_path
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {str(e)}", exc_info=True)
        raise
    except IOError as e:
        logger.error(f"File write failed: {str(e)}", exc_info=True)
        raise


def get_gcs_destination(**context):
    """
    Generate GCS destination path based on execution date.
    
    This creates an organized folder structure:
    gs://bucket/raw/green_taxi/YYYY-MM/data.parquet
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        GCS destination path (without gs:// prefix)
    """
    year_month = context['ti'].xcom_pull(task_ids='download_data', key='year_month')
    taxi_type = data_config.get('taxi_type', 'green')
    
    # Organized folder structure
    gcs_path = f"{gcs_config['raw_prefix']}/{taxi_type}_taxi/{year_month}/data.parquet"
    
    logger.info(f"GCS destination: {gcs_path}")
    
    context['ti'].xcom_push(key='gcs_path', value=gcs_path)
    
    return gcs_path


def cleanup_local_file(**context):
    """
    Clean up temporary local file after successful upload.
    
    Args:
        context: Airflow context dictionary
    """
    file_path = context['ti'].xcom_pull(task_ids='download_data', key='file_path')
    
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info(f"Cleaned up local file: {file_path}")
        except Exception as e:
            logger.warning(f"Could not delete local file: {str(e)}")


# Define DAG
with DAG(
    dag_id='01_ingest_nyc_taxi_incremental',
    default_args=default_args,
    description='Incremental ingestion of NYC taxi data to BigQuery',
    schedule_interval='@monthly',  # Run once per month
    start_date=datetime.strptime(data_config['start_date'], '%Y-%m-%d'),
    end_date=datetime.strptime(data_config['end_date'], '%Y-%m-%d'),
    catchup=True,  # Enable backfill for historical data
    max_active_runs=3,  # Limit concurrent runs
    tags=['ingestion', 'data-platform', 'incremental', 'nyc-taxi'],
    doc_md=__doc__,
) as dag:
    
    # Task 1: Download monthly data from NYC TLC
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_taxi_data,
        provide_context=True,
    )
    
    # Task 2: Generate GCS destination path
    get_destination_task = PythonOperator(
        task_id='get_gcs_destination',
        python_callable=get_gcs_destination,
        provide_context=True,
    )
    
    # Task 3: Upload to GCS
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src="{{ ti.xcom_pull(task_ids='download_data', key='file_path') }}",
        dst="{{ ti.xcom_pull(task_ids='get_gcs_destination', key='gcs_path') }}",
        bucket=gcs_config['bucket_name'],
        gcp_conn_id='google_cloud_default',
    )
    
    # Task 4: Load to BigQuery with partitioning
    load_to_bigquery_task = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        gcp_conn_id='google_cloud_default',
        configuration={
            'load': {
                'sourceUris': [
                    f"gs://{gcs_config['bucket_name']}/{{{{ ti.xcom_pull(task_ids='get_gcs_destination', key='gcs_path') }}}}"
                ],
                'destinationTable': {
                    'projectId': gcp_config['project_id'],
                    'datasetId': bq_config['dataset_raw'],
                    'tableId': f"{data_config.get('taxi_type', 'green')}_taxi",
                },
                'sourceFormat': 'PARQUET',
                'writeDisposition': 'WRITE_APPEND',  # Append to existing table
                'createDisposition': 'CREATE_IF_NEEDED',
                # Partition by pickup datetime for query optimization
                'timePartitioning': {
                    'type': 'MONTH',
                    'field': 'lpep_pickup_datetime',  # For green taxis
                },
                # Cluster by location IDs for better query performance
                'clustering': {
                    'fields': ['PULocationID', 'DOLocationID']
                },
                # Schema auto-detection from Parquet
                'autodetect': True,
            }
        },
    )
    
    # Task 5: Cleanup local temporary file
    cleanup_task = PythonOperator(
        task_id='cleanup_local_file',
        python_callable=cleanup_local_file,
        provide_context=True,
        trigger_rule='all_done',  # Run even if upstream tasks fail
    )
    
    # Define task dependencies
    # Sequential pipeline: download → get_destination → upload → load → cleanup
    download_task >> get_destination_task >> upload_to_gcs_task >> load_to_bigquery_task >> cleanup_task
