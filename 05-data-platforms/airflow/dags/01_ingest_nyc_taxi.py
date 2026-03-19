"""
NYC Taxi Data Ingestion Pipeline - FIXED VERSION

Key improvements:
1. Uses explicit BigQuery schema to prevent type mismatch errors
2. ehail_fee is consistently FLOAT across all months
3. No more manual 'bq rm' commands needed!
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator

# Import explicit schema
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'config'))
from taxi_schema import GREEN_TAXI_SCHEMA


# Configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'nyc-taxi-zoomcamp-490519')
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'nyc-taxi-zoomcamp-490519-data')
TAXI_TYPE = os.getenv('TAXI_TYPE', 'green')
START_DATE = os.getenv('DATA_START_DATE', '2019-01-01')
END_DATE = os.getenv('DATA_END_DATE', '2020-12-31')

# Paths
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
TMP_DIR = '/tmp'


def download_taxi_data(**context):
    """Download NYC Taxi Parquet file for the given month"""
    import requests
    
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    # NYC TLC URL pattern
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"
    
    # Local file path
    local_file = f"{TMP_DIR}/{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"
    
    print(f"📥 Downloading: {url}")
    print(f"📁 Saving to: {local_file}")
    
    # Download with streaming
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(local_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    file_size_mb = os.path.getsize(local_file) / (1024 * 1024)
    print(f"✅ Downloaded {file_size_mb:.2f} MB")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='local_file', value=local_file)
    
    return local_file


def get_gcs_destination(**context):
    """Generate GCS destination path"""
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    gcs_path = f"raw/{TAXI_TYPE}_taxi/{year}-{month:02d}/data.parquet"
    
    print(f"🎯 GCS destination: gs://{BUCKET_NAME}/{gcs_path}")
    
    context['task_instance'].xcom_push(key='gcs_path', value=gcs_path)
    
    return gcs_path


# DAG Definition
default_args = {
    'owner': 'ellie',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='01_ingest_nyc_taxi_incremental',
    default_args=default_args,
    description='Incremental NYC Taxi data ingestion with explicit schema',
    schedule_interval='@monthly',
    start_date=datetime.strptime(START_DATE, '%Y-%m-%d'),
    end_date=datetime.strptime(END_DATE, '%Y-%m-%d'),
    catchup=True,
    max_active_runs=3,
    tags=['nyc_taxi', 'ingestion', 'bigquery', 'fixed'],
) as dag:
    
    # Task 1: Download data
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_taxi_data,
        provide_context=True,
    )
    
    # Task 2: Get GCS destination
    get_gcs_destination = PythonOperator(
        task_id='get_gcs_destination',
        python_callable=get_gcs_destination,
        provide_context=True,
    )
    
    # Task 3: Upload to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src="{{ task_instance.xcom_pull(task_ids='download_data', key='local_file') }}",
        dst="{{ task_instance.xcom_pull(task_ids='get_gcs_destination', key='gcs_path') }}",
        bucket=BUCKET_NAME,
        gcp_conn_id='google_cloud_default',
    )
    
    # Task 4: Load to BigQuery with EXPLICIT SCHEMA
    load_to_bigquery = BigQueryInsertJobOperator(
        task_id='load_to_bigquery',
        gcp_conn_id='google_cloud_default',
        configuration={
            'load': {
                'sourceUris': [
                    f"gs://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids='get_gcs_destination', key='gcs_path') }}}}"
                ],
                'destinationTable': {
                    'projectId': PROJECT_ID,
                    'datasetId': 'raw_data',
                    'tableId': f'{TAXI_TYPE}_taxi',
                },
                'sourceFormat': 'PARQUET',
                'writeDisposition': 'WRITE_APPEND',
                'createDisposition': 'CREATE_IF_NEEDED',
                
                # CRITICAL: Use explicit schema, NOT autodetect!
                'autodetect': False,
                'schema': {
                    'fields': GREEN_TAXI_SCHEMA
                },
                
                # Allow new fields to be added (for future-proofing)
                'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION'],
                
                # Partitioning and clustering (same as before)
                'timePartitioning': {
                    'type': 'MONTH',
                    'field': 'lpep_pickup_datetime',
                },
                'clustering': {
                    'fields': ['PULocationID', 'DOLocationID']
                },
            }
        },
    )
    
    # Task 5: Cleanup local file
    cleanup_local_file = BashOperator(
        task_id='cleanup_local_file',
        bash_command="rm -f {{ task_instance.xcom_pull(task_ids='download_data', key='local_file') }}",
    )
    
    # Task dependencies
    download_data >> get_gcs_destination >> upload_to_gcs >> load_to_bigquery >> cleanup_local_file
