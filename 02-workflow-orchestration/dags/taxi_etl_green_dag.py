# =============================================================================
# Green Taxi ETL Pipeline
# =============================================================================
# Similar to Yellow Taxi but with lpep_* datetime columns instead of tpep_*
# =============================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
import requests
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TAXI_TYPE = 'green'
CSV_BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
TEMP_DIR = "/tmp/taxi_data"

POSTGRES_CONN = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'ny_taxi'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_green_taxi(**context):
    """
    Extract Green Taxi CSV from GitHub
    
    NOTE: Green taxi files are compressed (.csv.gz) on GitHub
    We download the .gz file and decompress it
    """
    import gzip
    import shutil
    
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    # Green taxi files are .csv.gz on GitHub
    filename_gz = f"{TAXI_TYPE}_tripdata_{year}-{month:02d}.csv.gz"
    filename_csv = f"{TAXI_TYPE}_tripdata_{year}-{month:02d}.csv"
    url = f"{CSV_BASE_URL}/{TAXI_TYPE}/{filename_gz}"
    
    logger.info(f"📥 Downloading {filename_gz}")
    
    os.makedirs(TEMP_DIR, exist_ok=True)
    local_filepath_gz = os.path.join(TEMP_DIR, filename_gz)
    local_filepath_csv = os.path.join(TEMP_DIR, filename_csv)
    
    try:
        # Download .csv.gz file
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        with open(local_filepath_gz, 'wb') as f:
            f.write(response.content)
        
        gz_size_mb = os.path.getsize(local_filepath_gz) / (1024 * 1024)
        logger.info(f"✅ Download complete - {gz_size_mb:.1f} MiB (compressed)")
        
        # Decompress .gz to .csv
        logger.info(f"📦 Decompressing {filename_gz}...")
        with gzip.open(local_filepath_gz, 'rb') as f_in:
            with open(local_filepath_csv, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        csv_size_mb = os.path.getsize(local_filepath_csv) / (1024 * 1024)
        logger.info(f"✅ Decompressed - {csv_size_mb:.1f} MiB (uncompressed)")
        
        # Clean up .gz file
        os.remove(local_filepath_gz)
        
        return local_filepath_csv
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Download failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Decompression failed: {str(e)}")
        raise

def transform_green_taxi(**context):
    """NOTE: Green taxi uses lpep_pickup_datetime, not tpep_pickup_datetime"""
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='extract_green_taxi')
    
    logger.info(f"🔄 Transforming {filepath}")
    
    df = pd.read_csv(filepath)
    df.columns = df.columns.str.lower()
    
    # Drop nulls (lpep for green, not tpep)
    original_count = len(df)
    df = df.dropna(subset=['lpep_pickup_datetime'])
    dropped_count = original_count - len(df)
    if dropped_count > 0:
        logger.warning(f"⚠️ Dropped {dropped_count} null rows")
    
    # Convert dates
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    
    # Add partitions
    df['year'] = df['lpep_pickup_datetime'].dt.year
    df['month'] = df['lpep_pickup_datetime'].dt.month
    
    logger.info(f"✅ Transform complete: {len(df)} rows")
    return df

def load_green_taxi(**context):
    ti = context['ti']
    df = ti.xcom_pull(task_ids='transform_green_taxi')
    
    if df is None or len(df) == 0:
        raise ValueError("❌ No data from transform")
    
    logger.info(f"📤 Loading {len(df)} rows")
    
    table_name = f"{TAXI_TYPE}_taxi_trips"
    conn_string = (
        f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}"
        f"@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
    )
    engine = create_engine(conn_string)
    
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=10000
        )
        logger.info(f"✅ Loaded to {table_name}")
        
    except Exception as e:
        logger.error(f"❌ Load failed: {str(e)}")
        raise
    finally:
        engine.dispose()

def cleanup_green_taxi(**context):
    """Clean up temporary CSV files after successful load"""
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='extract_green_taxi')
    
    if filepath and os.path.exists(filepath):
        try:
            os.remove(filepath)
            logger.info(f"🧹 Cleaned up: {filepath}")
        except Exception as e:
            logger.warning(f"⚠️ Cleanup failed (non-critical): {str(e)}")
    else:
        logger.info("ℹ️ No file to clean up")

VALIDATION_SQL = """
SELECT 
    COUNT(*) as row_count,
    MIN(lpep_pickup_datetime) as earliest_pickup,
    MAX(lpep_pickup_datetime) as latest_pickup
FROM green_taxi_trips;
"""

with DAG(
    dag_id='taxi_etl_green',
    default_args=default_args,
    description='Green Taxi ETL Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['taxi', 'etl', 'green', 'nyc'],
    max_active_runs=1,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_green_taxi',
        python_callable=extract_green_taxi,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform_green_taxi',
        python_callable=transform_green_taxi,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_green_taxi',
        python_callable=load_green_taxi,
        provide_context=True,
    )
    
    validate_task = PostgresOperator(
        task_id='validate_green_taxi',
        postgres_conn_id='postgres_default',
        sql=VALIDATION_SQL,
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_green_taxi',
        python_callable=cleanup_green_taxi,
        provide_context=True,
    )
    
    extract_task >> transform_task >> load_task >> validate_task >> cleanup_task
