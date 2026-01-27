# =============================================================================
# Green Taxi ETL Pipeline - CORRECTED VERSION (ChatGPT Best Practices)
# =============================================================================
# Similar to Yellow Taxi but with lpep_* datetime columns instead of tpep_*
# 
# CHANGES FROM ORIGINAL:
# 1. schedule=None instead of @daily → NO automatic 2026 runs
# 2. if_exists='append' + DELETE partition → idempotent, no data loss
# 3. Validation targets specific partition → accurate per-month validation
# 4. logical_date instead of execution_date → Airflow 2.x best practice
# =============================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
import requests
from sqlalchemy import create_engine, text
import gzip
import shutil

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
    
    CHANGED: Uses logical_date instead of execution_date (Airflow 2.x)
    """
    # CHANGED: logical_date instead of execution_date
    execution_date = context['logical_date']
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
        response.raise_for_status()  # Will raise 404 if file doesn't exist
        
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
    """
    Transform Green taxi data
    
    NOTE: Green taxi uses lpep_pickup_datetime, not tpep_pickup_datetime
    CHANGED: Use logical_date for partitioning (more reliable)
    """
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='extract_green_taxi')
    
    # CHANGED: logical_date instead of execution_date
    execution_date = context['logical_date']
    year = execution_date.year
    month = execution_date.month
    
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
    
    # CHANGED: Use logical_date for partitioning (more reliable)
    df['year'] = year
    df['month'] = month
    
    logger.info(f"✅ Transform complete: {len(df)} rows")
    return df

def load_green_taxi(**context):
    """
    Load dataframe to Postgres with idempotent partition overwrite
    
    CHANGED: 
    - append instead of replace (preserves other months)
    - DELETE partition before INSERT (idempotent, rejouable)
    """
    ti = context['ti']
    df = ti.xcom_pull(task_ids='transform_green_taxi')
    
    # CHANGED: logical_date instead of execution_date
    execution_date = context['logical_date']
    year = execution_date.year
    month = execution_date.month
    
    if df is None or len(df) == 0:
        raise ValueError("❌ No data from transform")
    
    logger.info(f"📤 Loading {len(df)} rows (year={year}, month={month})")
    
    table_name = f"{TAXI_TYPE}_taxi_trips"
    conn_string = (
        f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}"
        f"@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
    )
    engine = create_engine(conn_string)
    
    try:
        with engine.begin() as conn:
            # ADDED: Idempotent - delete only current partition before loading
            logger.info(f"🗑️ Deleting existing data for {year}-{month:02d}")
            result = conn.execute(
                text("DELETE FROM green_taxi_trips WHERE year = :y AND month = :m"),
                {"y": year, "m": month}
            )
            logger.info(f"🗑️ Deleted {result.rowcount} existing rows")
            
            # CHANGED: append instead of replace
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists='append',  # CHANGED: Was 'replace' (lost data!)
                index=False,
                method='multi',
                chunksize=10000
            )
        
        logger.info(f"✅ Loaded {len(df)} rows to {table_name}")
        
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

# CHANGED: Validate only current partition, not entire table
VALIDATION_SQL = """
SELECT 
    COUNT(*) as row_count,
    MIN(lpep_pickup_datetime) as earliest_pickup,
    MAX(lpep_pickup_datetime) as latest_pickup
FROM green_taxi_trips
WHERE year = {{ logical_date.year }}
  AND month = {{ logical_date.month }};
"""

with DAG(
    dag_id='taxi_etl_green',
    default_args=default_args,
    description='Green Taxi ETL Pipeline',
    schedule=None,  # CHANGED: Was '@daily', now MANUAL TRIGGER ONLY (no 2026 runs!)
    start_date=datetime(2020, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,  # Start paused by default
    tags=['taxi', 'etl', 'green', 'nyc'],
    max_active_runs=1,  # ADDED: Prevent parallel runs
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_green_taxi',
        python_callable=extract_green_taxi,
    )
    
    transform_task = PythonOperator(
        task_id='transform_green_taxi',
        python_callable=transform_green_taxi,
    )
    
    load_task = PythonOperator(
        task_id='load_green_taxi',
        python_callable=load_green_taxi,
    )
    
    validate_task = PostgresOperator(
        task_id='validate_green_taxi',
        postgres_conn_id='postgres_default',
        sql=VALIDATION_SQL,  # CHANGED: Now validates only current partition
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_green_taxi',
        python_callable=cleanup_green_taxi,
    )
    
    extract_task >> transform_task >> load_task >> validate_task >> cleanup_task
