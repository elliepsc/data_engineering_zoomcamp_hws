# =============================================================================
# Yellow Taxi ETL Pipeline
# =============================================================================
# Pipeline: Extract → Transform → Load → Validate
# Schedule: Daily (@daily)
# Data Source: GitHub NYC TLC data releases
# Target: Postgres table yellow_taxi_trips
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

# Configuration
TAXI_TYPE = 'yellow'
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

# =============================================================================
# TASK 1: EXTRACT - Download CSV from GitHub
# =============================================================================

def extract_yellow_taxi(**context):
    """
    Download Yellow Taxi CSV from GitHub
    
    NOTE: Files are compressed (.csv.gz) on GitHub
    We download the .gz file and decompress it
    
    Returns:
        str: Local filepath of downloaded CSV
    """
    import gzip
    import shutil
    
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    # Files are .csv.gz on GitHub
    filename_gz = f"{TAXI_TYPE}_tripdata_{year}-{month:02d}.csv.gz"
    filename_csv = f"{TAXI_TYPE}_tripdata_{year}-{month:02d}.csv"
    url = f"{CSV_BASE_URL}/{TAXI_TYPE}/{filename_gz}"
    
    logger.info(f"📥 Downloading {filename_gz}")
    logger.info(f"URL: {url}")
    
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
        logger.info(f"✅ Download complete")
        logger.info(f"📦 Compressed size: {gz_size_mb:.1f} MiB")
        
        # Decompress .gz to .csv
        logger.info(f"📦 Decompressing...")
        with gzip.open(local_filepath_gz, 'rb') as f_in:
            with open(local_filepath_csv, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Log file size (HOMEWORK Q1: Yellow Dec 2020)
        file_size_mb = os.path.getsize(local_filepath_csv) / (1024 * 1024)
        logger.info(f"✅ Decompression complete")
        logger.info(f"📦 File size: {file_size_mb:.1f} MiB")
        logger.info(f"💾 Saved to: {local_filepath_csv}")
        
        # Clean up .gz file
        os.remove(local_filepath_gz)
        
        return local_filepath_csv
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Download failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Decompression failed: {str(e)}")
        raise

# =============================================================================
# TASK 2: TRANSFORM - Clean and normalize data
# =============================================================================

def transform_yellow_taxi(**context):
    """
    Transform: lowercase columns, drop nulls, add year/month partitions
    
    Returns:
        pd.DataFrame: Cleaned dataframe
    """
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='extract_yellow_taxi')
    
    logger.info(f"🔄 Transforming {filepath}")
    
    df = pd.read_csv(filepath)
    logger.info(f"📊 Original: {len(df)} rows, {len(df.columns)} columns")
    
    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()
    
    # Drop rows with null pickup_datetime
    original_count = len(df)
    df = df.dropna(subset=['tpep_pickup_datetime'])
    dropped_count = original_count - len(df)
    if dropped_count > 0:
        logger.warning(f"⚠️ Dropped {dropped_count} rows with null pickup_datetime")
    
    # Convert date columns
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    # Add year/month for partitioning (needed for SQL queries)
    df['year'] = df['tpep_pickup_datetime'].dt.year
    df['month'] = df['tpep_pickup_datetime'].dt.month
    
    logger.info(f"✅ Transform complete: {len(df)} rows")
    
    return df

# =============================================================================
# TASK 3: LOAD - Load to Postgres
# =============================================================================

def load_yellow_taxi(**context):
    """
    Load dataframe to Postgres using pandas.to_sql()
    
    Idempotence: if_exists='replace' ensures re-runnable pipeline
    """
    ti = context['ti']
    df = ti.xcom_pull(task_ids='transform_yellow_taxi')
    
    if df is None or len(df) == 0:
        raise ValueError("❌ No data from transform task")
    
    logger.info(f"📤 Loading {len(df)} rows to Postgres")
    
    table_name = f"{TAXI_TYPE}_taxi_trips"
    
    # Create SQLAlchemy engine
    conn_string = (
        f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}"
        f"@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
    )
    engine = create_engine(conn_string)
    
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # Idempotent: replace existing data
            index=False,
            method='multi',
            chunksize=10000
        )
        
        logger.info(f"✅ Loaded {len(df)} rows to {table_name}")
        
        return {
            'table': table_name,
            'rows_loaded': len(df),
            'execution_date': context['execution_date'].strftime('%Y-%m-%d')
        }
        
    except Exception as e:
        logger.error(f"❌ Load failed: {str(e)}")
        raise
    finally:
        engine.dispose()

# =============================================================================
# TASK 4: VALIDATE - Check data quality
# =============================================================================

VALIDATION_SQL = """
SELECT 
    COUNT(*) as row_count,
    MIN(tpep_pickup_datetime) as earliest_pickup,
    MAX(tpep_pickup_datetime) as latest_pickup
FROM yellow_taxi_trips;
"""

# =============================================================================
# TASK 5: CLEANUP - Remove temporary CSV files
# =============================================================================

def cleanup_yellow_taxi(**context):
    """
    Clean up temporary CSV files after successful load
    
    Removes CSV file from /tmp/taxi_data to save disk space
    """
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='extract_yellow_taxi')
    
    if filepath and os.path.exists(filepath):
        try:
            os.remove(filepath)
            logger.info(f"🧹 Cleaned up: {filepath}")
        except Exception as e:
            logger.warning(f"⚠️ Cleanup failed (non-critical): {str(e)}")
    else:
        logger.info("ℹ️ No file to clean up")

# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id='taxi_etl_yellow',
    default_args=default_args,
    description='Yellow Taxi ETL: Extract from GitHub → Transform → Load to Postgres',
    schedule_interval='@daily',
    start_date=datetime(2020, 1, 1),
    catchup=False,  # Set to True for backfill
    tags=['taxi', 'etl', 'yellow', 'nyc'],
    max_active_runs=1,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_yellow_taxi',
        python_callable=extract_yellow_taxi,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform_yellow_taxi',
        python_callable=transform_yellow_taxi,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_yellow_taxi',
        python_callable=load_yellow_taxi,
        provide_context=True,
    )
    
    validate_task = PostgresOperator(
        task_id='validate_yellow_taxi',
        postgres_conn_id='postgres_default',
        sql=VALIDATION_SQL,
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_yellow_taxi',
        python_callable=cleanup_yellow_taxi,
        provide_context=True,
    )
    
    # Pipeline: Extract → Transform → Load → Validate → Cleanup
    extract_task >> transform_task >> load_task >> validate_task >> cleanup_task
