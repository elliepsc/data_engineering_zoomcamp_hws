"""
GCP Helper Functions for Airflow Data Platform

Provides reusable functions for common GCP operations:
- Google Cloud Storage (upload/download)
- BigQuery (query execution, table operations)

Benefits:
- DRY principle: Write once, use everywhere
- Consistent error handling and retry logic
- Centralized GCP credential management
- Better testability

Usage:
    from utils.gcp_helpers import upload_to_gcs, run_bigquery_query
    
    upload_to_gcs('/tmp/data.parquet', 'my-bucket', 'raw/data.parquet')
    results = run_bigquery_query('SELECT COUNT(*) FROM dataset.table')
"""

import os
from typing import List, Optional, Dict, Any
from google.cloud import storage, bigquery
from google.api_core import retry
from .logger import get_logger

logger = get_logger(__name__)


def get_gcs_client() -> storage.Client:
    """
    Get authenticated GCS client.
    
    Returns:
        Google Cloud Storage client instance
        
    Note:
        Credentials are loaded from GOOGLE_APPLICATION_CREDENTIALS env var
    """
    project_id = os.getenv('GCP_PROJECT_ID')
    return storage.Client(project=project_id)


def get_bigquery_client() -> bigquery.Client:
    """
    Get authenticated BigQuery client.
    
    Returns:
        BigQuery client instance
    """
    project_id = os.getenv('GCP_PROJECT_ID')
    return bigquery.Client(project=project_id)


@retry.Retry(predicate=retry.if_exception_type(Exception))
def upload_to_gcs(
    local_path: str,
    bucket_name: str,
    blob_name: str,
    content_type: Optional[str] = None
) -> str:
    """
    Upload a file to Google Cloud Storage with automatic retry.
    
    Args:
        local_path: Path to local file
        bucket_name: GCS bucket name (without gs:// prefix)
        blob_name: Destination path in bucket (e.g., 'raw/data.parquet')
        content_type: Optional MIME type
        
    Returns:
        GCS URI (gs://bucket/path)
        
    Example:
        >>> uri = upload_to_gcs('/tmp/data.parquet', 'my-bucket', 'raw/2019-01/data.parquet')
        >>> print(uri)
        'gs://my-bucket/raw/2019-01/data.parquet'
    """
    logger.info(
        f"Uploading {local_path} to gs://{bucket_name}/{blob_name}",
        extra={'local_path': local_path, 'bucket': bucket_name, 'blob': blob_name}
    )
    
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if content_type:
            blob.content_type = content_type
        
        blob.upload_from_filename(local_path)
        
        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        logger.info(f"Upload successful: {gcs_uri}")
        
        return gcs_uri
        
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        raise


@retry.Retry(predicate=retry.if_exception_type(Exception))
def download_from_gcs(
    bucket_name: str,
    blob_name: str,
    local_path: str
) -> str:
    """
    Download a file from Google Cloud Storage with automatic retry.
    
    Args:
        bucket_name: GCS bucket name
        blob_name: Source path in bucket
        local_path: Destination local path
        
    Returns:
        Local file path
        
    Example:
        >>> path = download_from_gcs('my-bucket', 'raw/data.parquet', '/tmp/data.parquet')
    """
    logger.info(
        f"Downloading gs://{bucket_name}/{blob_name} to {local_path}",
        extra={'bucket': bucket_name, 'blob': blob_name, 'local_path': local_path}
    )
    
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.download_to_filename(local_path)
        
        logger.info(f"Download successful: {local_path}")
        return local_path
        
    except Exception as e:
        logger.error(f"Download failed: {str(e)}", exc_info=True)
        raise


def run_bigquery_query(
    query: str,
    project_id: Optional[str] = None,
    timeout: int = 300
) -> List[Dict[str, Any]]:
    """
    Execute a BigQuery query and return results.
    
    Args:
        query: SQL query string
        project_id: GCP project ID (defaults to env var)
        timeout: Query timeout in seconds
        
    Returns:
        List of row dictionaries
        
    Example:
        >>> results = run_bigquery_query('SELECT COUNT(*) as cnt FROM dataset.table')
        >>> print(results[0]['cnt'])
        1000
    """
    if not project_id:
        project_id = os.getenv('GCP_PROJECT_ID')
    
    logger.info(f"Executing BigQuery query in project {project_id}")
    
    try:
        client = get_bigquery_client()
        
        query_job = client.query(query, project=project_id)
        results = query_job.result(timeout=timeout)
        
        rows = [dict(row) for row in results]
        
        logger.info(f"Query returned {len(rows)} rows")
        return rows
        
    except Exception as e:
        logger.error(f"BigQuery query failed: {str(e)}", exc_info=True)
        raise


def create_bigquery_dataset(
    dataset_id: str,
    location: str = "EU",
    exists_ok: bool = True
) -> None:
    """
    Create a BigQuery dataset if it doesn't exist.
    
    Args:
        dataset_id: Dataset name
        location: Dataset location (EU, US, etc.)
        exists_ok: Don't raise error if dataset exists
        
    Example:
        >>> create_bigquery_dataset('raw_data', location='EU')
    """
    logger.info(f"Creating BigQuery dataset: {dataset_id}")
    
    try:
        client = get_bigquery_client()
        
        dataset_ref = f"{client.project}.{dataset_id}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        
        dataset = client.create_dataset(dataset, exists_ok=exists_ok)
        
        logger.info(f"Dataset {dataset_id} ready")
        
    except Exception as e:
        logger.error(f"Dataset creation failed: {str(e)}", exc_info=True)
        raise


def delete_gcs_blob(bucket_name: str, blob_name: str) -> None:
    """
    Delete a blob from GCS.
    
    Args:
        bucket_name: GCS bucket name
        blob_name: Blob path to delete
    """
    logger.info(f"Deleting gs://{bucket_name}/{blob_name}")
    
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.delete()
        
        logger.info("Deletion successful")
        
    except Exception as e:
        logger.error(f"Deletion failed: {str(e)}", exc_info=True)
        raise
