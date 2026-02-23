"""
Configuration Loader for Airflow Data Platform

Centralized configuration management to avoid hardcoded values in DAGs.

Benefits:
- Single source of truth for all configuration
- Easy environment switching (dev/staging/prod)
- Type-safe configuration access
- Better security (no credentials in code)

Usage:
    from utils.config_loader import load_config, get_gcp_config
    
    config = load_config()
    project_id = config['gcp']['project_id']
    
    # Or use helper functions
    gcp_config = get_gcp_config()
"""

import os
from typing import Dict, Any


def load_config() -> Dict[str, Any]:
    """
    Load all configuration from environment variables.
    
    Returns:
        Dictionary containing all configuration values
        
    Example:
        >>> config = load_config()
        >>> print(config['gcp']['project_id'])
        'my-project-123'
    """
    return {
        'gcp': {
            'project_id': os.getenv('GCP_PROJECT_ID'),
            'region': os.getenv('GCP_REGION', 'europe-west1'),
            'credentials_path': os.getenv('GCP_CREDENTIALS_PATH', './config/gcp-credentials.json'),
        },
        'gcs': {
            'bucket_name': os.getenv('GCS_BUCKET_NAME'),
            'raw_prefix': 'raw',
            'staging_prefix': 'staging',
        },
        'bigquery': {
            'dataset_raw': os.getenv('BQ_DATASET_RAW', 'raw_data'),
            'dataset_staging': os.getenv('BQ_DATASET_STAGING', 'staging'),
            'dataset_analytics': os.getenv('BQ_DATASET_ANALYTICS', 'analytics'),
        },
        'data': {
            'start_date': os.getenv('DATA_START_DATE', '2019-01-01'),
            'end_date': os.getenv('DATA_END_DATE', '2020-12-31'),
            'taxi_type': os.getenv('TAXI_TYPE', 'green'),
        },
        'logging': {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'format': os.getenv('LOG_FORMAT', 'json'),
        },
    }


def get_gcp_config() -> Dict[str, str]:
    """
    Get GCP-specific configuration.
    
    Returns:
        Dictionary with GCP settings
    """
    config = load_config()
    return config['gcp']


def get_bigquery_config() -> Dict[str, str]:
    """
    Get BigQuery-specific configuration.
    
    Returns:
        Dictionary with BigQuery dataset names
    """
    config = load_config()
    return config['bigquery']


def get_gcs_config() -> Dict[str, str]:
    """
    Get GCS-specific configuration.
    
    Returns:
        Dictionary with GCS bucket settings
    """
    config = load_config()
    return config['gcs']


def get_data_config() -> Dict[str, str]:
    """
    Get data processing configuration.
    
    Returns:
        Dictionary with data source settings
    """
    config = load_config()
    return config['data']


def validate_config() -> bool:
    """
    Validate that all required configuration is present.
    
    Returns:
        True if configuration is valid
        
    Raises:
        ValueError: If required configuration is missing
    """
    config = load_config()
    
    required_fields = [
        ('gcp', 'project_id'),
        ('gcs', 'bucket_name'),
    ]
    
    missing = []
    for section, field in required_fields:
        if not config.get(section, {}).get(field):
            missing.append(f"{section}.{field}")
    
    if missing:
        raise ValueError(f"Missing required configuration: {', '.join(missing)}")
    
    return True
