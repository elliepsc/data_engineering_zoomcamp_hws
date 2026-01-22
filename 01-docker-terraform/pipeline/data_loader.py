"""
Data loading utilities
"""
import logging
import pandas as pd
from pathlib import Path
from pipeline.config import config

logger = logging.getLogger(__name__)

class DataLoader:
    """Handle data file loading"""
    
    @staticmethod
    def normalize_columns(df):
        """
        Normalize column names to lowercase snake_case.
        
        WHY: PostgreSQL lowercases unquoted identifiers.
             Prevents need to quote in SQL: SELECT "PULocationID" vs pulocationid
        
        CRITICAL for dbt: dbt queries won't need quotes everywhere
        """
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        logger.info(f"✓ Normalized {len(df.columns)} column names to lowercase")
        return df

    @staticmethod
    def load_parquet(filepath):
        """Load parquet file into DataFrame"""
        try:
            logger.info(f"Loading parquet file: {filepath}")
            df = pd.read_parquet(filepath)
            df = DataLoader.normalize_columns(df)    # Normalize columns
            logger.info(f"✓ Loaded {len(df):,} records with {len(df.columns)} columns")
            return df
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            logger.info(f"Download with: wget {config.PARQUET_URL} -P {config.DATA_DIR}")
            raise
        except Exception as e:
            logger.error(f"Failed to load parquet: {e}")
            raise
    
    @staticmethod
    def load_csv(filepath):
        """Load CSV file into DataFrame"""
        try:
            logger.info(f"Loading CSV file: {filepath}")
            df = pd.read_csv(filepath)
            df = DataLoader.normalize_columns(df)    # Normalize columns
            logger.info(f"✓ Loaded {len(df):,} records with {len(df.columns)} columns")
            return df
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            logger.info(f"Download with: wget {config.ZONES_URL} -P {config.DATA_DIR}")
            raise
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise
    
    @staticmethod
    def ensure_data_dir():
        """Ensure data directory exists"""
        config.DATA_DIR.mkdir(exist_ok=True)
        logger.info(f"Data directory ready: {config.DATA_DIR}")
