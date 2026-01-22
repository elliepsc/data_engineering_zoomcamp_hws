
"""
Configuration management for data pipeline
"""
import os                          # For environment variable access
import logging                     # For messages: logging warnings, info, error)
from pathlib import Path           # For file path manipulations (where to store data)
from dotenv import load_dotenv     # To load .env files

logger = logging.getLogger(__name__)
# basic logging configuration
# logger.info("✓ Data directory created")    # Message normal
# logger.warning("⚠️  Port missing")          # Attention
# logger.error("❌ Connection failed")        # Erreur grave


# Load environment variables from .env file
load_dotenv()

class Config:
    """
    Application configuration with validation.

    Loads from .env file with fail-fast validation.
    """
    
    def __init__(self):
        """Initialize and validate configuration"""
        # Database
        self.DB_USER = os.getenv('POSTGRES_USER', 'postgres')
        self.DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
        self.DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
        self.DB_PORT = os.getenv('POSTGRES_PORT', '5433')
        self.DB_NAME = os.getenv('POSTGRES_DB', 'ny_taxi')
        
        # Data paths
        self.DATA_DIR = Path(os.getenv('DATA_DIR', 'data'))
        self.PARQUET_FILE = os.getenv('PARQUET_FILE', 'green_tripdata_2025-11.parquet')
        self.ZONES_FILE = os.getenv('ZONES_FILE', 'taxi_zone_lookup.csv')
        
        # Download URLs
        self.PARQUET_URL = os.getenv('PARQUET_URL', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet')
        self.ZONES_URL = os.getenv('ZONES_URL', 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')
        
        self._validate()
    
    def _validate(self):
        """
        Validate required configuration (fail fast).
        
        WHY: Better to fail at startup than during pipeline execution.
        """
        # Validate port is integer
        try:
            int(self.DB_PORT)
        except ValueError:
            raise ValueError(f"POSTGRES_PORT must be integer, got: {self.DB_PORT}")
        
        # Ensure data directory exists
        if not self.DATA_DIR.exists():
            logger.warning(f"Data directory not found: {self.DATA_DIR}")
            self.DATA_DIR.mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ Created data directory: {self.DATA_DIR}")
    
    @property
    def database_url(self):
        """
        Build database connection string.
        
        Returns:
            str: PostgreSQL connection URL
        """
        return f'postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}'
    
    @property
    def parquet_path(self):
        """Get full path to parquet file"""
        return self.DATA_DIR / self.PARQUET_FILE
    
    @property
    def zones_path(self):
        """Get full path to zones CSV"""
        return self.DATA_DIR / self.ZONES_FILE

# Global config instance with validation
config = Config()
