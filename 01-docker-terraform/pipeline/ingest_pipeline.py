"""
Main data ingestion pipeline script for NYC Taxi data.
Clean, modular, production-ready approach
"""
import logging
import sys
from pipeline.database import Database
from pipeline.data_loader import DataLoader
from pipeline.config import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def run_pipeline():
    """Execute the data ingestion pipeline"""
    
    logger.info("=" * 60)
    logger.info("NYC Taxi Data Ingestion Pipeline")
    logger.info("=" * 60)
    
    try:
        # Initialize components
        db = Database()
        loader = DataLoader()
        
        # Ensure data directory exists
        loader.ensure_data_dir()
        
        # Connect to database
        db.connect()
        
        # Load green taxi trips data
        logger.info("\n--- Loading Green Taxi Trips ---")
        df_trips = loader.load_parquet(config.parquet_path)
        db.load_dataframe(df_trips, 'green_taxi_trips')
        
        # Load taxi zones data
        logger.info("\n--- Loading Taxi Zones ---")
        df_zones = loader.load_csv(config.zones_path)
        db.load_dataframe(df_zones, 'taxi_zones')
        
        # Verify data
        logger.info("\n--- Verification ---")
        trips_count = db.verify_data('green_taxi_trips')
        zones_count = db.verify_data('taxi_zones')
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("✅ Pipeline completed successfully!")
        logger.info("=" * 60)
        logger.info(f"Loaded {trips_count:,} taxi trips")
        logger.info(f"Loaded {zones_count:,} taxi zones")
        logger.info("\nNext steps:")
        logger.info("1. Access pgAdmin at http://localhost:8080")
        logger.info("   - Email: pgadmin@pgadmin.com")
        logger.info("   - Password: pgadmin")
        logger.info("2. Connect to database:")
        logger.info("   - Host: db")
        logger.info("   - Port: 5432")
        logger.info("   - Database: ny_taxi")
        logger.info("   - Username: postgres")
        
        # Close connection
        db.close()
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"\n❌ Data files not found")
        logger.error("Please download the required files:")
        logger.error(f"  wget {config.PARQUET_URL} -P {config.DATA_DIR}")
        logger.error(f"  wget {config.ZONES_URL} -P {config.DATA_DIR}")
        return 1
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}")
        logger.error("\nTroubleshooting:")
        logger.error("1. Ensure Docker containers are running: docker compose up -d")
        logger.error("2. Verify database is accessible")
        logger.error("3. Check .env configuration")
        return 1

if __name__ == "__main__":
    sys.exit(run_pipeline())
