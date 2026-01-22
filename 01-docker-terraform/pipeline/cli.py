"""
Command Line Interface for NYC Taxi Pipeline
Using Click for flexible execution
"""
import logging
import click
from pathlib import Path
from pipeline.database import Database
from pipeline.data_loader import DataLoader
from pipeline.config import config

logger = logging.getLogger(__name__)

@click.group()
def cli():
    """NYC Taxi Data Pipeline CLI"""
    pass

@cli.command()
@click.option('--pg-host', envvar='POSTGRES_HOST', default=None, 
              help='PostgreSQL host (overrides .env)')
@click.option('--pg-port', envvar='POSTGRES_PORT', default=None, type=int,
              help='PostgreSQL port (overrides .env)')
@click.option('--pg-user', envvar='POSTGRES_USER', default=None,
              help='PostgreSQL user (overrides .env)')
@click.option('--pg-password', envvar='POSTGRES_PASSWORD', default=None,
              help='PostgreSQL password (overrides .env)')
@click.option('--pg-db', envvar='POSTGRES_DB', default=None,
              help='PostgreSQL database (overrides .env)')
@click.option('--skip-trips', is_flag=True, help='Skip loading trip data')
@click.option('--skip-zones', is_flag=True, help='Skip loading zone data')
@click.option('--force', is_flag=True, help='Force reload even if data exists')
def ingest(pg_host, pg_port, pg_user, pg_password, pg_db, skip_trips, skip_zones, force):
    """
    Ingest NYC taxi data into PostgreSQL
    
    Examples:
        # Using .env configuration
        python -m pipeline.cli ingest
        
        # Override specific settings
        python -m pipeline.cli ingest --pg-host=localhost --pg-port=5433
        
        # Load only zones
        python -m pipeline.cli ingest --skip-trips
        
        # Force reload
        python -m pipeline.cli ingest --force
    """
    # Override config if CLI options provided
    if pg_host:
        config.DB_HOST = pg_host
    if pg_port:
        config.DB_PORT = str(pg_port)
    if pg_user:
        config.DB_USER = pg_user
    if pg_password:
        config.DB_PASSWORD = pg_password
    if pg_db:
        config.DB_NAME = pg_db
    
    logger.info("=" * 60)
    logger.info("NYC Taxi Data Ingestion Pipeline")
    logger.info("=" * 60)
    logger.info(f"Database: {config.DB_NAME}@{config.DB_HOST}:{config.DB_PORT}")
    
    try:
        # Initialize components
        db = Database()
        loader = DataLoader()
        
        # Ensure data directory
        loader.ensure_data_dir()
        
        # Connect to database
        db.connect()
        
        # Check if data already exists
        if not force:
            if not skip_trips and db.table_exists('green_taxi_trips'):
                count = db.get_row_count('green_taxi_trips')
                if count > 0:
                    logger.warning(f"⚠️  Table 'green_taxi_trips' already has {count:,} rows")
                    if not click.confirm("Do you want to replace it?"):
                        skip_trips = True
            
            if not skip_zones and db.table_exists('taxi_zones'):
                count = db.get_row_count('taxi_zones')
                if count > 0:
                    logger.warning(f"⚠️  Table 'taxi_zones' already has {count:,} rows")
                    if not click.confirm("Do you want to replace it?"):
                        skip_zones = True
        
        # Load trips
        if not skip_trips:
            logger.info("\n--- Loading Green Taxi Trips ---")
            df_trips = loader.load_parquet(config.parquet_path)
            db.load_dataframe(df_trips, 'green_taxi_trips')
            db.create_indexes('green_taxi_trips', ['lpep_pickup_datetime', 'pulocationid', 'dolocationid'])
        else:
            logger.info("\n--- Skipping trips data (--skip-trips) ---")
        
        # Load zones
        if not skip_zones:
            logger.info("\n--- Loading Taxi Zones ---")
            df_zones = loader.load_csv(config.zones_path)
            db.load_dataframe(df_zones, 'taxi_zones')
            db.create_indexes('taxi_zones', ['locationid'])
        else:
            logger.info("\n--- Skipping zones data (--skip-zones) ---")
        
        # Verify
        if not (skip_trips and skip_zones):
            logger.info("\n--- Verification ---")
            if not skip_trips:
                trips_count = db.verify_data('green_taxi_trips')
            if not skip_zones:
                zones_count = db.verify_data('taxi_zones')
        
        # Success
        logger.info("\n" + "=" * 60)
        logger.info("✅ Pipeline completed successfully!")
        logger.info("=" * 60)
        
        db.close()
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}")
        raise click.ClickException(str(e))

@cli.command()
@click.argument('table_name')
def verify(table_name):
    """
    Verify data in a table
    
    Example:
        python -m pipeline.cli verify green_taxi_trips
    """
    try:
        db = Database()
        db.connect()
        
        if not db.table_exists(table_name):
            raise click.ClickException(f"Table '{table_name}' does not exist")
        
        count = db.get_row_count(table_name)
        logger.info(f"✅ Table '{table_name}' contains {count:,} rows")
        
        # Show sample
        sample = db.get_sample(table_name, limit=5)
        logger.info(f"\nSample data (first 5 rows):")
        logger.info(sample)
        
        db.close()
        
    except Exception as e:
        raise click.ClickException(str(e))

@cli.command()
def check_connection():
    """
    Test database connection
    
    Example:
        python -m pipeline.cli check-connection
    """
    try:
        db = Database()
        db.connect()
        logger.info(f"✅ Connected to {config.DB_NAME}@{config.DB_HOST}:{config.DB_PORT}")
        db.close()
    except Exception as e:
        raise click.ClickException(f"❌ Connection failed: {e}")

@cli.command()
@click.argument('table_name')
@click.confirmation_option(prompt='Are you sure you want to drop this table?')
def drop_table(table_name):
    """
    Drop a table (with confirmation)
    
    Example:
        python -m pipeline.cli drop-table green_taxi_trips
    """
    try:
        db = Database()
        db.connect()
        db.drop_table(table_name)
        logger.info(f"✅ Table '{table_name}' dropped")
        db.close()
    except Exception as e:
        raise click.ClickException(str(e))

@cli.command()
def list_tables():
    """
    List all tables in the database
    
    Example:
        python -m pipeline.cli list-tables
    """
    try:
        db = Database()
        db.connect()
        tables = db.list_tables()
        
        logger.info(f"\n📋 Tables in '{config.DB_NAME}':")
        logger.info("=" * 60)
        
        if tables:
            for table in tables:
                count = db.get_row_count(table)
                logger.info(f"  • {table}: {count:,} rows")
        else:
            logger.info("  (no tables)")
        
        db.close()
        
    except Exception as e:
        raise click.ClickException(str(e))

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    cli()