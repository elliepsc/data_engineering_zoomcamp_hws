"""
Database connection and operations
Enhanced with idempotence, validation, and utility methods
Objective: Manage all database operations in one place
"""
import logging
from sqlalchemy import create_engine, text, inspect
import pandas as pd
from pipeline.config import config

logger = logging.getLogger(__name__)

class Database:
    """
    Database connection manager with production-ready features.
    
    Features:
    - Connection pooling for performance
    - Idempotence checks (table_exists before operations)
    - Utility methods (get_row_count, create_indexes, etc.)
    """
    
    def __init__(self):
        self.engine = None
        self.inspector = None
    
    def connect(self):
        """
        Create database connection with pooling.
        
        WHY: Connection pooling reuses connections (much faster)
        HOW: SQLAlchemy manages pool automatically
        """
        try:
            logger.info(f"Connecting to database: {config.DB_NAME}")
            
            self.engine = create_engine(
                config.database_url,
                pool_pre_ping=True,  # WHY: Check connection health before using
                pool_size=5,          # WHY: Keep 5 connections ready
                max_overflow=10       # WHY: Allow 10 more if needed
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # WHY: Inspector allows checking tables/columns without queries
            self.inspector = inspect(self.engine)
            
            logger.info("✓ Database connection established")
            return self.engine
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def table_exists(self, table_name):
        """
        Check if table exists in database.
        
        WHY: Enables idempotence checks before operations
        CRITICAL: Used by CLI and tests for safe operations
        
        Args:
            table_name: Name of table to check
            
        Returns:
            bool: True if table exists
        """
        if not self.inspector:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        return table_name in self.inspector.get_table_names(schema="public")
    
    def get_row_count(self, table_name):
        """
        Get number of rows in table.
        
        WHY: Validation and idempotence checks
        
        Args:
            table_name: Name of table
            
        Returns:
            int: Number of rows (0 if error)
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to count rows in '{table_name}': {e}")
            return 0
    
    def get_sample(self, table_name, limit=5):
        """
        Get sample rows from table.
        
        WHY: Quick data inspection without loading full table
        
        Args:
            table_name: Name of table
            limit: Number of rows to return (default: 5)
            
        Returns:
            DataFrame: Sample data or None if error
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logger.error(f"Failed to get sample from '{table_name}': {e}")
            return None
    
    def load_dataframe(self, df, table_name, if_exists='replace', chunksize=10000):
        """
        Load pandas DataFrame into database table.
        
        WHY: Centralize data loading with validation and idempotence
        HOW: Chunked loading prevents OOM on large datasets
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: 'replace', 'append', or 'fail'
            chunksize: Rows per batch (default: 10000)
        """
        try:
            # Validate DataFrame
            if df.empty:
                logger.warning(f"DataFrame is empty, skipping load for '{table_name}'")
                return
            
            # WHY: Warn user if replacing existing data (idempotence awareness)
            if if_exists == 'replace' and self.table_exists(table_name):
                existing_count = self.get_row_count(table_name)
                if existing_count > 0:
                    logger.warning(f"Replacing {existing_count:,} existing rows in '{table_name}'")
            
            logger.info(f"Loading {len(df):,} rows into table '{table_name}'")
            
            # WHY: method='multi' uses faster multi-row INSERT
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists=if_exists, 
                index=False, 
                chunksize=chunksize,
                method='multi'
            )
            # Force commit for tests
            with self.engine.connect() as conn:
                conn.commit()

            logger.info(f"✓ Table '{table_name}' loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load table '{table_name}': {e}")
            raise
    
    def create_indexes(self, table_name, columns):
        """
        Create indexes on table columns for query performance.
        
        WHY: Speeds up queries with WHERE/JOIN on these columns
        WHEN: After data loading (indexes slow down inserts)
        
        Args:
            table_name: Table to index
            columns: List of column names to index
        """
        try:
            with self.engine.connect() as conn:
                for col in columns:
                    index_name = f"idx_{table_name}_{col}"
                    
                    # WHY: Check if index exists (idempotent operation)
                    check_query = text(f"""
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = '{table_name}' 
                        AND indexname = '{index_name}'
                    """)
                    
                    result = conn.execute(check_query)
                    if result.fetchone():
                        logger.info(f"Index '{index_name}' already exists")
                        continue
                    
                    # Create index
                    create_query = text(f"""
                        CREATE INDEX {index_name} 
                        ON {table_name} ({col})
                    """)
                    
                    conn.execute(create_query)
                    conn.commit()
                    logger.info(f"✓ Created index '{index_name}'")
                    
        except Exception as e:
            logger.error(f"Failed to create indexes on '{table_name}': {e}")
            # WHY: Don't raise - indexes are optional optimization
    
    def verify_data(self, table_name):
        """
        Verify data was loaded correctly with detailed stats.
        
        WHY: Validation step after loading
        RETURNS: Row count for compatibility with simple verify
        
        Args:
            table_name: Table to verify
            
        Returns:
            int: Number of rows in table
        """
        try:
            stats = {}
            
            with self.engine.connect() as conn:
                # Row count
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                stats['row_count'] = result.fetchone()[0]
                
                # Column count
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """))
                stats['column_count'] = result.fetchone()[0]
                
                # Table size
                result = conn.execute(text(f"""
                    SELECT pg_size_pretty(pg_total_relation_size('{table_name}'))
                """))
                stats['size'] = result.fetchone()[0]
            
            logger.info(f"Table '{table_name}': {stats['row_count']:,} rows, "
                       f"{stats['column_count']} columns, {stats['size']}")
            
            return stats['row_count']
                
        except Exception as e:
            logger.error(f"Failed to verify table '{table_name}': {e}")
            raise
    
    def drop_table(self, table_name):
        """
        Drop a table.
        
        WHY: Cleanup utility for CLI/testing
        DANGER: Deletes data permanently (use with caution)
        
        Args:
            table_name: Table to drop
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()
            logger.info(f"✓ Dropped table '{table_name}'")
        except Exception as e:
            logger.error(f"Failed to drop table '{table_name}': {e}")
            raise
    
    def list_tables(self):
        """
        List all tables in database.
        
        WHY: Useful for CLI status commands
        
        Returns:
            list: Table names in public schema
        """
        if not self.inspector:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        return self.inspector.get_table_names(schema="public")
    
    def close(self):
        """
        Close database connection and dispose of pool.
        
        WHY: Clean resource cleanup
        """
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
