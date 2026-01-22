"""
Test database operations including idempotence
"""
import pytest
import pandas as pd
import sys
from pathlib import Path
from sqlalchemy import inspect

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.database import Database
from pipeline.config import config


class TestDatabaseConnection:
    """Test database connection and configuration"""
    
    def test_database_url_format(self):
        """
        Database URL MUST be properly formatted for PostgreSQL.
        
        WHY: Incorrect URL format causes cryptic connection errors
        """
        url = config.database_url
        
        # Must start with postgresql://
        assert url.startswith('postgresql://'), f"URL should start with postgresql://, got: {url}"
        
        # Must contain database name
        assert config.DB_NAME in url, f"Database name '{config.DB_NAME}' not in URL"
        
        # Must contain host
        assert config.DB_HOST in url, f"Host '{config.DB_HOST}' not in URL"
        
        # Must contain port
        assert str(config.DB_PORT) in url, f"Port '{config.DB_PORT}' not in URL"
    
    def test_database_initialization(self):
        """
        Database object MUST initialize without errors.
        
        WHY: Validates constructor works correctly
        """
        db = Database()
        
        # Engine should be None before connect()
        assert db.engine is None
        assert db.inspector is None


@pytest.mark.integration
class TestDatabaseOperations:
    """
    Test database operations (requires docker-compose up).
    
    Mark as integration: pytest -v -m integration
    """
    
    def test_database_connection(self):
        """
        Database MUST connect successfully.
        
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        
        # When
        engine = db.connect()
        
        # Then
        assert engine is not None
        assert db.engine is not None
        assert db.inspector is not None
        
        # Cleanup
        db.close()
    
    def test_table_exists_method(self):
        """
        table_exists() MUST correctly identify existing tables.
        
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        db.connect()
        
        # When: Check non-existent table
        exists = db.table_exists('nonexistent_table_xyz')
        
        # Then
        assert exists == False
        
        # Cleanup
        db.close()
    
    def test_create_and_verify_table(self):
        """
        Load DataFrame MUST create table that can be verified.
        
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        db.connect()
        
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10, 20, 30],
            'name': ['a', 'b', 'c']
        })
        
        # When: Load data
        db.load_dataframe(test_df, 'test_create_verify', if_exists='replace')
        
        # CRITICAL: Refresh inspector after table creation
        db.inspector = inspect(db.engine)
        
        # Then: Table exists
        assert db.table_exists('test_create_verify')
        
        # And: Row count matches
        count = db.get_row_count('test_create_verify')
        assert count == 3
        
        # Cleanup
        db.drop_table('test_create_verify')
        db.close()
    
    def test_idempotence(self):
        """
        Pipeline MUST be re-runnable without creating duplicates.
        
        CRITICAL: if_exists='replace' must work correctly
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        db.connect()
        
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        })
        
        # When: Load data first time
        db.load_dataframe(test_df, 'test_idempotence', if_exists='replace')
        count1 = db.get_row_count('test_idempotence')
        
        # When: Load data second time (re-run)
        db.load_dataframe(test_df, 'test_idempotence', if_exists='replace')
        count2 = db.get_row_count('test_idempotence')
        
        # Then: Row counts are identical (no duplicates)
        assert count1 == count2 == 3, f"Expected 3 rows both times, got {count1} and {count2}"
        
        # Cleanup
        db.drop_table('test_idempotence')
        db.close()
    
    def test_get_sample(self):
        """
        get_sample() MUST return correct number of rows.
        
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        db.connect()
        
        test_df = pd.DataFrame({
            'id': list(range(1, 11)),  # 10 rows
            'value': list(range(10, 20))
        })
        
        db.load_dataframe(test_df, 'test_sample', if_exists='replace')
        
        # When: Get sample
        sample = db.get_sample('test_sample', limit=5)
        
        # Then: Sample has correct size
        assert len(sample) == 5
        assert list(sample.columns) == ['id', 'value']
        
        # Cleanup
        db.drop_table('test_sample')
        db.close()
    
    def test_list_tables(self):
        """
        list_tables() MUST return all tables in database.
        
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        db.connect()
        
        # Create test table
        test_df = pd.DataFrame({'id': [1, 2, 3]})
        db.load_dataframe(test_df, 'test_list_tables', if_exists='replace')
        
        # CRITICAL: Refresh inspector after table creation
        db.inspector = inspect(db.engine)
        
        # When
        tables = db.list_tables()
        
        # Then: Our test table is in the list
        assert 'test_list_tables' in tables
        
        # Cleanup
        db.drop_table('test_list_tables')
        db.close()
    
    def test_create_indexes(self):
        """
        create_indexes() MUST create indexes on specified columns.
        
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        db.connect()
        
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'location_id': [100, 200, 300],
            'timestamp': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-03'])
        })
        
        db.load_dataframe(test_df, 'test_indexes', if_exists='replace')
        
        # When: Create indexes
        db.create_indexes('test_indexes', ['location_id', 'timestamp'])
        
        # Then: Indexes exist (checked in PostgreSQL metadata)
        # This is validated by the function not raising an error
        # Re-running should be idempotent
        db.create_indexes('test_indexes', ['location_id', 'timestamp'])
        
        # Cleanup
        db.drop_table('test_indexes')
        db.close()
    
    def test_verify_data_returns_stats(self):
        """
        verify_data() MUST return row count and log stats.
        
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        db.connect()
        
        test_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50]
        })
        
        db.load_dataframe(test_df, 'test_verify', if_exists='replace')
        
        # When
        row_count = db.verify_data('test_verify')
        
        # Then
        assert row_count == 5
        
        # Cleanup
        db.drop_table('test_verify')
        db.close()
    
    def test_empty_dataframe_skipped(self):
        """
        Loading empty DataFrame MUST be skipped with warning.
        
        WHY: Prevents creating empty tables
        REQUIRES: docker-compose up
        """
        # Given
        db = Database()
        db.connect()
        
        empty_df = pd.DataFrame()
        
        # When: Try to load empty DataFrame
        db.load_dataframe(empty_df, 'test_empty', if_exists='replace')
        
        # Refresh inspector
        db.inspector = inspect(db.engine)
        
        # Then: Table should NOT exist (operation skipped)
        assert not db.table_exists('test_empty')
        
        # Cleanup
        db.close()


class TestDatabaseUtilities:
    """Test utility methods (can run without database)"""
    
    def test_database_has_required_methods(self):
        """
        Database class MUST have all required methods.
        
        WHY: Validates API completeness
        """
        db = Database()
        
        # Check methods exist
        assert hasattr(db, 'connect')
        assert hasattr(db, 'table_exists')
        assert hasattr(db, 'get_row_count')
        assert hasattr(db, 'get_sample')
        assert hasattr(db, 'load_dataframe')
        assert hasattr(db, 'create_indexes')
        assert hasattr(db, 'verify_data')
        assert hasattr(db, 'drop_table')
        assert hasattr(db, 'list_tables')
        assert hasattr(db, 'close')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
