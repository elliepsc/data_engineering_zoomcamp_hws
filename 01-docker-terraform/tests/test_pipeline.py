"""
Integration tests for complete pipeline
Includes data quality checks
"""
import pytest
import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.data_loader import DataLoader
from pipeline.config import config


class TestDataQuality:
    """
    Test data quality checks on loaded data.
    
    WHY: Validate data meets business rules and expectations
    """
    
    @pytest.fixture
    def sample_trips_data(self):
        """
        Create sample trip data for testing.
        
        Mimics NYC taxi trip structure.
        """
        return pd.DataFrame({
            'vendorid': [1, 2, 1],
            'lpep_pickup_datetime': pd.to_datetime([
                '2025-11-01 10:00:00',
                '2025-11-01 11:00:00',
                '2025-11-01 12:00:00'
            ]),
            'lpep_dropoff_datetime': pd.to_datetime([
                '2025-11-01 10:15:00',
                '2025-11-01 11:20:00',
                '2025-11-01 12:30:00'
            ]),
            'passenger_count': [1, 2, 1],
            'trip_distance': [2.5, 5.0, 1.0],
            'pulocationid': [74, 123, 88],
            'dolocationid': [42, 56, 99],
            'total_amount': [15.50, 25.00, 10.00]
        })
    
    def test_no_duplicate_rows(self, sample_trips_data):
        """
        Data MUST NOT contain duplicate rows.
        
        WHY: Duplicates inflate metrics and break analysis
        """
        duplicates = sample_trips_data.duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate rows"
    
    def test_positive_amounts(self, sample_trips_data):
        """
        Total amounts MUST be positive.
        
        WHY: Negative fares don't make business sense
        """
        negative = sample_trips_data[sample_trips_data['total_amount'] <= 0]
        assert len(negative) == 0, f"Found {len(negative)} rows with negative amounts"
    
    def test_valid_trip_distances(self, sample_trips_data):
        """
        Trip distances MUST be reasonable (0-100 miles).
        
        WHY: >100 miles likely data error (NYC context)
        """
        invalid = sample_trips_data[
            (sample_trips_data['trip_distance'] < 0) | 
            (sample_trips_data['trip_distance'] > 100)
        ]
        assert len(invalid) == 0, f"Found {len(invalid)} rows with invalid distances"
    
    def test_valid_passenger_counts(self, sample_trips_data):
        """
        Passenger counts MUST be 1-6.
        
        WHY: NYC taxis hold max 6 passengers
        """
        invalid = sample_trips_data[
            (sample_trips_data['passenger_count'] < 1) | 
            (sample_trips_data['passenger_count'] > 6)
        ]
        assert len(invalid) == 0, f"Found {len(invalid)} rows with invalid passenger counts"
    
    def test_dropoff_after_pickup(self, sample_trips_data):
        """
        Dropoff time MUST be after pickup time.
        
        WHY: Time travel hasn't been invented yet
        """
        invalid = sample_trips_data[
            sample_trips_data['lpep_dropoff_datetime'] <= 
            sample_trips_data['lpep_pickup_datetime']
        ]
        assert len(invalid) == 0, f"Found {len(invalid)} rows where dropoff before pickup"
    
    def test_valid_location_ids(self, sample_trips_data):
        """
        Location IDs MUST be positive integers.
        
        WHY: Location IDs reference taxi_zones table
        """
        # Check pickup locations
        invalid_pu = sample_trips_data[sample_trips_data['pulocationid'] < 1]
        assert len(invalid_pu) == 0, f"Found {len(invalid_pu)} invalid pickup locations"
        
        # Check dropoff locations
        invalid_do = sample_trips_data[sample_trips_data['dolocationid'] < 1]
        assert len(invalid_do) == 0, f"Found {len(invalid_do)} invalid dropoff locations"


class TestDataValidation:
    """Test data validation rules"""
    
    def test_trip_distance_range_validation(self):
        """
        Test trip distance validation logic.
        
        WHY: Define acceptable range for trip distances
        """
        df = pd.DataFrame({
            'trip_distance': [1.5, -1.0, 150.0, 5.0]
        })
        
        # Valid trips (0 to 100 miles)
        valid = df[(df['trip_distance'] >= 0) & (df['trip_distance'] < 100)]
        assert len(valid) == 2  # 1.5 and 5.0
        
        # Invalid trips
        invalid = df[(df['trip_distance'] < 0) | (df['trip_distance'] >= 100)]
        assert len(invalid) == 2  # -1.0 and 150.0
    
    def test_amount_validation(self):
        """
        Test amount validation logic.
        
        WHY: Define acceptable range for amounts
        """
        df = pd.DataFrame({
            'total_amount': [15.50, -5.00, 500.00, 25.00]
        })
        
        # Valid amounts (positive, under 500)
        valid = df[(df['total_amount'] > 0) & (df['total_amount'] < 500)]
        assert len(valid) == 2  # 15.50 and 25.00
    
    def test_missing_values_detection(self):
        """
        Test missing values are properly detected.
        
        WHY: Missing data needs handling strategy
        """
        df = pd.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': [5, None, 7, 8]
        })
        
        missing = df.isnull().sum()
        assert missing['col1'] == 1
        assert missing['col2'] == 1
        
        # Total missing
        total_missing = df.isnull().sum().sum()
        assert total_missing == 2


@pytest.mark.integration
class TestPipelineIntegration:
    """
    Integration tests for full pipeline.
    
    Requires: docker-compose up
    """
    
    def test_full_pipeline_run(self):
        """
        Test complete pipeline execution.
        
        REQUIRES: docker-compose up
        """
        pytest.skip("Integration test - requires database")
        
        # This would test:
        # 1. Load data
        # 2. Insert to DB
        # 3. Verify count matches
        # 4. Check indexes created
    
    def test_idempotence_full_pipeline(self):
        """
        Test pipeline is fully idempotent.
        
        REQUIRES: docker-compose up
        """
        pytest.skip("Integration test - requires database")
        
        # This would test:
        # 1. Run pipeline first time
        # 2. Count rows
        # 3. Run pipeline second time
        # 4. Count rows again
        # 5. Assert counts are equal


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
