"""Test data loading and normalization"""
import pytest
import pandas as pd
from pipeline.data_loader import DataLoader

def test_normalize_columns(sample_dataframe):
    """Columns MUST be normalized to lowercase snake_case"""
    # Given: DataFrame with mixed case columns
    df = sample_dataframe
    
    # When: Normalize columns
    result = DataLoader.normalize_columns(df)
    
    # Then: All columns lowercase
    assert 'pulocationid' in result.columns
    assert 'dolocationid' in result.columns
    assert 'trip_distance' in result.columns
    
    # And: Original case removed
    assert 'PULocationID' not in result.columns
    assert 'DOLocationID' not in result.columns
    assert 'Trip_Distance' not in result.columns

def test_normalize_columns_with_spaces():
    """Spaces MUST be replaced with underscores"""
    # Given: DataFrame with spaces in columns
    df = pd.DataFrame({'Column Name': [1, 2], 'Another Col': [3, 4]})
    
    # When: Normalize
    result = DataLoader.normalize_columns(df)
    
    # Then: Spaces become underscores
    assert 'column_name' in result.columns
    assert 'another_col' in result.columns

def test_load_csv_normalizes_columns(sample_csv):
    """CSV loading MUST normalize columns"""
    # When: Load CSV
    df = DataLoader.load_csv(sample_csv)
    
    # Then: Columns are lowercase
    assert all(col.islower() for col in df.columns)