"""Pytest fixtures"""
import pytest
import pandas as pd

@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing"""
    return pd.DataFrame({
        'PULocationID': [1, 2, 3],
        'DOLocationID': [4, 5, 6],
        'Trip_Distance': [1.5, 2.3, 3.1]
    })

@pytest.fixture
def sample_csv(tmp_path):
    """Create sample CSV file"""
    csv_path = tmp_path / "test.csv"
    df = pd.DataFrame({'Col1': [1, 2], 'Col2': ['a', 'b']})
    df.to_csv(csv_path, index=False)
    return csv_path