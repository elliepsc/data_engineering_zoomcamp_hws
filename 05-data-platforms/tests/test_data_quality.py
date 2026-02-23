"""
Data Quality Operator Tests

Unit tests for the custom DataQualityOperator.

Usage:
    pytest tests/test_data_quality.py -v
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from airflow.models import TaskInstance, DagRun
from airflow import DAG
from datetime import datetime
import sys
import os

# Add plugins to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'plugins'))

from operators.data_quality_operator import DataQualityOperator


class TestDataQualityOperator:
    """Test suite for DataQualityOperator."""
    
    @pytest.fixture
    def mock_bigquery_client(self):
        """Mock BigQuery client."""
        with patch('operators.data_quality_operator.bigquery.Client') as mock:
            yield mock
    
    @pytest.fixture
    def sample_dag(self):
        """Create a sample DAG for testing."""
        return DAG(
            dag_id='test_dag',
            start_date=datetime(2024, 1, 1),
        )
    
    def test_operator_initialization(self, sample_dag):
        """Test that operator initializes with correct parameters."""
        operator = DataQualityOperator(
            task_id='test_quality',
            table_id='project.dataset.table',
            check_type='row_count',
            threshold=100,
            comparison='gt',
            dag=sample_dag,
        )
        
        assert operator.table_id == 'project.dataset.table'
        assert operator.check_type == 'row_count'
        assert operator.threshold == 100
        assert operator.comparison == 'gt'
    
    def test_null_check_requires_column(self, sample_dag):
        """Test that null_check requires column parameter."""
        with pytest.raises(ValueError, match="null_check requires 'column' parameter"):
            DataQualityOperator(
                task_id='test_null',
                table_id='project.dataset.table',
                check_type='null_check',
                dag=sample_dag,
            )
    
    def test_custom_sql_requires_sql(self, sample_dag):
        """Test that custom_sql requires custom_sql parameter."""
        with pytest.raises(ValueError, match="custom_sql check_type requires 'custom_sql' parameter"):
            DataQualityOperator(
                task_id='test_custom',
                table_id='project.dataset.table',
                check_type='custom_sql',
                dag=sample_dag,
            )
    
    def test_row_count_query_building(self, sample_dag):
        """Test that row_count check builds correct query."""
        operator = DataQualityOperator(
            task_id='test_row_count',
            table_id='project.dataset.table',
            check_type='row_count',
            threshold=100,
            comparison='gt',
            dag=sample_dag,
        )
        
        query = operator._build_query()
        
        assert 'COUNT(*)' in query
        assert 'project.dataset.table' in query
    
    def test_row_count_with_partition(self, sample_dag):
        """Test that row_count query includes partition filter."""
        operator = DataQualityOperator(
            task_id='test_row_count_partition',
            table_id='project.dataset.table',
            check_type='row_count',
            threshold=100,
            comparison='gt',
            partition_date='2024-01-01',
            dag=sample_dag,
        )
        
        query = operator._build_query()
        
        assert '_PARTITIONTIME' in query
        assert '2024-01-01' in query
    
    def test_null_check_query_building(self, sample_dag):
        """Test that null_check builds correct query."""
        operator = DataQualityOperator(
            task_id='test_null_check',
            table_id='project.dataset.table',
            check_type='null_check',
            column='test_column',
            dag=sample_dag,
        )
        
        query = operator._build_query()
        
        assert 'test_column IS NULL' in query
    
    def test_comparison_operators(self, sample_dag):
        """Test all comparison operators work correctly."""
        operator = DataQualityOperator(
            task_id='test_comparison',
            table_id='project.dataset.table',
            check_type='row_count',
            threshold=100,
            comparison='gt',
            dag=sample_dag,
        )
        
        # Test greater than
        assert operator._compare(150, 100) is True
        assert operator._compare(50, 100) is False
        
        # Test less than
        operator.comparison = 'lt'
        assert operator._compare(50, 100) is True
        assert operator._compare(150, 100) is False
        
        # Test equal
        operator.comparison = 'eq'
        assert operator._compare(100, 100) is True
        assert operator._compare(50, 100) is False
        
        # Test greater than or equal
        operator.comparison = 'gte'
        assert operator._compare(100, 100) is True
        assert operator._compare(150, 100) is True
        assert operator._compare(50, 100) is False
        
        # Test less than or equal
        operator.comparison = 'lte'
        assert operator._compare(100, 100) is True
        assert operator._compare(50, 100) is True
        assert operator._compare(150, 100) is False
    
    @patch.dict(os.environ, {'GCP_PROJECT_ID': 'test-project'})
    def test_row_count_validation_success(self, sample_dag, mock_bigquery_client):
        """Test successful row count validation."""
        # Mock BigQuery response
        mock_result = [{'value': 1000}]
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_bigquery_client.return_value.query.return_value = mock_query_job
        
        operator = DataQualityOperator(
            task_id='test_row_count',
            table_id='project.dataset.table',
            check_type='row_count',
            threshold=100,
            comparison='gt',
            dag=sample_dag,
        )
        
        # Should not raise
        operator.execute(context={})
    
    @patch.dict(os.environ, {'GCP_PROJECT_ID': 'test-project'})
    def test_row_count_validation_failure(self, sample_dag, mock_bigquery_client):
        """Test failed row count validation."""
        # Mock BigQuery response
        mock_result = [{'value': 50}]
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_bigquery_client.return_value.query.return_value = mock_query_job
        
        operator = DataQualityOperator(
            task_id='test_row_count',
            table_id='project.dataset.table',
            check_type='row_count',
            threshold=100,
            comparison='gt',
            dag=sample_dag,
        )
        
        # Should raise ValueError
        with pytest.raises(ValueError, match="Row count check failed"):
            operator.execute(context={})
    
    @patch.dict(os.environ, {'GCP_PROJECT_ID': 'test-project'})
    def test_null_check_success(self, sample_dag, mock_bigquery_client):
        """Test successful null check (no nulls found)."""
        # Mock BigQuery response
        mock_result = [{'value': 0}]  # 0 NULL values
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_bigquery_client.return_value.query.return_value = mock_query_job
        
        operator = DataQualityOperator(
            task_id='test_null_check',
            table_id='project.dataset.table',
            check_type='null_check',
            column='test_column',
            dag=sample_dag,
        )
        
        # Should not raise
        operator.execute(context={})
    
    @patch.dict(os.environ, {'GCP_PROJECT_ID': 'test-project'})
    def test_null_check_failure(self, sample_dag, mock_bigquery_client):
        """Test failed null check (nulls found)."""
        # Mock BigQuery response
        mock_result = [{'value': 10}]  # 10 NULL values
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_bigquery_client.return_value.query.return_value = mock_query_job
        
        operator = DataQualityOperator(
            task_id='test_null_check',
            table_id='project.dataset.table',
            check_type='null_check',
            column='test_column',
            dag=sample_dag,
        )
        
        # Should raise ValueError
        with pytest.raises(ValueError, match="NULL check failed"):
            operator.execute(context={})
    
    @patch.dict(os.environ, {'GCP_PROJECT_ID': 'test-project'})
    def test_custom_sql_check(self, sample_dag, mock_bigquery_client):
        """Test custom SQL check."""
        # Mock BigQuery response
        mock_result = [{'is_valid': True}]
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_bigquery_client.return_value.query.return_value = mock_query_job
        
        operator = DataQualityOperator(
            task_id='test_custom_sql',
            table_id='project.dataset.table',
            check_type='custom_sql',
            custom_sql='SELECT TRUE as is_valid',
            dag=sample_dag,
        )
        
        # Should not raise
        operator.execute(context={})


class TestDataQualityIntegration:
    """Integration tests with Airflow DAG."""
    
    @pytest.fixture
    def sample_dag(self):
        """Create a sample DAG for integration testing."""
        return DAG(
            dag_id='test_quality_dag',
            start_date=datetime(2024, 1, 1),
        )
    
    def test_operator_in_dag(self, sample_dag):
        """Test that operator can be added to a DAG."""
        operator = DataQualityOperator(
            task_id='quality_check',
            table_id='project.dataset.table',
            check_type='row_count',
            threshold=100,
            comparison='gt',
            dag=sample_dag,
        )
        
        assert operator in sample_dag.tasks
        assert operator.task_id == 'quality_check'
    
    def test_multiple_quality_checks_in_dag(self, sample_dag):
        """Test multiple quality checks can be chained."""
        check1 = DataQualityOperator(
            task_id='check_row_count',
            table_id='project.dataset.table',
            check_type='row_count',
            threshold=100,
            comparison='gt',
            dag=sample_dag,
        )
        
        check2 = DataQualityOperator(
            task_id='check_nulls',
            table_id='project.dataset.table',
            check_type='null_check',
            column='test_column',
            dag=sample_dag,
        )
        
        # Chain tasks
        check1 >> check2
        
        # Verify dependencies
        assert check2 in check1.downstream_list
        assert check1 in check2.upstream_list


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
