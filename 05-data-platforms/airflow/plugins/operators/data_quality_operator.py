"""
Custom Data Quality Operator for Airflow

This operator encapsulates data quality check logic that would otherwise be
repeated across multiple DAGs.

Benefits of Custom Operators:
- Reusability: Write once, use in multiple DAGs
- Testability: Unit test the operator independently
- Maintainability: Fix bugs in one place
- Professionalism: Shows engineering maturity

Usage in DAG:
    from operators.data_quality_operator import DataQualityOperator
    
    check_freshness = DataQualityOperator(
        task_id='check_data_freshness',
        table_id='raw_data.green_taxi',
        check_type='row_count',
        threshold=0,
        comparison='gt',
    )
"""

from typing import Optional, Literal
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import bigquery
import os


class DataQualityOperator(BaseOperator):
    """
    Custom operator to perform data quality checks on BigQuery tables.
    
    Supports multiple check types:
    - row_count: Verify table has data
    - null_check: Verify no NULL values in specified column
    - range_check: Verify values are within expected range
    - custom_sql: Run custom SQL query that returns boolean
    
    Args:
        table_id: BigQuery table ID (format: 'dataset.table')
        check_type: Type of quality check to perform
        threshold: Threshold value for comparison
        comparison: Comparison operator ('gt', 'lt', 'eq', 'gte', 'lte')
        column: Column name (required for null_check and range_check)
        custom_sql: Custom SQL query (required for custom_sql check_type)
        partition_date: Optional partition date for partitioned tables
        
    Example:
        # Check table has at least 1000 rows
        >>> check = DataQualityOperator(
        ...     task_id='check_row_count',
        ...     table_id='raw_data.green_taxi',
        ...     check_type='row_count',
        ...     threshold=1000,
        ...     comparison='gte'
        ... )
        
        # Check no NULL values in a column
        >>> check = DataQualityOperator(
        ...     task_id='check_nulls',
        ...     table_id='raw_data.green_taxi',
        ...     check_type='null_check',
        ...     column='lpep_pickup_datetime'
        ... )
    """
    
    template_fields = ('table_id', 'partition_date')
    ui_color = '#e8f7ff'
    
    @apply_defaults
    def __init__(
        self,
        table_id: str,
        check_type: Literal['row_count', 'null_check', 'range_check', 'custom_sql'],
        threshold: Optional[float] = None,
        comparison: Literal['gt', 'lt', 'eq', 'gte', 'lte'] = 'gt',
        column: Optional[str] = None,
        custom_sql: Optional[str] = None,
        partition_date: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_id = table_id
        self.check_type = check_type
        self.threshold = threshold
        self.comparison = comparison
        self.column = column
        self.custom_sql = custom_sql
        self.partition_date = partition_date
        
        # Validation
        if check_type in ['null_check', 'range_check'] and not column:
            raise ValueError(f"{check_type} requires 'column' parameter")
        
        if check_type == 'custom_sql' and not custom_sql:
            raise ValueError("custom_sql check_type requires 'custom_sql' parameter")
    
    def execute(self, context):
        """
        Execute the data quality check.
        
        Args:
            context: Airflow context dictionary
            
        Raises:
            ValueError: If quality check fails
        """
        self.log.info(f"Running {self.check_type} check on {self.table_id}")
        
        # Get BigQuery client
        project_id = os.getenv('GCP_PROJECT_ID')
        client = bigquery.Client(project=project_id)
        
        # Build query based on check type
        query = self._build_query()
        
        self.log.info(f"Executing query: {query}")
        
        # Run query
        query_job = client.query(query)
        results = list(query_job.result())
        
        # Validate results
        self._validate_results(results)
        
        self.log.info(f"✅ Quality check passed for {self.table_id}")
    
    def _build_query(self) -> str:
        """
        Build SQL query based on check type.
        
        Returns:
            SQL query string
        """
        partition_filter = ""
        if self.partition_date:
            partition_filter = f"WHERE DATE(_PARTITIONTIME) = '{self.partition_date}'"
        
        if self.check_type == 'row_count':
            return f"""
                SELECT COUNT(*) as value
                FROM `{self.table_id}`
                {partition_filter}
            """
        
        elif self.check_type == 'null_check':
            return f"""
                SELECT COUNT(*) as value
                FROM `{self.table_id}`
                WHERE {self.column} IS NULL
                {partition_filter}
            """
        
        elif self.check_type == 'range_check':
            return f"""
                SELECT
                    MIN({self.column}) as min_value,
                    MAX({self.column}) as max_value,
                    AVG({self.column}) as avg_value
                FROM `{self.table_id}`
                {partition_filter}
            """
        
        elif self.check_type == 'custom_sql':
            return self.custom_sql
        
        else:
            raise ValueError(f"Unknown check_type: {self.check_type}")
    
    def _validate_results(self, results):
        """
        Validate query results against threshold.
        
        Args:
            results: Query results
            
        Raises:
            ValueError: If validation fails
        """
        if not results:
            raise ValueError("Query returned no results")
        
        result = results[0]
        
        if self.check_type == 'row_count':
            value = result['value']
            if not self._compare(value, self.threshold):
                raise ValueError(
                    f"Row count check failed: {value} {self.comparison} {self.threshold}"
                )
            self.log.info(f"Row count: {value:,} (threshold: {self.threshold})")
        
        elif self.check_type == 'null_check':
            null_count = result['value']
            if null_count > 0:
                raise ValueError(
                    f"NULL check failed: found {null_count} NULL values in {self.column}"
                )
            self.log.info(f"No NULL values found in {self.column}")
        
        elif self.check_type == 'range_check':
            min_val = result['min_value']
            max_val = result['max_value']
            avg_val = result['avg_value']
            
            self.log.info(
                f"Range check results - Min: {min_val}, Max: {max_val}, Avg: {avg_val}"
            )
            
            if self.threshold and not self._compare(max_val, self.threshold):
                raise ValueError(
                    f"Range check failed: max value {max_val} {self.comparison} {self.threshold}"
                )
        
        elif self.check_type == 'custom_sql':
            # Expect boolean result or count
            if 'is_valid' in result:
                if not result['is_valid']:
                    raise ValueError("Custom SQL check failed")
            elif 'value' in result:
                if not self._compare(result['value'], self.threshold):
                    raise ValueError(
                        f"Custom SQL check failed: {result['value']} {self.comparison} {self.threshold}"
                    )
    
    def _compare(self, value: float, threshold: float) -> bool:
        """
        Compare value against threshold using specified operator.
        
        Args:
            value: Value to compare
            threshold: Threshold value
            
        Returns:
            True if comparison passes
        """
        comparisons = {
            'gt': lambda v, t: v > t,
            'lt': lambda v, t: v < t,
            'eq': lambda v, t: v == t,
            'gte': lambda v, t: v >= t,
            'lte': lambda v, t: v <= t,
        }
        
        return comparisons[self.comparison](value, threshold)
