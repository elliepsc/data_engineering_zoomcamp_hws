"""
Custom Operators Package
Contains reusable Airflow operators for the data platform.

Operators are imported directly by DAGs:
    from operators.data_quality_operator import DataQualityOperator
"""

# No need to expose operators at package level
# DAGs import directly from submodules

__all__ = []
