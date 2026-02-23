"""
DAG Integrity Tests

These tests validate that all DAGs are properly configured and can be imported
by Airflow without errors. Run these tests in CI/CD before deploying.

Usage:
    pytest tests/test_dag_integrity.py -v
"""

import pytest
from airflow.models import DagBag
import os


class TestDAGIntegrity:
    """Test suite for DAG validation."""
    
    DAGS_FOLDER = os.path.join(
        os.path.dirname(__file__),
        '..',
        'airflow',
        'dags'
    )
    
    @pytest.fixture(scope='class')
    def dagbag(self):
        """Load all DAGs from the dags folder."""
        return DagBag(dag_folder=self.DAGS_FOLDER, include_examples=False)
    
    def test_no_import_errors(self, dagbag):
        """
        Test that all DAGs can be imported without errors.
        
        Common import errors:
        - Missing dependencies
        - Syntax errors
        - Invalid imports
        """
        assert len(dagbag.import_errors) == 0, \
            f"DAG import errors: {dagbag.import_errors}"
    
    def test_expected_dags_loaded(self, dagbag):
        """
        Test that all expected DAGs are loaded.
        
        Expected DAGs:
        - 01_ingest_nyc_taxi_incremental
        - 02_transform_dbt_models
        - 03_quality_monitoring
        """
        expected_dags = [
            '01_ingest_nyc_taxi_incremental',
            '02_transform_dbt_models',
            '03_quality_monitoring',
        ]
        
        loaded_dag_ids = list(dagbag.dags.keys())
        
        for expected_dag in expected_dags:
            assert expected_dag in loaded_dag_ids, \
                f"Expected DAG '{expected_dag}' not found in {loaded_dag_ids}"
    
    def test_dags_have_tags(self, dagbag):
        """
        Test that all DAGs have tags for categorization.
        
        Tags help organize DAGs in the Airflow UI.
        """
        for dag_id, dag in dagbag.dags.items():
            assert len(dag.tags) > 0, \
                f"DAG '{dag_id}' has no tags"
    
    def test_dags_have_owners(self, dagbag):
        """
        Test that all DAGs have an owner specified.
        
        Owner is important for accountability and alerting.
        """
        for dag_id, dag in dagbag.dags.items():
            assert dag.owner is not None, \
                f"DAG '{dag_id}' has no owner"
            assert dag.owner != 'airflow', \
                f"DAG '{dag_id}' has default 'airflow' owner - should be specific person/team"
    
    def test_dags_have_retries(self, dagbag):
        """
        Test that all DAGs have retry configuration.
        
        Retries are essential for handling transient failures.
        """
        for dag_id, dag in dagbag.dags.items():
            assert dag.default_args.get('retries') is not None, \
                f"DAG '{dag_id}' has no retry configuration"
    
    def test_task_count_reasonable(self, dagbag):
        """
        Test that DAGs have a reasonable number of tasks.
        
        Too many tasks can indicate poor DAG design.
        Too few might indicate missing functionality.
        """
        for dag_id, dag in dagbag.dags.items():
            task_count = len(dag.tasks)
            assert 1 <= task_count <= 50, \
                f"DAG '{dag_id}' has {task_count} tasks (expected 1-50)"
    
    def test_no_cycles_in_dag(self, dagbag):
        """
        Test that DAGs don't have circular dependencies.
        
        Circular dependencies will cause Airflow to fail.
        """
        for dag_id, dag in dagbag.dags.items():
            # This will raise if there's a cycle
            try:
                dag.test_cycle()
            except Exception as e:
                pytest.fail(f"DAG '{dag_id}' has a cycle: {str(e)}")
    
    def test_tasks_have_unique_ids(self, dagbag):
        """
        Test that all tasks within a DAG have unique IDs.
        
        Duplicate task IDs will cause runtime errors.
        """
        for dag_id, dag in dagbag.dags.items():
            task_ids = [task.task_id for task in dag.tasks]
            assert len(task_ids) == len(set(task_ids)), \
                f"DAG '{dag_id}' has duplicate task IDs: {task_ids}"


class TestSpecificDAGs:
    """Tests for specific DAG configurations."""
    
    DAGS_FOLDER = os.path.join(
        os.path.dirname(__file__),
        '..',
        'airflow',
        'dags'
    )
    
    @pytest.fixture(scope='class')
    def dagbag(self):
        """Load all DAGs."""
        return DagBag(dag_folder=self.DAGS_FOLDER, include_examples=False)
    
    def test_ingestion_dag_has_catchup(self, dagbag):
        """
        Test that ingestion DAG has catchup enabled.
        
        Catchup is required for backfilling historical data.
        """
        dag = dagbag.get_dag('01_ingest_nyc_taxi_incremental')
        assert dag.catchup is True, \
            "Ingestion DAG should have catchup=True for backfill support"
    
    def test_ingestion_dag_schedule(self, dagbag):
        """
        Test that ingestion DAG has monthly schedule.
        """
        dag = dagbag.get_dag('01_ingest_nyc_taxi_incremental')
        assert dag.schedule_interval == '@monthly', \
            f"Expected @monthly schedule, got {dag.schedule_interval}"
    
    def test_transform_dag_waits_for_ingestion(self, dagbag):
        """
        Test that transformation DAG has ExternalTaskSensor.
        
        This ensures data is ingested before transformation.
        """
        dag = dagbag.get_dag('02_transform_dbt_models')
        
        # Check for ExternalTaskSensor
        sensor_tasks = [
            task for task in dag.tasks
            if 'ExternalTaskSensor' in str(type(task))
        ]
        
        assert len(sensor_tasks) > 0, \
            "Transform DAG should wait for ingestion via ExternalTaskSensor"
    
    def test_quality_dag_has_no_retries(self, dagbag):
        """
        Test that quality monitoring DAG has 0 retries.
        
        Quality checks should fail fast, not retry.
        """
        dag = dagbag.get_dag('03_quality_monitoring')
        assert dag.default_args.get('retries') == 0, \
            "Quality monitoring DAG should have retries=0 (fail fast)"


class TestTaskConfiguration:
    """Tests for individual task configurations."""
    
    DAGS_FOLDER = os.path.join(
        os.path.dirname(__file__),
        '..',
        'airflow',
        'dags'
    )
    
    @pytest.fixture(scope='class')
    def dagbag(self):
        """Load all DAGs."""
        return DagBag(dag_folder=self.DAGS_FOLDER, include_examples=False)
    
    def test_all_tasks_have_descriptions(self, dagbag):
        """
        Test that all tasks have docstrings or descriptions.
        
        Documentation is essential for maintainability.
        """
        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                # Check if task has docstring
                if hasattr(task, 'python_callable'):
                    assert task.python_callable.__doc__ is not None, \
                        f"Task '{task.task_id}' in DAG '{dag_id}' has no docstring"
    
    def test_cleanup_tasks_use_all_done_trigger(self, dagbag):
        """
        Test that cleanup tasks use 'all_done' trigger rule.
        
        Cleanup should run even if upstream tasks fail.
        """
        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                if 'cleanup' in task.task_id.lower():
                    assert task.trigger_rule == 'all_done', \
                        f"Cleanup task '{task.task_id}' should use trigger_rule='all_done'"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
