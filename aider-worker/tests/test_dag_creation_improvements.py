"""
Unit tests for DAG creation improvements.

Tests the fixes for:
- Naming consistency issues
- Task count validation
- Configuration management
- Parallel file uploads
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path
from uuid import UUID

from app.core.dag_generator import (
    generate_smart_dag_name,
    is_valid_dag_id,
    sanitize_dag_name
)
from app.core.config import (
    DAGConfig,
    ComplexityLevel,
    LogLevel,
    config
)
from app.core.supabase_io import upload_file_async
from app.models.dto import CreateDagReq


class TestNamingConsistency:
    """Test naming consistency improvements."""

    def test_llm_dag_id_priority(self):
        """Test that LLM-provided DAG IDs are prioritized."""
        llm_dag_id = "ecommerce_order_processing_pipeline"
        description = "Create a daily e-commerce pipeline"

        result = generate_smart_dag_name(
            description=description,
            complexity="medium",
            llm_dag_id=llm_dag_id
        )

        assert result == llm_dag_id

    def test_invalid_llm_dag_id_fallback(self):
        """Test fallback to generated name when LLM DAG ID is invalid."""
        invalid_dag_id = "invalid-dag-name!"  # Invalid characters
        description = "Create a daily e-commerce pipeline"

        result = generate_smart_dag_name(
            description=description,
            complexity="medium",
            llm_dag_id=invalid_dag_id
        )

        # Should fall back to generated name, not use invalid ID
        assert result != invalid_dag_id
        assert is_valid_dag_id(result)

    def test_valid_dag_id_validation(self):
        """Test DAG ID validation function."""
        # Valid IDs
        assert is_valid_dag_id("valid_dag_name")
        assert is_valid_dag_id("dag123")
        assert is_valid_dag_id("_private_dag")
        assert is_valid_dag_id("MyDag")  # Mixed case

        # Invalid IDs
        assert not is_valid_dag_id("")  # Empty
        assert not is_valid_dag_id("123invalid")  # Starts with number
        assert not is_valid_dag_id("invalid-name")  # Hyphen
        assert not is_valid_dag_id("invalid name")  # Space
        assert not is_valid_dag_id("invalid@name")  # Special char


class TestTaskCountValidation:
    """Test task count estimation improvements."""

    def test_ecommerce_pattern_recognition(self):
        """Test that e-commerce patterns are recognized."""
        description = "Create a daily e-commerce pipeline that processes new orders, validates inventory, calculates shipping costs, and sends confirmation emails."

        # This should trigger e-commerce domain detection
        result = sanitize_dag_name(description)
        assert "commerce" in result or "order" in result

    def test_complexity_based_limits(self):
        """Test that complexity levels enforce task limits."""
        test_config = DAGConfig()

        # Simple complexity
        assert test_config.get_max_tasks_for_complexity(ComplexityLevel.SIMPLE) == 5

        # Medium complexity
        assert test_config.get_max_tasks_for_complexity(ComplexityLevel.MEDIUM) == 15

        # Complex complexity
        assert test_config.get_max_tasks_for_complexity(ComplexityLevel.COMPLEX) == 30


class TestConfigurationManagement:
    """Test configuration validation and management."""

    def test_dag_name_validation(self):
        """Test DAG name validation."""
        test_config = DAGConfig()

        # Valid names
        assert len(test_config.validate_dag_name("Valid DAG Name")) == 0
        assert len(test_config.validate_dag_name("Another Valid Name")) == 0

        # Invalid names
        assert len(test_config.validate_dag_name("")) > 0  # Empty
        assert len(test_config.validate_dag_name("a" * 100)) > 0  # Too long

    def test_dag_id_validation(self):
        """Test DAG ID validation."""
        test_config = DAGConfig()

        # Valid IDs
        assert len(test_config.validate_dag_id("valid_dag_id")) == 0
        assert len(test_config.validate_dag_id("dag123")) == 0

        # Invalid IDs
        assert len(test_config.validate_dag_id("")) > 0  # Empty
        assert len(test_config.validate_dag_id("123invalid")) > 0  # Starts with number
        assert len(test_config.validate_dag_id("invalid-name")) > 0  # Hyphen

    def test_task_name_validation(self):
        """Test task name validation."""
        test_config = DAGConfig()

        # Valid names
        assert len(test_config.validate_task_name("valid_task_name")) == 0
        assert len(test_config.validate_task_name("extract_data")) == 0

        # Invalid names
        assert len(test_config.validate_task_name("")) > 0  # Empty
        assert len(test_config.validate_task_name("Invalid-Name")) > 0  # Hyphen
        assert len(test_config.validate_task_name("123invalid")) > 0  # Starts with number

    def test_schedule_interval_validation(self):
        """Test schedule interval validation."""
        test_config = DAGConfig()

        # Valid schedules
        assert len(test_config.validate_schedule_interval("@daily")) == 0
        assert len(test_config.validate_schedule_interval("@hourly")) == 0
        assert len(test_config.validate_schedule_interval("0 0 * * *")) == 0  # Valid cron

        # Invalid schedules
        assert len(test_config.validate_schedule_interval("")) > 0  # Empty
        assert len(test_config.validate_schedule_interval("invalid")) > 0  # Invalid

    def test_task_count_validation(self):
        """Test task count validation against complexity."""
        test_config = DAGConfig()

        # Valid counts
        assert len(test_config.validate_task_count(3, ComplexityLevel.SIMPLE)) == 0
        assert len(test_config.validate_task_count(10, ComplexityLevel.MEDIUM)) == 0
        assert len(test_config.validate_task_count(20, ComplexityLevel.COMPLEX)) == 0

        # Invalid counts
        assert len(test_config.validate_task_count(10, ComplexityLevel.SIMPLE)) > 0  # Too many for simple
        assert len(test_config.validate_task_count(25, ComplexityLevel.MEDIUM)) > 0  # Too many for medium

    def test_retry_config_validation(self):
        """Test retry configuration validation."""
        test_config = DAGConfig()

        # Valid configs
        assert len(test_config.validate_retry_config(2, 10)) == 0
        assert len(test_config.validate_retry_config(3, 30)) == 0

        # Invalid configs
        assert len(test_config.validate_retry_config(-1, 10)) > 0  # Negative retries
        assert len(test_config.validate_retry_config(10, 10)) > 0  # Too many retries
        assert len(test_config.validate_retry_config(2, 120)) > 0  # Delay too long

    def test_comprehensive_validation(self):
        """Test comprehensive DAG configuration validation."""
        test_config = DAGConfig()

        # Valid configuration
        valid_config = {
            'dag_name': 'Test DAG',
            'dag_id': 'test_dag',
            'schedule_interval': '@daily',
            'complexity': 'medium',
            'tasks': [
                {'task_id': 'task1', 'task_type': 'python'},
                {'task_id': 'task2', 'task_type': 'python'}
            ]
        }

        result = test_config.validate_all(valid_config)
        assert result['is_valid'] is True
        assert len(result['errors']) == 0

        # Invalid configuration
        invalid_config = {
            'dag_name': '',  # Empty name
            'dag_id': '123invalid',  # Invalid ID
            'schedule_interval': 'invalid',  # Invalid schedule
            'complexity': 'medium',
            'tasks': [{'task_id': 'invalid-task', 'task_type': 'python'}]  # Invalid task name
        }

        result = test_config.validate_all(invalid_config)
        assert result['is_valid'] is False
        assert len(result['errors']) > 0


class TestParallelUploads:
    """Test parallel file upload improvements."""

    @pytest.mark.asyncio
    async def test_upload_file_async(self):
        """Test async file upload function."""
        project_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        path = "test/file.txt"
        content = b"test content"

        with patch('app.core.supabase_io.supabase_client') as mock_client:
            mock_storage = Mock()
            mock_client.return_value.storage.from_.return_value.upload.return_value = None

            result = await upload_file_async(project_id, path, content)

            assert result['path'] == path
            assert result['size'] == len(content)
            mock_client.return_value.storage.from_.assert_called()
            mock_client.return_value.storage.from_.return_value.upload.assert_called_once()


class TestEnvironmentConfiguration:
    """Test environment-based configuration loading."""

    @patch.dict('os.environ', {
        'DAG_UPLOAD_BATCH_SIZE': '10',
        'DAG_MAX_CONCURRENT_UPLOADS': '8',
        'DAG_LOG_LEVEL': 'DEBUG'
    })
    def test_environment_override(self):
        """Test that environment variables override default config."""
        from app.core.config import load_config_from_env, LogLevel

        test_config = load_config_from_env()

        assert test_config.upload_batch_size == 10
        assert test_config.max_concurrent_uploads == 8
        assert test_config.log_level == LogLevel.DEBUG


class TestIntegration:
    """Integration tests combining multiple improvements."""

    def test_full_dag_creation_workflow(self):
        """Test the complete DAG creation workflow with all improvements."""
        # Create a sample DAG request with LLM-provided DAG ID
        project_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        llm_dag_id = "ecommerce_order_processing_pipeline"

        req = CreateDagReq(
            project_id=project_id,
            dag_name="E-commerce Order Processing Pipeline",
            dag_id=llm_dag_id,  # LLM-provided ID
            description="Create a daily e-commerce pipeline that processes new orders, validates inventory, calculates shipping costs, and sends confirmation emails.",
            schedule_interval="@hourly",
            owner="data-team",
            tasks=[
                {
                    'task_id': 'extract_new_orders',
                    'task_type': 'python',
                    'python_code': 'print("Extracting orders...")'
                },
                {
                    'task_id': 'validate_inventory',
                    'task_type': 'python',
                    'python_code': 'print("Validating inventory...")',
                    'dependencies': ['extract_new_orders']
                }
            ]
        )

        # Test that the DAG ID is preserved
        dag_id = generate_smart_dag_name(req.description, "medium", req.dag_id)
        assert dag_id == llm_dag_id

        # Test configuration validation
        validation_result = config.validate_all({
            'dag_name': req.dag_name,
            'dag_id': req.dag_id,
            'schedule_interval': req.schedule_interval,
            'complexity': 'medium',
            'tasks': req.tasks
        })

        assert validation_result['is_valid'] is True

