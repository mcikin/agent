"""
Configuration management for DAG creation and validation.

This module provides centralized configuration with validation for all
DAG creation parameters and constraints.
"""

import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum


class ComplexityLevel(Enum):
    """DAG complexity levels."""
    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"


class LogLevel(Enum):
    """Logging levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class DAGConfig:
    """Configuration for DAG creation and validation."""

    # Task limits by complexity - adjusted based on test analytics
    max_tasks_simple: int = 5
    max_tasks_medium: int = 15
    max_tasks_complex: int = 25  # Reduced from 30 to be more realistic

    # Minimum task thresholds for assessment
    min_tasks_simple: int = 1
    min_tasks_medium: int = 4
    min_tasks_complex: int = 6   # Reduced from 10 to be more reasonable

    # Naming constraints
    max_dag_name_length: int = 50
    max_task_name_length: int = 30
    max_dag_id_length: int = 250

    # Performance settings - optimized based on test results
    upload_batch_size: int = 5  # Proven to work well in tests
    max_concurrent_uploads: int = 5  # Matches batch size for optimal performance

    # Timeout settings (in minutes)
    task_execution_timeout_min: int = 5
    task_execution_timeout_max: int = 120
    dag_run_timeout_hours: int = 2

    # Retry settings
    default_retries: int = 1
    max_retries: int = 5
    default_retry_delay_minutes: int = 5
    max_retry_delay_minutes: int = 60

    # File size limits (in MB) - adjusted based on test results
    max_file_size_mb: int = 10  # Individual file size limit
    max_total_upload_size_mb: int = 100  # Increased for DAGs with multiple files (14+ files typical)

    # Supported schedule intervals
    supported_schedules: List[str] = field(default_factory=lambda: [
        "@hourly", "@daily", "@weekly", "@monthly",
        "0 * * * *", "0 0 * * *", "0 0 * * 0", "0 0 1 * *"
    ])

    # Logging configuration
    log_level: LogLevel = LogLevel.INFO
    log_file: Optional[str] = None
    enable_structured_logging: bool = True

    def get_max_tasks_for_complexity(self, complexity: ComplexityLevel) -> int:
        """Get maximum tasks allowed for a complexity level."""
        mapping = {
            ComplexityLevel.SIMPLE: self.max_tasks_simple,
            ComplexityLevel.MEDIUM: self.max_tasks_medium,
            ComplexityLevel.COMPLEX: self.max_tasks_complex,
        }
        return mapping.get(complexity, self.max_tasks_medium)

    def validate_dag_name(self, name: str) -> List[str]:
        """Validate DAG name constraints."""
        errors = []

        if not name or not name.strip():
            errors.append("DAG name cannot be empty")
            return errors

        if len(name) > self.max_dag_name_length:
            errors.append(
                f"DAG name too long: {len(name)} chars, max {self.max_dag_name_length}"
            )

        return errors

    def validate_dag_id(self, dag_id: str) -> List[str]:
        """Validate DAG ID follows Airflow conventions."""
        errors = []

        if not dag_id or not dag_id.strip():
            errors.append("DAG ID cannot be empty")
            return errors

        if len(dag_id) > self.max_dag_id_length:
            errors.append(
                f"DAG ID too long: {len(dag_id)} chars, max {self.max_dag_id_length}"
            )

        # Airflow DAG ID rules
        import re
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', dag_id):
            errors.append(
                "DAG ID must start with letter/underscore and contain only alphanumeric/underscore"
            )

        return errors

    def validate_task_name(self, name: str) -> List[str]:
        """Validate task name constraints."""
        errors = []

        if not name or not name.strip():
            errors.append("Task name cannot be empty")
            return errors

        if len(name) > self.max_task_name_length:
            errors.append(
                f"Task name too long: {len(name)} chars, max {self.max_task_name_length}"
            )

        # Task name rules (snake_case)
        import re
        if not re.match(r'^[a-z][a-z0-9_]*$', name):
            errors.append("Task name must be snake_case starting with lowercase letter")

        return errors

    def validate_schedule_interval(self, interval: str) -> List[str]:
        """Validate schedule interval."""
        errors = []

        if not interval or not interval.strip():
            errors.append("Schedule interval cannot be empty")
            return errors

        if interval not in self.supported_schedules:
            # Check if it's a valid cron expression (basic validation)
            import re
            cron_pattern = r'^(\*|[0-9,-]+|\*/[0-9]+)\s+(\*|[0-9,-]+|\*/[0-9]+)\s+(\*|[0-9,-]+|\*/[0-9]+)\s+(\*|[0-9,-]+|\*/[0-9]+)\s+(\*|[0-9,-]+|\*/[0-9]+)$'
            if not re.match(cron_pattern, interval):
                errors.append(
                    f"Invalid schedule interval. Use presets {self.supported_schedules} or valid cron expressions"
                )

        return errors

    def validate_task_count(self, task_count: int, complexity: ComplexityLevel) -> List[str]:
        """Validate task count against complexity constraints."""
        errors = []

        if task_count < 1:
            errors.append("DAG must have at least 1 task")
            return errors

        max_tasks = self.get_max_tasks_for_complexity(complexity)
        if task_count > max_tasks:
            errors.append(
                f"Too many tasks for {complexity.value} complexity: {task_count}, max {max_tasks}"
            )

        return errors

    def validate_file_size(self, size_bytes: int) -> List[str]:
        """Validate file size constraints."""
        errors = []

        size_mb = size_bytes / (1024 * 1024)
        if size_mb > self.max_file_size_mb:
            errors.append(
                f"File too large: {size_mb:.1f}MB, max {self.max_file_size_mb}MB"
            )

        return errors

    def validate_retry_config(self, retries: int, retry_delay: int) -> List[str]:
        """Validate retry configuration."""
        errors = []

        if retries < 0:
            errors.append("Retries cannot be negative")
        elif retries > self.max_retries:
            errors.append(f"Too many retries: {retries}, max {self.max_retries}")

        if retry_delay < 0:
            errors.append("Retry delay cannot be negative")
        elif retry_delay > self.max_retry_delay_minutes:
            errors.append(
                f"Retry delay too long: {retry_delay}min, max {self.max_retry_delay_minutes}min"
            )

        return errors

    def validate_all(self, dag_config: Dict[str, Any]) -> Dict[str, List[str]]:
        """Comprehensive validation of DAG configuration."""
        errors = {}
        warnings = []

        # Validate basic fields
        if 'dag_name' in dag_config:
            name_errors = self.validate_dag_name(dag_config['dag_name'])
            if name_errors:
                errors['dag_name'] = name_errors

        if 'dag_id' in dag_config:
            id_errors = self.validate_dag_id(dag_config['dag_id'])
            if id_errors:
                errors['dag_id'] = id_errors

        if 'schedule_interval' in dag_config:
            schedule_errors = self.validate_schedule_interval(dag_config['schedule_interval'])
            if schedule_errors:
                errors['schedule_interval'] = schedule_errors

        # Validate tasks
        if 'tasks' in dag_config:
            task_count = len(dag_config['tasks'])
            complexity = ComplexityLevel(dag_config.get('complexity', 'medium'))
            task_count_errors = self.validate_task_count(task_count, complexity)
            if task_count_errors:
                errors['task_count'] = task_count_errors

            # Validate individual task names
            task_name_errors = []
            for i, task in enumerate(dag_config['tasks']):
                if 'task_id' in task:
                    task_errors = self.validate_task_name(task['task_id'])
                    if task_errors:
                        task_name_errors.extend([f"Task {i+1}: {err}" for err in task_errors])

            if task_name_errors:
                errors['task_names'] = task_name_errors

        return {
            'errors': errors,
            'warnings': warnings,
            'is_valid': len(errors) == 0
        }


# Global configuration instance
config = DAGConfig()

# Environment variable overrides
def load_config_from_env():
    """Load configuration from environment variables."""
    global config

    # Performance settings
    if 'DAG_UPLOAD_BATCH_SIZE' in os.environ:
        config.upload_batch_size = int(os.environ['DAG_UPLOAD_BATCH_SIZE'])

    if 'DAG_MAX_CONCURRENT_UPLOADS' in os.environ:
        config.max_concurrent_uploads = int(os.environ['DAG_MAX_CONCURRENT_UPLOADS'])

    # Task limits
    if 'DAG_MAX_TASKS_SIMPLE' in os.environ:
        config.max_tasks_simple = int(os.environ['DAG_MAX_TASKS_SIMPLE'])

    if 'DAG_MAX_TASKS_MEDIUM' in os.environ:
        config.max_tasks_medium = int(os.environ['DAG_MAX_TASKS_MEDIUM'])

    if 'DAG_MAX_TASKS_COMPLEX' in os.environ:
        config.max_tasks_complex = int(os.environ['DAG_MAX_TASKS_COMPLEX'])

    # Logging
    if 'DAG_LOG_LEVEL' in os.environ:
        config.log_level = LogLevel(os.environ['DAG_LOG_LEVEL'])

    if 'DAG_LOG_FILE' in os.environ:
        config.log_file = os.environ['DAG_LOG_FILE']

    # File size limits
    if 'DAG_MAX_FILE_SIZE_MB' in os.environ:
        config.max_file_size_mb = int(os.environ['DAG_MAX_FILE_SIZE_MB'])

    return config

# Load configuration on import
config = load_config_from_env()
