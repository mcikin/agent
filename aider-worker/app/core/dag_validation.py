"""
DAG validation module.

This module provides comprehensive validation for DAG specifications,
task configurations, and dependency management.
"""

import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from .config import config, ComplexityLevel
from .logging_config import get_logger

logger = get_logger(__name__)


class DAGValidator:
    """Comprehensive DAG validation utilities."""

    @staticmethod
    def validate_dag_name(name: str) -> Dict[str, Any]:
        """Validate DAG name constraints."""
        errors = config.validate_dag_name(name)
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': []
        }

    @staticmethod
    def validate_dag_id(dag_id: str) -> Dict[str, Any]:
        """Validate DAG ID follows Airflow conventions."""
        errors = config.validate_dag_id(dag_id)
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': []
        }

    @staticmethod
    def validate_task(task: Dict[str, Any]) -> Dict[str, Any]:
        """Validate individual task configuration."""
        errors = []
        warnings = []

        # Required fields
        required_fields = ['task_id', 'task_type']
        for field in required_fields:
            if field not in task:
                errors.append(f"Missing required field: {field}")

        if errors:
            return {
                'is_valid': False,
                'errors': errors,
                'warnings': warnings
            }

        # Validate task ID
        if 'task_id' in task:
            task_id_errors = config.validate_task_name(task['task_id'])
            errors.extend(task_id_errors)

        # Validate task type
        valid_task_types = ['python', 'bash', 'http', 'dummy']
        if task.get('task_type') not in valid_task_types:
            errors.append(f"Invalid task_type: {task.get('task_type')}. Must be one of {valid_task_types}")

        # Validate Python-specific fields
        if task.get('task_type') == 'python':
            if 'python_code' not in task and 'python_callable' not in task:
                errors.append("Python tasks must have either 'python_code' or 'python_callable'")

        # Validate Bash-specific fields
        if task.get('task_type') == 'bash':
            if 'bash_command' not in task:
                errors.append("Bash tasks must have 'bash_command'")

        # Validate dependencies
        if 'dependencies' in task:
            if not isinstance(task['dependencies'], list):
                errors.append("'dependencies' must be a list")
            else:
                for dep in task['dependencies']:
                    if not isinstance(dep, str):
                        errors.append(f"Dependency must be a string, got {type(dep)}")

        # Validate retry configuration
        if 'retries' in task:
            retry_errors = config.validate_retry_config(
                task['retries'],
                task.get('retry_delay_minutes', config.default_retry_delay_minutes)
            )
            errors.extend(retry_errors)

        # Validate execution timeout
        if 'execution_timeout_minutes' in task:
            timeout = task['execution_timeout_minutes']
            if timeout < config.task_execution_timeout_min or timeout > config.task_execution_timeout_max:
                errors.append(
                    f"Execution timeout {timeout}min out of range "
                    f"({config.task_execution_timeout_min}-{config.task_execution_timeout_max})"
                )

        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }

    @staticmethod
    def validate_dependencies(tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate task dependencies for cycles and invalid references."""
        errors = []
        warnings = []

        # Build task ID set
        task_ids = {task.get('task_id') for task in tasks if 'task_id' in task}

        # Check each task's dependencies
        for task in tasks:
            task_id = task.get('task_id')
            dependencies = task.get('dependencies', [])

            # Check for invalid dependency references
            for dep in dependencies:
                if dep not in task_ids:
                    errors.append(f"Task '{task_id}' depends on non-existent task '{dep}'")

            # Check for self-dependency
            if task_id in dependencies:
                errors.append(f"Task '{task_id}' cannot depend on itself")

        # Check for circular dependencies using topological sort
        cycle_errors = DAGValidator._detect_cycles(tasks)
        errors.extend(cycle_errors)

        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }

    @staticmethod
    def _detect_cycles(tasks: List[Dict[str, Any]]) -> List[str]:
        """Detect circular dependencies using topological sort."""
        errors = []

        # Build adjacency list and in-degree map
        adj_list = {}
        in_degree = {}
        task_ids = {task.get('task_id') for task in tasks if 'task_id' in task}

        for task in tasks:
            task_id = task.get('task_id')
            if task_id:
                adj_list[task_id] = task.get('dependencies', [])
                in_degree[task_id] = 0

        # Calculate in-degrees
        for task in tasks:
            for dep in task.get('dependencies', []):
                if dep in in_degree:
                    in_degree[dep] += 1

        # Topological sort using Kahn's algorithm
        queue = [task_id for task_id in in_degree if in_degree[task_id] == 0]
        visited_count = 0

        while queue:
            current = queue.pop(0)
            visited_count += 1

            for neighbor in adj_list.get(current, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # If not all tasks were visited, there's a cycle
        if visited_count < len(task_ids):
            errors.append("Circular dependency detected in task graph")

        return errors

    @staticmethod
    def validate_schedule_interval(interval: str) -> Dict[str, Any]:
        """Validate schedule interval."""
        errors = config.validate_schedule_interval(interval)
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': []
        }

    @staticmethod
    def validate_task_count(task_count: int, complexity: str) -> Dict[str, Any]:
        """Validate task count against complexity constraints."""
        try:
            complexity_level = ComplexityLevel(complexity)
        except ValueError:
            return {
                'is_valid': False,
                'errors': [f"Invalid complexity level: {complexity}"],
                'warnings': []
            }

        errors = config.validate_task_count(task_count, complexity_level)
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': []
        }

    @staticmethod
    def validate_dag_spec(dag_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive validation of complete DAG specification."""
        all_errors = []
        all_warnings = []

        logger.info(f"Validating DAG spec with {len(dag_spec.get('tasks', []))} tasks")

        # Validate basic fields
        if 'dag_name' in dag_spec:
            name_validation = DAGValidator.validate_dag_name(dag_spec['dag_name'])
            all_errors.extend(name_validation['errors'])
            all_warnings.extend(name_validation['warnings'])

        if 'dag_id' in dag_spec:
            id_validation = DAGValidator.validate_dag_id(dag_spec['dag_id'])
            all_errors.extend(id_validation['errors'])
            all_warnings.extend(id_validation['warnings'])

        if 'schedule_interval' in dag_spec:
            schedule_validation = DAGValidator.validate_schedule_interval(dag_spec['schedule_interval'])
            all_errors.extend(schedule_validation['errors'])
            all_warnings.extend(schedule_validation['warnings'])

        # Validate tasks
        tasks = dag_spec.get('tasks', [])
        if not tasks:
            all_errors.append("DAG must contain at least one task")
        else:
            # Validate task count
            task_count_validation = DAGValidator.validate_task_count(
                len(tasks),
                dag_spec.get('complexity', 'medium')
            )
            all_errors.extend(task_count_validation['errors'])

            # Validate individual tasks
            for i, task in enumerate(tasks):
                task_validation = DAGValidator.validate_task(task)
                if not task_validation['is_valid']:
                    all_errors.extend([f"Task {i+1}: {error}" for error in task_validation['errors']])
                all_warnings.extend([f"Task {i+1}: {warning}" for warning in task_validation['warnings']])

            # Validate dependencies
            dep_validation = DAGValidator.validate_dependencies(tasks)
            all_errors.extend(dep_validation['errors'])
            all_warnings.extend(dep_validation['warnings'])

        # Check for reasonable complexity
        task_count = len(tasks)
        complexity = dag_spec.get('complexity', 'medium')

        if complexity == 'simple' and task_count > 3:
            all_warnings.append(f"Simple complexity with {task_count} tasks may be too complex")
        elif complexity == 'complex' and task_count < 10:
            all_warnings.append(f"Complex complexity with only {task_count} tasks may be overkill")

        # Generate summary
        validation_summary = {
            'total_checks': 6,  # Basic fields + tasks + dependencies + complexity
            'passed_checks': 6 - len(all_errors),
            'critical_issues': len([e for e in all_errors if any(word in e.lower() for word in ['missing', 'invalid', 'required'])]),
            'warnings_count': len(all_warnings)
        }

        result = {
            'is_valid': len(all_errors) == 0,
            'issues': all_errors,
            'warnings': all_warnings,
            'suggestions': DAGValidator._generate_suggestions(all_errors, dag_spec),
            'task_count_assessment': DAGValidator._assess_task_count(task_count, dag_spec.get('complexity', 'medium')),
            'complexity_score': DAGValidator._calculate_complexity_score(dag_spec),
            'structure_score': DAGValidator._calculate_structure_score(tasks),
            'validation_summary': validation_summary
        }

        logger.info(f"Validation complete: {len(all_errors)} errors, {len(all_warnings)} warnings")
        return result

    @staticmethod
    def _generate_suggestions(errors: List[str], dag_spec: Dict[str, Any]) -> List[str]:
        """Generate helpful suggestions based on validation errors."""
        suggestions = []

        if any('task_id' in error.lower() for error in errors):
            suggestions.append("Use snake_case for task IDs (e.g., 'extract_data' instead of 'extractData')")

        if any('dependencies' in error.lower() for error in errors):
            suggestions.append("Ensure all task dependencies reference valid task IDs")

        if any('cycle' in error.lower() for error in errors):
            suggestions.append("Remove circular dependencies by restructuring task flow")

        if any('complexity' in error.lower() for error in errors):
            task_count = len(dag_spec.get('tasks', []))
            if task_count <= 5:
                suggestions.append("Consider 'simple' complexity for small DAGs")
            elif task_count <= 15:
                suggestions.append("Consider 'medium' complexity for standard pipelines")
            else:
                suggestions.append("Consider 'complex' complexity for large workflows")

        return suggestions

    @staticmethod
    def _assess_task_count(task_count: int, complexity: str = "medium") -> str:
        """Assess if task count is reasonable based on complexity."""
        if task_count == 0:
            return "empty"

        # Get thresholds based on complexity
        from .config import config, ComplexityLevel

        try:
            complexity_level = ComplexityLevel(complexity)
        except ValueError:
            complexity_level = ComplexityLevel.MEDIUM

        min_tasks = getattr(config, f'min_tasks_{complexity_level.value}', 1)
        max_tasks = getattr(config, f'max_tasks_{complexity_level.value}', 15)

        if task_count < min_tasks:
            return "too_few"
        elif task_count <= max_tasks:
            if task_count <= min_tasks + 2:
                return "minimal"
            elif task_count <= (max_tasks + min_tasks) // 2:
                return "reasonable"
            else:
                return "moderate"
        else:
            if task_count <= max_tasks + 5:
                return "high"
            else:
                return "too_high"

    @staticmethod
    def _calculate_complexity_score(dag_spec: Dict[str, Any]) -> int:
        """Calculate complexity score based on various factors."""
        score = 0
        tasks = dag_spec.get('tasks', [])

        # Task count factor
        score += min(len(tasks) * 2, 20)

        # Dependency complexity
        total_deps = sum(len(task.get('dependencies', [])) for task in tasks)
        score += min(total_deps * 3, 20)

        # Task type diversity
        task_types = set(task.get('task_type', 'python') for task in tasks)
        score += len(task_types) * 5

        return min(score, 100)

    @staticmethod
    def _calculate_structure_score(tasks: List[Dict[str, Any]]) -> int:
        """Calculate structure score based on DAG organization."""
        if not tasks:
            return 0

        score = 50  # Base score

        # Check for proper dependency structure
        has_dependencies = any(task.get('dependencies', []) for task in tasks)
        if has_dependencies:
            score += 20

        # Check for reasonable task distribution
        task_count = len(tasks)
        if 3 <= task_count <= 20:
            score += 15

        # Check for proper naming
        proper_names = sum(1 for task in tasks if re.match(r'^[a-z][a-z0-9_]*$', task.get('task_id', '')))
        if proper_names == task_count:
            score += 15

        return min(score, 100)
