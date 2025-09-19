"""
Enhanced error handling and recovery mechanisms for DAG creation system.

This module provides comprehensive error handling, recovery strategies,
and graceful degradation for various failure scenarios.
"""

import asyncio
import json
import traceback
from typing import Dict, Any, Optional, Callable, List
from enum import Enum

from .logging_config import get_logger
from .metrics import metrics_collector

logger = get_logger(__name__)


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for better classification."""
    NETWORK = "network"
    PARSING = "parsing"
    VALIDATION = "validation"
    STORAGE = "storage"
    CONFIGURATION = "configuration"
    RESOURCE = "resource"
    UNKNOWN = "unknown"


class DAGError(Exception):
    """Custom exception for DAG creation errors."""

    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        recoverable: bool = True,
        context: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.category = category
        self.severity = severity
        self.recoverable = recoverable
        self.context = context or {}


class ErrorHandler:
    """Handles errors with recovery strategies and graceful degradation."""

    def __init__(self):
        self.recovery_strategies = {
            ErrorCategory.NETWORK: self._handle_network_error,
            ErrorCategory.PARSING: self._handle_parsing_error,
            ErrorCategory.VALIDATION: self._handle_validation_error,
            ErrorCategory.STORAGE: self._handle_storage_error,
            ErrorCategory.CONFIGURATION: self._handle_configuration_error,
            ErrorCategory.RESOURCE: self._handle_resource_error,
        }

    async def handle_error(
        self,
        error: Exception,
        operation: str,
        context: Optional[Dict[str, Any]] = None,
        max_retries: int = 3
    ) -> Optional[Any]:
        """
        Handle an error with appropriate recovery strategy.

        Args:
            error: The exception that occurred
            operation: Description of the operation that failed
            context: Additional context about the error
            max_retries: Maximum number of retry attempts

        Returns:
            Recovery result or None if recovery failed
        """
        context = context or {}

        # Classify the error
        category, severity = self._classify_error(error)

        # Log the error with full context
        self._log_error(error, operation, category, severity, context)

        # Record error metrics
        metrics_collector.record_websocket_event("error")

        # Attempt recovery if error is recoverable
        if self._is_recoverable(error, category):
            return await self._attempt_recovery(
                error, operation, category, context, max_retries
            )

        # If not recoverable, return None
        return None

    def _classify_error(self, error: Exception) -> tuple[ErrorCategory, ErrorSeverity]:
        """Classify an error into category and severity."""
        error_type = type(error).__name__
        error_message = str(error).lower()

        # Network-related errors
        if any(keyword in error_message for keyword in ['connection', 'timeout', 'network', 'http']):
            return ErrorCategory.NETWORK, ErrorSeverity.HIGH

        # Parsing-related errors
        if any(keyword in error_message for keyword in ['json', 'parse', 'decode', 'syntax']):
            return ErrorCategory.PARSING, ErrorSeverity.MEDIUM

        # Validation-related errors
        if any(keyword in error_message for keyword in ['validation', 'invalid', 'missing', 'required']):
            return ErrorCategory.VALIDATION, ErrorSeverity.LOW

        # Storage-related errors
        if any(keyword in error_message for keyword in ['storage', 'upload', 'file', 'database']):
            return ErrorCategory.STORAGE, ErrorSeverity.HIGH

        # Configuration-related errors
        if any(keyword in error_message for keyword in ['config', 'setting', 'environment']):
            return ErrorCategory.CONFIGURATION, ErrorSeverity.MEDIUM

        # Resource-related errors
        if any(keyword in error_message for keyword in ['memory', 'disk', 'cpu', 'resource']):
            return ErrorCategory.RESOURCE, ErrorSeverity.CRITICAL

        # Default classification
        return ErrorCategory.UNKNOWN, ErrorSeverity.MEDIUM

    def _is_recoverable(self, error: Exception, category: ErrorCategory) -> bool:
        """Determine if an error is recoverable."""
        # Network errors are often recoverable
        if category == ErrorCategory.NETWORK:
            return True

        # Some parsing errors might be recoverable
        if category == ErrorCategory.PARSING:
            return True

        # Storage errors might be recoverable
        if category == ErrorCategory.STORAGE:
            return True

        # Resource errors are usually not recoverable
        if category == ErrorCategory.RESOURCE:
            return False

        # Validation errors are usually not recoverable
        if category == ErrorCategory.VALIDATION:
            return False

        # Default to recoverable for unknown errors
        return True

    async def _attempt_recovery(
        self,
        error: Exception,
        operation: str,
        category: ErrorCategory,
        context: Dict[str, Any],
        max_retries: int
    ) -> Optional[Any]:
        """Attempt to recover from an error."""
        recovery_strategy = self.recovery_strategies.get(category, self._handle_generic_error)

        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting recovery (attempt {attempt + 1}/{max_retries}) for {operation}")
                result = await recovery_strategy(error, context, attempt)
                if result is not None:
                    logger.info(f"Recovery successful for {operation}")
                    return result
            except Exception as recovery_error:
                logger.warning(f"Recovery attempt {attempt + 1} failed: {recovery_error}")

        logger.error(f"All recovery attempts failed for {operation}")
        return None

    async def _handle_network_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        attempt: int
    ) -> Optional[Any]:
        """Handle network-related errors with exponential backoff."""
        delay = min(2 ** attempt, 30)  # Exponential backoff, max 30 seconds

        logger.info(f"Network error recovery: waiting {delay}s before retry")
        await asyncio.sleep(delay)

        # For network errors, we typically want to retry the operation
        # The calling code should handle the actual retry logic
        return {"retry": True, "delay": delay}

    async def _handle_parsing_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        attempt: int
    ) -> Optional[Any]:
        """Handle parsing errors by attempting alternative parsing strategies."""
        if attempt == 0:
            # First attempt: try to clean the data
            logger.info("Parsing error recovery: attempting data cleaning")

            if 'response' in context:
                response = context['response']
                # Try to extract JSON with more lenient regex
                import re
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response, re.DOTALL)
                if json_match:
                    try:
                        cleaned_json = json_match.group()
                        parsed_data = json.loads(cleaned_json)
                        return parsed_data
                    except json.JSONDecodeError:
                        pass

        # Return fallback data for parsing errors
        return {
            "dag_name": "error_recovery_dag",
            "description": f"DAG created after parsing error recovery: {str(error)}",
            "schedule_interval": "@daily",
            "owner": "error_handler",
            "tags": ["error_recovery"],
            "tasks": []
        }

    async def _handle_validation_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        attempt: int
    ) -> Optional[Any]:
        """Handle validation errors by attempting to fix common issues."""
        if 'dag_spec' in context:
            dag_spec = context['dag_spec']

            # Try to fix common validation issues
            if 'dag_id' not in dag_spec and 'description' in dag_spec:
                from .dag_naming import generate_smart_dag_name
                dag_spec['dag_id'] = generate_smart_dag_name(
                    dag_spec['description'], 'medium', None
                )
                logger.info("Validation recovery: added missing dag_id")
                return dag_spec

            if 'owner' not in dag_spec:
                dag_spec['owner'] = 'system'
                logger.info("Validation recovery: added default owner")
                return dag_spec

        return None

    async def _handle_storage_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        attempt: int
    ) -> Optional[Any]:
        """Handle storage errors with retry logic."""
        delay = min(5 * (attempt + 1), 60)  # Linear backoff, max 60 seconds

        logger.info(f"Storage error recovery: waiting {delay}s before retry")
        await asyncio.sleep(delay)

        return {"retry": True, "delay": delay}

    async def _handle_configuration_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        attempt: int
    ) -> Optional[Any]:
        """Handle configuration errors by using defaults."""
        logger.info("Configuration error recovery: using default values")

        # Return default configuration values
        return {
            "upload_batch_size": 5,
            "max_concurrent_uploads": 5,
            "max_tasks_complex": 25,
            "default_retries": 2
        }

    async def _handle_resource_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        attempt: int
    ) -> Optional[Any]:
        """Handle resource errors (usually not recoverable)."""
        logger.error("Resource error: usually not recoverable")
        return None

    async def _handle_generic_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        attempt: int
    ) -> Optional[Any]:
        """Handle generic errors with basic retry logic."""
        delay = min(10 * (attempt + 1), 120)  # Longer backoff for generic errors

        logger.info(f"Generic error recovery: waiting {delay}s before retry")
        await asyncio.sleep(delay)

        return {"retry": True, "delay": delay}

    def _log_error(
        self,
        error: Exception,
        operation: str,
        category: ErrorCategory,
        severity: ErrorSeverity,
        context: Dict[str, Any]
    ):
        """Log error with full context."""
        error_details = {
            'operation': operation,
            'category': category.value,
            'severity': severity.value,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc(),
            'context': context
        }

        if severity == ErrorSeverity.CRITICAL:
            logger.critical(f"Critical error in {operation}: {error}", extra=error_details)
        elif severity == ErrorSeverity.HIGH:
            logger.error(f"High severity error in {operation}: {error}", extra=error_details)
        elif severity == ErrorSeverity.MEDIUM:
            logger.warning(f"Medium severity error in {operation}: {error}", extra=error_details)
        else:
            logger.info(f"Low severity error in {operation}: {error}", extra=error_details)


# Global error handler instance
error_handler = ErrorHandler()


async def with_error_recovery(
    operation: Callable,
    operation_name: str,
    context: Optional[Dict[str, Any]] = None,
    max_retries: int = 3
) -> Optional[Any]:
    """
    Execute an operation with automatic error recovery.

    Args:
        operation: Async callable to execute
        operation_name: Name of the operation for logging
        context: Additional context for error handling
        max_retries: Maximum number of retry attempts

    Returns:
        Operation result or None if all recovery attempts fail
    """
    try:
        result = await operation()
        return result
    except Exception as e:
        return await error_handler.handle_error(e, operation_name, context, max_retries)


def create_fallback_dag_spec(error_message: str) -> Dict[str, Any]:
    """Create a fallback DAG specification when recovery fails."""
    return {
        "dag_name": "error_recovery_dag",
        "dag_id": "error_recovery_dag",
        "description": f"DAG created after error recovery: {error_message}",
        "schedule_interval": "@daily",
        "owner": "system",
        "tags": ["error_recovery"],
        "complexity": "simple",
        "tasks": [
            {
                "task_id": "error_notification",
                "task_type": "python",
                "description": "Send error notification",
                "python_code": f"print('Error occurred: {error_message}')",
                "dependencies": []
            }
        ]
    }

