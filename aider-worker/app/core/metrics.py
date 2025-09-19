"""
Performance metrics collection for DAG creation system.

This module provides comprehensive metrics tracking for performance monitoring,
error rates, and system health indicators.
"""

import time
import psutil
from typing import Dict, Any, Optional
from contextlib import contextmanager
from datetime import datetime, timedelta

from .logging_config import get_logger

logger = get_logger(__name__)


class MetricsCollector:
    """Collects and tracks performance metrics for DAG creation operations."""

    def __init__(self):
        self.metrics = {
            'dag_creation': {
                'total_count': 0,
                'success_count': 0,
                'error_count': 0,
                'durations': [],
                'last_operation_time': None
            },
            'llm_operations': {
                'total_requests': 0,
                'successful_requests': 0,
                'failed_requests': 0,
                'response_times': [],
                'tokens_used': [],
                'last_request_time': None
            },
            'file_operations': {
                'files_created': 0,
                'files_uploaded': 0,
                'upload_failures': 0,
                'upload_durations': [],
                'total_bytes_uploaded': 0
            },
            'memory_usage': {
                'peak_usage_mb': 0,
                'average_usage_mb': 0,
                'measurements': []
            },
            'websocket_operations': {
                'connections_established': 0,
                'connections_closed': 0,
                'messages_processed': 0,
                'errors': 0
            }
        }

    def record_dag_creation_start(self, project_id: str, user_id: str):
        """Record the start of a DAG creation operation."""
        self.metrics['dag_creation']['total_count'] += 1
        self.metrics['dag_creation']['last_operation_time'] = datetime.now()
        logger.info(f"Started DAG creation for project {project_id}, user {user_id}")

    def record_dag_creation_complete(self, duration: float, success: bool = True):
        """Record completion of a DAG creation operation."""
        self.metrics['dag_creation']['durations'].append(duration)
        if success:
            self.metrics['dag_creation']['success_count'] += 1
        else:
            self.metrics['dag_creation']['error_count'] += 1

        # Keep only last 1000 measurements to prevent unbounded growth
        if len(self.metrics['dag_creation']['durations']) > 1000:
            self.metrics['dag_creation']['durations'] = self.metrics['dag_creation']['durations'][-1000:]

        status = "SUCCESS" if success else "FAILED"
        logger.info(f"DAG creation {status} in {duration:.2f}s")

    def record_llm_request(self, prompt_length: int):
        """Record an LLM request."""
        self.metrics['llm_operations']['total_requests'] += 1
        self.metrics['llm_operations']['last_request_time'] = datetime.now()
        logger.debug(f"LLM request with {prompt_length} characters")

    def record_llm_response(self, duration: float, success: bool = True, tokens_used: Optional[int] = None):
        """Record an LLM response."""
        self.metrics['llm_operations']['response_times'].append(duration)

        if success:
            self.metrics['llm_operations']['successful_requests'] += 1
        else:
            self.metrics['llm_operations']['failed_requests'] += 1

        if tokens_used:
            self.metrics['llm_operations']['tokens_used'].append(tokens_used)

        # Keep only last 1000 measurements
        if len(self.metrics['llm_operations']['response_times']) > 1000:
            self.metrics['llm_operations']['response_times'] = self.metrics['llm_operations']['response_times'][-1000:]

        if tokens_used and len(self.metrics['llm_operations']['tokens_used']) > 1000:
            self.metrics['llm_operations']['tokens_used'] = self.metrics['llm_operations']['tokens_used'][-1000:]

        status = "SUCCESS" if success else "FAILED"
        logger.info(f"LLM response {status} in {duration:.2f}s")

    def record_file_operation(self, operation: str, file_count: int = 1, bytes_uploaded: int = 0):
        """Record file operations."""
        if operation == "created":
            self.metrics['file_operations']['files_created'] += file_count
        elif operation == "uploaded":
            self.metrics['file_operations']['files_uploaded'] += file_count
            self.metrics['file_operations']['total_bytes_uploaded'] += bytes_uploaded
        elif operation == "upload_failed":
            self.metrics['file_operations']['upload_failures'] += file_count

    def record_upload_duration(self, duration: float):
        """Record file upload duration."""
        self.metrics['file_operations']['upload_durations'].append(duration)

        # Keep only last 1000 measurements
        if len(self.metrics['file_operations']['upload_durations']) > 1000:
            self.metrics['file_operations']['upload_durations'] = self.metrics['file_operations']['upload_durations'][-1000:]

    def record_memory_usage(self):
        """Record current memory usage."""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024

        self.metrics['memory_usage']['measurements'].append(memory_mb)
        self.metrics['memory_usage']['peak_usage_mb'] = max(
            self.metrics['memory_usage']['peak_usage_mb'],
            memory_mb
        )

        # Calculate running average
        measurements = self.metrics['memory_usage']['measurements'][-100:]  # Last 100 measurements
        if measurements:
            self.metrics['memory_usage']['average_usage_mb'] = sum(measurements) / len(measurements)

        # Keep only last 1000 measurements
        if len(self.metrics['memory_usage']['measurements']) > 1000:
            self.metrics['memory_usage']['measurements'] = self.metrics['memory_usage']['measurements'][-1000:]

    def record_websocket_event(self, event: str):
        """Record WebSocket events."""
        if event == "connected":
            self.metrics['websocket_operations']['connections_established'] += 1
        elif event == "disconnected":
            self.metrics['websocket_operations']['connections_closed'] += 1
        elif event == "message":
            self.metrics['websocket_operations']['messages_processed'] += 1
        elif event == "error":
            self.metrics['websocket_operations']['errors'] += 1

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics for all metrics."""
        stats = {}

        # DAG creation stats
        dag_stats = self.metrics['dag_creation']
        if dag_stats['durations']:
            stats['dag_creation'] = {
                'total_operations': dag_stats['total_count'],
                'success_rate': (dag_stats['success_count'] / dag_stats['total_count']) * 100 if dag_stats['total_count'] > 0 else 0,
                'average_duration': sum(dag_stats['durations']) / len(dag_stats['durations']),
                'min_duration': min(dag_stats['durations']),
                'max_duration': max(dag_stats['durations']),
                'last_operation': dag_stats['last_operation_time']
            }

        # LLM stats
        llm_stats = self.metrics['llm_operations']
        if llm_stats['response_times']:
            stats['llm_operations'] = {
                'total_requests': llm_stats['total_requests'],
                'success_rate': (llm_stats['successful_requests'] / llm_stats['total_requests']) * 100 if llm_stats['total_requests'] > 0 else 0,
                'average_response_time': sum(llm_stats['response_times']) / len(llm_stats['response_times']),
                'total_tokens_used': sum(llm_stats['tokens_used']) if llm_stats['tokens_used'] else 0,
                'last_request': llm_stats['last_request_time']
            }

        # File operations stats
        file_stats = self.metrics['file_operations']
        if file_stats['upload_durations']:
            stats['file_operations'] = {
                'files_created': file_stats['files_created'],
                'files_uploaded': file_stats['files_uploaded'],
                'upload_success_rate': ((file_stats['files_uploaded']) / (file_stats['files_uploaded'] + file_stats['upload_failures'])) * 100 if file_stats['files_uploaded'] > 0 else 0,
                'average_upload_time': sum(file_stats['upload_durations']) / len(file_stats['upload_durations']),
                'total_bytes_uploaded': file_stats['total_bytes_uploaded']
            }

        # Memory stats
        memory_stats = self.metrics['memory_usage']
        stats['memory_usage'] = {
            'current_usage_mb': memory_stats['measurements'][-1] if memory_stats['measurements'] else 0,
            'peak_usage_mb': memory_stats['peak_usage_mb'],
            'average_usage_mb': memory_stats['average_usage_mb']
        }

        # WebSocket stats
        ws_stats = self.metrics['websocket_operations']
        stats['websocket_operations'] = {
            'active_connections': ws_stats['connections_established'] - ws_stats['connections_closed'],
            'total_messages': ws_stats['messages_processed'],
            'error_rate': (ws_stats['errors'] / ws_stats['messages_processed']) * 100 if ws_stats['messages_processed'] > 0 else 0
        }

        return stats

    def reset(self):
        """Reset all metrics (useful for testing)."""
        for category in self.metrics.values():
            for key in category:
                if isinstance(category[key], list):
                    category[key].clear()
                elif isinstance(category[key], (int, float)):
                    category[key] = 0
                else:
                    category[key] = None


# Global metrics collector instance
metrics_collector = MetricsCollector()


@contextmanager
def track_dag_creation(project_id: str, user_id: str):
    """Context manager to track DAG creation performance."""
    start_time = time.time()
    metrics_collector.record_dag_creation_start(project_id, user_id)
    metrics_collector.record_memory_usage()

    try:
        yield
        duration = time.time() - start_time
        metrics_collector.record_dag_creation_complete(duration, success=True)
        metrics_collector.record_memory_usage()
    except Exception as e:
        duration = time.time() - start_time
        metrics_collector.record_dag_creation_complete(duration, success=False)
        metrics_collector.record_memory_usage()
        raise


@contextmanager
def track_llm_operation(prompt_length: int):
    """Context manager to track LLM operation performance."""
    start_time = time.time()
    metrics_collector.record_llm_request(prompt_length)

    try:
        yield
        duration = time.time() - start_time
        metrics_collector.record_llm_response(duration, success=True)
    except Exception as e:
        duration = time.time() - start_time
        metrics_collector.record_llm_response(duration, success=False)
        raise


@contextmanager
def track_file_upload(file_count: int):
    """Context manager to track file upload performance."""
    start_time = time.time()

    try:
        yield
        duration = time.time() - start_time
        metrics_collector.record_file_operation("uploaded", file_count)
        metrics_collector.record_upload_duration(duration)
    except Exception as e:
        metrics_collector.record_file_operation("upload_failed", file_count)
        raise


def log_performance_summary():
    """Log a summary of current performance metrics."""
    stats = metrics_collector.get_summary_stats()

    logger.info("=== PERFORMANCE SUMMARY ===")

    if 'dag_creation' in stats:
        dag = stats['dag_creation']
        logger.info(f"DAG Creation: {dag['total_operations']} ops, "
                   f"{dag['success_rate']:.1f}% success, "
                   f"{dag['average_duration']:.2f}s avg")

    if 'llm_operations' in stats:
        llm = stats['llm_operations']
        logger.info(f"LLM Operations: {llm['total_requests']} requests, "
                   f"{llm['success_rate']:.1f}% success, "
                   f"{llm['average_response_time']:.2f}s avg")

    if 'file_operations' in stats:
        files = stats['file_operations']
        logger.info(f"File Operations: {files['files_uploaded']} uploaded, "
                   f"{files['upload_success_rate']:.1f}% success, "
                   f"{files['average_upload_time']:.2f}s avg")

    if 'memory_usage' in stats:
        memory = stats['memory_usage']
        logger.info(f"Memory Usage: {memory['current_usage_mb']:.1f}MB current, "
                   f"{memory['peak_usage_mb']:.1f}MB peak, "
                   f"{memory['average_usage_mb']:.1f}MB avg")

    logger.info("==========================")

