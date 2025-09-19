"""
DAG naming utilities.

This module provides intelligent naming and sanitization for DAGs and tasks.
"""

import re
from typing import Optional

from .logging_config import get_logger

logger = get_logger(__name__)


def sanitize_dag_name(name: str) -> str:
    """
    Sanitize and intelligently generate DAG name to be valid for Airflow.

    This function creates descriptive, meaningful DAG names that are:
    - Valid for Airflow (start with letter/underscore, alphanumeric + underscores only)
    - Descriptive and meaningful
    - Properly formatted
    - Unique-ready (can be made unique by appending numbers)
    """
    if not name or not name.strip():
        return "custom_dag"

    name = name.strip()

    # Extract meaningful keywords from common patterns
    meaningful_patterns = [
        # "daily user analytics pipeline" -> "daily_user_analytics"
        r'\b(daily|hourly|weekly|monthly|yearly)\b.*?\b(\w+)\b.*?\b(\w+)\b',
        # "data pipeline for users" -> "data_pipeline_users"
        r'\b(data|etl|analytics|report|pipeline)\b.*?\b(for|of|with)\b.*?\b(\w+)\b',
        # "process user data" -> "process_user_data"
        r'\b(process|extract|transform|load|generate|create|run)\b.*?\b(\w+)\b.*?\b(\w+)\b',
        # "ML model training" -> "ml_model_training"
        r'\b(ml|machine.learning|ai|training|model)\b.*?\b(\w+)\b',
    ]

    # Try to match meaningful patterns
    for pattern in meaningful_patterns:
        match = re.search(pattern, name.lower())
        if match:
            # Extract meaningful words from the match
            words = []
            for group in match.groups():
                if group and len(group) > 2:  # Skip very short words
                    words.append(group.lower())

            if words:
                # Create a descriptive name from the extracted words
                base_name = '_'.join(words[:4])  # Limit to 4 words for readability
                break
    else:
        # Fallback: extract meaningful words from the entire name
        # Remove common stop words and extract key nouns/verbs
        stop_words = {
            'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
            'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
            'should', 'may', 'might', 'must', 'can', 'that', 'this', 'these',
            'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him',
            'her', 'us', 'them', 'my', 'your', 'his', 'its', 'our', 'their',
            'what', 'when', 'where', 'why', 'how', 'who', 'which'
        }

        # Extract words and filter
        words = re.findall(r'\b\w+\b', name.lower())
        meaningful_words = [
            word for word in words
            if len(word) > 2 and word not in stop_words
        ]

        if meaningful_words:
            base_name = '_'.join(meaningful_words[:4])
        else:
            base_name = "custom_dag"

    # Clean up the name
    base_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name)  # Replace invalid chars with underscore
    base_name = re.sub(r'_+', '_', base_name)  # Replace multiple underscores with single
    base_name = base_name.strip('_')  # Remove leading/trailing underscores

    # Ensure it starts with a letter or underscore
    if base_name and not base_name[0].isalpha() and base_name[0] != '_':
        base_name = 'dag_' + base_name

    # Final fallback
    if not base_name:
        base_name = "custom_dag"

    # Ensure reasonable length
    if len(base_name) > 50:
        base_name = base_name[:47] + "..."

    logger.debug(f"Sanitized DAG name: '{name}' -> '{base_name}'")
    return base_name


def is_valid_dag_id(dag_id: str) -> bool:
    """
    Validate if a DAG ID follows Airflow naming conventions.

    Args:
        dag_id: The DAG ID to validate

    Returns:
        bool: True if valid, False otherwise
    """
    import re

    if not dag_id or not isinstance(dag_id, str):
        return False

    # Airflow DAG ID rules:
    # - Must start with letter or underscore
    # - Can contain letters, numbers, and underscores only
    # - No spaces or special characters
    # - Maximum length of 250 characters (be reasonable)
    if len(dag_id) > 250 or len(dag_id) == 0:
        return False

    # Must match pattern: starts with letter/underscore, followed by alphanumeric/underscore
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
    return bool(re.match(pattern, dag_id))


def generate_smart_dag_name(
    description: str,
    complexity: str = "medium",
    llm_dag_id: Optional[str] = None
) -> str:
    """
    Generate an intelligent DAG name based on the description and complexity.

    This creates more descriptive and meaningful names than simple sanitization.
    If llm_dag_id is provided and valid, it will be used to preserve LLM naming consistency.

    Args:
        description: Natural language description of the DAG
        complexity: Complexity level ("simple", "medium", "complex")
        llm_dag_id: LLM-generated DAG ID to preserve naming consistency

    Returns:
        str: Generated DAG name
    """
    # If LLM provided a valid DAG ID, prioritize it for naming consistency
    if llm_dag_id and is_valid_dag_id(llm_dag_id):
        logger.info(f"Using LLM-provided DAG ID: {llm_dag_id}")
        return llm_dag_id

    logger.debug(f"Generating smart DAG name for: {description}")

    description_lower = description.lower()

    # Complexity prefixes for better organization
    complexity_prefixes = {
        "simple": "",
        "medium": "",
        "complex": "advanced_"
    }

    prefix = complexity_prefixes.get(complexity, "")

    # Domain-specific naming patterns
    domain_patterns = {
        "data": ["pipeline", "processing", "etl", "analytics"],
        "ml": ["training", "inference", "model", "learning"],
        "reporting": ["report", "dashboard", "metrics", "insights"],
        "automation": ["workflow", "automation", "process", "job"],
        "integration": ["sync", "integration", "connector", "bridge"]
    }

    # Detect domain from description
    detected_domain = "data"  # default
    for domain, keywords in domain_patterns.items():
        if any(keyword in description_lower for keyword in keywords):
            detected_domain = domain
            break

    # Get base name from description
    base_name = sanitize_dag_name(description)

    # Combine domain, prefix, and base name for a smart name
    if detected_domain != "data" and not base_name.startswith(detected_domain):
        smart_name = f"{detected_domain}_{prefix}{base_name}"
    else:
        smart_name = f"{prefix}{base_name}" if prefix else base_name

    # Ensure it's not too long
    if len(smart_name) > 50:
        smart_name = smart_name[:47] + "..."

    logger.info(f"Generated smart DAG name: {smart_name}")
    return smart_name


def generate_task_name(action: str, target: str, context: Optional[str] = None) -> str:
    """
    Generate a consistent task name following naming conventions.

    Args:
        action: Action verb (extract, transform, load, validate, etc.)
        target: Target of the action (data, orders, users, etc.)
        context: Optional context (raw, processed, validated, etc.)

    Returns:
        str: Generated task name in snake_case
    """
    # Normalize inputs
    action = action.lower().strip()
    target = target.lower().strip()
    if context:
        context = context.lower().strip()

    # Build name components
    parts = [action, target]
    if context:
        parts.append(context)

    # Create snake_case name
    task_name = '_'.join(parts)

    # Ensure it follows naming conventions
    task_name = re.sub(r'[^a-z0-9_]', '_', task_name)
    task_name = re.sub(r'_+', '_', task_name)
    task_name = task_name.strip('_')

    # Ensure it starts with lowercase letter
    if task_name and not task_name[0].isalpha():
        task_name = f"task_{task_name}"

    # Limit length
    if len(task_name) > 30:
        task_name = task_name[:27] + "..."

    logger.debug(f"Generated task name: {action} + {target} -> {task_name}")
    return task_name


def suggest_dag_name_improvements(dag_name: str, description: str) -> list[str]:
    """
    Suggest improvements for DAG naming based on description.

    Args:
        dag_name: Current DAG name
        description: DAG description

    Returns:
        list[str]: List of improvement suggestions
    """
    suggestions = []

    # Check if name reflects the description
    description_keywords = set(re.findall(r'\b\w+\b', description.lower()))
    name_keywords = set(re.findall(r'\b\w+\b', dag_name.lower()))

    overlap = description_keywords.intersection(name_keywords)
    if len(overlap) < 2:
        suggestions.append("Consider including more descriptive keywords from the description")

    # Check length
    if len(dag_name) < 10:
        suggestions.append("Consider a more descriptive name")
    elif len(dag_name) > 40:
        suggestions.append("Consider shortening the name for better readability")

    # Check for common patterns
    common_patterns = ["daily", "hourly", "weekly", "monthly", "data", "pipeline", "process"]
    has_common_pattern = any(pattern in dag_name.lower() for pattern in common_patterns)
    if not has_common_pattern:
        suggestions.append("Consider including frequency (daily/hourly) or type (pipeline/process) in the name")

    return suggestions

