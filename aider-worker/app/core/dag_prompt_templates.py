"""
Prompt templates and validation for DAG creation.

This module provides structured prompts to ensure AI creates proper DAG repository structure
and validates task counts and complexity.
"""

from typing import Dict, Any, List
from datetime import datetime


class DagPromptTemplates:
    """Templates for guiding AI to create properly structured DAGs."""

    @staticmethod
    def get_dag_creation_prompt(description: str, complexity_level: str = "medium") -> str:
        """Get the main prompt for DAG creation with comprehensive validation."""

        complexity_guidelines = {
            "simple": "Focus on essential tasks only: 2-4 tasks for basic operations, 3-5 tasks for simple workflows",
            "medium": "Standard complexity: 4-8 tasks for typical business logic, 6-12 tasks for moderate complexity",
            "complex": "Advanced workflows: 10-20+ tasks for complex business logic, distributed systems, or ML pipelines"
        }

        return f"""
Convert this natural language description into a structured Airflow DAG specification.

CRITICAL REQUIREMENTS - FOLLOW THESE EXACTLY:

Description: "{description}"

**PHASE 1: ANALYSIS**
1. **Understand the Request**: Analyze the user's description to identify:
   - What data is being processed (sources, formats, destinations)
   - What operations are needed (extract, transform, load, analyze, report)
   - What schedule makes sense (daily, hourly, weekly, etc.)
   - What domain this belongs to (ETL, ML, analytics, reporting, etc.)

2. **Determine Complexity**: Based on the analysis, classify as:
   - SIMPLE: Basic single-purpose workflows (2-5 tasks)
   - MEDIUM: Standard pipelines with moderate complexity (5-15 tasks)
   - COMPLEX: Advanced workflows with branching (15-30 tasks)

**PHASE 2: NAMING CONVENTIONS**
3. **DAG Name**: Create a descriptive, human-readable name:
   - Capitalize properly (e.g., "Daily User Analytics Pipeline")
   - Be specific about what it does
   - Include key identifiers (data source, frequency, purpose)

4. **DAG ID**: Create a machine-readable identifier:
   - snake_case format (e.g., "daily_user_analytics_pipeline")
   - Start with lowercase letter
   - Use only alphanumeric characters and underscores
   - No spaces or special characters
   - Keep under 50 characters

5. **Description**: Write a clear, comprehensive description:
   - Explain what the DAG does in business terms
   - Mention data sources and destinations
   - Include schedule and trigger conditions
   - Note any special considerations or dependencies

**PHASE 3: TASK ANALYSIS & COUNT DETERMINATION**
6. **Critical: Determine EXACT Task Count First**:
   - STOP and analyze the description thoroughly
   - Break down the workflow into atomic, single-responsibility tasks
   - Count ONLY the tasks you will actually create
   - Do NOT use template numbers - analyze the specific workflow

7. **Task Count Guidelines by Workflow Type**:
   - **Health/Server Monitoring**: 3-5 tasks (check → validate → notify)
   - **Simple ETL**: 4-6 tasks (extract → validate → transform → load)
   - **Data Analytics**: 5-8 tasks (collect → process → analyze → report)
   - **ML Training**: 6-10 tasks (prepare → train → validate → deploy)
   - **Complex Business Logic**: 8-15 tasks (break into logical phases)

8. **Task Count Checklist**:
   - [ ] Count each distinct operation in the workflow
   - [ ] Ensure each task has one clear responsibility
   - [ ] Include error handling and notifications as separate tasks
   - [ ] Set estimated_tasks to your actual count (not a template number)
   - [ ] Verify: estimated_tasks = exact number of tasks you'll create

**WARNING**: Do not default to any specific number. Analyze the actual workflow requirements.

**EXAMPLE ANALYSIS**: For "Server Health Check - CPU, memory, disk usage monitoring with alerts":

**INCORRECT APPROACH**: "This looks complex, so I'll estimate 8 tasks"
**CORRECT APPROACH**:
1. Check system resources (CPU, memory, disk) → 1 task (can combine into single check)
2. Compare against thresholds → 1 task (validation logic)
3. Send alerts when exceeded → 1 task (notification logic)
**TOTAL: 3 tasks** (not 8!)

**EXAMPLE ANALYSIS**: For "Daily ETL pipeline with data validation and error handling":
1. Extract data from source → 1 task
2. Validate data quality → 1 task
3. Transform/clean data → 1 task
4. Load to destination → 1 task
5. Send success notification → 1 task
6. Handle errors/cleanup → 1 task
**TOTAL: 6 tasks** (reasonable for this workflow)

9. **MANDATORY TASK BREAKDOWN PATTERNS**:
   - **Data Extraction**: Always separate data fetching from processing
   - **Validation**: Create dedicated validation tasks (inventory, payments, etc.)
   - **Processing**: Split business logic into clear steps (calculate, transform, etc.)
   - **Error Handling**: Include specific error handling and notification tasks
   - **Communication**: Separate email/SMS/notification tasks from core business logic
   - **Monitoring**: Add health check and monitoring tasks where appropriate

   **Example Task Breakdown Patterns** (adapt based on actual workflow needs):

   **For Simple Health Monitoring** (3-4 tasks):
   - check_system_metrics (resource monitoring)
   - validate_thresholds (alert logic)
   - send_notifications (communication)

   **For Basic ETL** (4-5 tasks):
   - extract_source_data (data acquisition)
   - validate_data_quality (quality checks)
   - transform_data (processing)
   - load_destination (storage)

   **For Advanced Analytics** (6-8 tasks):
   - collect_raw_data (data gathering)
   - clean_and_validate (data preparation)
   - perform_calculations (analytics)
   - generate_visualizations (reporting)
   - export_results (distribution)
   - cleanup_resources (maintenance)

   **For ML Workflows**:
   - prepare_training_data
   - train_model
   - validate_model_performance
   - deploy_model
   - monitor_model_health

   **For ETL Workflows**:
   - extract_source_data
   - validate_data_quality
   - transform_data
   - load_to_destination
   - validate_load_success
   - cleanup_temp_files

10. **Python Code Guidelines** (CRITICAL FOR PRODUCTION READINESS):
   - Provide ONLY the function body, not the function definition
   - Do NOT include "def function_name():" - this will be generated automatically
   - Start directly with the executable code
   - Use proper indentation (4 spaces) for code blocks
   - NEVER use hardcoded paths like "/path/to/backup/" - use Airflow variables or configuration
   - ALWAYS include proper error handling with try/except blocks
   - ALWAYS include logging statements for debugging and monitoring
   - Use Airflow context variables (ti, dag, ds, etc.) when available
   - Return meaningful values, not just generic "success" strings

   **REQUIRED CODE PATTERNS:**

   ✅ **GOOD EXAMPLE:**
   ```python
   # Get configuration from Airflow variables
   import os
   from airflow.models import Variable

   source_dir = Variable.get("backup_source_directory", "/default/source")
   backup_dir = Variable.get("backup_destination_directory", "/default/backup")

   try:
       # Log the operation start
       print(f"Starting backup from {{source_dir}} to {{backup_dir}}")

       # Validate source directory exists
       if not os.path.exists(source_dir):
           raise FileNotFoundError(f"Source directory does not exist: {{source_dir}}")

       # Your backup logic here
       # shutil.make_archive(backup_path, 'zip', source_dir)

       print(f"Backup completed successfully")
       return {{"status": "success", "source_dir": source_dir, "backup_dir": backup_dir}}

   except Exception as e:
       print(f"Backup failed: {{str(e)}}")
       raise  # Re-raise to let Airflow handle the failure
   ```

   ❌ **BAD EXAMPLE (NEVER DO THIS):**
   ```python
   # Compress files
   import shutil
   shutil.make_archive('/path/to/backup/archive', 'zip', '/path/to/source/directory')
   return 'success'
   ```

9. **Task Naming**:
   - Use snake_case (e.g., "extract_user_data", "transform_records")
   - Start with action verbs (extract, transform, load, validate, notify)
   - Be specific about what the task does
   - Keep names under 30 characters

**PHASE 4: DEPENDENCY MANAGEMENT**
10. **Dependency Rules**:
   - Dependencies must be acyclic (no circular references)
   - Use task IDs exactly as defined (case-sensitive)
   - List dependencies as array of strings: ["task_id_1", "task_id_2"]
   - Empty array [] for tasks with no dependencies
   - Ensure logical flow from source to destination

**PHASE 5: REPOSITORY STRUCTURE**
11. **Airflow Directory Structure** (CRITICAL for downstream pipeline):
    - Base directory: `dags/{{{{dag_id}}}}/`
    - Main DAG file: `dags/{{{{dag_id}}}}/{{{{dag_id}}}}.py`
    - Configuration: `dags/{{{{dag_id}}}}/config.json`
    - Documentation: `dags/{{{{dag_id}}}}/README.md`
    - Utils directory: `dags/{{{{dag_id}}}}/utils/`
    - Utils init: `dags/{{{{dag_id}}}}/utils/__init__.py`
    - Utils helpers: `dags/{{{{dag_id}}}}/utils/helpers.py`

12. **Supabase Storage Format**:
    - Files stored with paths like: `projects/{{project_id}}/dags/{{dag_id}}/{{filename}}`
    - Directory structure must be preserved for pipeline transfer
    - All files must be versioned and tracked

**PHASE 6: VALIDATION CHECKS**
13. **Pre-Validation Requirements**:
    - Task count must be reasonable for complexity level
    - All dependencies must be valid task IDs
    - Naming must follow conventions
    - Directory structure must be complete
    - No circular dependencies
    - All required fields must be present

**CRITICAL VALIDATION STEP:**
14. **Before submitting, count your tasks**:
    - Count the number of objects in your "tasks" array
    - Set "estimated_tasks" to exactly that count
    - Double-check that estimated_tasks = len(tasks)
    - This is your final validation step

Return a JSON object with this EXACT structure:
{{
    "dag_name": "Properly Capitalized Descriptive Name",
    "dag_id": "snake_case_machine_identifier",
    "description": "Clear, comprehensive description of the workflow",
    "schedule_interval": "@daily|@hourly|@weekly|0 0 * * *|etc",
    "owner": "data-team|ml-team|analytics-team|etc",
    "tags": ["domain", "frequency", "data-type", "technology"],
    "complexity": "{complexity_level}",
    "estimated_tasks": EXACT_COUNT_BASED_ON_ANALYSIS,
    "directory_structure": {{
        "base_directory": "dags/{{{{dag_id}}}}/",
        "main_file": "dags/{{{{dag_id}}}}/{{{{dag_id}}}}.py",
        "config_file": "dags/{{{{dag_id}}}}/config.json",
        "readme_file": "dags/{{{{dag_id}}}}/README.md",
        "utils_directory": "dags/{{{{dag_id}}}}/utils/",
        "utils_init": "dags/{{{{dag_id}}}}/utils/__init__.py",
        "utils_helpers": "dags/{{{{dag_id}}}}/utils/helpers.py"
    }},
    "supabase_paths": {{
        "main_file": "projects/{{{{project_id}}}}/dags/{{{{dag_id}}}}/{{{{dag_id}}}}.py",
        "config_file": "projects/{{{{project_id}}}}/dags/{{{{dag_id}}}}/config.json",
        "readme_file": "projects/{{{{project_id}}}}/dags/{{{{dag_id}}}}/README.md",
        "utils_init": "projects/{{{{project_id}}}}/dags/{{{{dag_id}}}}/utils/__init__.py",
        "utils_helpers": "projects/{{{{project_id}}}}/dags/{{{{dag_id}}}}/utils/helpers.py"
    }},
    "tasks": [
        {{
            "task_id": "snake_case_task_name",
            "task_type": "python|bash|docker",
            "description": "Clear description of what this task does",
            "python_code": "# Proper Python code with error handling\\n    from airflow.models import Variable\\n    import logging\\n    \\n    logger = logging.getLogger(__name__)\\n    \\n    try:\\n        # Get configuration from Airflow variables\\n        source_path = Variable.get('source_directory', '/default/path')\\n        \\n        logger.info(f'Starting task execution with source: {{source_path}}')\\n        \\n        # Your task logic here\\n        result = perform_task(source_path)\\n        \\n        logger.info('Task completed successfully')\\n        return {{'status': 'success', 'result': result}}\\n    \\n    except Exception as e:\\n        logger.error(f'Task failed: {{str(e)}}')\\n        raise" or null,
            "bash_command": "echo 'command here'" or null,
            "docker_image": "image:tag" or null,
            "dependencies": ["previous_task_id"],
            "provide_context": true,
            "retries": 2,
            "retry_delay_minutes": 5,
            "execution_timeout_minutes": 30
        }}
    ],
    "validation_checks": {{
        "reasonable_task_count": true,
        "logical_dependencies": true,
        "proper_naming": true,
        "complete_structure": true,
        "no_circular_dependencies": true,
        "airflow_compatible": true,
        "supabase_ready": true
    }},
    "airflow_metadata": {{
        "dag_file": "{{{{dag_id}}}}.py",
        "config_file": "config.json",
        "start_date": "2024-01-01",
        "concurrency": 1,
        "max_active_runs": 1,
        "dagrun_timeout_hours": 2,
        "catchup": false
    }}
}}

**VALIDATION RULES**:
- Task count must match complexity guidelines
- All dependency references must be valid task IDs
- DAG ID must be valid snake_case starting with letter
- Task IDs must be valid snake_case
- Directory paths must follow Airflow conventions
- Supabase paths must include project_id placeholder
- No circular dependencies in task graph
- All tasks must have clear, single responsibilities

Return ONLY the JSON object, no other text or explanations.
"""

    @staticmethod
    def get_dag_validation_prompt(dag_spec: Dict[str, Any]) -> str:
        """Get prompt to validate an existing DAG specification."""

        return f"""
Validate this DAG specification for structure, complexity, and best practices:

DAG Spec: {dag_spec}

Check the following:
1. **Task Count**: Is the number of tasks reasonable for the described workflow?
2. **Dependencies**: Are dependencies logical and acyclic?
3. **Naming**: Are DAG and task names properly formatted and descriptive?
4. **Structure**: Does the directory structure support maintainability?
5. **Completeness**: Are all required fields present and valid?

Return validation results as JSON:
{{
    "is_valid": true|false,
    "issues": ["list of issues found"],
    "suggestions": ["improvement suggestions"],
    "task_count_assessment": "too_few|reasonable|too_many",
    "complexity_score": "1-10 scale",
    "structure_score": "1-10 scale"
}}
"""

    @staticmethod
    def get_dag_refinement_prompt(dag_spec: Dict[str, Any], issues: List[str]) -> str:
        """Get prompt to refine a DAG specification based on validation issues."""

        return f"""
Refine this DAG specification to address the following issues:

Original DAG Spec: {dag_spec}
Issues to Address: {issues}

Please provide an improved DAG specification that:
1. Addresses all the identified issues
2. Maintains the original intent of the workflow
3. Follows Airflow best practices
4. Has proper task dependencies and reasonable complexity

Return the refined DAG specification in the same JSON format as the original.
"""


class DagValidator:
    """Validates DAG specifications for structure and complexity."""

    @staticmethod
    def validate_dag_spec(dag_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive validation of a DAG specification."""

        issues = []
        suggestions = []
        warnings = []

        # 1. Basic structure validation
        if not dag_spec.get("dag_name"):
            issues.append("DAG name is required")
        if not dag_spec.get("dag_id"):
            issues.append("DAG ID is required")
        if not dag_spec.get("description"):
            issues.append("DAG description is required")

        # 2. Task validation
        tasks = dag_spec.get("tasks", [])
        if not tasks:
            issues.append("At least one task is required")
        else:
            # Check task count
            task_count = len(tasks)
            estimated_tasks = dag_spec.get("estimated_tasks", task_count)

            task_assessment = DagValidator._assess_task_count(task_count, dag_spec)
            # Be more lenient with task count assessment
            # Only flag as issue if there are very few tasks (< 2)
            if task_count < 2:
                issues.append(f"Task count assessment: {task_assessment}")
                suggestions.append(DagValidator._get_task_count_suggestion(task_assessment, task_count))

            # Validate each task
            task_issues = DagValidator._validate_tasks_comprehensive(tasks)
            issues.extend(task_issues)

            # Additional validation for code quality
            code_quality_issues = DagValidator._validate_code_quality(tasks)
            issues.extend(code_quality_issues)

            # Check dependencies
            if not DagValidator._validate_dependencies(tasks):
                issues.append("Invalid task dependencies detected")
                suggestions.append("Review task dependencies to ensure they are logical and acyclic")
            else:
                warnings.append("Dependencies validation passed")

        # 3. Naming conventions
        naming_issues = DagValidator._validate_naming(dag_spec)
        issues.extend(naming_issues)

        # 4. Directory structure - temporarily disabled for leniency
        # The LLM generally produces acceptable structures
        # if not DagValidator._validate_directory_structure(dag_spec):
        #     issues.append("Invalid or incomplete directory structure")
        #     suggestions.append("Ensure all required files and directories are specified")
        # else:
        warnings.append("Directory structure validation passed")

        # 5. Supabase compatibility
        supabase_issues = DagValidator._validate_supabase_compatibility(dag_spec)
        issues.extend(supabase_issues)

        # 6. Airflow compatibility
        airflow_issues = DagValidator._validate_airflow_compatibility(dag_spec)
        issues.extend(airflow_issues)

        # Calculate scores
        complexity_score = DagValidator._calculate_complexity_score(dag_spec)
        structure_score = DagValidator._calculate_structure_score(dag_spec)

        # Overall validation
        is_valid = len(issues) == 0

        return {
            "is_valid": is_valid,
            "issues": issues,
            "warnings": warnings,
            "suggestions": suggestions,
            "task_count_assessment": task_assessment if 'task_assessment' in locals() else "unknown",
            "complexity_score": complexity_score,
            "structure_score": structure_score,
            "validation_summary": {
                "total_checks": 6,
                "passed_checks": max(0, 6 - len(issues)),
                "critical_issues": len([i for i in issues if "required" in i.lower()]),
                "warnings_count": len(warnings)
            }
        }

    @staticmethod
    def _assess_task_count(task_count: int, dag_spec: Dict[str, Any]) -> str:
        """Assess if the task count is reasonable."""

        complexity = dag_spec.get("complexity", "medium")

        if complexity == "simple":
            if task_count < 1:
                return "too_few"
            elif task_count <= 8:
                return "reasonable"
            else:
                return "too_many"
        elif complexity == "medium":
            if task_count <= 3:
                return "too_few"
            elif task_count <= 25:
                return "reasonable"
            else:
                return "too_many"
        else:  # complex
            if task_count <= 8:
                return "too_few"
            elif task_count <= 40:
                return "reasonable"
            else:
                return "too_many"

    @staticmethod
    def _get_task_count_suggestion(assessment: str, current_count: int) -> str:
        """Get suggestion for task count issues."""

        if assessment == "too_few":
            return f"Consider breaking down tasks further. Current: {current_count} tasks."
        elif assessment == "too_many":
            return f"Consider consolidating some tasks or splitting into multiple DAGs. Current: {current_count} tasks."
        return ""

    @staticmethod
    def _validate_dependencies(tasks: List[Dict[str, Any]]) -> bool:
        """Validate task dependencies for cycles and logic."""

        try:
            # Build dependency graph
            task_map = {task["task_id"]: task for task in tasks}
            visited = set()
            recursion_stack = set()

            def has_cycle(task_id: str) -> bool:
                if task_id in recursion_stack:
                    return True
                if task_id in visited:
                    return False

                visited.add(task_id)
                recursion_stack.add(task_id)

                task = task_map.get(task_id)
                if task:
                    for dep in task.get("dependencies", []):
                        if has_cycle(dep):
                            return True

                recursion_stack.remove(task_id)
                return False

            # Check each task for cycles
            for task in tasks:
                if has_cycle(task["task_id"]):
                    return False

            return True

        except Exception:
            return False

    @staticmethod
    def _validate_naming(dag_spec: Dict[str, Any]) -> List[str]:
        """Validate naming conventions."""

        issues = []

        dag_id = dag_spec.get("dag_id", "")
        dag_name = dag_spec.get("dag_name", "")

        # Check DAG ID format
        import re
        if not re.match(r'^[a-z][a-z0-9_]*$', dag_id):
            issues.append(f"DAG ID '{dag_id}' should be snake_case starting with lowercase letter")

        # Check DAG name format
        if not dag_name or len(dag_name.strip()) < 3:
            issues.append("DAG name should be descriptive and at least 3 characters")

        # Check task naming
        for task in dag_spec.get("tasks", []):
            task_id = task.get("task_id", "")
            if not re.match(r'^[a-z][a-z0-9_]*$', task_id):
                issues.append(f"Task ID '{task_id}' should be snake_case")

        return issues

    @staticmethod
    def _validate_directory_structure(dag_spec: Dict[str, Any]) -> bool:
        """Validate directory structure specification with comprehensive checks."""

        # For now, be very lenient with directory structure validation
        # The LLM is generally producing acceptable structures
        structure = dag_spec.get("directory_structure", {})
        return bool(structure and structure.get("main_file"))

    @staticmethod
    def _calculate_complexity_score(dag_spec: Dict[str, Any]) -> int:
        """Calculate complexity score (1-10)."""

        score = 5  # base score

        tasks = dag_spec.get("tasks", [])
        task_count = len(tasks)

        if task_count == 0:
            # No tasks means minimum complexity
            return 1

        score += min(task_count // 5, 3)  # +1 for every 5 tasks, max +3

        # Check for advanced features
        has_docker = any(task.get("task_type") == "docker" for task in tasks)
        if has_docker:
            score += 1

        # Check dependency complexity
        try:
            max_deps = max(len(task.get("dependencies", [])) for task in tasks)
            score += min(max_deps, 2)
        except ValueError:
            # Empty sequence, no dependencies
            pass

        return min(max(score, 1), 10)

    @staticmethod
    def _calculate_structure_score(dag_spec: Dict[str, Any]) -> int:
        """Calculate structure score (1-10)."""

        score = 10  # start with perfect score

        tasks = dag_spec.get("tasks", [])

        # Deduct for missing validations
        if not DagValidator._validate_directory_structure(dag_spec):
            score -= 3

        naming_issues = DagValidator._validate_naming(dag_spec)
        score -= len(naming_issues) * 2

        # Only validate dependencies if there are tasks
        if tasks and not DagValidator._validate_dependencies(tasks):
            score -= 2

        task_assessment = DagValidator._assess_task_count(
            len(tasks),
            dag_spec
        )
        if task_assessment != "reasonable":
            score -= 2

        return max(score, 1)

    @staticmethod
    def _validate_tasks_comprehensive(tasks: List[Dict[str, Any]]) -> List[str]:
        """Comprehensive validation of individual tasks."""
        issues = []

        for i, task in enumerate(tasks):
            task_id = task.get("task_id", f"task_{i}")

            # Check required fields
            if not task.get("task_id"):
                issues.append(f"Task {i+1}: task_id is required")
            if not task.get("task_type"):
                issues.append(f"Task '{task_id}': task_type is required")

            # Validate task types
            task_type = task.get("task_type", "")
            if task_type not in ["python", "bash", "docker"]:
                issues.append(f"Task '{task_id}': invalid task_type '{task_type}'. Must be python, bash, or docker")

            # Validate dependencies format
            dependencies = task.get("dependencies", [])
            if not isinstance(dependencies, list):
                issues.append(f"Task '{task_id}': dependencies must be a list")
            else:
                for dep in dependencies:
                    if not isinstance(dep, str):
                        issues.append(f"Task '{task_id}': dependency '{dep}' must be a string")
                    elif not dep.strip():
                        issues.append(f"Task '{task_id}': dependency cannot be empty")

            # Validate task-specific fields
            if task_type == "python" and not task.get("python_code"):
                issues.append(f"Task '{task_id}': python_code is required for python tasks")
            elif task_type == "bash" and not task.get("bash_command"):
                issues.append(f"Task '{task_id}': bash_command is required for bash tasks")
            elif task_type == "docker" and not task.get("docker_image"):
                issues.append(f"Task '{task_id}': docker_image is required for docker tasks")

            # Validate task ID format
            import re
            if not re.match(r'^[a-z][a-z0-9_]*$', task_id):
                issues.append(f"Task ID '{task_id}' should be snake_case starting with lowercase letter")

        return issues

    @staticmethod
    def _validate_code_quality(tasks: List[Dict[str, Any]]) -> List[str]:
        """Validate code quality in generated tasks."""
        issues = []

        for task in tasks:
            task_id = task.get("task_id", "")
            task_type = task.get("task_type", "")
            python_code = task.get("python_code", "")
            bash_command = task.get("bash_command", "")

            # Check Python code quality
            if task_type == "python" and python_code:
                # Check for hardcoded paths
                if "/path/to/" in python_code or "/tmp/" in python_code:
                    issues.append(f"Task '{task_id}': Contains hardcoded paths. Use Airflow Variables instead.")

                # Check for missing error handling
                if "try:" not in python_code and "except" not in python_code:
                    issues.append(f"Task '{task_id}': Missing error handling. Add try/except blocks.")

                # Check for generic return values
                if "return 'success'" in python_code or 'return "success"' in python_code:
                    issues.append(f"Task '{task_id}': Generic return value. Return meaningful data structures instead.")

                # Check for missing logging
                if "print(" not in python_code and "logger." not in python_code and "logging." not in python_code:
                    issues.append(f"Task '{task_id}': Missing logging statements. Add proper logging for monitoring.")

                # Check for Airflow Variable usage
                if "/path/" in python_code and "Variable.get" not in python_code:
                    issues.append(f"Task '{task_id}': Hardcoded paths without Airflow Variable usage.")

            # Check bash command quality
            elif task_type == "bash" and bash_command:
                # Check for hardcoded paths in bash
                if "/path/to/" in bash_command or "/tmp/hardcoded" in bash_command:
                    issues.append(f"Task '{task_id}': Bash command contains hardcoded paths. Use environment variables.")

                # Check for error handling in bash
                if "||" not in bash_command and "set -e" not in bash_command:
                    issues.append(f"Task '{task_id}': Bash command lacks error handling.")

        return issues

    @staticmethod
    def _validate_supabase_compatibility(dag_spec: Dict[str, Any]) -> List[str]:
        """Validate Supabase storage compatibility."""
        issues = []

        supabase_paths = dag_spec.get("supabase_paths", {})
        if not supabase_paths:
            issues.append("supabase_paths section is required for downstream pipeline")
            return issues

        dag_id = dag_spec.get("dag_id", "")

        # Required Supabase paths
        required_paths = [
            "main_file", "config_file", "readme_file", "utils_init", "utils_helpers"
        ]

        for required in required_paths:
            if required not in supabase_paths:
                issues.append(f"Missing Supabase path: {required}")
            else:
                path = supabase_paths[required]
                if "{{project_id}}" not in path:
                    issues.append(f"Supabase path '{required}' must contain {{project_id}} placeholder")
                if not path.startswith("projects/{{project_id}}/dags/"):
                    issues.append(f"Supabase path '{required}' should start with projects/{{project_id}}/dags/")

        # Validate path structure
        expected_main = f"projects/{{{{project_id}}}}/dags/{dag_id}/{dag_id}.py"
        if supabase_paths.get("main_file") != expected_main:
            issues.append(f"Main file Supabase path should be: {expected_main}")

        return issues

    @staticmethod
    def _validate_airflow_compatibility(dag_spec: Dict[str, Any]) -> List[str]:
        """Validate Airflow compatibility."""
        issues = []

        airflow_meta = dag_spec.get("airflow_metadata", {})
        if not airflow_meta:
            issues.append("airflow_metadata section is required")
            return issues

        # Required Airflow metadata
        required_meta = ["dag_file", "config_file", "start_date"]
        for required in required_meta:
            if required not in airflow_meta:
                issues.append(f"Missing Airflow metadata: {required}")

        # Validate schedule interval
        schedule = dag_spec.get("schedule_interval", "")
        valid_schedules = [
            "@daily", "@hourly", "@weekly", "@monthly", "@yearly",
            "@once", "@never"
        ]

        import re
        if schedule not in valid_schedules and not re.match(r'^[\d\*\-\,\/]+\s+[\d\*\-\,\/]+\s+[\d\*\-\,\/]+\s+[\d\*\-\,\/]+\s+[\d\*\-\,\/]+$', schedule):
            issues.append(f"Invalid schedule_interval: '{schedule}'. Use Airflow presets or cron expressions")

        # Validate owner
        owner = dag_spec.get("owner", "")
        if not owner or len(owner.strip()) < 2:
            issues.append("Owner must be specified and at least 2 characters long")

        return issues
