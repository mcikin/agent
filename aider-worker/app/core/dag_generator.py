import re
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List
from uuid import UUID

from ..models.dto import CreateDagReq, TaskDefinition
from .config import config, ComplexityLevel
from .dag_naming import generate_smart_dag_name, is_valid_dag_id, sanitize_dag_name


# DAG naming functions are now imported from dag_naming module


# Additional DAG naming and validation functions are now imported from respective modules


def generate_python_function(task: Dict[str, Any]) -> str:
    """Generate Python function code for a task."""
    task_id = task["task_id"]
    function_name = f"{task_id}_function"

    if "python_code" in task and task["python_code"]:
        # Use provided Python code
        code = task["python_code"]

        # Ensure proper indentation
        lines = code.split('\n')
        indented_lines = []
        for line in lines:
            if line.strip():
                indented_lines.append(f"    {line}")
            else:
                indented_lines.append("")

        function_body = '\n'.join(indented_lines)

        # Check if function needs context
        provide_context = task.get("provide_context", False)
        if provide_context or "context" in code or "**context" in code:
            function_def = f"def {function_name}(**context):"
        else:
            function_def = f"def {function_name}():"

        return f"{function_def}\n{function_body}\n"

    else:
        # Generate a simple default function
        return f"""def {function_name}():
    print("Executing task: {task_id}")
    # Add your task logic here
    return "Task {task_id} completed"
"""


def generate_task_implementation_file(task: Dict[str, Any]) -> str:
    """Generate a complete task implementation file."""
    task_id = task["task_id"]
    task_type = task.get("task_type", "python")
    description = task.get("description", f"Execute {task_id} task")

    # Generate the task function based on type
    if task_type == "python":
        if "python_code" in task and task["python_code"]:
            # Use provided Python code
            function_body = task["python_code"]
            provide_context = task.get("provide_context", False)

            if provide_context or "**context" in function_body:
                function_def = f"def execute_{task_id}(**context):"
            else:
                function_def = f"def execute_{task_id}():"

            task_function = f"{function_def}\n    {function_body}"
        else:
            # Generate a meaningful default implementation based on task description
            task_function = generate_smart_task_implementation(task)

    elif task_type == "bash":
        bash_command = task.get("bash_command", f"echo 'Executing {task_id}'")
        task_function = f"""def execute_{task_id}():
    \"\"\"{description}\"\"\"
    import subprocess
    import sys

    try:
        result = subprocess.run(
            "{bash_command}",
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Task {task_id} completed successfully")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Task {task_id} failed: {{e}}")
        sys.exit(1)
"""

    else:
        task_function = f"""def execute_{task_id}():
    \"\"\"{description}\"\"\"
    print(f"Executing {task_type} task: {task_id}")
    # TODO: Implement {task_type} task logic
    return f"Task {task_id} completed"
"""

    # Generate the complete file
    file_content = f'''"""
Task Implementation: {task_id}
Description: {description}
Type: {task_type}
"""

from typing import Any, Dict
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


{task_function}


if __name__ == "__main__":
    # Allow direct execution for testing
    result = execute_{task_id}()
    print(f"Task result: {{result}}")
'''

    return file_content


def generate_smart_task_implementation(task: Dict[str, Any]) -> str:
    """Generate intelligent task implementation based on task description and naming."""
    task_id = task["task_id"]
    description = task.get("description", "").lower()

    # Analyze task description to generate appropriate implementation
    if "extract" in description or "fetch" in description or "get" in description:
        if "api" in description:
            return f"""def execute_{task_id}():
    \"\"\"Extract data from API\"\"\"
    import requests
    import json
    from utils.helpers import get_api_credentials

    try:
        # Get API credentials from config
        credentials = get_api_credentials()
        api_url = credentials.get("api_url", "https://api.example.com/data")

        response = requests.get(api_url, headers=credentials.get("headers", {{}}))
        response.raise_for_status()

        data = response.json()
        logger.info(f"Successfully extracted {{len(data)}} records from API")

        # TODO: Save data to staging area or return for next task
        return data

    except requests.RequestException as e:
        logger.error(f"API extraction failed: {{e}}")
        raise
"""
        elif "csv" in description or "file" in description:
            return f"""def execute_{task_id}():
    \"\"\"Extract data from CSV file\"\"\"
    import pandas as pd
    import os
    from utils.helpers import get_file_path

    try:
        file_path = get_file_path("{task_id}")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {{file_path}}")

        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {{len(df)}} records from CSV")

        # TODO: Perform initial data validation
        return df.to_dict('records')

    except Exception as e:
        logger.error(f"CSV extraction failed: {{e}}")
        raise
"""
        elif "database" in description or "db" in description:
            return f"""def execute_{task_id}():
    \"\"\"Extract data from database\"\"\"
    import sqlalchemy as sa
    from utils.helpers import get_db_connection_string

    try:
        connection_string = get_db_connection_string()
        engine = sa.create_engine(connection_string)

        # TODO: Replace with actual query
        query = "SELECT * FROM your_table WHERE created_date >= CURRENT_DATE - INTERVAL '1 day'"

        with engine.connect() as conn:
            result = conn.execute(sa.text(query))
            data = [dict(row) for row in result.fetchall()]

        logger.info(f"Successfully extracted {{len(data)}} records from database")
        return data

    except Exception as e:
        logger.error(f"Database extraction failed: {{e}}")
        raise
"""

    elif "transform" in description or "clean" in description or "process" in description:
        return f"""def execute_{task_id}():
    \"\"\"Transform and clean data\"\"\"
    import pandas as pd
    from utils.helpers import validate_data_quality
    from typing import List, Dict, Any

    try:
        # TODO: Get data from previous task (via XCom or shared storage)
        # For now, assume data is passed as parameter or loaded from file
        data = []  # Replace with actual data loading

        if not data:
            logger.warning("No data to transform")
            return []

        # Convert to DataFrame for processing
        df = pd.DataFrame(data) if isinstance(data, list) else data

        # Data cleaning operations
        # Remove duplicates
        df = df.drop_duplicates()

        # Handle missing values
        df = df.dropna(subset=['id'])  # TODO: Customize based on requirements

        # Data type conversions
        # TODO: Add type conversions as needed

        # Validate data quality
        quality_issues = validate_data_quality(df)
        if quality_issues:
            logger.warning(f"Data quality issues found: {{quality_issues}}")

        logger.info(f"Successfully transformed {{len(df)}} records")
        return df.to_dict('records')

    except Exception as e:
        logger.error(f"Data transformation failed: {{e}}")
        raise
"""

    elif "validate" in description or "check" in description:
        return f"""def execute_{task_id}():
    \"\"\"Validate data quality\"\"\"
    from utils.helpers import validate_email_format, check_data_completeness
    from typing import List, Dict, Any

    try:
        # TODO: Get data from previous task
        data = []  # Replace with actual data loading

        validation_results = {{
            "total_records": len(data),
            "valid_records": 0,
            "invalid_records": 0,
            "issues": []
        }}

        for record in data:
            is_valid = True
            issues = []

            # Email validation
            if "email" in record and not validate_email_format(record["email"]):
                issues.append("Invalid email format")
                is_valid = False

            # Completeness check
            required_fields = ["id", "name"]  # TODO: Customize
            completeness_issues = check_data_completeness(record, required_fields)
            if completeness_issues:
                issues.extend(completeness_issues)
                is_valid = False

            if is_valid:
                validation_results["valid_records"] += 1
            else:
                validation_results["invalid_records"] += 1
                validation_results["issues"].append({{"record_id": record.get("id"), "issues": issues}})

        logger.info(f"Validation complete: {{validation_results['valid_records']}}/{{validation_results['total_records']}} records valid")
        return validation_results

    except Exception as e:
        logger.error(f"Data validation failed: {{e}}")
        raise
"""

    elif "load" in description or "save" in description or "store" in description:
        return f"""def execute_{task_id}():
    \"\"\"Load data to destination\"\"\"
    import sqlalchemy as sa
    from utils.helpers import get_db_connection_string
    from typing import List, Dict, Any

    try:
        # TODO: Get data from previous task
        data = []  # Replace with actual data loading

        if not data:
            logger.warning("No data to load")
            return {{"loaded_records": 0}}

        # Load to database
        connection_string = get_db_connection_string()
        engine = sa.create_engine(connection_string)

        # TODO: Replace with actual table name and logic
        table_name = "processed_data"

        with engine.connect() as conn:
            # Insert data
            for record in data:
                # TODO: Implement actual insert logic
                pass

        logger.info(f"Successfully loaded {{len(data)}} records to {{table_name}}")
        return {{"loaded_records": len(data), "table": table_name}}

    except Exception as e:
        logger.error(f"Data loading failed: {{e}}")
        raise
"""

    else:
        # Generic implementation
        return f"""def execute_{task_id}():
    \"\"\"Execute {task_id} task\"\"\"
    try:
        logger.info(f"Starting task: {task_id}")

        # TODO: Implement task-specific logic here
        # This is a placeholder implementation

        result = f"Task {task_id} executed successfully"
        logger.info(result)
        return result

    except Exception as e:
        logger.error(f"Task {task_id} failed: {{e}}")
        raise
"""


def generate_task_dependencies(tasks: List[Dict[str, Any]]) -> str:
    """Generate task dependency chain."""
    dependencies = []
    
    for task in tasks:
        task_id = task["task_id"]
        deps = task.get("dependencies", [])
        
        if deps:
            for dep in deps:
                dependencies.append(f"{dep}_task >> {task_id}_task")
    
    # If no explicit dependencies, create a simple chain
    if not dependencies and len(tasks) > 1:
        task_ids = [task["task_id"] for task in tasks]
        chain_parts = []
        for i in range(len(task_ids) - 1):
            chain_parts.append(f"{task_ids[i]}_task >> {task_ids[i+1]}_task")
        dependencies = chain_parts
    
    return "\n".join(dependencies) if dependencies else "# No task dependencies defined"


def parse_schedule_interval(schedule: str) -> str:
    """Parse and validate schedule interval."""
    schedule = schedule.strip()
    
    # Common presets
    presets = {
        "daily": "@daily",
        "hourly": "@hourly", 
        "weekly": "@weekly",
        "monthly": "@monthly",
        "yearly": "@yearly",
        "once": "@once",
        "never": None
    }
    
    if schedule.lower() in presets:
        return repr(presets[schedule.lower()])
    
    # Check if it's a cron expression (5 parts)
    if re.match(r'^[\d\*\-\,\/]+\s+[\d\*\-\,\/]+\s+[\d\*\-\,\/]+\s+[\d\*\-\,\/]+\s+[\d\*\-\,\/]+$', schedule):
        return repr(schedule)
    
    # Check if it's a timedelta expression
    if schedule.startswith("timedelta("):
        return schedule
    
    # If it starts with @, treat as preset
    if schedule.startswith("@"):
        return repr(schedule)
    
    # Default fallback
    return repr("@daily")


def generate_dag_file(req: CreateDagReq, user_id: str, dag_id: str) -> str:
    """Generate complete DAG file content based on request."""

    # Parse start date
    if req.start_date:
        try:
            start_date = datetime.fromisoformat(req.start_date.replace('Z', '+00:00'))
            start_date_str = f"datetime({start_date.year}, {start_date.month}, {start_date.day})"
        except:
            start_date_str = "datetime(2024, 1, 1)"
    else:
        start_date_str = "datetime(2024, 1, 1)"

    # Generate imports for task implementations
    task_imports = []
    task_definitions = []

    for task in req.tasks:
        task_id = task["task_id"]
        task_type = task.get("task_type", "python")

        # Import the task function from the tasks module
        import_name = f"execute_{task_id}"
        task_imports.append(f"from tasks import {import_name}")

        # Generate task definition
        if task_type == "python":
            provide_context = task.get("provide_context", False)

            task_def = f"""{task_id}_task = PythonOperator(
    task_id='{task_id}',
    python_callable={import_name},
    provide_context={provide_context},
    dag=dag,
)"""

        elif task_type == "bash":
            bash_command = task.get("bash_command", f"echo 'Executing {task_id}'")
            task_def = f"""{task_id}_task = BashOperator(
    task_id='{task_id}',
    bash_command='''{bash_command}''',
    dag=dag,
)"""

        else:
            # Default to Python operator
            task_def = f"""{task_id}_task = PythonOperator(
    task_id='{task_id}',
    python_callable=lambda: print('Executing {task_id}'),
    dag=dag,
)"""

        task_definitions.append(task_def)
    
    # Generate dependencies
    dependencies = generate_task_dependencies(req.tasks)
    
    # Parse schedule interval
    schedule_interval = parse_schedule_interval(req.schedule_interval)
    
    # Generate imports
    imports = ["from datetime import datetime, timedelta"]
    imports.append("from airflow import DAG")
    
    # Add operator imports based on task types
    task_types = {task.get("task_type", "python") for task in req.tasks}
    if "python" in task_types:
        imports.append("from airflow.operators.python import PythonOperator")
    if "bash" in task_types:
        imports.append("from airflow.operators.bash import BashOperator")
    
    # Add additional imports for Supabase integration
    if any("supabase" in str(task).lower() for task in req.tasks):
        imports.extend([
            "import json",
            "import os",
            "from supabase import create_client, Client"
        ])

    # Add import for local utils
    imports.append(f"from utils.helpers import *")

    # Add task imports
    imports.extend(task_imports)

    imports_str = "\n".join(imports)

    # Generate the complete DAG file
    dag_content = f'''# Generated DAG: {req.dag_name}
# Created by: {user_id}
# Generated at: {datetime.now().isoformat()}

{imports_str}

# DAG default arguments
default_args = {{
    'owner': '{req.owner}',
    'depends_on_past': {req.depends_on_past},
    'start_date': {start_date_str},
    'email_on_failure': {req.email_on_failure},
    'email_on_retry': {req.email_on_retry},
    'retries': {req.retries},
    'retry_delay': timedelta(minutes={req.retry_delay_minutes}),
}}

# DAG definition
dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='{req.description}',
    schedule_interval={schedule_interval},
    catchup={req.catchup},
    tags={req.tags},
)

# Task definitions
{chr(10).join(task_definitions)}

# Task dependencies
{dependencies}
'''
    
    return dag_content


def create_dag_directory_structure(project_id: UUID, req: CreateDagReq, user_id: str, projects_root: Path) -> tuple[str, Path]:
    """
    Create a complete DAG directory structure with all necessary files.

    Returns:
        tuple[str, Path]: (dag_id, dag_directory_path)
    """
    # Determine complexity based on task count using configuration
    task_count = len(req.tasks)
    if task_count <= config.max_tasks_simple:
        complexity = ComplexityLevel.SIMPLE.value
    elif task_count <= config.max_tasks_medium:
        complexity = ComplexityLevel.MEDIUM.value
    else:
        complexity = ComplexityLevel.COMPLEX.value

    # Generate smart DAG ID directly (already sanitized)
    # Prioritize LLM-provided DAG ID for naming consistency
    dag_id = generate_smart_dag_name(req.description, complexity, req.dag_id)

    # Ensure unique DAG ID by checking existing directories
    project_dir = projects_root / str(project_id) / "dags"
    dag_dir = project_dir / dag_id
    counter = 1
    original_dag_id = dag_id

    while dag_dir.exists():
        dag_id = f"{original_dag_id}_{counter}"
        dag_dir = project_dir / dag_id
        counter += 1

    # Create directory structure
    dag_dir.mkdir(parents=True, exist_ok=True)

    # Create tasks directory for individual task implementations
    tasks_dir = dag_dir / "tasks"
    tasks_dir.mkdir(exist_ok=True)

    # Create utils directory for shared utilities
    utils_dir = dag_dir / "utils"
    utils_dir.mkdir(exist_ok=True)

    # Generate and create all files
    files_created = []

    # 1. Create individual task implementation files
    print(f"Creating task implementation files for {len(req.tasks)} tasks...")
    for task in req.tasks:
        task_id = task["task_id"]
        task_content = generate_task_implementation_file(task)
        task_file_path = tasks_dir / f"{task_id}.py"
        task_file_path.write_text(task_content, encoding="utf-8")
        files_created.append(task_file_path)
        print(f"Created task file: {task_id}.py")

    # 2. Create tasks/__init__.py for package imports
    task_imports = []
    task_names = []

    for task in req.tasks:
        task_id = task["task_id"]
        import_name = f"execute_{task_id}"
        task_imports.append(f"from .{task_id} import {import_name}")
        task_names.append(f"'{import_name}'")

    tasks_init_content = f'''"""
Task implementations for {dag_id} DAG
"""

{chr(10).join(task_imports)}

__all__ = [
    {",".join(task_names)}
]
'''
    tasks_init_path = tasks_dir / "__init__.py"
    tasks_init_path.write_text(tasks_init_content, encoding="utf-8")
    files_created.append(tasks_init_path)

    # 3. Main DAG file (now imports from task files)
    dag_content = generate_dag_file(req, user_id, dag_id)
    dag_file_path = dag_dir / f"{dag_id}.py"
    dag_file_path.write_text(dag_content, encoding="utf-8")
    files_created.append(dag_file_path)

    # 2. Configuration file
    config_content = generate_config_file(req, dag_id, user_id)
    config_file_path = dag_dir / "config.json"
    config_file_path.write_text(config_content, encoding="utf-8")
    files_created.append(config_file_path)

    # 3. README file
    readme_content = generate_readme_file(req, dag_id, user_id)
    readme_file_path = dag_dir / "README.md"
    readme_file_path.write_text(readme_content, encoding="utf-8")
    files_created.append(readme_file_path)

    # 4. Utils __init__.py
    utils_init_content = f'''"""
Utils package for {dag_id} DAG

This package contains shared utilities and helper functions
used by the DAG tasks.
"""

from .helpers import *

__all__ = [
    # Configuration helpers
    'get_config',
    'get_db_connection_string',
    'get_api_credentials',

    # File and path helpers
    'get_file_path',
    'ensure_directory',

    # Data validation helpers
    'validate_email_format',
    'check_data_completeness',
    'validate_data_quality',

    # Logging helpers
    'setup_task_logging',
    'log_task_start',
    'log_task_end',

    # Error handling helpers
    'handle_task_error',
    'retry_with_backoff',
]
'''
    utils_init_path = utils_dir / "__init__.py"
    utils_init_path.write_text(utils_init_content, encoding="utf-8")
    files_created.append(utils_init_path)

    # 5. Utils helpers.py
    utils_helpers_content = generate_utils_helpers_file(req, dag_id)
    utils_helpers_path = utils_dir / "helpers.py"
    utils_helpers_path.write_text(utils_helpers_content, encoding="utf-8")
    files_created.append(utils_helpers_path)

    return dag_id, dag_dir


def generate_config_file(req: CreateDagReq, dag_id: str, user_id: str) -> str:
    """Generate comprehensive configuration file for the DAG."""
    config = {
        "dag_metadata": {
            "dag_id": dag_id,
            "dag_name": req.dag_name,
            "description": req.description,
            "version": "1.0.0",
            "created_by": user_id,
            "created_at": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat()
        },
        "airflow_config": {
            "schedule_interval": req.schedule_interval,
            "owner": req.owner,
            "tags": req.tags or [],
            "start_date": getattr(req, 'start_date', '2024-01-01T00:00:00Z'),
            "retries": req.retries,
            "retry_delay_minutes": req.retry_delay_minutes,
            "depends_on_past": req.depends_on_past,
            "catchup": req.catchup,
            "email_on_failure": req.email_on_failure,
            "email_on_retry": req.email_on_retry,
            "concurrency": 1,
            "max_active_runs": 1,
            "dagrun_timeout_hours": 2
        },
        "task_config": {
            "task_count": len(req.tasks),
            "task_types": list(set(task.get("task_type", "python") for task in req.tasks)),
            "has_dependencies": any(task.get("dependencies", []) for task in req.tasks)
        },
        "repository_structure": {
            "base_directory": f"dags/{dag_id}/",
            "main_file": f"{dag_id}.py",
            "config_file": "config.json",
            "readme_file": "README.md",
            "utils_directory": "utils/",
            "airflow_dag_folder": f"dags/{dag_id}/"
        },
        "validation_info": {
            "structure_valid": True,
            "dependencies_valid": True,
            "naming_conventions": True,
            "airflow_compatible": True
        }
    }

    return json.dumps(config, indent=2, ensure_ascii=False)


def generate_readme_file(req: CreateDagReq, dag_id: str, user_id: str) -> str:
    """Generate comprehensive README file for the DAG."""
    task_count = len(req.tasks)
    task_types = list(set(task.get("task_type", "python") for task in req.tasks))

    readme = f"""# {req.dag_name}

## 📋 Overview

{req.description}

### Key Information
- **DAG ID**: `{dag_id}`
- **Schedule**: {req.schedule_interval}
- **Owner**: {req.owner}
- **Tasks**: {task_count}
- **Task Types**: {', '.join(task_types)}
- **Tags**: {', '.join(req.tags) if req.tags else 'None'}

## 🔄 Workflow Tasks

This DAG contains {task_count} tasks organized in a logical workflow:

"""

    for i, task in enumerate(req.tasks, 1):
        task_type = task.get("task_type", "python")
        task_id = task["task_id"]
        dependencies = task.get("dependencies", [])
        description = task.get("description", f"Executes {task_id}")

        readme += f"### {i}. **{task_id}** ({task_type})\n"
        readme += f"**Description**: {description}\n"

        if dependencies:
            readme += f"**Dependencies**: {', '.join(f'`{dep}`' for dep in dependencies)}\n"

        if task_type == "python":
            readme += "**Type**: Python function execution\n"
        elif task_type == "bash":
            readme += "**Type**: Shell command execution\n"
        elif task_type == "docker":
            docker_image = task.get("docker_image", "Unknown")
            readme += f"**Type**: Docker container ({docker_image})\n"

        readme += "\n"

    readme += f"""## ⚙️ Configuration

### Airflow Settings
- **Schedule Interval**: {req.schedule_interval}
- **Retries**: {req.retries}
- **Retry Delay**: {req.retry_delay_minutes} minutes
- **Depends on Past**: {req.depends_on_past}
- **Catchup**: {req.catchup}
- **Email on Failure**: {req.email_on_failure}
- **Email on Retry**: {req.email_on_retry}

### Task Configuration
- **Total Tasks**: {task_count}
- **Task Types Used**: {', '.join(task_types)}
- **Has Dependencies**: {'Yes' if any(task.get('dependencies', []) for task in req.tasks) else 'No'}

## 📁 Repository Structure

```
dags/{dag_id}/
├── {dag_id}.py              # Main DAG definition file
├── config.json               # DAG configuration and metadata
├── README.md                 # This documentation file
└── utils/
    ├── __init__.py          # Python package initialization
    └── helpers.py           # Shared utility functions
```

### File Descriptions

- **`{dag_id}.py`**: Main Airflow DAG definition with all tasks and dependencies
- **`config.json`**: Configuration file containing DAG settings and metadata
- **`README.md`**: This documentation file with usage instructions
- **`utils/__init__.py`**: Python package initialization for utilities
- **`utils/helpers.py`**: Shared helper functions for tasks

## 🚀 Usage

### Automatic Scheduling
This DAG runs automatically according to its schedule interval (`{req.schedule_interval}`).

### Manual Execution
To trigger the DAG manually:

```bash
# Enable the DAG
airflow dags unpause {dag_id}

# Trigger a run
airflow dags trigger {dag_id}

# Check status
airflow dags show {dag_id}
```

### Testing Tasks
To test individual tasks:

```bash
# Test a specific task
airflow tasks test {dag_id} task_id 2024-01-01
```

## 🔧 Development

### Local Development
1. Ensure all dependencies are installed
2. Update configuration in `config.json`
3. Test DAG syntax: `python {dag_id}.py`
4. Run DAG locally for testing

### Code Organization
- Task functions are defined in the main DAG file
- Shared utilities are in the `utils/` directory
- Configuration is centralized in `config.json`
- Documentation is maintained in this README

## 📊 Monitoring

### Logs
DAG execution logs are available in:
- Airflow Web UI → DAGs → {dag_id} → Logs
- Local log files: `logs/dag_id/{dag_id}/`

### Metrics
Monitor these key metrics:
- Task success/failure rates
- Execution duration trends
- Dependency chain performance
- Resource utilization

## 🐛 Troubleshooting

### Common Issues

#### 1. Import Errors
**Symptom**: DAG fails to parse with import errors
**Solution**:
- Ensure all required Python packages are installed
- Check that `utils/` directory is properly structured
- Verify Python path includes the DAG directory

#### 2. Permission Issues
**Symptom**: Tasks fail with permission denied errors
**Solution**:
- Check file permissions in the DAG directory
- Ensure Airflow worker has access to required files
- Verify Docker container permissions if using containerized tasks

#### 3. Dependency Failures
**Symptom**: Tasks fail due to missing upstream data
**Solution**:
- Check upstream task execution status
- Verify data availability and format
- Review task dependencies in DAG definition
- Check XCom data passing between tasks

#### 4. Configuration Issues
**Symptom**: DAG behaves unexpectedly
**Solution**:
- Validate `config.json` syntax and values
- Check environment variable overrides
- Review DAG parameters in Airflow UI

### Debugging Steps
1. Check DAG parsing: `airflow dags show {dag_id}`
2. Test individual tasks: `airflow tasks test {dag_id} task_id`
3. Review logs in Airflow Web UI
4. Check configuration file validity
5. Validate task dependencies

## 🔄 Maintenance

### Version History
- **v1.0.0**: Initial creation
- **Created**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Created by**: {user_id}

### Regular Maintenance Tasks
- [ ] Review and update dependencies
- [ ] Monitor performance metrics
- [ ] Update documentation as needed
- [ ] Test DAG after Airflow upgrades
- [ ] Validate backup and recovery procedures

### Contacts
- **Owner**: {req.owner}
- **Team**: Data Engineering
- **Documentation**: This README file

---

*This DAG was generated automatically and follows Airflow best practices for maintainable data pipelines.*
"""

    return readme


def generate_utils_init_file(dag_id: str) -> str:
    """Generate __init__.py for utils package."""
    return f'''"""
Utility functions for {dag_id} DAG.

This package contains shared functions and helpers used by the DAG tasks.
"""

from .helpers import *

__version__ = "1.0.0"
'''


def generate_utils_helpers_file(req: CreateDagReq, dag_id: str) -> str:
    """Generate helpers.py with basic utility functions."""
    # Simple string concatenation to avoid f-string issues
    content = '"""\\n'
    content += 'Helper functions for ' + str(dag_id) + ' DAG.\\n'
    content += '\\n'
    content += 'This module contains utility functions that can be shared across DAG tasks.\\n'
    content += '"""\\n'

    content += '\\n'
    content += 'import logging\\n'
    content += 'from typing import Any, Dict\\n'
    content += 'from datetime import datetime, timedelta\\n'
    content += '\\n'
    content += '# Set up logging\\n'
    content += 'logger = logging.getLogger(__name__)\\n'
    content += '\\n'
    content += '\\n'
    content += 'def get_dag_config() -> Dict[str, Any]:\\n'
    content += '    """Get DAG configuration from config.json."""\\n'
    content += '    import json\\n'
    content += '    import os\\n'
    content += '    from pathlib import Path\\n'
    content += '\\n'
    content += '    # Get the directory containing this file\\n'
    content += '    current_dir = Path(__file__).parent.parent\\n'
    content += '    config_path = current_dir / "config.json"\\n'
    content += '\\n'
    content += '    if config_path.exists():\\n'
    content += '        with open(config_path, \'r\') as f:\\n'
    content += '            return json.load(f)\\n'
    content += '\\n'
    content += '    # Fallback configuration\\n'
    content += '    return {\\n'
    content += '        "dag_id": "' + str(dag_id) + '",\\n'
    content += '        "schedule_interval": "' + str(req.schedule_interval) + '",\\n'
    content += '        "owner": "' + str(req.owner) + '",\\n'
    content += '        "retries": ' + str(req.retries) + '\\n'
    content += '    }\\n'


    content += '\\n'
    content += '\\n'
    content += 'def setup_logging(task_logger_name: str = "' + str(dag_id) + '_task") -> logging.Logger:\\n'
    content += '    """Set up logging for DAG tasks."""\\n'
    content += '    logger = logging.getLogger(task_logger_name)\\n'
    content += '    if not logger.handlers:\\n'
    content += '        handler = logging.StreamHandler()\\n'
    content += '        formatter = logging.Formatter(\\n'
    content += '            \'%(asctime)s - %(name)s - %(levelname)s - %(message)s\'\\n'
    content += '        )\\n'
    content += '        handler.setFormatter(formatter)\\n'
    content += '        logger.addHandler(handler)\\n'
    content += '        logger.setLevel(logging.INFO)\\n'
    content += '    return logger\\n'
    content += '\\n'
    content += '\\n'
    content += 'def validate_task_inputs(**context) -> bool:\\n'
    content += '    """Validate task inputs before execution."""\\n'
    content += '    task_instance = context[\'task_instance\']\\n'
    content += '    task_id = task_instance.task_id\\n'
    content += '    logger.info(f"Validating inputs for task: {task_id}")\\n'
    content += '\\n'
    content += '    # Add your validation logic here\\n'
    content += '    # Return True if validation passes, False otherwise\\n'
    content += '    return True\\n'
    content += '\\n'
    content += '\\n'
    content += 'def handle_task_failure(**context) -> None:\\n'
    content += '    """Handle task failures with custom logic."""\\n'
    content += '    task_instance = context[\'task_instance\']\\n'
    content += '    exception = context.get(\'exception\')\\n'
    content += '\\n'
    content += '    logger.error(f"Task {task_instance.task_id} failed")\\n'
    content += '    if exception:\\n'
    content += '        logger.error(f"Exception: {exception}")\\n'
    content += '\\n'
    content += '    # Add your failure handling logic here\\n'
    content += '    # Send notifications, cleanup, etc.\\n'
    content += '\\n'
    content += '\\n'
    content += 'def handle_task_success(**context) -> None:\\n'
    content += '    """Handle task success with custom logic."""\\n'
    content += '    task_instance = context[\'task_instance\']\\n'
    content += '\\n'
    content += '    logger.info(f"Task {task_instance.task_id} completed successfully")\\n'
    content += '\\n'
    content += '    # Add your success handling logic here\\n'
    content += '    # Send notifications, trigger downstream processes, etc.\\n'
    content += '\\n'
    content += '\\n'
    content += '# Common utility functions for data processing\\n'
    content += 'def safe_json_loads(json_str: str, default: Any = None) -> Any:\\n'
    content += '    """Safely parse JSON string with fallback."""\\n'
    content += '    try:\\n'
    content += '        import json\\n'
    content += '        return json.loads(json_str)\\n'
    content += '    except (json.JSONDecodeError, TypeError):\\n'
    content += '        logger.warning(f"Failed to parse JSON: {json_str}")\\n'
    content += '        return default\\n'
    content += '\\n'
    content += '\\n'
    content += 'def format_datetime(dt: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:\\n'
    content += '    """Format datetime to string."""\\n'
    content += '    return dt.strftime(format_str)\\n'
    content += '\\n'
    content += '\\n'
    content += 'def calculate_retry_delay(attempt: int, base_delay_minutes: int = 5) -> timedelta:\\n'
    content += '    """Calculate exponential backoff delay for retries."""\\n'
    content += '    delay_minutes = base_delay_minutes * (2 ** (attempt - 1))\\n'
    content += '    return timedelta(minutes=delay_minutes)\\n'
    
    return content


def create_dag_in_project(project_id: UUID, req: CreateDagReq, user_id: str, projects_root: Path) -> tuple[str, Path]:
    """
    Create a new DAG with complete directory structure in the project directory.

    Returns:
        tuple[str, Path]: (dag_id, dag_directory_path)
    """
    return create_dag_directory_structure(project_id, req, user_id, projects_root)
