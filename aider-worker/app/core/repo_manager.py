import os
from pathlib import Path
from typing import List
from uuid import UUID

import httpx
from git import Repo

from .supabase_io import list_latest_files


def _workdir() -> Path:
    base = os.getenv("WORKDIR", "/srv/aider")
    p = Path(base)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _project_dir(project_id: UUID) -> Path:
    projects_root = _workdir() / "projects"
    projects_root.mkdir(parents=True, exist_ok=True)
    return projects_root / str(project_id)


def _write_dag_readme(root: Path) -> None:
    """Create README for DAGs directory"""
    content = """# Sample DAGs for Flutter App Testing

This directory contains sample Airflow DAGs that you can use to test your Flutter app integration with the Aider worker service.

## Available DAGs

### 1. `hello_world_dag.py`
- **Purpose**: Simple test DAG with basic task flow
- **Schedule**: Daily
- **Tasks**: hello_world → process_data → send_notification
- **Use Case**: Basic functionality testing

### 2. `data_pipeline_dag.py` 
- **Purpose**: ETL pipeline simulation
- **Schedule**: Daily
- **Tasks**: extract_data → transform_data → load_data → quality_check
- **Use Case**: Data processing workflows

### 3. `flutter_api_dag.py`
- **Purpose**: Flutter app integration testing
- **Schedule**: Every 6 hours
- **Tasks**: call_flutter_api → process_flutter_data → send_results_to_supabase
- **Use Case**: Testing API communication between DAGs and Flutter/Supabase

## Testing with Flutter App

### 1. Open a Project
```bash
curl -X POST http://localhost:8080/open \\
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{"project_id": "123e4567-e89b-12d3-a456-426614174000"}'
```

### 2. Read DAG Files
```bash
# Read a specific DAG
curl -X GET "http://localhost:8080/file?project_id=123e4567-e89b-12d3-a456-426614174000&path=dags/hello_world_dag.py" \\
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

### 3. Edit DAGs with Aider
Use your Flutter app to send prompts like:
- "Add error handling to the hello_world_dag"
- "Modify the data_pipeline_dag to include data validation"
- "Update flutter_api_dag to call my actual API endpoint"

Happy testing! 🚀
"""
    (root / "dags" / "README.md").write_text(content.lstrip(), encoding="utf-8")


def _write_hello_world_dag(root: Path) -> None:
    """Create hello world DAG"""
    content = """from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world_task():
    print("Hello World from Aider!")
    return "Hello World from Aider!"

def process_data_task():
    print("Processing some data...")
    data = [1, 2, 3, 4, 5]
    result = sum(data)
    print(f"Processed data result: {result}")
    return result

def send_notification_task():
    print("Sending notification...")
    print("Task completed successfully!")
    return "Notification sent"

default_args = {
    'owner': 'flutter-app',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG for testing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example', 'hello-world'],
)

hello_task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world_task,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data_task,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification_task,
    dag=dag,
)

hello_task >> process_task >> notify_task
"""
    (root / "dags" / "hello_world_dag.py").write_text(content.lstrip(), encoding="utf-8")


def _write_data_pipeline_dag(root: Path) -> None:
    """Create data pipeline DAG"""
    content = """from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def extract_data():
    print("Extracting data from source...")
    data = {
        'users': ['alice', 'bob', 'charlie'],
        'transactions': [100, 250, 75],
        'timestamp': datetime.now().isoformat()
    }
    print(f"Extracted data: {data}")
    return data

def transform_data(**context):
    print("Transforming data...")
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    
    transformed = {
        'user_count': len(data['users']),
        'total_transactions': sum(data['transactions']),
        'avg_transaction': sum(data['transactions']) / len(data['transactions']),
        'processed_at': datetime.now().isoformat()
    }
    print(f"Transformed data: {transformed}")
    return transformed

def load_data(**context):
    print("Loading data to destination...")
    ti = context['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    
    print(f"Loading data: {data}")
    print("Data successfully loaded!")
    return "Load complete"

default_args = {
    'owner': 'flutter-app',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='A sample ETL data pipeline DAG',
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'etl', 'data-pipeline'],
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

quality_check = BashOperator(
    task_id='quality_check',
    bash_command='echo "Running data quality checks..." && echo "Quality check passed!"',
    dag=dag,
)

extract_task >> transform_task >> load_task >> quality_check
"""
    (root / "dags" / "data_pipeline_dag.py").write_text(content.lstrip(), encoding="utf-8")


def _write_flutter_api_dag(root: Path) -> None:
    """Create Flutter API DAG"""
    content = """from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json

def call_flutter_api():
    print("Calling Flutter app API...")
    
    api_data = {
        'status': 'success',
        'message': 'DAG executed from Aider',
        'timestamp': datetime.now().isoformat(),
        'project_info': {
            'id': 'test-project-123',
            'name': 'Flutter DAG Test',
            'version': '1.0.0'
        }
    }
    
    print(f"API Response: {json.dumps(api_data, indent=2)}")
    return api_data

def process_flutter_data(**context):
    print("Processing Flutter app data...")
    
    ti = context['ti']
    api_data = ti.xcom_pull(task_ids='call_flutter_api')
    
    processed_data = {
        'processed_at': datetime.now().isoformat(),
        'source': 'flutter_api',
        'project_id': api_data.get('project_info', {}).get('id'),
        'status': 'processed',
        'record_count': 1
    }
    
    print(f"Processed data: {json.dumps(processed_data, indent=2)}")
    return processed_data

def send_results_to_supabase(**context):
    print("Sending results to Supabase...")
    
    ti = context['ti']
    processed_data = ti.xcom_pull(task_ids='process_flutter_data')
    
    supabase_payload = {
        'table': 'dag_executions',
        'data': processed_data,
        'inserted_at': datetime.now().isoformat()
    }
    
    print(f"Supabase payload: {json.dumps(supabase_payload, indent=2)}")
    print("Successfully sent to Supabase!")
    return supabase_payload

default_args = {
    'owner': 'flutter-app',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flutter_api_dag',
    default_args=default_args,
    description='DAG that interacts with Flutter app and Supabase',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['flutter', 'api', 'supabase'],
)

api_task = PythonOperator(
    task_id='call_flutter_api',
    python_callable=call_flutter_api,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_flutter_data',
    python_callable=process_flutter_data,
    provide_context=True,
    dag=dag,
)

supabase_task = PythonOperator(
    task_id='send_results_to_supabase',
    python_callable=send_results_to_supabase,
    provide_context=True,
    dag=dag,
)

api_task >> process_task >> supabase_task
"""
    (root / "dags" / "flutter_api_dag.py").write_text(content.lstrip(), encoding="utf-8")


def _write_skeleton(root: Path) -> None:
    (root / "dags").mkdir(parents=True, exist_ok=True)
    (root / "dags" / "__init__.py").write_text("", encoding="utf-8")
    
    # Create all DAG files
    _write_dag_readme(root)
    _write_hello_world_dag(root)
    _write_data_pipeline_dag(root)
    _write_flutter_api_dag(root)

    (root / ".aider.conf").write_text("""
# Aider configuration file
# Add repo-specific aider settings here if needed.
""".lstrip(), encoding="utf-8")

    (root / ".aiderignore").write_text("""
# Ignore large or transient files from aider context
*.log
*.tmp
.venv/
venv/
node_modules/
dist/
build/
__pycache__/
""".lstrip(), encoding="utf-8")

    (root / ".gitignore").write_text("""
# Conservative gitignore
__pycache__/
*.py[cod]
*$py.class
.venv/
venv/
node_modules/
dist/
build/
*.log
*.tmp
""".lstrip(), encoding="utf-8")


def ensure_repo(project_id: UUID) -> Repo:
    proj_dir = _project_dir(project_id)
    proj_dir.mkdir(parents=True, exist_ok=True)

    git_dir = proj_dir / ".git"
    if not git_dir.exists():
        repo = Repo.init(proj_dir)
        _write_skeleton(proj_dir)
        repo.git.add(all=True)
        repo.index.commit("bootstrap empty repo")
    else:
        repo = Repo(proj_dir)

    # Skip hydration if repo already has recent commits (performance optimization)
    try:
        commits = list(repo.iter_commits(max_count=5))
        if len(commits) >= 2:  # Has commits beyond just the bootstrap
            print(f"📝 Repository already hydrated (found {len(commits)} commits), skipping download")
            return repo
    except Exception:
        pass  # Continue with hydration if git operations fail
    
    # Hydrate from Supabase with parallel downloads and progress tracking
    latest = list_latest_files(project_id)
    created_or_updated = False
    if latest:
        print(f"🔄 Hydrating {len(latest)} files from Supabase...")
        
        # Group files into batches for progress tracking
        batch_size = 10
        total_batches = (len(latest) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(latest))
            batch = latest[start_idx:end_idx]
            
            print(f"📦 Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} files)...")
            
            # Use shorter timeout for faster failure detection
            with httpx.Client(timeout=5.0) as client:
                for item in batch:
                    path = item.get("path")
                    url = item.get("signed_url")
                    if not path or not url:
                        continue
                    try:
                        r = client.get(url)
                        r.raise_for_status()
                        content = r.content
                        
                        abs_path = proj_dir / path
                        abs_path.parent.mkdir(parents=True, exist_ok=True)
                        before = abs_path.exists() and abs_path.read_bytes()
                        abs_path.write_bytes(content)
                        after = abs_path.read_bytes()
                        if before != after:
                            created_or_updated = True
                            
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code in [400, 404]:
                            # File doesn't exist - skip quickly without retry
                            print(f"⚠️ File {path} not accessible (HTTP {e.response.status_code}), skipping...")
                            continue
                        else:
                            print(f"❌ Error downloading {path}: HTTP {e.response.status_code}")
                            continue  # Skip on other HTTP errors too
                    except Exception as e:
                        print(f"❌ Error downloading {path}: {str(e)}")
                        continue  # Skip on any error to avoid blocking
        
        print(f"✅ Repository hydration complete")

        if created_or_updated:
            repo.git.add(A=True)
            # If nothing staged, skip commit
            if repo.is_dirty(index=True, working_tree=True, untracked_files=True):
                repo.index.commit("hydrate from Supabase snapshot")

    return repo


def changed_paths(repo: Repo) -> List[str]:
    # Names at HEAD (last commit) compared to parent
    try:
        output = repo.git.diff_tree("--no-commit-id", "--name-only", "-r", "HEAD")
    except Exception:
        return []
    if not output:
        return []
    return [line.strip() for line in output.splitlines() if line.strip()]


def head_commit(repo: Repo) -> str:
    try:
        return repo.head.commit.hexsha
    except Exception:
        return ""


def write_file(repo: Repo, path: str, content: bytes) -> None:
    abs_path = Path(repo.working_tree_dir) / path
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    abs_path.write_bytes(content)




