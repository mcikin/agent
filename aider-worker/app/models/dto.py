from typing import List, Optional, Dict, Any
from uuid import UUID

from pydantic import BaseModel


class OpenReq(BaseModel):
    project_id: UUID


class OpenRes(BaseModel):
    head_commit: str
    branch: str
    files: List[str]


class SaveReq(BaseModel):
    project_id: UUID
    message: str


class SaveRes(BaseModel):
    commit_id: str
    changed_paths: List[str]


class CreateDagReq(BaseModel):
    project_id: UUID
    dag_name: str
    dag_id: Optional[str] = None  # LLM-generated DAG ID to preserve naming consistency
    description: str
    schedule_interval: str  # e.g., "@daily", "0 0 * * *", "timedelta(hours=6)"
    owner: str
    tasks: List[Dict[str, Any]]  # List of task definitions
    tags: Optional[List[str]] = []
    start_date: Optional[str] = None  # ISO format date string
    retries: Optional[int] = 1
    retry_delay_minutes: Optional[int] = 5
    depends_on_past: Optional[bool] = False
    catchup: Optional[bool] = False
    email_on_failure: Optional[bool] = False
    email_on_retry: Optional[bool] = False


class CreateDagRes(BaseModel):
    dag_id: str
    file_path: str
    commit_id: str
    created_at: str


class TaskDefinition(BaseModel):
    task_id: str
    task_type: str  # "python", "bash", "http", etc.
    callable_name: Optional[str] = None  # For Python tasks
    python_code: Optional[str] = None  # Inline Python code
    bash_command: Optional[str] = None  # For Bash tasks
    dependencies: Optional[List[str]] = []  # Task IDs this task depends on
    provide_context: Optional[bool] = False





