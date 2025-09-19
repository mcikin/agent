import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from supabase import create_client, Client
from .concurrency import StaleObjectError


# Re-use StaleObjectError from concurrency


def _bucket_name() -> str:
    bucket = os.getenv("SUPABASE_BUCKET")
    if not bucket:
        raise RuntimeError("SUPABASE_BUCKET is not set")
    return bucket


def supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not set")
    return create_client(url, key)


def _storage_key(project_id: UUID, path: str) -> str:
    return f"projects/{project_id}/files/{path}"


def signed_url_for(project_id: UUID, path: str, expires_in: int = 300) -> str:
    sb = supabase_client()
    storage_path = _storage_key(project_id, path)
    
    try:
        res = sb.storage.from_(_bucket_name()).create_signed_url(storage_path, expires_in)
        if isinstance(res, dict):
            return (
                res.get("signedURL")
                or res.get("signed_url")
                or res.get("signedUrl")
                or (res.get("data") or {}).get("signedURL")
                or (res.get("data") or {}).get("signed_url")
                or (res.get("data") or {}).get("signedUrl")
                or ""
            )
        return getattr(res, "signed_url", getattr(res, "signedURL", ""))
    except Exception as e:
        print(f"⚠️ Failed to create signed URL for {path}: {e}")
        return ""  # Return empty string if URL generation fails


def list_latest_files(project_id: UUID) -> List[Dict[str, Any]]:
    """
    Return latest file_versions per path with fields: path, signed_url, created_at, sha256
    Implementation: get rows ordered by path asc, created_at desc and collapse in Python.
    """
    sb = supabase_client()
    res = (
        sb.table("file_versions")
        .select("path,created_at,sha256")
        .eq("project_id", str(project_id))
        .order("path", desc=False)
        .order("created_at", desc=True)
        .execute()
    )
    rows = getattr(res, "data", []) or []
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        p = row.get("path")
        if p not in latest:
            latest[p] = row
    result: List[Dict[str, Any]] = []
    for path, row in latest.items():
        signed_url = signed_url_for(project_id, path)
        # Only include files that have valid signed URLs
        if signed_url:
            result.append(
                {
                    "path": path,
                    "ts": row.get("created_at"),
                    "sha256": row.get("sha256"),
                    "signed_url": signed_url,
                }
            )
        else:
            print(f"⚠️ Skipping file {path} - no valid signed URL")
    return result


def insert_commit(
    project_id: UUID,
    commit_id: str,
    author: str,
    message: str,
    changed_paths: List[str],
    branch: Optional[str] = None,
) -> Dict[str, Any]:
    sb = supabase_client()
    payload = {
        "project_id": str(project_id),
        "commit_id": commit_id,
        "author": author,
        "message": message,
        "changed_paths": changed_paths,
        "branch": branch,
    }
    res = sb.table("project_commits").insert(payload).execute()
    data = getattr(res, "data", None)
    return (data or [{}])[0]


def insert_file_version(
    project_id: UUID,
    path: str,
    commit_id: Optional[str],
    sha256: str,
    size: int,
    modified_by: Optional[str],
) -> Dict[str, Any]:
    sb = supabase_client()
    payload = {
        "project_id": str(project_id),
        "path": path,
        "commit_id": commit_id,
        "sha256": sha256,
        "size_bytes": size,
        "modified_by": modified_by,
    }
    res = sb.table("file_versions").insert(payload).execute()
    data = getattr(res, "data", None)
    return (data or [{}])[0]


def latest_sha_for_path(project_id: UUID, path: str) -> Optional[str]:
    sb = supabase_client()
    res = (
        sb.table("file_versions")
        .select("sha256")
        .eq("project_id", str(project_id))
        .eq("path", path)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    data = getattr(res, "data", []) or []
    if not data:
        return None
    return data[0].get("sha256")


async def upload_file_async(
    project_id: UUID,
    path: str,
    content: bytes,
    *,
    if_match_sha: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Async version of upload_file for parallel uploads.
    Upload file to Storage at projects/{pid}/files/{path}.
    If if_match_sha is provided, verify it equals the latest stored sha256 for the path.
    Raises StaleObjectError if mismatch.
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.debug(f"🔄 upload_file_async called with project_id={project_id}, path={path}")

    if if_match_sha is not None:
        current = latest_sha_for_path(project_id, path)
        logger.debug(f"🔍 SHA check for {path}: current={current}, expected={if_match_sha}")
        if current is not None and current != if_match_sha:
            raise StaleObjectError(
                f"if_match_sha mismatch for {path}: expected {current}, got {if_match_sha}"
            )

    sb = supabase_client()
    storage_path = _storage_key(project_id, path)
    logger.debug(f"📤 Uploading to storage path: {storage_path}")

    # upsert True to overwrite existing object
    sb.storage.from_(_bucket_name()).upload(storage_path, content, {
        "contentType": "application/octet-stream",
        "upsert": "true",
    })
    # Return a small descriptor
    return {"path": path, "size": len(content)}


def upload_file(
    project_id: UUID,
    path: str,
    content: bytes,
    *,
    if_match_sha: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Upload file to Storage at projects/{pid}/files/{path}.
    If if_match_sha is provided, verify it equals the latest stored sha256 for the path.
    Raises StaleObjectError if mismatch.
    """
    if if_match_sha is not None:
        current = latest_sha_for_path(project_id, path)
        if current is not None and current != if_match_sha:
            raise StaleObjectError(
                f"if_match_sha mismatch for {path}: expected {current}, got {if_match_sha}"
            )

    sb = supabase_client()
    storage_path = _storage_key(project_id, path)
    # upsert True to overwrite existing object
    sb.storage.from_(_bucket_name()).upload(storage_path, content, {
        "contentType": "application/octet-stream",
        "upsert": "true",
    })
    # Return a small descriptor
    return {
        "path": path,
        "signed_url": signed_url_for(project_id, path),
    }


def insert_user_dag(
    project_id: UUID,
    user_id: UUID,
    dag_id: str,
    dag_name: str,
    description: str,
    file_path: str,
    schedule_interval: str,
    owner: str,
    tags: List[str],
    task_count: int,
    tasks: List[Dict[str, Any]],  # Complete task specifications for Flutter app
    commit_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Insert a new user-created DAG record into the database."""
    sb = supabase_client()
    payload = {
        "project_id": str(project_id),
        "user_id": str(user_id),
        "dag_id": dag_id,
        "dag_name": dag_name,
        "description": description,
        "file_path": file_path,
        "schedule_interval": schedule_interval,
        "owner": owner,
        "tags": tags,
        "task_count": task_count,
        "tasks": tasks,  # Store complete task specifications
        "commit_id": commit_id,
        "is_active": True,
    }
    res = sb.table("user_dags").insert(payload).execute()
    data = getattr(res, "data", None)
    return (data or [{}])[0]


def update_user_dag(
    project_id: UUID,
    dag_id: str,
    **updates
) -> Dict[str, Any]:
    """Update an existing user DAG record."""
    sb = supabase_client()
    
    # Add updated_at timestamp
    updates["updated_at"] = datetime.now().isoformat()
    
    res = (
        sb.table("user_dags")
        .update(updates)
        .eq("project_id", str(project_id))
        .eq("dag_id", dag_id)
        .execute()
    )
    data = getattr(res, "data", None)
    return (data or [{}])[0] if data else {}


def get_user_dag(project_id: UUID, dag_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific user DAG by project and dag ID."""
    sb = supabase_client()
    res = (
        sb.table("user_dags")
        .select("*")
        .eq("project_id", str(project_id))
        .eq("dag_id", dag_id)
        .execute()
    )
    data = getattr(res, "data", []) or []
    return data[0] if data else None


def list_user_dags(
    project_id: UUID, 
    user_id: Optional[UUID] = None,
    is_active: Optional[bool] = None
) -> List[Dict[str, Any]]:
    """List user DAGs for a project, optionally filtered by user or active status."""
    sb = supabase_client()
    query = sb.table("user_dags").select("*").eq("project_id", str(project_id))
    
    if user_id is not None:
        query = query.eq("user_id", str(user_id))
    
    if is_active is not None:
        query = query.eq("is_active", is_active)
    
    query = query.order("created_at", desc=True)
    
    res = query.execute()
    return getattr(res, "data", []) or []


def delete_user_dag(project_id: UUID, dag_id: str) -> bool:
    """Delete a user DAG record (soft delete by setting is_active=false)."""
    sb = supabase_client()
    res = (
        sb.table("user_dags")
        .update({"is_active": False, "updated_at": datetime.now().isoformat()})
        .eq("project_id", str(project_id))
        .eq("dag_id", dag_id)
        .execute()
    )
    data = getattr(res, "data", []) or []
    return len(data) > 0
