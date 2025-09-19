import asyncio
import hashlib
import json
import logging
import os
import time
from pathlib import Path as PathLibPath
from typing import List
from uuid import UUID

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Path, Query, WebSocket
from fastapi.responses import PlainTextResponse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from .core.aider_runner import stream_edit
from .core.concurrency import StaleObjectError, with_project_lock, handle_conflict
from .core.repo_manager import ensure_repo, head_commit
from .core.supabase_io import upload_file_async
from .core.telemetry import configure_logging
from .core.logging_config import get_logger
from .core.config import config
from .core.metrics import metrics_collector, track_dag_creation, track_llm_operation, track_file_upload, log_performance_summary
from .core.supabase_io import insert_file_version
from .core.error_handling import error_handler, with_error_recovery, create_fallback_dag_spec

# Add memory monitoring at startup
metrics_collector.record_memory_usage()

# Initialize logger
logger = get_logger(__name__)
from .core.auth import get_bearer_token, verify_and_get_user, assert_member

# Import for LLM client
try:
    import litellm
    LLM_AVAILABLE = True
    print("LLM_AVAILABLE: True - litellm imported successfully")
except ImportError as e:
    LLM_AVAILABLE = False
    litellm = None
    print(f"LLM_AVAILABLE: False - litellm import failed: {e}")
from .core.supabase_io import (
    insert_commit,
    insert_file_version,
    supabase_client,
    upload_file,
    latest_sha_for_path,
    insert_user_dag,
    get_user_dag,
    list_user_dags,
)
from .models.dto import OpenReq, OpenRes, SaveReq, SaveRes, CreateDagReq, CreateDagRes


# Progress tracking utility for Flutter progress bars
class ProgressTracker:
    def __init__(self, total_steps: int, websocket, dag_spec: dict):
        self.total_steps = total_steps
        self.current_step = 0
        self.websocket = websocket
        self.dag_spec = dag_spec
        self.start_time = time.time()

    async def send_progress(self, message: str, stage: str, progress_percent: int, metadata: dict = None):
        """Send detailed progress update to Flutter app for progress bar"""
        elapsed_time = time.time() - self.start_time

        progress_data = {
            "type": "progress",
            "message": message,
            "stage": stage,
            "progress": progress_percent,
            "elapsed_time": round(elapsed_time, 2),
            "dag_spec": self.dag_spec,
            "timestamp": time.time()
        }

        # Add metadata if provided
        if metadata:
            progress_data.update(metadata)

        await self.websocket.send_json(progress_data)

    def get_progress_bar_data(self) -> dict:
        """Get data for Flutter progress bar widget"""
        return {
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "progress_percent": min(100, (self.current_step / self.total_steps) * 100),
            "elapsed_time": time.time() - self.start_time
        }


configure_logging()
app = FastAPI(title="aider-worker", version="0.1.0")


def get_llm_client():
    """Get an LLM client for agent command processing."""
    print(f"get_llm_client called. LLM_AVAILABLE: {LLM_AVAILABLE}")

    if not LLM_AVAILABLE:
        print("LLM not available - litellm not imported")
        return None

    try:
        # Configure litellm with environment variables
        api_base = os.getenv("AIDER_OPENAI_API_BASE", "https://openrouter.ai/api/v1")
        api_key = os.getenv("AIDER_OPENAI_API_KEY", "")
        openrouter_key = os.getenv("OPENROUTER_API_KEY", "")
        model = os.getenv("AIDER_MODEL", "qwen/qwq-32b")

        print(f"Environment variables - api_base: {api_base}, api_key: {'***' + api_key[-4:] if api_key else 'None'}, openrouter_key: {'***' + openrouter_key[-4:] if openrouter_key else 'None'}, model: {model}")

        # Use the appropriate API key
        if openrouter_key:
            api_key = openrouter_key
            print("Using OPENROUTER_API_KEY")
        elif api_key:
            print("Using AIDER_OPENAI_API_KEY")
        else:
            print("Warning: No LLM API key found, agent commands will use fallback parsing")
            return None

        # Set environment variables for litellm
        os.environ["OPENROUTER_API_KEY"] = api_key

        # Extract model name for litellm (remove openrouter/ prefix if present)
        if model.startswith("openrouter/"):
            litellm_model = model  # Keep the full model name for litellm
        else:
            litellm_model = f"openrouter/{model}"

        print(f"Using litellm model: {litellm_model}")

        # Test the connection
        try:
            # Simple test call to verify the setup
            test_response = litellm.completion(
                model=litellm_model,
                messages=[{"role": "user", "content": "test"}],
                max_tokens=10
            )
            print("LLM connection test successful")
        except Exception as test_e:
            print(f"LLM connection test failed: {test_e}")
            # Continue anyway, the main call might work

        # Create a simple LLM client wrapper
        class LLMClient:
            def __init__(self, model_name: str):
                self.model_name = model_name
                print(f"LLMClient initialized with model: {model_name}")

            async def generate(self, prompt: str) -> str:
                """Generate response from LLM."""
                try:
                    print(f"LLMClient generating response for prompt (length: {len(prompt)} chars)")
                    response = await litellm.acompletion(
                        model=self.model_name,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.1,
                        max_tokens=2000
                    )
                    content = response.choices[0].message.content
                    print(f"LLMClient received response (length: {len(content) if content else 0} chars)")
                    return content
                except Exception as e:
                    print(f"LLM generation failed: {e}")
                    import traceback
                    traceback.print_exc()
                    return None

        client = LLMClient(litellm_model)
        print(f"LLM client created successfully: {client}")
        return client

    except Exception as e:
        print(f"Failed to create LLM client: {e}")
        import traceback
        traceback.print_exc()
        return None


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    return {"ready": True}


def _verify_and_member(project_id: UUID, authorization: str | None):
    token = get_bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Missing Authorization")
    try:
        user = verify_and_get_user(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    try:
        assert_member(project_id, UUID(user["user_id"]))
    except Exception as e:
        raise HTTPException(status_code=403, detail=str(e))
    return user


@app.post("/open", response_model=OpenRes)
def open_project(req: OpenReq, authorization: str | None = Header(None)):
    user = _verify_and_member(req.project_id, authorization)
    repo = ensure_repo(req.project_id)
    files = [str(p.relative_to(repo.working_tree_dir)) for p in PathLibPath(repo.working_tree_dir).rglob("*") if p.is_file() and ".git/" not in str(p)]
    return OpenRes(head_commit=head_commit(repo), branch=str(repo.active_branch) if not repo.head.is_detached else "HEAD", files=sorted(files))


@app.get("/file", response_class=PlainTextResponse)
def get_file(project_id: UUID = Query(...), path: str = Query(...), authorization: str | None = Header(None)):
    _verify_and_member(project_id, authorization)
    repo = ensure_repo(project_id)
    abs_path = PathLibPath(repo.working_tree_dir) / path
    if not abs_path.exists() or not abs_path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    try:
        return abs_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # Return binary as hex fallback
        return abs_path.read_bytes().hex()


def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


@app.post("/save", response_model=SaveRes)
async def save_project(req: SaveReq, authorization: str | None = Header(None)):
    user = _verify_and_member(req.project_id, authorization)
    repo = ensure_repo(req.project_id)

    async with with_project_lock(req.project_id):
        # Stage and commit
        repo.git.add(A=True)
        # Only commit if there are changes
        if repo.is_dirty(index=True, working_tree=True, untracked_files=True):
            repo.index.commit(req.message)

        commit_id = head_commit(repo)

        # Build manifest and upload only changed files
        changed: List[str] = []
        root = PathLibPath(repo.working_tree_dir)
        # Use porcelain to detect changes in the new commit vs working tree
        status = repo.git.status("--porcelain")
        candidates: List[str] = []
        for line in status.splitlines():
            line = line.strip()
            if not line or len(line) <= 3:
                continue
            rel = line[3:]
            candidates.append(rel)
        # If porcelain is empty (nothing staged), fall back to all tracked files
        if not candidates:
            candidates = [
                str(p.relative_to(root))
                for p in root.rglob("*")
                if p.is_file() and ".git/" not in str(p)
            ]

        for rel in sorted(set(candidates)):
            p = root / rel
            if not p.exists() or not p.is_file():
                continue
            data = p.read_bytes()
            sha = _sha256_bytes(data)
            size = len(data)
            try:
                latest = latest_sha_for_path(req.project_id, rel)
                upload_file(req.project_id, rel, data, if_match_sha=latest)
            except StaleObjectError:
                payload = await handle_conflict(repo, req.project_id)
                raise HTTPException(status_code=409, detail=payload)

            insert_file_version(
                req.project_id,
                rel,
                commit_id,
                sha,
                size,
                modified_by=str(user["user_id"]),  # Ensure it's a string
            )
            changed.append(rel)

        # Upload manifest JSON
        manifest = {"commit": commit_id, "files": changed}
        sb = supabase_client()
        manifest_path = f"projects/{req.project_id}/manifests/{commit_id}.json"
        sb.storage.from_(os.getenv("SUPABASE_BUCKET")).upload(
            manifest_path,
            json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
            {"contentType": "application/json", "upsert": "true"},
        )

        # Insert project commit row
        insert_commit(
            req.project_id,
            commit_id,
            author="worker",
            message=req.message,
            changed_paths=changed,
            branch=str(repo.active_branch) if not repo.head.is_detached else "HEAD",
        )

        return SaveRes(commit_id=commit_id, changed_paths=changed)


@app.websocket("/edit/stream")
async def edit_stream(websocket: WebSocket, project_id: UUID, prompt: str, token: str = Query(...)):
    # Verify JWT and membership before accepting WebSocket
    try:
        user = verify_and_get_user(token)
        assert_member(project_id, UUID(user["user_id"]))
    except Exception as e:
        print(f"WebSocket auth failed: {e}")
        await websocket.close(code=1000, reason=f"Unauthorized: {e}")
        return

    await websocket.accept()
    print(f"WebSocket accepted for project {project_id}, prompt: {prompt}")

    # Track start time for progress updates
    start_time = time.time()

    # Import agent command processor
    from .core.agent_commands import AgentCommandProcessor

    # Check if this is an /agent command
    agent_processor = AgentCommandProcessor()
    print(f"Checking if agent command: {prompt}")
    print(f"Is agent command: {agent_processor.is_agent_command(prompt)}")

    if agent_processor.is_agent_command(prompt):
        try:
            print(f"🎯 WEBSOCKET HANDLER: Processing agent command: {prompt}")
            # Get LLM client for enhanced processing
            print("🔍 WEBSOCKET HANDLER: Getting LLM client...")
            llm_client = get_llm_client()
            print(f"🤖 WEBSOCKET HANDLER: LLM client result: {llm_client}")
            if llm_client:
                print(f"✅ WEBSOCKET HANDLER: Using LLM client: {llm_client.model_name}")
            else:
                print("❌ WEBSOCKET HANDLER: No LLM client available, using fallback parsing")

            # Send progress update before LLM processing
            if llm_client:
                await websocket.send_json({
                    "type": "progress",
                    "message": "🤖 Generating DAG specification with AI...",
                    "stage": "llm_processing",
                    "progress": 5,
                    "elapsed_time": time.time() - start_time,
                    "timestamp": time.time()
                })

            # Process the agent command with WebSocket progress tracking
            command_result = await agent_processor.process_command(
                prompt, project_id, user["user_id"], llm_client, websocket, start_time
            )
            print(f"Command result: {command_result}")

            # The LLM processing now sends its own completion message, so we skip duplicate progress updates

            # For DAG creation, keep connection open and handle streaming
            if command_result["success"] and command_result["action"] == "create_dag":
                print("Starting DAG creation stream...")
                await _handle_dag_creation_stream(websocket, command_result["data"], project_id, user)
            else:
                # For other commands, send response and close
                response_data = {
                    "type": "agent_response",
                    "message": command_result["response_text"],
                    "action": command_result["action"],
                    "success": command_result["success"]
                }
                print(f"Sending response: {response_data}")
                await websocket.send_json(response_data)

                # Handle different actions that don't need streaming
                if command_result["success"]:
                    if command_result["action"] == "list_dags":
                        await _handle_list_dags_stream(websocket, command_result["data"])
                    elif command_result["action"] == "show_dag":
                        await _handle_show_dag_stream(websocket, command_result["data"])
                    elif command_result["action"] == "edit_dag":
                        await _handle_edit_dag_stream(websocket, command_result["data"])
                    elif command_result["action"] == "help":
                        # Help response already sent above
                        pass
            
        except Exception as e:
            print(f"Exception processing agent command: {str(e)}")
            import traceback
            traceback.print_exc()
            await websocket.send_json({
                "type": "dag_created",
                "dag_id": "agent_command_error",
                "file_path": "dags/agent_command_error",
                "commit_id": "agent_command_error",
                "message": f"❌ **Agent Command Failed**\n\n" +
                          f"An error occurred while processing your agent command.\n\n" +
                          f"**Error:** {str(e)}\n\n" +
                          f"Please try again or contact support if the issue persists.",
                "structure": {},
                "stage": "completed",
                "progress": 100,
                "performance_summary": {"error": True}
            })
    else:
        print(f"Not an agent command, treating as regular aider command: {prompt}")
        # Regular DAG editing with aider
        repo = ensure_repo(project_id)
        try:
            async for evt in stream_edit(str(PathLibPath(repo.working_tree_dir)), prompt):
                await websocket.send_json(evt)
        except Exception as e:
            await websocket.send_json({
                "type": "dag_created",
                "dag_id": "aider_command_error",
                "file_path": "dags/aider_command_error",
                "commit_id": "aider_command_error",
                "message": f"❌ **Command Processing Failed**\n\n" +
                          f"An error occurred while processing your command.\n\n" +
                          f"**Error:** {str(e)}\n\n" +
                          f"Please try again or contact support if the issue persists.",
                "structure": {},
                "stage": "completed",
                "progress": 100,
                "performance_summary": {"error": True}
            })

    print("WebSocket handler completed, closing connection")
    # Close websocket
    try:
        await websocket.close()
    except:
        pass


async def _handle_dag_creation_stream(websocket: WebSocket, data: dict, project_id: UUID, user: dict):
    """Handle DAG creation through streaming interface."""

    # Send immediate progress update - let user know we started
    await websocket.send_json({
        "type": "progress",
        "message": "🚀 Starting DAG creation process...",
        "stage": "initializing",
        "progress": 0,
        "elapsed_time": 0.0,
        "timestamp": time.time()
    })

    print(f"Starting DAG creation stream for project {project_id}")
    print(f"Data received: {data}")
    print(f"Data keys: {list(data.keys()) if data else 'No data'}")
    try:
        print("🔍 Parsing DAG spec from data...")
        # Convert the agent command data to CreateDagReq
        if "dag_spec" not in data:
            logger.error("dag_spec not found in WebSocket data")
            raise KeyError("dag_spec not found in data")

        dag_spec = data["dag_spec"]
        logger.info(f"Processing DAG spec with {len(dag_spec.get('tasks', []))} tasks")
        logger.debug(f"DAG spec keys: {list(dag_spec.keys()) if dag_spec else 'No dag_spec'}")

        # Record memory usage after parsing DAG spec
        metrics_collector.record_memory_usage()

        # Send initial DAG structure for visualization
        logger.info(f"Sending dag_creation_start message for DAG: {dag_spec['dag_name']}")
        await websocket.send_json({
            "type": "dag_creation_start",
            "dag_spec": dag_spec,
            "message": f"🤖 Starting DAG creation: **{dag_spec['dag_name']}**",
            "stage": "parsing",
            "progress": 10
        })

        # Send progress update
        await websocket.send_json({
            "type": "progress",
            "message": "🔧 Generating DAG file...",
            "stage": "generating",
            "progress": 25
        })
        
        create_req = CreateDagReq(
            project_id=project_id,
            dag_name=dag_spec["dag_name"],
            dag_id=dag_spec.get("dag_id"),  # Preserve LLM-generated DAG ID for naming consistency
            description=dag_spec["description"],
            schedule_interval=dag_spec["schedule_interval"],
            owner=dag_spec["owner"],
            tasks=dag_spec["tasks"],
            tags=dag_spec.get("tags", [])
        )
        
        # Import here to avoid circular imports
        from .core.dag_generator import create_dag_in_project
        from .core.repo_manager import ensure_repo

        logger.info(f"Ensuring repository for project {project_id}")
        repo = ensure_repo(project_id)
        logger.info(f"Repository ensured: {repo.working_tree_dir}")
        projects_root = PathLibPath(repo.working_tree_dir).parent
        logger.debug(f"Projects root directory: {projects_root}")
        
        async with with_project_lock(project_id):
            # Initialize progress tracking
            progress_tracker = ProgressTracker(total_steps=8, websocket=websocket, dag_spec=dag_spec)
            await progress_tracker.send_progress("📋 Processing DAG specification...", "parsing_spec", 18)

            # Track DAG creation performance
            with track_dag_creation(project_id, user["user_id"]):
                await progress_tracker.send_progress("📝 Generating task implementations...", "writing_files", 25)
            
            await progress_tracker.send_progress("🔧 Creating directory structure...", "creating_files", 35)

            print("📝 Creating DAG files...")
            dag_id, dag_directory_path = create_dag_in_project(
                project_id, create_req, user["user_id"], projects_root
            )
            print(f"✅ DAG created: {dag_id} at {dag_directory_path}")

            # Calculate relative path from repo root for the main DAG file
            repo_root = PathLibPath(repo.working_tree_dir)
            dag_file_path = dag_directory_path / f"{dag_id}.py"
            relative_path = dag_file_path.relative_to(repo_root)
            relative_dir_path = dag_directory_path.relative_to(repo_root)
            print(f"📍 Relative paths calculated: dir={relative_dir_path}, file={relative_path}")

            await progress_tracker.send_progress("💾 Staging files for git...", "staging", 50)
            
            # Stage and commit the entire DAG directory
            print("💾 Staging and committing files...")
            repo.git.add(str(dag_directory_path))
            
            await progress_tracker.send_progress("📝 Creating git commit...", "committing", 60)
            commit_message = f"Create DAG: {create_req.dag_name}"
            repo.index.commit(commit_message)
            commit_id = head_commit(repo)
            print(f"✅ Commit created: {commit_id[:8]}...")

            await progress_tracker.send_progress("☁️ Preparing files for upload...", "preparing_upload", 70)
            
            # Upload all files in the DAG directory to Supabase storage
            logger.info("Starting parallel file uploads to Supabase")
            uploaded_files = []

            # Prepare file upload tasks
            upload_tasks = []
            dag_dir_name = dag_directory_path.name
            logger.info(f"Preparing upload tasks for DAG directory: {dag_dir_name}")

            for file_path in dag_directory_path.rglob("*"):
                if file_path.is_file():
                    file_content = file_path.read_bytes()
                    sha = _sha256_bytes(file_content)
                    size = len(file_content)
                    relative_file_path = file_path.relative_to(repo_root)

                    # Validate that the file path belongs to the current DAG
                    relative_path_str = str(relative_file_path)
                    logger.debug(f"🔍 Checking file: {relative_path_str} for DAG: {dag_dir_name}")

                    if not relative_path_str.startswith(f"dags/{dag_dir_name}"):
                        logger.error(f"❌ Path validation failed! File {relative_path_str} doesn't belong to DAG {dag_dir_name}")
                        logger.error(f"Expected prefix: dags/{dag_dir_name}")
                        logger.error(f"File absolute path: {file_path}")
                        logger.error(f"DAG directory path: {dag_directory_path}")
                        continue

                    logger.debug(f"✅ File validated: {relative_path_str}")
                    upload_tasks.append({
                        'file_path': file_path,
                        'relative_path': relative_path_str,
                        'content': file_content,
                        'sha': sha,
                        'size': size
                    })

            logger.info(f"Prepared {len(upload_tasks)} upload tasks for DAG {dag_dir_name}")

            # Record memory usage before file uploads
            metrics_collector.record_memory_usage()

            # Upload files in parallel batches with dynamic sizing and error handling
            batch_size = min(config.upload_batch_size, len(upload_tasks))
            total_batches = (len(upload_tasks) + batch_size - 1) // batch_size

            for batch_index in range(total_batches):
                start_idx = batch_index * batch_size
                end_idx = min(start_idx + batch_size, len(upload_tasks))
                batch = upload_tasks[start_idx:end_idx]
                batch_file_count = len(batch)

                # Calculate progress percentage for upload phase (75-95%)
                upload_progress = 75 + int((batch_index / total_batches) * 20)

                await progress_tracker.send_progress(
                    f"☁️ Uploading batch {batch_index + 1}/{total_batches} ({batch_file_count} files)...",
                    "uploading",
                    upload_progress,
                    {
                        "batch_info": {
                            "current_batch": batch_index + 1,
                            "total_batches": total_batches,
                            "files_in_batch": batch_file_count,
                            "uploaded_so_far": start_idx
                        }
                    }
                )

                logger.info(f"Uploading batch {batch_index + 1} with {batch_file_count} files")

                # Track file upload performance
                with track_file_upload(batch_file_count):
                    # Parallel upload within batch with error handling
                    upload_coroutines = []
                    for task in batch:
                        logger.debug(f"📤 Uploading: {task['relative_path']}")
                        logger.debug(f"📤 Task details - file_path: {task['file_path']}, relative_path: {task['relative_path']}, dag_dir_name: {dag_dir_name}")

                        # Double-check path validation before upload
                        if not task['relative_path'].startswith(f"dags/{dag_dir_name}"):
                            logger.error(f"🚨 CRITICAL: About to upload file with wrong path!")
                            logger.error(f"🚨 Wrong path: {task['relative_path']}")
                            logger.error(f"🚨 Expected prefix: dags/{dag_dir_name}")
                            logger.error(f"🚨 Skipping this file to prevent corruption")
                            continue

                        upload_coroutines.append(
                            upload_file_async(
                                project_id,
                                task['relative_path'],
                                task['content']
                            )
                        )

                    # Wait for batch to complete with error handling
                    if upload_coroutines:
                        try:
                            upload_results = await asyncio.gather(*upload_coroutines, return_exceptions=True)

                            # Check for upload errors
                            failed_uploads = []
                            for i, result in enumerate(upload_results):
                                if isinstance(result, Exception):
                                    task = batch[i]
                                    logger.error(f"❌ Upload failed for {task['relative_path']}: {str(result)}")

                                    # Log additional context for debugging
                                    logger.error(f"Debug - Project ID: {project_id}")
                                    logger.error(f"Debug - DAG directory: {dag_directory_path}")
                                    logger.error(f"Debug - Task relative path: {task['relative_path']}")

                                    failed_uploads.append((task, str(result)))

                            # Retry failed uploads individually
                            if failed_uploads:
                                logger.info(f"🔄 Retrying {len(failed_uploads)} failed uploads...")
                                for task, error_msg in failed_uploads:
                                    try:
                                        await upload_file_async(
                                            project_id,
                                            task['relative_path'],
                                            task['content']
                                        )
                                        logger.info(f"✅ Retry successful for {task['relative_path']}")
                                    except Exception as retry_error:
                                        logger.error(f"❌ Retry failed for {task['relative_path']}: {str(retry_error)}")
                                        # Continue with other files - don't fail the entire batch

                        except Exception as batch_error:
                            logger.error(f"❌ Batch upload error: {str(batch_error)}")
                            # Continue processing other batches

                # Record file versions for this batch with error handling
                for task in batch:
                    try:
                        logger.debug(f"📝 Recording file version: {task['relative_path']}")
                        insert_file_version(
                            project_id,
                            task['relative_path'],
                            commit_id,
                            task['sha'],
                            task['size'],
                            str(user["user_id"]),
                        )
                    except Exception as version_error:
                        logger.error(f"❌ Failed to record file version for {task['relative_path']}: {str(version_error)}")
                        # Continue with other files

                    # Track successfully processed files
                    uploaded_files.append(task['relative_path'])

            logger.info(f"✅ Successfully uploaded {len(uploaded_files)} files to Supabase")

            await progress_tracker.send_progress("📋 Recording commit in database...", "recording_commit", 95)

            # Record memory usage after file uploads
            metrics_collector.record_memory_usage()

            # Log performance summary
            log_performance_summary()

            # Record the commit
            print("📋 Recording commit in database...")
            insert_commit(
                project_id,
                commit_id,
                author=create_req.owner,
                message=commit_message,
                changed_paths=uploaded_files,
                branch=str(repo.active_branch) if not repo.head.is_detached else "HEAD",
            )
            print("✅ Commit recorded in database")

            await progress_tracker.send_progress("🏷️ Recording DAG metadata...", "recording_metadata", 98)

            # Record DAG metadata in Supabase
            print("🏷️ Recording DAG metadata in database...")
            dag_record = insert_user_dag(
                project_id=project_id,
                user_id=UUID(user["user_id"]),
                dag_id=dag_id,
                dag_name=create_req.dag_name,
                description=create_req.description,
                file_path=str(relative_dir_path),  # Store directory path
                schedule_interval=create_req.schedule_interval,
                owner=create_req.owner,
                tags=create_req.tags or [],
                task_count=len(create_req.tasks),
                tasks=create_req.tasks,  # Store complete task specifications for Flutter app
                commit_id=commit_id,
            )
            print("✅ DAG metadata recorded in database")

            # Send final success response with complete DAG structure
            await progress_tracker.send_progress("✅ DAG creation completed successfully!", "completed", 100)

            await websocket.send_json({
                "type": "dag_created",
                "dag_id": dag_id,
                "file_path": str(relative_dir_path),
                "commit_id": commit_id,
                "message": f"✅ **DAG Created Successfully!**\n\n" +
                          f"🆔 **DAG ID:** `{dag_id}`\n" +
                          f"📁 **Directory:** `{relative_dir_path}`\n" +
                          f"🔗 **Commit:** `{commit_id[:8]}...`\n" +
                          f"📄 **Files Created:** {len(uploaded_files)}\n\n" +
                          f"Your DAG is now ready with proper directory structure and synced with Supabase. " +
                          f"You can edit it by typing `/agent edit {dag_id}` or view it with `/agent show {dag_id}`.",
                "structure": {
                    "dag_name": create_req.dag_name,
                    "description": create_req.description,
                    "schedule_interval": create_req.schedule_interval,
                    "owner": create_req.owner,
                    "tags": create_req.tags,
                    "tasks": create_req.tasks,
                    "directory_structure": {
                        "main_file": str(relative_dir_path / f"{dag_id}.py"),
                        "config_file": str(relative_dir_path / "config.json"),
                        "readme_file": str(relative_dir_path / "README.md"),
                        "utils_directory": str(relative_dir_path / "utils")
                    }
                },
                "stage": "completed",
                "progress": 100,
                "performance_summary": progress_tracker.get_progress_bar_data()
            })
            # End of track_dag_creation context manager
            pass

    except Exception as e:
        logger.error(f"Exception in DAG creation: {str(e)}")

        # Use enhanced error handling
        error_context = {
            'project_id': str(project_id) if 'project_id' in locals() else None,
            'user_id': user.get('user_id') if user else None,
            'operation': 'dag_creation_stream'
        }

        recovery_result = await error_handler.handle_error(
            e, 'dag_creation_stream', error_context, max_retries=2
        )

        try:
            if recovery_result and isinstance(recovery_result, dict):
                # If recovery provided a fallback DAG spec, use it
                if 'dag_name' in recovery_result:
                    await websocket.send_json({
                        "type": "dag_created",
                        "dag_id": recovery_result.get('dag_id', 'error_recovery_dag'),
                        "file_path": "dags/error_recovery_dag",
                        "commit_id": "error_recovery",
                        "message": "⚠️ **DAG Created with Recovery**\n\n" +
                                  f"Original error: {str(e)}\n" +
                                  "A fallback DAG was created to maintain system stability.",
                        "structure": recovery_result,
                        "stage": "completed",
                        "progress": 100
                    })
                else:
                    await websocket.send_json({
                        "type": "dag_created",
                        "dag_id": "error_recovery_attempt",
                        "file_path": "dags/error_recovery_attempt",
                        "commit_id": "error_recovery_attempt",
                        "message": f"❌ **DAG Creation Failed**\n\n" +
                                  f"Recovery was attempted but failed.\n\n" +
                                  f"**Error:** {str(e)}\n\n" +
                                  f"Please try again or contact support if the issue persists.",
                        "structure": {},
                        "stage": "completed",
                        "progress": 100,
                        "performance_summary": {"error": True}
                    })
            else:
                await websocket.send_json({
                    "type": "dag_created",
                    "dag_id": "creation_failed",
                    "file_path": "dags/creation_failed",
                    "commit_id": "creation_failed",
                    "message": f"❌ **DAG Creation Failed**\n\n" +
                              f"An error occurred during DAG creation.\n\n" +
                              f"**Error:** {str(e)}\n\n" +
                              f"Please try again or contact support if the issue persists.",
                    "structure": {},
                    "stage": "completed",
                    "progress": 100,
                    "performance_summary": {"error": True}
                })
        except Exception as send_error:
            logger.error(f"Failed to send error message via websocket: {send_error}")


async def _handle_list_dags_stream(websocket: WebSocket, data: dict):
    """Handle DAG listing through streaming interface."""
    try:
        project_id = UUID(data["project_id"])
        user_id = data.get("user_id")
        
        # Get DAGs from database
        dags = list_user_dags(project_id, UUID(user_id) if user_id else None, is_active=True)
        
        if not dags:
            await websocket.send_json({
                "type": "dag_list",
                "dags": [],
                "message": "📋 **No DAGs found**\n\nYou haven't created any DAGs yet. Create one with:\n`/agent create your description here`"
            })
        else:
            # Format DAG list
            dag_list_text = "📋 **Your DAGs:**\n\n"
            for dag in dags:
                dag_list_text += f"🤖 **{dag['dag_name']}** (`{dag['dag_id']}`)\n"
                dag_list_text += f"   📝 {dag['description']}\n"
                dag_list_text += f"   ⏰ {dag['schedule_interval']} | 🔧 {dag['task_count']} tasks\n"
                dag_list_text += f"   📅 Created: {dag['created_at'][:10]}\n\n"
            
            dag_list_text += f"💡 **Commands:**\n"
            dag_list_text += f"- `/agent show [dag_id]` - View DAG details\n"
            dag_list_text += f"- `/agent edit [dag_id]` - Edit DAG in canvas\n"
            dag_list_text += f"- `/agent create [description]` - Create new DAG"
            
            await websocket.send_json({
                "type": "dag_list",
                "dags": dags,
                "message": dag_list_text
            })
            
    except Exception as e:
        await websocket.send_json({
            "type": "dag_list",
            "dags": [],
            "message": f"❌ **Failed to List DAGs**\n\n" +
                      f"An error occurred while retrieving your DAGs.\n\n" +
                      f"**Error:** {str(e)}\n\n" +
                      f"Please try again or contact support if the issue persists.",
            "stage": "completed",
            "progress": 100,
            "performance_summary": {"error": True}
        })


async def _handle_show_dag_stream(websocket: WebSocket, data: dict):
    """Handle DAG details display through streaming interface."""
    try:
        project_id = UUID(data["project_id"])
        dag_id = data["dag_id"]
        
        # Get DAG from database
        dag = get_user_dag(project_id, dag_id)
        
        if not dag:
            await websocket.send_json({
                "type": "error",
                "message": f"❌ DAG `{dag_id}` not found. Use `/agent list` to see available DAGs."
            })
            return
        
        # Format DAG details
        details_text = f"🤖 **{dag['dag_name']}**\n\n"
        details_text += f"🆔 **ID:** `{dag['dag_id']}`\n"
        details_text += f"📝 **Description:** {dag['description']}\n"
        details_text += f"⏰ **Schedule:** {dag['schedule_interval']}\n"
        details_text += f"👤 **Owner:** {dag['owner']}\n"
        details_text += f"🔧 **Tasks:** {dag['task_count']}\n"
        details_text += f"📁 **File:** `{dag['file_path']}`\n"
        details_text += f"🏷️ **Tags:** {', '.join(dag['tags']) if dag['tags'] else 'None'}\n"
        details_text += f"📅 **Created:** {dag['created_at']}\n"
        details_text += f"🔄 **Updated:** {dag['updated_at']}\n"
        details_text += f"✅ **Active:** {'Yes' if dag['is_active'] else 'No'}\n\n"
        details_text += f"💡 **Actions:**\n"
        details_text += f"- `/agent edit {dag_id}` - Edit this DAG\n"
        details_text += f"- View file content: Use file browser to open `{dag['file_path']}`"
        
        await websocket.send_json({
            "type": "dag_details",
            "dag": dag,
            "message": details_text,
            "structure": {  # Include complete DAG structure for Flutter visualization
                "dag_name": dag["dag_name"],
                "description": dag["description"],
                "schedule_interval": dag["schedule_interval"],
                "owner": dag["owner"],
                "tags": dag.get("tags", []),
                "tasks": dag.get("tasks", [])
            }
        })
        
    except Exception as e:
        await websocket.send_json({
            "type": "dag_details",
            "dag": None,
            "message": f"❌ **Failed to Show DAG Details**\n\n" +
                      f"An error occurred while retrieving DAG details.\n\n" +
                      f"**Error:** {str(e)}\n\n" +
                      f"Please try again or contact support if the issue persists.",
            "stage": "completed",
            "progress": 100,
            "performance_summary": {"error": True}
        })


async def _handle_edit_dag_stream(websocket: WebSocket, data: dict):
    """Handle DAG editing through streaming interface."""
    try:
        project_id = UUID(data["project_id"])
        dag_id = data["dag_id"]
        instructions = data.get("instructions", "")

        print(f"Retrieving DAG {dag_id} from database for project {project_id}")
        # Get DAG from database
        dag = get_user_dag(project_id, dag_id)
        print(f"DAG retrieved: {dag}")

        if not dag:
            print(f"DAG {dag_id} not found in database")
            await websocket.send_json({
                "type": "error",
                "message": f"❌ DAG `{dag_id}` not found. Use `/agent list` to see available DAGs."
            })
            return

        print(f"DAG data keys: {list(dag.keys()) if dag else 'None'}")
        print(f"DAG tasks: {dag.get('tasks', 'NO TASKS KEY')}")
        print(f"DAG task_count: {dag.get('task_count', 'NO TASK_COUNT')}")
        
        # Send canvas open response
        canvas_message = f"🎨 **Opening Visual Canvas for {dag['dag_name']}**\n\n"
        canvas_message += f"📊 **DAG Structure:**\n"
        canvas_message += f"- **{dag['task_count']} tasks** with dependencies\n"
        canvas_message += f"- **Schedule:** {dag['schedule_interval']}\n"
        canvas_message += f"- **File:** `{dag['file_path']}`\n\n"
        canvas_message += f"🖱️ **Canvas Features:**\n"
        canvas_message += f"- Drag tasks to reorder\n"
        canvas_message += f"- Click connections to modify dependencies\n"
        canvas_message += f"- Double-click tasks to edit code\n"
        canvas_message += f"- Use toolbar to add new tasks\n\n"
        
        if instructions:
            canvas_message += f"📝 **Your Instructions:** {instructions}\n\n"
        
        canvas_message += f"💾 Changes are auto-saved and synced to Supabase."

        response_data = {
            "type": "open_canvas",
            "dag": dag,
            "instructions": instructions,
            "message": canvas_message,
            "structure": {  # Include complete DAG structure for Flutter visualization
                "dag_name": dag["dag_name"],
                "description": dag["description"],
                "schedule_interval": dag["schedule_interval"],
                "owner": dag["owner"],
                "tags": dag.get("tags", []),
                "tasks": dag.get("tasks", [])
            }
        }

        print(f"Sending open_canvas response: {response_data}")
        print(f"Structure tasks being sent: {response_data['structure']['tasks']}")

        await websocket.send_json(response_data)
        
    except Exception as e:
        await websocket.send_json({
            "type": "open_canvas",
            "dag": None,
            "instructions": "",
            "message": f"❌ **Failed to Open DAG Editor**\n\n" +
                      f"An error occurred while opening the DAG editor.\n\n" +
                      f"**Error:** {str(e)}\n\n" +
                      f"Please try again or contact support if the issue persists.",
            "stage": "completed",
            "progress": 100,
            "performance_summary": {"error": True}
        })


@app.post("/create-dag", response_model=CreateDagRes)
async def create_dag(req: CreateDagReq, authorization: str | None = Header(None)):
    """Create a new DAG from user input and sync with Supabase."""
    user = _verify_and_member(req.project_id, authorization)
    user_id = UUID(user["user_id"])

    # Import here to avoid circular imports
    from .core.dag_generator import create_dag_in_project
    from .core.repo_manager import ensure_repo

    repo = ensure_repo(req.project_id)
    projects_root = PathLibPath(repo.working_tree_dir).parent

    async with with_project_lock(req.project_id):
        try:
            # Generate and create the DAG directory structure
            dag_id, dag_directory_path = create_dag_in_project(
                req.project_id, req, user["user_id"], projects_root
            )

            # Calculate relative paths
            repo_root = PathLibPath(repo.working_tree_dir)
            dag_file_path = dag_directory_path / f"{dag_id}.py"
            relative_path = dag_file_path.relative_to(repo_root)
            relative_dir_path = dag_directory_path.relative_to(repo_root)

            # Stage and commit the entire DAG directory
            repo.git.add(str(dag_directory_path))
            commit_message = f"Create DAG: {req.dag_name}"
            repo.index.commit(commit_message)
            commit_id = head_commit(repo)

            # Upload all files in the DAG directory to Supabase storage
            uploaded_files = []
            for file_path in dag_directory_path.rglob("*"):
                if file_path.is_file():
                    file_content = file_path.read_bytes()
                    sha = _sha256_bytes(file_content)
                    size = len(file_content)

                    # Calculate relative path for each file
                    relative_file_path = file_path.relative_to(repo_root)
                    upload_file(req.project_id, str(relative_file_path), file_content)

                    # Record file version for each file
                    insert_file_version(
                        req.project_id,
                        str(relative_file_path),
                        commit_id,
                        sha,
                        size,
                        str(user["user_id"]),  # Ensure it's a string
                    )
                    uploaded_files.append(str(relative_file_path))

            # Record the commit
            insert_commit(
                req.project_id,
                commit_id,
                author=req.owner,
                message=commit_message,
                changed_paths=uploaded_files,
                branch=str(repo.active_branch) if not repo.head.is_detached else "HEAD",
            )

            # Record DAG metadata in Supabase
            dag_record = insert_user_dag(
                project_id=req.project_id,
                user_id=user_id,
                dag_id=dag_id,
                dag_name=req.dag_name,
                description=req.description,
                file_path=str(relative_dir_path),  # Store directory path
                schedule_interval=req.schedule_interval,
                owner=req.owner,
                tags=req.tags or [],
                task_count=len(req.tasks),
                tasks=req.tasks,  # Store complete task specifications for Flutter app
                commit_id=commit_id,
            )

            return CreateDagRes(
                dag_id=dag_id,
                file_path=str(relative_dir_path),  # Return directory path
                commit_id=commit_id,
                created_at=dag_record.get("created_at", ""),
            )

        except Exception as e:
            # If anything fails, try to clean up the directory
            if 'dag_directory_path' in locals() and dag_directory_path.exists():
                import shutil
                shutil.rmtree(dag_directory_path)
            raise HTTPException(status_code=500, detail=f"Failed to create DAG: {str(e)}")


@app.get("/dags")
async def list_dags(
    project_id: UUID = Query(...),
    user_id: UUID = Query(None),
    authorization: str | None = Header(None)
):
    """List all DAGs for a project, optionally filtered by user."""
    _verify_and_member(project_id, authorization)
    
    try:
        dags = list_user_dags(project_id, user_id, is_active=True)
        return {"dags": dags}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list DAGs: {str(e)}")


@app.get("/dags/{dag_id}")
async def get_dag(
    dag_id: str = Path(...),
    project_id: UUID = Query(...),
    authorization: str | None = Header(None)
):
    """Get details of a specific DAG."""
    _verify_and_member(project_id, authorization)

    try:
        dag = get_user_dag(project_id, dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail="DAG not found")
        return dag
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get DAG: {str(e)}")


@app.get("/dags/{dag_id}/structure")
async def get_dag_structure(
    dag_id: str = Path(...),
    project_id: UUID = Query(...),
    authorization: str | None = Header(None)
):
    """Get DAG structure in Flutter app visualization format."""
    _verify_and_member(project_id, authorization)

    try:
        dag = get_user_dag(project_id, dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail="DAG not found")

        # Return DAG in the exact format expected by Flutter app for visualization
        return {
            "dag_name": dag["dag_name"],
            "description": dag["description"],
            "schedule_interval": dag["schedule_interval"],
            "owner": dag["owner"],
            "tags": dag.get("tags", []),
            "tasks": dag.get("tasks", [])  # Complete task specifications for visualization
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get DAG structure: {str(e)}")


