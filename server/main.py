import os
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional
import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn
from supabase import create_client, Client
import aiofiles

# Import Aider components
from aider.main import main as aider_main
from aider.io import InputOutput
from aider.models import Model
from aider import __version__


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    stream: bool = True
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    supabase_connected: bool
    llm_configured: bool
    llm_provider: str


class AiderServerConfig:
    def __init__(self):
        # Supabase configuration
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.supabase_bucket = os.getenv("SUPABASE_BUCKET", "code-files")
        
        # LLM configuration
        self.llm_api_base = os.getenv("AIDER_OPENAI_API_BASE", "https://openrouter.ai/api/v1")
        self.llm_api_key = os.getenv("AIDER_OPENAI_API_KEY", "")
        self.llm_model = os.getenv("AIDER_MODEL", "openrouter/qwen/qwq-32b")
        
        # Detect provider type
        if "openrouter.ai" in self.llm_api_base:
            self.provider = "OpenRouter"
        elif "localhost" in self.llm_api_base or "127.0.0.1" in self.llm_api_base:
            self.provider = "Self-hosted"
        elif "api.openai.com" in self.llm_api_base:
            self.provider = "OpenAI"
        else:
            self.provider = "Custom"
        
        # Working directory for Aider sessions
        self.work_dir = Path(os.getenv("AIDER_WORK_DIR", "/tmp/aider-sessions"))
        self.work_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Supabase client
        if self.supabase_url and self.supabase_service_key:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_service_key)
        else:
            self.supabase = None


config = AiderServerConfig()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print(f"🚀 Aider Server v{__version__} starting up...")
    print(f"💾 Supabase: {'✅ Connected' if config.supabase else '❌ Not configured'}")
    print(f"🤖 LLM Provider: {config.provider}")
    print(f"🔗 LLM Base: {config.llm_api_base}")
    print(f"🎯 Default Model: {config.llm_model}")
    print(f"📁 Work directory: {config.work_dir}")
    yield
    # Shutdown
    print("🛑 Aider Server shutting down...")


app = FastAPI(
    title="Aider HTTP API Server",
    description="HTTP API wrapper for Aider with Supabase file storage",
    version=__version__,
    lifespan=lifespan
)


class AiderSession:
    def __init__(self, session_id: str, project_files: List[str] = None):
        self.session_id = session_id
        self.session_dir = config.work_dir / session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.project_files = project_files or []
        
    async def download_files_from_supabase(self, file_paths: List[str]):
        """Download project files from Supabase to local session directory"""
        if not config.supabase:
            raise HTTPException(status_code=500, detail="Supabase not configured")
            
        downloaded_files = []
        for file_path in file_paths:
            try:
                # Download file from Supabase storage
                response = config.supabase.storage.from_(config.supabase_bucket).download(file_path)
                
                # Create local file path
                local_path = self.session_dir / file_path
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Write file content
                async with aiofiles.open(local_path, 'wb') as f:
                    await f.write(response)
                    
                downloaded_files.append(str(local_path))
                print(f"📥 Downloaded: {file_path} -> {local_path}")
                
            except Exception as e:
                print(f"❌ Failed to download {file_path}: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to download {file_path}: {e}")
                
        return downloaded_files
        
    async def upload_files_to_supabase(self, local_files: List[str]):
        """Upload modified files back to Supabase"""
        if not config.supabase:
            raise HTTPException(status_code=500, detail="Supabase not configured")
            
        uploaded_files = []
        for local_file in local_files:
            try:
                local_path = Path(local_file)
                if not local_path.exists():
                    continue
                    
                # Calculate relative path from session directory
                relative_path = local_path.relative_to(self.session_dir)
                
                # Read file content
                async with aiofiles.open(local_path, 'rb') as f:
                    file_content = await f.read()
                
                # Upload to Supabase (upsert to overwrite existing)
                config.supabase.storage.from_(config.supabase_bucket).upload(
                    str(relative_path), 
                    file_content,
                    file_options={"upsert": "true"}
                )
                
                uploaded_files.append(str(relative_path))
                print(f"📤 Uploaded: {local_path} -> {relative_path}")
                
            except Exception as e:
                print(f"❌ Failed to upload {local_file}: {e}")
                
        return uploaded_files
        
    def cleanup(self):
        """Clean up session directory"""
        if self.session_dir.exists():
            shutil.rmtree(self.session_dir)
            print(f"🧹 Cleaned up session: {self.session_id}")


# Store active sessions
sessions: Dict[str, AiderSession] = {}


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        version=__version__,
        supabase_connected=config.supabase is not None,
        llm_configured=bool(config.llm_api_base and config.llm_api_key),
        llm_provider=config.provider
    )


@app.head("/v1/chat/completions")
async def chat_completions_head():
    """HEAD endpoint for connection testing - returns 200 if available"""
    return {"status": "available"}

@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest, background_tasks: BackgroundTasks):
    """OpenAI-compatible chat completions endpoint with Aider integration"""
    
    if not request.stream:
        raise HTTPException(status_code=400, detail="Only streaming responses are supported")
    
    # Extract session info and files from the conversation
    session_id = "default"  # You can extract from request metadata if needed
    project_files = []
    user_message = ""
    
    # Get the latest user message
    for msg in reversed(request.messages):
        if msg.role == "user":
            user_message = msg.content
            break
    
    # Create or get session
    if session_id not in sessions:
        sessions[session_id] = AiderSession(session_id, project_files)
    
    session = sessions[session_id]
    
    async def generate_aider_response():
        try:
            # Use the model from request, or fall back to config default
            model_to_use = request.model if request.model != "aider" else config.llm_model
            
            # Set up environment for Aider
            env = os.environ.copy()
            env.update({
                "OPENAI_API_BASE": config.llm_api_base,
                "OPENAI_API_KEY": config.llm_api_key,
                "AIDER_STREAM": "true",
                "AIDER_AUTO_COMMITS": "true",
                "AIDER_PRETTY": "false",
            })
            
            # Prepare aider arguments
            aider_args = [
                "--model", model_to_use,
                "--message", user_message,
                "--no-git",  # Disable git since we're using Supabase
                "--yes-always",  # Don't prompt for confirmations
            ]
            
            # Add downloaded files to aider session
            if session.project_files:
                local_files = await session.download_files_from_supabase(session.project_files)
                aider_args.extend(local_files)
            
            # Change to session directory
            original_cwd = os.getcwd()
            os.chdir(session.session_dir)
            
            # Capture aider output
            import sys
            from io import StringIO
            
            captured_output = StringIO()
            original_stdout = sys.stdout
            
            try:
                # Run aider with captured output
                sys.stdout = captured_output
                
                # This is a simplified version - you might need to adapt based on Aider's actual API
                result = aider_main(aider_args, return_coder=True)
                
                # Get the output
                output = captured_output.getvalue()
                
                # Stream the response in OpenAI format
                lines = output.split('\n')
                for i, line in enumerate(lines):
                    if line.strip():
                        chunk = {
                            "id": f"chatcmpl-{session_id}-{i}",
                            "object": "chat.completion.chunk",
                            "created": int(asyncio.get_event_loop().time()),
                            "model": request.model,
                            "choices": [{
                                "index": 0,
                                "delta": {"content": line + "\n"},
                                "finish_reason": None
                            }]
                        }
                        yield f"data: {json.dumps(chunk)}\n\n"
                        await asyncio.sleep(0.1)  # Small delay for streaming effect
                
                # Final chunk
                final_chunk = {
                    "id": f"chatcmpl-{session_id}-final",
                    "object": "chat.completion.chunk", 
                    "created": int(asyncio.get_event_loop().time()),
                    "model": request.model,
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "stop"
                    }]
                }
                yield f"data: {json.dumps(final_chunk)}\n\n"
                yield "data: [DONE]\n\n"
                
            finally:
                sys.stdout = original_stdout
                os.chdir(original_cwd)
                
                # Upload modified files back to Supabase
                modified_files = list(session.session_dir.rglob("*"))
                modified_files = [str(f) for f in modified_files if f.is_file()]
                if modified_files:
                    background_tasks.add_task(session.upload_files_to_supabase, modified_files)
                
        except Exception as e:
            error_chunk = {
                "error": {
                    "message": f"Aider execution failed: {str(e)}",
                    "type": "aider_error"
                }
            }
            yield f"data: {json.dumps(error_chunk)}\n\n"
    
    return StreamingResponse(
        generate_aider_response(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        }
    )


@app.post("/sessions/{session_id}/files")
async def add_files_to_session(session_id: str, file_paths: List[str]):
    """Add files from Supabase to an Aider session"""
    if session_id not in sessions:
        sessions[session_id] = AiderSession(session_id)
    
    session = sessions[session_id]
    session.project_files.extend(file_paths)
    
    # Download files immediately
    downloaded = await session.download_files_from_supabase(file_paths)
    
    return {
        "session_id": session_id,
        "added_files": file_paths,
        "downloaded_files": downloaded
    }


@app.delete("/sessions/{session_id}")
async def cleanup_session(session_id: str, background_tasks: BackgroundTasks):
    """Clean up a session"""
    if session_id in sessions:
        session = sessions[session_id]
        background_tasks.add_task(session.cleanup)
        del sessions[session_id]
        return {"message": f"Session {session_id} cleaned up"}
    else:
        raise HTTPException(status_code=404, detail="Session not found")


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5005))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    ) 