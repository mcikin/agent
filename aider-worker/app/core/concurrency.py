import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List

from git import Repo


class StaleObjectError(Exception):
    pass


_lock_registry: Dict[str, asyncio.Lock] = {}


def _get_lock(key: str) -> asyncio.Lock:
    lock = _lock_registry.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _lock_registry[key] = lock
    return lock


@asynccontextmanager
async def with_project_lock(project_id) -> AsyncIterator[None]:
    key = str(project_id)
    lock = _get_lock(key)
    await lock.acquire()
    try:
        yield
    finally:
        lock.release()


def _read_preview(path: Path, max_lines: int = 40) -> str:
    if not path.exists() or not path.is_file():
        return ""
    lines: List[str] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for _ in range(max_lines):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
    except Exception:
        return ""
    return "".join(lines)


async def handle_conflict(repo: Repo, project_id) -> Dict[str, Any]:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    branch = f"conflict/{ts}"
    try:
        repo.git.checkout("-b", branch)
    except Exception:
        # If branch exists, reuse it
        repo.git.checkout(branch)

    # Collect changed or conflicted paths from porcelain output
    try:
        status = repo.git.status("--porcelain")
    except Exception:
        status = ""

    paths: List[str] = []
    for line in status.splitlines():
        line = line.rstrip()
        if not line:
            continue
        if len(line) > 3:
            code = line[:2]
            p = line[3:]
            # Prioritize merge conflicts (UU, AA, etc.), otherwise include modified/added
            if "U" in code or code.strip() or code != "??":
                paths.append(p)

    files: List[Dict[str, Any]] = []
    root = Path(repo.working_tree_dir or ".")
    for rel in sorted(set(paths)):
        mine_preview = _read_preview(root / rel)
        files.append({"path": rel, "mine": mine_preview, "theirs": ""})

    return {
        "project_id": str(project_id),
        "branch": branch,
        "files": files,
    }







