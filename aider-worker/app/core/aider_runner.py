import asyncio
import os
from pathlib import Path
from typing import AsyncGenerator, Dict, Set


MAX_PREVIEW_LINES = 40


async def _preview_first_lines(path: Path, max_lines: int = MAX_PREVIEW_LINES) -> str:
    try:
        if not path.exists() or not path.is_file():
            return ""
        lines = []
        # Use text read with utf-8 fallback
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for _ in range(max_lines):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
        return "".join(lines)
    except Exception:
        return ""


async def _git_changed_paths(repo_path: Path) -> Set[str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            "git",
            "status",
            "--porcelain",
            cwd=str(repo_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        changed: Set[str] = set()
        for raw in stdout.decode(errors="replace").splitlines():
            raw = raw.strip()
            if not raw:
                continue
            # format: XY<space>path
            if len(raw) > 3:
                changed.add(raw[3:])
        return changed
    except Exception:
        return set()


async def stream_edit(repo_path: str, prompt: str) -> AsyncGenerator[Dict, None]:
    """
    Spawn Aider and stream its stdout. Yields events:
      - {"type":"token","text":"..."}
      - {"type":"file_changed","path": str, "preview": str}

    Runner does not commit; it only edits files in place.
    """
    workdir = Path(repo_path)
    workdir.mkdir(parents=True, exist_ok=True)

    aider_bin = os.getenv("AIDER_BIN", "/usr/local/bin/aider")

    # Prefer no git commits; --message sends a single-turn prompt
    # --yes-always to avoid interactive confirmations; --pretty false to reduce noise
    cmd = [
        aider_bin,
        "--no-git",
        "--yes-always",
        "--pretty",
        "false",
        "--message",
        prompt,
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(workdir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    seen_changes: Set[str] = set()

    # Read stdout line by line
    assert proc.stdout is not None
    while True:
        raw = await proc.stdout.readline()
        if not raw:
            break
        text = raw.decode(errors="replace")
        
        # Emit token chunks as-is
        yield {"type": "token", "text": text}

        # Detect known edit markers printed by aider
        lowered = text.lower()
        markers = [
            "applied edit to ",
            "creating empty file ",
            "wrote changes to ",
            "updated file ",
        ]
        for m in markers:
            if m in lowered:
                # Grab the tail after the marker as a path guess
                try:
                    frag = text[lowered.index(m) + len(m) :].strip()
                    # Trim trailing punctuation
                    frag = frag.strip(" .\n\r")
                    # Only relative paths within repo
                    rel = frag
                    p = (workdir / rel).resolve()
                    if str(p).startswith(str(workdir.resolve())) and p.exists():
                        if rel not in seen_changes:
                            seen_changes.add(rel)
                            preview = await _preview_first_lines(p)
                            yield {
                                "type": "file_changed",
                                "path": rel,
                                "preview": preview,
                            }
                except Exception:
                    # Ignore parsing errors
                    pass
                break

    # Wait for process to exit
    await proc.wait()

    # Fallback: if no changes detected, check git status
    if not seen_changes:
        fallback = await _git_changed_paths(workdir)
        for rel in sorted(fallback):
            p = (workdir / rel)
            preview = await _preview_first_lines(p)
            yield {"type": "file_changed", "path": rel, "preview": preview}






