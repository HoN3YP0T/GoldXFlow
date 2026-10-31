"""Filesystem/database helpers that work on restricted mounts (e.g. a Windows folder shared into a Linux VM
where unlink is not permitted and SQLite WAL/DELETE journals fail).

GOLDFLOW_SQLITE_JOURNAL  (env) SQLite journal mode: WAL (default) or PERSIST for such mounts.
"""
from __future__ import annotations
import os, sqlite3, time
from pathlib import Path

def journal_mode() -> str:
    return os.environ.get("GOLDFLOW_SQLITE_JOURNAL", "WAL").upper()

def connect(path, **kw) -> sqlite3.Connection:
    db = sqlite3.connect(str(path), **kw)
    db.execute(f"PRAGMA journal_mode={journal_mode()}")
    db.execute("PRAGMA synchronous=FULL")
    return db

def safe_remove(path: Path) -> str:
    """Delete a file; where deletion is not permitted, move it into <data_root>/_to_delete/ instead.
    Never leaves the file at its original path (the caller relies on that)."""
    path = Path(path)
    if not path.exists(): return "absent"
    try:
        path.unlink(); return "deleted"
    except PermissionError:
        root = next((p for p in path.parents if p.name == "data"), path.parent)
        trash = root / "_to_delete"; trash.mkdir(exist_ok=True)
        os.replace(path, trash / f"{time.strftime('%Y%m%dT%H%M%S')}-{time.monotonic_ns()}-{path.name}")
        return "moved_to__to_delete"
