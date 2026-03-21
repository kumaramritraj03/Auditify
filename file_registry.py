"""
File Registry — persistent store for all uploaded/downloaded files.

Stores: file_id, original_name, local_path, upload_time, source (upload|url|gdrive)
Registry is a single JSON file: uploads/file_registry.json
"""

import json
import os
import uuid
from datetime import datetime, timezone

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_UPLOAD_DIR = os.path.join(_BASE_DIR, "uploads")
_REGISTRY_PATH = os.path.join(_UPLOAD_DIR, "file_registry.json")

os.makedirs(_UPLOAD_DIR, exist_ok=True)


def _load() -> list:
    if not os.path.exists(_REGISTRY_PATH):
        return []
    try:
        with open(_REGISTRY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def _save(entries: list):
    with open(_REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2)


def register_file(
    file_id: str,
    original_name: str,
    local_path: str,
    source: str = "upload",
) -> dict:
    """Add a file to the registry. Returns the registry entry."""
    entry = {
        "file_id": file_id,
        "original_name": original_name,
        "local_path": local_path,
        "upload_time": datetime.now(timezone.utc).isoformat(),
        "source": source,  # "upload" | "url" | "gdrive"
    }
    entries = _load()
    # Avoid duplicates by file_id
    entries = [e for e in entries if e.get("file_id") != file_id]
    entries.append(entry)
    _save(entries)
    return entry


def get_all_files() -> list:
    """Return all registry entries, newest first."""
    entries = _load()
    # Filter to only files that still exist on disk
    alive = [e for e in entries if os.path.exists(e.get("local_path", ""))]
    if len(alive) != len(entries):
        _save(alive)
    return list(reversed(alive))


def get_file(file_id: str) -> dict | None:
    """Look up a single entry by file_id."""
    for e in _load():
        if e.get("file_id") == file_id:
            return e
    return None


def delete_file(file_id: str) -> bool:
    """Remove from registry (does NOT delete the actual file)."""
    entries = _load()
    new_entries = [e for e in entries if e.get("file_id") != file_id]
    if len(new_entries) == len(entries):
        return False
    _save(new_entries)
    return True
