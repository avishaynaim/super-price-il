"""Tiny key/value config store for runtime-tunable settings.

Settings live in `data/settings.json` so the CLI, the prune job, and the API
all see the same value. Only one knob today (`retention_days`) but the file
shape is open for future ones (refresh hour, default city, etc.).

Why a JSON file and not a SQL table: prune.py runs in a separate process from
the FastAPI server, and SQLite WAL writers from two processes can stall behind
each other during a long scrape. A flat file is racey only on writes (rare,
human-driven) and reads are atomic.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

SETTINGS_PATH = Path(__file__).resolve().parents[2] / "data" / "settings.json"

DEFAULTS: dict[str, Any] = {
    "retention_days": 7,
}


def load() -> dict[str, Any]:
    if not SETTINGS_PATH.exists():
        return dict(DEFAULTS)
    try:
        data = json.loads(SETTINGS_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return dict(DEFAULTS)
    out = dict(DEFAULTS)
    out.update({k: v for k, v in data.items() if k in DEFAULTS})
    return out


def save(settings: dict[str, Any]) -> dict[str, Any]:
    """Atomically write settings: write to a temp file in the same directory
    then rename. Validates and clamps known keys before persisting."""
    merged = load()
    if "retention_days" in settings:
        try:
            n = int(settings["retention_days"])
        except (TypeError, ValueError):
            n = merged["retention_days"]
        merged["retention_days"] = max(1, min(90, n))

    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(SETTINGS_PATH.parent), prefix="settings.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        os.replace(tmp, SETTINGS_PATH)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return merged


def get(key: str) -> Any:
    return load().get(key, DEFAULTS.get(key))
