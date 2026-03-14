"""JSON-based state tracking for resume support."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any


STATE_FILE = Path(__file__).parent / ".state.json"

# Status progression: downloaded → converted → extracted → pushed
STATUS_ORDER = ["downloaded", "converted", "extracted", "pushed"]


def load_state() -> dict[str, dict[str, Any]]:
    """Load the state file. Returns empty dict if not found."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_state(state: dict[str, dict[str, Any]]) -> None:
    """Save state to disk."""
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def mark(state: dict, url: str, status: str, **extra: Any) -> None:
    """Mark a URL with a status and optional extra data."""
    if url not in state:
        state[url] = {}
    state[url]["status"] = status
    state[url]["updated_at"] = datetime.now().isoformat()
    for k, v in extra.items():
        state[url][k] = v
    save_state(state)


def get_by_status(state: dict, status: str) -> list[str]:
    """Get all URLs at a given status."""
    return [url for url, info in state.items() if info.get("status") == status]


def get_at_least(state: dict, min_status: str) -> list[str]:
    """Get URLs at min_status or beyond in the pipeline."""
    min_idx = STATUS_ORDER.index(min_status)
    return [
        url
        for url, info in state.items()
        if info.get("status") in STATUS_ORDER
        and STATUS_ORDER.index(info["status"]) >= min_idx
    ]


def summary(state: dict) -> dict[str, int]:
    """Count documents by status."""
    counts: dict[str, int] = {}
    for info in state.values():
        s = info.get("status", "unknown")
        counts[s] = counts.get(s, 0) + 1
    return counts


def reset_state() -> None:
    """Delete the state file."""
    if STATE_FILE.exists():
        STATE_FILE.unlink()
