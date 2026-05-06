from __future__ import annotations

import os
from typing import Any, Dict, Optional
from uuid import uuid4


def env(name: str, default: Optional[str] = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(f"missing env {name}")
    return val


def new_root_envelope(agent: str, payload: Dict[str, Any], hop: int = 0) -> Dict[str, Any]:
    corr = str(uuid4())
    return {
        "task_id": str(uuid4()),
        "correlation_id": corr,
        "agent": agent,
        "schema_version": "1",
        "hop": hop,
        "payload": payload,
    }


def child_envelope(parent: Dict[str, Any], agent: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    hop = int(parent.get("hop", 0)) + 1
    corr = parent.get("correlation_id") or str(uuid4())
    return {
        "task_id": str(uuid4()),
        "correlation_id": corr,
        "parent_task_id": parent.get("task_id"),
        "agent": agent,
        "schema_version": "1",
        "hop": hop,
        "payload": payload,
    }
