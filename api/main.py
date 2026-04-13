"""FastAPI read API for Markov audit data."""

from __future__ import annotations

import os
import sys
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Allow running without editable install when repo root is on PYTHONPATH
_SDK = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sdk"))
if _SDK not in sys.path:
    sys.path.insert(0, _SDK)

from markov.storage import default_db_path, get_storage

app = FastAPI(title="Markov API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _storage():
    raw = os.environ.get("MARKOV_DB_PATH", default_db_path())
    path = (raw or "").strip() or default_db_path()
    return get_storage(path)


@app.get("/executions")
def list_executions() -> list[dict[str, Any]]:
    rows = _storage().list_executions_for_api()
    return [
        {
            "execution_id": r["execution_id"],
            "agent_id": r["agent_id"],
            "task_context": r["task_context"],
            "timestamp": r["timestamp"],
            "divergence_score": r["divergence_score"],
            "object_count": r["object_count"],
        }
        for r in rows
    ]


@app.get("/executions/{execution_id}")
def get_execution(execution_id: str) -> dict[str, Any]:
    row = _storage().get_execution_row(execution_id)
    if not row:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {
        "execution_id": row["execution_id"],
        "agent_id": row["agent_id"],
        "task_context": row["task_context"],
        "timestamp": row["timestamp"],
        "divergence_score": row["divergence_score"],
        "divergence_flags": row.get("aggregated_divergence_flags", []),
    }


@app.get("/executions/{execution_id}/objects")
def list_objects(execution_id: str) -> list[dict[str, Any]]:
    s = _storage()
    if not s.get_execution_row(execution_id):
        raise HTTPException(status_code=404, detail="Execution not found")
    return s.list_object_actions_for_api(execution_id)
