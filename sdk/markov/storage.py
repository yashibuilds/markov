"""SQLite storage adapter behind an abstract interface."""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class MarkovStorage(ABC):
    """Abstract storage for Markov audit records."""

    @abstractmethod
    def upsert_execution(
        self,
        execution_id: str,
        agent_id: str,
        task_context: str,
        timestamp: Optional[str] = None,
        divergence_score: Optional[float] = None,
    ) -> None:
        pass

    @abstractmethod
    def insert_object_action(
        self,
        execution_id: str,
        action: str,
        bucket: str,
        key: str,
        size_bytes: int,
        last_modified: str,
        content_type: str,
        metadata_snapshot: dict[str, Any],
        divergence_flags: list[dict[str, Any]],
    ) -> int:
        pass

    @abstractmethod
    def update_execution_divergence_score(
        self, execution_id: str, divergence_score: float
    ) -> None:
        pass


class SQLiteStorage(MarkovStorage):
    """SQLite implementation."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS executions (
                    execution_id TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL,
                    task_context TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    divergence_score REAL
                );

                CREATE TABLE IF NOT EXISTS object_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    last_modified TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    metadata_snapshot TEXT NOT NULL,
                    divergence_flags TEXT NOT NULL,
                    FOREIGN KEY (execution_id) REFERENCES executions(execution_id)
                );

                CREATE INDEX IF NOT EXISTS idx_object_actions_execution
                ON object_actions(execution_id);
                """
            )

    def upsert_execution(
        self,
        execution_id: str,
        agent_id: str,
        task_context: str,
        timestamp: Optional[str] = None,
        divergence_score: Optional[float] = None,
    ) -> None:
        ts = timestamp or _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO executions (execution_id, agent_id, task_context, timestamp, divergence_score)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(execution_id) DO UPDATE SET
                    agent_id = excluded.agent_id,
                    task_context = excluded.task_context,
                    timestamp = excluded.timestamp,
                    divergence_score = COALESCE(excluded.divergence_score, executions.divergence_score)
                """,
                (execution_id, agent_id, task_context, ts, divergence_score),
            )

    def insert_object_action(
        self,
        execution_id: str,
        action: str,
        bucket: str,
        key: str,
        size_bytes: int,
        last_modified: str,
        content_type: str,
        metadata_snapshot: dict[str, Any],
        divergence_flags: list[dict[str, Any]],
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO object_actions (
                    execution_id, action, bucket, key, size_bytes, last_modified,
                    content_type, metadata_snapshot, divergence_flags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    execution_id,
                    action,
                    bucket,
                    key,
                    size_bytes,
                    last_modified,
                    content_type,
                    json.dumps(metadata_snapshot),
                    json.dumps(divergence_flags),
                ),
            )
            return int(cur.lastrowid)

    def update_execution_divergence_score(
        self, execution_id: str, divergence_score: float
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE executions SET divergence_score = ? WHERE execution_id = ?",
                (divergence_score, execution_id),
            )

    # --- Reads (used by API and client finalize) ---

    def count_objects_for_execution(self, execution_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM object_actions
                WHERE execution_id = ? AND action IN ('delete', 'delete_objects')
                """,
                (execution_id,),
            ).fetchone()
            return int(row[0]) if row else 0

    def prior_object_counts_same_task(
        self, task_context: str, exclude_execution_id: str
    ) -> list[int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT e.execution_id, COUNT(o.id) AS cnt
                FROM executions e
                LEFT JOIN object_actions o ON o.execution_id = e.execution_id
                  AND o.action IN ('delete', 'delete_objects')
                WHERE e.task_context = ? AND e.execution_id != ?
                GROUP BY e.execution_id
                """,
                (task_context, exclude_execution_id),
            ).fetchall()
        return [int(r[1]) for r in rows]

    def fetch_object_actions_raw(self, execution_id: str) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM object_actions WHERE execution_id = ? ORDER BY id ASC",
                    (execution_id,),
                ).fetchall()
            )

    def delete_volume_placeholder(self, execution_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM object_actions
                WHERE execution_id = ? AND key = '__markov__/volume'
                """,
                (execution_id,),
            )

    def list_executions_for_api(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT e.execution_id, e.agent_id, e.task_context, e.timestamp,
                       e.divergence_score,
                       COUNT(
                         CASE WHEN o.action IN ('delete', 'delete_objects') THEN 1 END
                       ) AS object_count
                FROM executions e
                LEFT JOIN object_actions o ON o.execution_id = e.execution_id
                GROUP BY e.execution_id
                ORDER BY (CASE WHEN COALESCE(e.divergence_score, 0) > 0 THEN 0 ELSE 1 END),
                         e.timestamp DESC
                """
            ).fetchall()
        return [
            {
                "execution_id": r[0],
                "agent_id": r[1],
                "task_context": r[2],
                "timestamp": r[3],
                "divergence_score": r[4],
                "object_count": int(r[5]),
            }
            for r in rows
        ]

    def get_execution_row(self, execution_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM executions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        with self._connect() as conn:
            flags = conn.execute(
                """
                SELECT divergence_flags FROM object_actions
                WHERE execution_id = ? AND divergence_flags IS NOT NULL
                  AND divergence_flags != '[]'
                """,
                (execution_id,),
            ).fetchall()
        agg: list[dict[str, Any]] = []
        seen: set[tuple[Any, Any, Any]] = set()
        for (fj,) in flags:
            for item in json.loads(fj):
                k = (item.get("type"), item.get("object_key"), item.get("reason"))
                if k not in seen:
                    seen.add(k)
                    agg.append(item)
        d["aggregated_divergence_flags"] = agg
        return d

    def list_object_actions_for_api(self, execution_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM object_actions WHERE execution_id = ?
                ORDER BY
                  CASE WHEN divergence_flags IS NULL OR divergence_flags = '[]' THEN 1 ELSE 0 END,
                  id ASC
                """,
                (execution_id,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r["id"],
                    "execution_id": r["execution_id"],
                    "action": r["action"],
                    "bucket": r["bucket"],
                    "key": r["key"],
                    "size_bytes": r["size_bytes"],
                    "last_modified": r["last_modified"],
                    "content_type": r["content_type"],
                    "metadata_snapshot": json.loads(r["metadata_snapshot"]),
                    "divergence_flags": json.loads(r["divergence_flags"]),
                }
            )
        return out


def default_db_path() -> str:
    return os.environ.get("MARKOV_DB_PATH", os.path.join(os.getcwd(), "markov.db"))


def get_storage(db_path: Optional[str] = None) -> SQLiteStorage:
    return SQLiteStorage(db_path or default_db_path())


def new_execution_id() -> str:
    return str(uuid.uuid4())
