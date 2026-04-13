"""Audited S3 client — drop-in wrapper around boto3 with intent auditing."""

from __future__ import annotations

import atexit
import json
import os
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError

from .detector import (
    check_scope,
    check_temporal,
    check_volume,
    divergence_score_from_objects,
    merge_flags,
)
from .storage import SQLiteStorage, default_db_path, get_storage


def _serialize_head(head: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in head.items():
        if k == "Body":
            continue
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat().replace("+00:00", "Z")
        elif isinstance(v, (bytes, bytearray)):
            out[k] = "<binary>"
        else:
            try:
                json.dumps(v)
                out[k] = v
            except TypeError:
                out[k] = repr(v)
    return out


class AuditedS3Client:
    """
    Same surface as boto3.client('s3') for non-intercepted calls.
    Intercepts delete_object and delete_objects to snapshot metadata and persist audits.
    """

    def __init__(
        self,
        task_context: str,
        execution_id: str,
        *,
        agent_id: str = "agent",
        db_path: Optional[str] = None,
        **client_kwargs: Any,
    ) -> None:
        self._task_context = task_context
        self._execution_id = execution_id
        self._agent_id = agent_id
        self._db_path = db_path or os.environ.get("MARKOV_DB_PATH") or default_db_path()
        self._storage: SQLiteStorage = get_storage(self._db_path)
        self._storage.upsert_execution(execution_id, agent_id, task_context)
        self._s3 = boto3.client("s3", **client_kwargs)
        self._finalized = False
        atexit.register(self._finalize)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._s3, name)

    def delete_object(self, **kwargs: Any) -> Any:
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        head: dict[str, Any] = {}
        last_modified = ""
        size_bytes = 0
        content_type = ""
        try:
            head = self._s3.head_object(Bucket=bucket, Key=key)
            lm = head.get("LastModified")
            if lm is not None:
                last_modified = lm.isoformat().replace("+00:00", "Z")
            size_bytes = int(head.get("ContentLength", 0))
            content_type = str(head.get("ContentType") or "")
            head = dict(head)
        except ClientError:
            head = {}

        result = self._s3.delete_object(**kwargs)

        flags = merge_flags(
            check_temporal(self._task_context, key, last_modified),
            check_scope(self._task_context, key),
        )

        self._storage.insert_object_action(
            execution_id=self._execution_id,
            action="delete",
            bucket=bucket,
            key=key,
            size_bytes=size_bytes,
            last_modified=last_modified or "1970-01-01T00:00:00Z",
            content_type=content_type or "application/octet-stream",
            metadata_snapshot=_serialize_head(head) if head else {},
            divergence_flags=flags,
        )
        return result

    def delete_objects(self, **kwargs: Any) -> Any:
        bucket = kwargs["Bucket"]
        delete_spec = kwargs.get("Delete") or {}
        objects = list(delete_spec.get("Objects") or [])

        snapshots: list[
            tuple[str, dict[str, Any], str, int, str]
        ] = []  # key, head_raw, last_mod, size, ctype

        for obj in objects:
            key = obj["Key"]
            head: dict[str, Any] = {}
            last_modified = ""
            size_bytes = 0
            content_type = ""
            try:
                head = self._s3.head_object(Bucket=bucket, Key=key)
                lm = head.get("LastModified")
                if lm is not None:
                    last_modified = lm.isoformat().replace("+00:00", "Z")
                size_bytes = int(head.get("ContentLength", 0))
                content_type = str(head.get("ContentType") or "")
                head = dict(head)
            except ClientError:
                head = {}

            snapshots.append(
                (key, head, last_modified, size_bytes, content_type or "application/octet-stream")
            )

        result = self._s3.delete_objects(**kwargs)

        for key, head, last_modified, size_bytes, content_type in snapshots:
            flags = merge_flags(
                check_temporal(self._task_context, key, last_modified),
                check_scope(self._task_context, key),
            )
            self._storage.insert_object_action(
                execution_id=self._execution_id,
                action="delete_objects",
                bucket=bucket,
                key=key,
                size_bytes=size_bytes,
                last_modified=last_modified or "1970-01-01T00:00:00Z",
                content_type=content_type,
                metadata_snapshot=_serialize_head(head) if head else {},
                divergence_flags=flags,
            )

        return result

    def finalize(self) -> None:
        """Compute volume anomaly, divergence score, and persist execution summary."""
        self._finalize()

    def _finalize(self) -> None:
        if self._finalized:
            return
        self._finalized = True

        self._storage.delete_volume_placeholder(self._execution_id)

        n = self._storage.count_objects_for_execution(self._execution_id)
        priors = self._storage.prior_object_counts_same_task(
            self._task_context, self._execution_id
        )
        vflags = check_volume(self._task_context, "", n, priors)

        if vflags:
            vf = vflags[0].to_dict()
            self._storage.insert_object_action(
                execution_id=self._execution_id,
                action="volume_anomaly",
                bucket="",
                key="__markov__/volume",
                size_bytes=0,
                last_modified="1970-01-01T00:00:00Z",
                content_type="application/octet-stream",
                metadata_snapshot={},
                divergence_flags=[vf],
            )

        raw = self._storage.fetch_object_actions_raw(self._execution_id)
        delete_rows = [
            r
            for r in raw
            if r["action"] in ("delete", "delete_objects")
        ]
        volume_triggered = any(r["key"] == "__markov__/volume" for r in raw)

        flag_lists: list[list[dict[str, Any]]] = []
        for r in delete_rows:
            flag_lists.append(json.loads(r["divergence_flags"]))

        score = divergence_score_from_objects(
            flag_lists, len(delete_rows), volume_triggered
        )
        self._storage.update_execution_divergence_score(self._execution_id, score)
