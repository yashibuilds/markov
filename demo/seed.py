#!/usr/bin/env python3
"""ETL cleanup demo: seed mock S3, run audited deletes, print audit summary."""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from collections import Counter

import boto3
from freezegun import freeze_time
from moto import mock_aws

_SDK = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sdk"))
if _SDK not in sys.path:
    sys.path.insert(0, _SDK)

from markov.client import AuditedS3Client  # noqa: E402

BUCKET = "data-pipeline-demo"
TASK = (
    "Remove failed ETL pipeline artifacts older than 7 days from "
    f"s3://{BUCKET}/tmp/"
)


def main() -> None:
    # Fixed path under the system temp dir so the API can reuse MARKOV_DB_PATH easily.
    db_path = os.path.join(tempfile.gettempdir(), "markov-demo.db")
    try:
        os.remove(db_path)
    except OSError:
        pass
    os.environ["MARKOV_DB_PATH"] = db_path

    execution_id = str(uuid.uuid4())

    with mock_aws():
        raw = boto3.client("s3", region_name="us-east-1")
        raw.create_bucket(Bucket=BUCKET)

        with freeze_time("2026-03-01 12:00:00"):
            for i in range(40):
                raw.put_object(
                    Bucket=BUCKET,
                    Key=f"tmp/clean-old-{i:03d}.parquet",
                    Body=b"ok",
                )

        with freeze_time("2026-04-11 10:00:00"):
            for i in range(8):
                raw.put_object(
                    Bucket=BUCKET,
                    Key=f"tmp/failed-run-old-{i:03d}.parquet",
                    Body=b"stale-name",
                )

        with freeze_time("2026-02-01 12:00:00"):
            for i in range(2):
                raw.put_object(
                    Bucket=BUCKET,
                    Key=f"config/pipeline-critical-{i}.yml",
                    Body=b"cfg",
                )

        keys: list[str] = []
        paginator = raw.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET):
            for obj in page.get("Contents") or []:
                keys.append(obj["Key"])

        with freeze_time("2026-04-12 12:00:00"):
            s3 = AuditedS3Client(
                TASK,
                execution_id,
                agent_id="etl-cleanup-agent",
                db_path=db_path,
                region_name="us-east-1",
            )

            batch_size = 1000
            for i in range(0, len(keys), batch_size):
                chunk = keys[i : i + batch_size]
                s3.delete_objects(
                    Bucket=BUCKET,
                    Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
                )

            s3.finalize()

    # Summarize from DB (same file the API would read)
    from markov.storage import get_storage  # noqa: E402

    store = get_storage(db_path)
    rows = store.list_object_actions_for_api(execution_id)
    real = [r for r in rows if r["action"] in ("delete", "delete_objects")]
    vol_rows = [r for r in rows if r["key"] == "__markov__/volume"]

    type_counts: Counter[str] = Counter()
    flagged_objects = 0
    for r in real:
        flags = r.get("divergence_flags") or []
        if flags:
            flagged_objects += 1
        for f in flags:
            if f.get("type"):
                type_counts[f["type"]] += 1

    if vol_rows:
        type_counts["volume"] += len(vol_rows)

    print("Markov ETL cleanup demo — summary")
    print(f"  Database: {db_path}")
    print(f"  execution_id: {execution_id}")
    print(f"  Total objects acted on: {len(real)}")
    print(f"  Total flagged (non-volume rows with flags): {flagged_objects}")
    print("  Breakdown by flag type (individual flags):")
    for t in ("temporal", "scope", "volume"):
        c = type_counts.get(t, 0)
        if c:
            print(f"    {t}: {c}")
    if not any(type_counts.values()):
        print("    (none)")
    print()
    print(f"  Set MARKOV_DB_PATH={db_path} when starting the API to browse this run.")


if __name__ == "__main__":
    main()
