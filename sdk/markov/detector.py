"""Divergence detection: temporal, scope, and volume — regex only (no LLM)."""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional


@dataclass
class Flag:
    type: str  # temporal | scope | volume
    object_key: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "object_key": self.object_key, "reason": self.reason}


def _parse_iso(dt: str) -> datetime:
    s = dt.replace("Z", "+00:00")
    d = datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d


def _fmt_relative_days(days: float) -> str:
    if days < 1:
        hours = int(days * 24)
        return f"{hours} hour(s) ago" if hours else "less than a day ago"
    d = int(round(days))
    return f"{d} day(s) ago"


def parse_temporal_window(
    task_context: str, reference: Optional[datetime] = None
) -> Optional[tuple[datetime, str]]:
    """
    Returns (cutoff_utc, description) where objects with last_modified AFTER cutoff
    violate an 'older than N days' style constraint.
    If task specifies 'before DATE', cutoff semantics differ — see branches.
    """
    ref = reference or datetime.now(timezone.utc)
    text = task_context.lower()

    m = re.search(r"older\s+than\s+(\d+)\s*days?", text)
    if m:
        n = int(m.group(1))
        cutoff = ref - timedelta(days=n)
        return (cutoff, f"older than {n} days")

    m = re.search(r"older\s+than\s+(\d+)\s*weeks?", text)
    if m:
        n = int(m.group(1))
        cutoff = ref - timedelta(weeks=n)
        return (cutoff, f"older than {n} week(s)")

    m = re.search(r"older\s+than\s+(\d+)\s*months?", text)
    if m:
        n = int(m.group(1))
        cutoff = ref - timedelta(days=30 * n)
        return (cutoff, f"older than {n} month(s)")

    if re.search(r"from\s+last\s+week", text):
        cutoff = ref - timedelta(days=7)
        return (cutoff, "from last week")

    m = re.search(r"before\s+(\d{4})-(\d{2})-(\d{2})", task_context)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        cutoff = datetime(y, mo, d, tzinfo=timezone.utc)
        return (cutoff, f"before {y}-{mo:02d}-{d:02d}")

    return None


def extract_scope_prefixes(task_context: str) -> list[str]:
    """Return normalized key prefixes (e.g. tmp/) that objects must stay under."""
    found: list[str] = []

    for m in re.finditer(r"s3://[\w.-]+/([^\s\"']+)", task_context):
        p = m.group(1).strip()
        if p and not p.endswith("/"):
            p = p + "/"
        if p:
            found.append(p)

    for m in re.finditer(r"['\"]([a-zA-Z0-9_./-]+/)['\"]", task_context):
        p = m.group(1)
        if "/" in p or p.endswith("/"):
            if not p.endswith("/"):
                p = p + "/"
            found.append(p)

    for m in re.finditer(r"\b([a-zA-Z0-9_-]+/)(?:\s|$|[,.)])", task_context):
        p = m.group(1)
        if len(p) > 1:
            found.append(p)

    seen: set[str] = set()
    out: list[str] = []
    for p in found:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def check_temporal(
    task_context: str, object_key: str, last_modified_iso: str
) -> list[Flag]:
    window = parse_temporal_window(task_context)
    if not window:
        return []
    cutoff, desc = window
    lm = _parse_iso(last_modified_iso)
    if lm.tzinfo is None:
        lm = lm.replace(tzinfo=timezone.utc)
    ref = datetime.now(timezone.utc)
    delta = ref - lm
    days_old = delta.total_seconds() / 86400.0

    if desc.startswith("before "):
        if lm >= cutoff:
            return [
                Flag(
                    type="temporal",
                    object_key=object_key,
                    reason=(
                        f"Task specified '{desc}' — this object was last modified "
                        f"{lm.date().isoformat()}, which is not before the cutoff."
                    ),
                )
            ]
        return []

    if lm > cutoff:
        return [
            Flag(
                type="temporal",
                object_key=object_key,
                reason=(
                    f"Task specified '{desc}' — this object was last modified "
                    f"{_fmt_relative_days(days_old)}."
                ),
            )
        ]
    return []


def check_scope(task_context: str, object_key: str) -> list[Flag]:
    prefixes = extract_scope_prefixes(task_context)
    if not prefixes:
        return []
    if any(object_key.startswith(p) for p in prefixes):
        return []
    top = object_key.split("/", 1)[0] + "/" if "/" in object_key else f"{object_key}/"
    shown = prefixes[0] if len(prefixes) == 1 else ", ".join(f"'{p}'" for p in prefixes)
    return [
        Flag(
            type="scope",
            object_key=object_key,
            reason=(f"Task specified prefix {shown} — this object was in '{top}'"),
        )
    ]


def check_volume(
    task_context: str,
    object_key: str,
    current_count: int,
    prior_counts: list[int],
) -> list[Flag]:
    if len(prior_counts) < 1:
        return []
    mean = statistics.mean(prior_counts)
    if len(prior_counts) >= 2:
        std = statistics.stdev(prior_counts)
    else:
        std = 0.0
    std_eff = max(std, 1e-9)
    if current_count <= mean + 3 * std_eff:
        return []
    return [
        Flag(
            type="volume",
            object_key="",
            reason=(
                f"Agent deleted {current_count} objects — prior runs averaged "
                f"{mean:.0f}."
            ),
        )
    ]


def merge_flags(*groups: list[Flag]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for g in groups:
        for f in g:
            t = (f.type, f.object_key, f.reason)
            if t not in seen:
                seen.add(t)
                out.append(f.to_dict())
    return out


def divergence_score_from_objects(
    object_flag_lists: list[list[dict[str, Any]]],
    total_objects: int,
    volume_triggered: bool,
) -> float:
    if total_objects == 0:
        return 0.0
    flagged = 0
    for fl in object_flag_lists:
        has_obj_flag = any(
            x.get("type") in ("temporal", "scope") for x in fl
        )
        if has_obj_flag:
            flagged += 1
    if volume_triggered:
        flagged = total_objects
    return flagged / total_objects
