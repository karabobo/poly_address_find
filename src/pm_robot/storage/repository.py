"""Persistence boundaries for the L0-L6 wallet research pipeline."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import fields
from typing import Any

from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.pipeline_terms import ACTIVE_PIPELINE_JOB_TYPES
from pm_robot.storage.db import retry_sqlite_locked

_FEATURE_TEXT_FIELDS = {"hygiene_status", "primary_category"}
PIPELINE_TERMINAL_FAILED_STATUS = "terminal_failed"
_FEATURE_FIELDS = tuple(
    field.name
    for field in fields(WalletFeatures)
    if field.name not in {"address", "extra"}
)


def upsert_candidate(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    now: int | None = None,
    preloaded_existing: CandidateAddress | None = None,
    existing_preloaded: bool = False,
) -> None:
    """Merge one qualified candidate and record its source provenance."""

    ts = int(time.time()) if now is None else int(now)
    address = candidate.address.strip().lower()
    existing = preloaded_existing
    if not existing_preloaded:
        row = conn.execute(
            "SELECT sources, labels, notes, links FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()
        existing = (
            CandidateAddress(
                address=address,
                sources=str(row["sources"] or ""),
                labels=str(row["labels"] or ""),
                notes=str(row["notes"] or ""),
                links=str(row["links"] or ""),
            )
            if row is not None
            else None
        )
    sources = candidate.sources
    labels = candidate.labels
    notes = candidate.notes
    links = candidate.links
    if existing is not None:
        sources = _merge_text(existing.sources, candidate.sources)
        labels = _merge_text(existing.labels, candidate.labels)
        notes = _merge_text(existing.notes, candidate.notes)
        links = _merge_text(existing.links, candidate.links)
    conn.execute(
        """
        INSERT INTO candidate_wallets(
            address, sources, labels, notes, links, status,
            first_seen_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            sources = excluded.sources,
            labels = excluded.labels,
            notes = excluded.notes,
            links = excluded.links,
            status = excluded.status,
            updated_at = excluded.updated_at
        """,
        (address, sources, labels, notes, links, candidate.status, ts, ts),
    )
    record_candidate_source_event(
        conn,
        CandidateAddress(
            address=address,
            sources=candidate.sources,
            labels=candidate.labels,
            notes=candidate.notes,
            links=candidate.links,
            status=candidate.status,
        ),
        observed_at=ts,
        recorded_at=ts,
    )


def upsert_candidates(
    conn: sqlite3.Connection,
    candidates: list[CandidateAddress],
) -> int:
    """Persist a curated candidate batch as one transaction."""

    for candidate in candidates:
        upsert_candidate(conn, candidate)
    conn.commit()
    return len(candidates)


def record_candidate_source_event(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    observed_at: int,
    recorded_at: int | None = None,
    evidence: dict[str, Any] | None = None,
) -> None:
    """Keep one bounded provenance summary per wallet and source."""

    address = candidate.address.strip().lower()
    recorded = int(time.time()) if recorded_at is None else int(recorded_at)
    evidence_json = _json_dump(evidence or {})
    conn.execute(
        """
        INSERT INTO candidate_source_events(
            address, source, status, labels, notes, links,
            evidence_json, observed_at, recorded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(address, source) DO UPDATE SET
            status = CASE
                WHEN excluded.recorded_at >= candidate_source_events.recorded_at
                THEN excluded.status ELSE candidate_source_events.status END,
            labels = CASE
                WHEN excluded.recorded_at >= candidate_source_events.recorded_at
                THEN excluded.labels ELSE candidate_source_events.labels END,
            notes = CASE
                WHEN excluded.recorded_at >= candidate_source_events.recorded_at
                THEN excluded.notes ELSE candidate_source_events.notes END,
            links = CASE
                WHEN excluded.recorded_at >= candidate_source_events.recorded_at
                THEN excluded.links ELSE candidate_source_events.links END,
            evidence_json = CASE
                WHEN excluded.recorded_at >= candidate_source_events.recorded_at
                THEN excluded.evidence_json ELSE candidate_source_events.evidence_json END,
            observed_at = MIN(candidate_source_events.observed_at, excluded.observed_at),
            recorded_at = MAX(candidate_source_events.recorded_at, excluded.recorded_at)
        """,
        (
            address,
            candidate.sources,
            candidate.status,
            candidate.labels,
            candidate.notes,
            candidate.links,
            evidence_json,
            int(observed_at),
            recorded,
        ),
    )


def upsert_wallet_feature(
    conn: sqlite3.Connection,
    feature: WalletFeatures,
    *,
    preloaded_extra: dict[str, Any] | None = None,
) -> None:
    """Merge current research features while preserving unspecified values."""

    address = feature.address.strip().lower()
    if preloaded_extra is None:
        existing = conn.execute(
            "SELECT extra_json FROM wallet_features WHERE address = ?",
            (address,),
        ).fetchone()
        existing_extra = _json_object(existing["extra_json"]) if existing is not None else {}
    else:
        existing_extra = preloaded_extra
    extra = {**existing_extra, **feature.extra}
    db_columns = (*_FEATURE_FIELDS, "extra_json", "updated_at")
    insert_columns = ("address", *db_columns)
    values = [address]
    values.extend(getattr(feature, name) for name in _FEATURE_FIELDS)
    values.extend((_json_dump(extra), int(time.time())))
    updates: list[str] = []
    for name in _FEATURE_FIELDS:
        if name in _FEATURE_TEXT_FIELDS:
            updates.append(
                f"{name} = CASE WHEN excluded.{name} != '' "
                f"THEN excluded.{name} ELSE wallet_features.{name} END"
            )
        else:
            updates.append(
                f"{name} = COALESCE(excluded.{name}, wallet_features.{name})"
            )
    updates.extend(("extra_json = excluded.extra_json", "updated_at = excluded.updated_at"))
    placeholders = ", ".join("?" for _ in insert_columns)
    conn.execute(
        f"""
        INSERT INTO wallet_features({', '.join(insert_columns)})
        VALUES ({placeholders})
        ON CONFLICT(address) DO UPDATE SET
            {', '.join(updates)}
        """,
        tuple(values),
    )


def get_wallet_features(conn: sqlite3.Connection) -> dict[str, WalletFeatures]:
    """Return the compact feature snapshot keyed by normalized wallet."""

    rows = conn.execute("SELECT * FROM wallet_features").fetchall()
    return {str(row["address"]): _feature_from_row(row) for row in rows}


def get_wallet_features_for_addresses(
    conn: sqlite3.Connection,
    addresses: list[str] | tuple[str, ...] | set[str],
) -> dict[str, WalletFeatures]:
    """Return compact feature rows for a bounded wallet-address snapshot."""

    normalized = sorted({address.strip().lower() for address in addresses if address.strip()})
    if not normalized:
        return {}
    snapshot: dict[str, WalletFeatures] = {}
    chunk_size = max(1, _sqlite_variable_limit(conn))
    for offset in range(0, len(normalized), chunk_size):
        chunk = normalized[offset : offset + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT * FROM wallet_features WHERE address IN ({placeholders})",
            tuple(chunk),
        ).fetchall()
        snapshot.update({str(row["address"]): _feature_from_row(row) for row in rows})
    return snapshot


def get_candidates_for_addresses(
    conn: sqlite3.Connection,
    addresses: list[str] | tuple[str, ...] | set[str],
) -> dict[str, CandidateAddress]:
    """Return candidate rows for a bounded wallet-address snapshot."""

    normalized = sorted({address.strip().lower() for address in addresses if address.strip()})
    if not normalized:
        return {}
    snapshot: dict[str, CandidateAddress] = {}
    chunk_size = max(1, _sqlite_variable_limit(conn))
    for offset in range(0, len(normalized), chunk_size):
        chunk = normalized[offset : offset + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT address, sources, labels, notes, links, status
            FROM candidate_wallets
            WHERE address IN ({placeholders})
            """,
            tuple(chunk),
        ).fetchall()
        snapshot.update(
            {
                str(row["address"]): CandidateAddress(
                    address=str(row["address"]),
                    sources=str(row["sources"] or ""),
                    labels=str(row["labels"] or ""),
                    notes=str(row["notes"] or ""),
                    links=str(row["links"] or ""),
                    status=str(row["status"] or ""),
                )
                for row in rows
            }
        )
    return snapshot


def enqueue_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    wallet: str = "",
    job_action: str = "",
    job_scope: str = "",
    priority: int = 100,
    shard: int = 0,
    input_data: dict[str, Any] | None = None,
    max_attempts: int = 3,
    next_attempt_at: int = 0,
    now: int | None = None,
) -> bool:
    """Enqueue one supported research job without executing it inline."""

    if job_type not in ACTIVE_PIPELINE_JOB_TYPES:
        raise ValueError(f"unsupported pipeline job type: {job_type}")
    ts = int(time.time()) if now is None else int(now)
    before = conn.total_changes
    conn.execute(
        """
        INSERT INTO pipeline_jobs(
            job_type, wallet, job_action, job_scope, priority, shard, status,
            lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
            input_json, output_json, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'queued', NULL, 0, 0, ?, ?, ?, '{}', '', ?, ?)
        ON CONFLICT(job_type, wallet, job_scope, job_action) DO UPDATE SET
            priority = MIN(pipeline_jobs.priority, excluded.priority),
            shard = excluded.shard,
            status = 'queued',
            attempts = CASE WHEN pipeline_jobs.status = 'failed' THEN 0 ELSE pipeline_jobs.attempts END,
            max_attempts = excluded.max_attempts,
            next_attempt_at = CASE
                WHEN pipeline_jobs.status = 'failed' THEN excluded.next_attempt_at
                ELSE MIN(pipeline_jobs.next_attempt_at, excluded.next_attempt_at)
            END,
            input_json = excluded.input_json,
            terminal_reason = '',
            terminal_at = NULL,
            terminal_policy_version = '',
            updated_at = excluded.updated_at
        WHERE pipeline_jobs.status NOT IN ('running', 'done', 'terminal_failed')
          AND (pipeline_jobs.status != 'failed' OR pipeline_jobs.next_attempt_at <= excluded.updated_at)
        """,
        (
            job_type,
            wallet.strip().lower(),
            job_action,
            job_scope,
            int(priority),
            int(shard),
            max(1, int(max_attempts)),
            int(next_attempt_at),
            _json_dump(input_data or {}),
            ts,
            ts,
        ),
    )
    return conn.total_changes > before


def claim_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    shard: int,
    worker_id: str,
    lease_seconds: int,
    priority_aging_seconds: int = 0,
    now: int | None = None,
) -> dict[str, Any] | None:
    """Atomically lease one due job; this never changes wallet level."""

    if job_type not in ACTIVE_PIPELINE_JOB_TYPES:
        raise ValueError(f"unsupported pipeline job type: {job_type}")
    ts = int(time.time()) if now is None else int(now)
    aging_seconds = max(0, int(priority_aging_seconds))
    aging_cutoff = ts - aging_seconds
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        """
        SELECT *
        FROM pipeline_jobs
        WHERE job_type = ? AND shard = ? AND next_attempt_at <= ?
          AND attempts < max_attempts
          AND (status = 'queued' OR (status = 'running' AND lease_until <= ?))
        ORDER BY
            CASE WHEN status = 'running' AND lease_until <= ? THEN 0 ELSE 1 END,
            CASE WHEN ? > 0 AND updated_at <= ? THEN 0 ELSE 1 END,
            CASE WHEN ? > 0 AND updated_at <= ? THEN updated_at END,
            priority, updated_at, job_id
        LIMIT 1
        """,
        (
            job_type,
            int(shard),
            ts,
            ts,
            ts,
            aging_seconds,
            aging_cutoff,
            aging_seconds,
            aging_cutoff,
        ),
    ).fetchone()
    if row is None:
        conn.commit()
        return None
    lease_until = ts + max(1, int(lease_seconds))
    conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = 'running', lease_owner = ?, lease_until = ?,
            attempts = attempts + 1, updated_at = ?
        WHERE job_id = ?
        """,
        (worker_id, lease_until, ts, int(row["job_id"])),
    )
    conn.commit()
    job = dict(row)
    job.update(
        status="running",
        attempts=int(job.get("attempts") or 0) + 1,
        lease_owner=worker_id,
        lease_until=lease_until,
    )
    return job


def complete_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    output_data: dict[str, Any] | None = None,
    now: int | None = None,
) -> bool:
    """Finish a job only while the caller still owns its lease."""

    ts = int(time.time()) if now is None else int(now)
    updated = conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = 'done', lease_owner = NULL, lease_until = 0,
            output_json = ?, last_error = '', completed_at = ?, updated_at = ?
        WHERE job_id = ? AND status = 'running' AND lease_owner = ?
        """,
        (_json_dump(output_data or {}), ts, ts, int(job_id), worker_id),
    ).rowcount
    return bool(updated)


def retry_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    error: str,
    next_attempt_at: int,
    count_attempt: bool = True,
    fail_permanently: bool = False,
    terminal_reason: str = "",
    terminal_policy_version: str = "",
    now: int | None = None,
) -> bool:
    """Release a leased job for retry, or fail it after its attempt budget."""

    ts = int(time.time()) if now is None else int(now)
    row = conn.execute(
        "SELECT attempts, max_attempts FROM pipeline_jobs WHERE job_id = ?",
        (int(job_id),),
    ).fetchone()
    attempts = int(row["attempts"] or 0) if row is not None else 0
    max_attempts = int(row["max_attempts"] or 3) if row is not None else 3
    effective_attempts = attempts if count_attempt else max(0, attempts - 1)
    failed = bool(fail_permanently or (row is not None and effective_attempts >= max_attempts))
    effective_terminal_reason = (
        terminal_reason.strip() if terminal_reason.strip() else "permanent_failure"
    )
    terminal = bool(fail_permanently)
    updated = conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = ?, lease_owner = NULL, lease_until = 0,
            next_attempt_at = ?, last_error = ?,
            attempts = CASE
                WHEN ? THEN max_attempts
                WHEN ? THEN attempts
                ELSE MAX(0, attempts - 1)
            END,
            terminal_reason = CASE WHEN ? THEN ? ELSE terminal_reason END,
            terminal_at = CASE WHEN ? THEN ? ELSE terminal_at END,
            terminal_policy_version = CASE WHEN ? THEN ? ELSE terminal_policy_version END,
            updated_at = ?
        WHERE job_id = ? AND status = 'running' AND lease_owner = ?
        """,
        (
            PIPELINE_TERMINAL_FAILED_STATUS if terminal else ("failed" if failed else "queued"),
            int(next_attempt_at),
            error[:1000],
            1 if fail_permanently else 0,
            1 if count_attempt else 0,
            1 if terminal else 0,
            effective_terminal_reason[:200],
            1 if terminal else 0,
            ts,
            1 if terminal else 0,
            terminal_policy_version.strip()[:100],
            ts,
            int(job_id),
            worker_id,
        ),
    ).rowcount
    return bool(updated)


def pipeline_job_summary(conn: sqlite3.Connection, *, job_type: str = "") -> dict[str, Any]:
    """Summarize only active discovery queues, excluding legacy backlog."""

    if job_type and job_type not in ACTIVE_PIPELINE_JOB_TYPES:
        raise ValueError(f"unsupported pipeline job type: {job_type}")
    selected = (job_type,) if job_type else ACTIVE_PIPELINE_JOB_TYPES
    placeholders = ", ".join("?" for _ in selected)
    status_rows = conn.execute(
        f"""
        SELECT job_type, status, COUNT(*) AS count
        FROM pipeline_jobs
        WHERE job_type IN ({placeholders})
        GROUP BY job_type, status
        ORDER BY job_type, count DESC, status
        """,
        selected,
    ).fetchall()
    shard_rows = conn.execute(
        f"""
        SELECT job_type, shard, status, COUNT(*) AS count
        FROM pipeline_jobs
        WHERE job_type IN ({placeholders})
        GROUP BY job_type, shard, status
        ORDER BY job_type, shard, status
        """,
        selected,
    ).fetchall()
    return {
        "statuses": [dict(row) for row in status_rows],
        "by_shard": [dict(row) for row in shard_rows],
    }


def record_runtime_heartbeat(
    conn: sqlite3.Connection,
    name: str,
    *,
    status: str = "ok",
    rows_written: int = 0,
    error: str = "",
    now: int | None = None,
    started_at: int | None = None,
    finished_at: int | None = None,
) -> int:
    """Append a loop heartbeat without claiming queue ownership."""

    ts = int(time.time()) if now is None else int(now)
    started = ts if started_at is None else int(started_at)
    finished = max(started, ts if finished_at is None else int(finished_at))

    def write() -> int:
        cursor = conn.execute(
            """
            INSERT INTO runtime_heartbeats(
                name, started_at, finished_at, status, rows_written, error
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name, started, finished, status, int(rows_written), error[:1000]),
        )
        conn.commit()
        return int(cursor.lastrowid)

    return retry_sqlite_locked(write, rollback=conn.rollback, attempts=2, sleep_seconds=0.5)


def log_api_request(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    endpoint: str,
    status_code: int | None,
    latency_ms: int,
    retry_count: int,
    error_type: str = "",
    ok: bool = False,
) -> None:
    """Persist compact upstream request telemetry."""

    conn.execute(
        """
        INSERT INTO api_request_log(
            ts, base_url, endpoint, status_code, latency_ms,
            retry_count, error_type, ok
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time()),
            base_url,
            endpoint,
            status_code,
            int(latency_ms),
            int(retry_count),
            error_type[:120],
            1 if ok else 0,
        ),
    )
    conn.commit()


def api_request_summary(conn: sqlite3.Connection, *, since_seconds: int = 3600) -> dict[str, Any]:
    """Return bounded request-health metrics for operations and the UI."""

    cutoff = int(time.time()) - max(0, int(since_seconds))
    row = conn.execute(
        """
        SELECT COUNT(*) AS request_count,
               SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) AS ok_count,
               SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS error_count,
               AVG(latency_ms) AS avg_latency_ms,
               MAX(latency_ms) AS max_latency_ms
        FROM api_request_log
        WHERE ts >= ?
        """,
        (cutoff,),
    ).fetchone()
    errors = conn.execute(
        """
        SELECT error_type, COUNT(*) AS count
        FROM api_request_log
        WHERE ts >= ? AND ok = 0
        GROUP BY error_type
        ORDER BY count DESC, error_type
        LIMIT 10
        """,
        (cutoff,),
    ).fetchall()
    output = dict(row)
    output["window_seconds"] = max(0, int(since_seconds))
    output["errors_by_type"] = [dict(item) for item in errors]
    return output


def _feature_from_row(row: sqlite3.Row) -> WalletFeatures:
    kwargs: dict[str, Any] = {"address": str(row["address"])}
    row_keys = set(row.keys())
    for name in _FEATURE_FIELDS:
        kwargs[name] = row[name] if name in row_keys else None
    kwargs["extra"] = _json_object(row["extra_json"] if "extra_json" in row_keys else "{}")
    return WalletFeatures(**kwargs)


def _sqlite_variable_limit(conn: sqlite3.Connection) -> int:
    try:
        rows = conn.execute("PRAGMA compile_options").fetchall()
    except sqlite3.Error:
        return 999
    for row in rows:
        option = str(row[0])
        if option.startswith("MAX_VARIABLE_NUMBER="):
            try:
                return max(1, int(option.partition("=")[2]))
            except ValueError:
                return 999
    return 999


def _merge_text(existing: str, incoming: str, *, max_len: int = 4000) -> str:
    values: list[str] = []
    seen: set[str] = set()
    for raw in (existing, incoming):
        for part in raw.split("|"):
            value = part.strip()
            if value and value not in seen:
                seen.add(value)
                values.append(value)
    return " | ".join(values)[:max_len]


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}
