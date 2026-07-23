"""Operations for the discovery-only L0-L6 wallet research service."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import quote

from pm_robot.config import RobotSettings
from pm_robot.pipeline_terms import ACTIVE_PIPELINE_JOB_TYPES
from pm_robot.research.current_elite import (
    current_elite_wallet_count,
    current_verified_l6_wallet_count,
)
from pm_robot.storage.api_rate_limit import (
    api_rate_limit_summary,
    api_rate_limit_summary_from_path,
)
from pm_robot.storage.db import (
    connect,
    connect_readonly,
    is_sqlite_locked_error,
    pending_migration_versions,
    retry_sqlite_locked,
    run_migrations,
)
from pm_robot.storage.repository import api_request_summary


DAY_SECONDS = 86_400
WAL_CHECKPOINT_MODES = ("none", "passive", "truncate")
DEFAULT_FAILED_JOB_COOLDOWN_SECONDS = 21_600
ACTIVE_RESEARCH_RUNTIME_EVENTS = (
    "loop_discovery_activity",
    "loop_discovery_leaderboard",
    "loop_maintenance",
    "loop_rtds_discovery",
    "loop_wallet_history_planner",
    "loop_wallet_history_worker_0",
    "loop_wallet_history_worker_1",
    "loop_wallet_history_worker_2",
    "loop_wallet_level_control",
    "loop_wallet_l6_validation_worker",
    "loop_wallet_screen_planner",
    "loop_wallet_screen_worker_0",
    "loop_wallet_screen_worker_1",
    "loop_wallet_screen_worker_2",
)
REQUIRED_RESEARCH_TABLES = {
    "api_rate_limit_state",
    "api_request_log",
    "candidate_source_events",
    "candidate_wallets",
    "observed_wallets",
    "runtime_heartbeats",
    "wallet_features",
    "wallet_history_summaries",
    "wallet_level_events",
    "wallet_levels",
    "wallet_pnl_summaries",
    "wallet_screen_summaries",
    "wallet_history_artifacts",
    "wallet_level_selections",
    "wallet_l6_validations",
    "pipeline_jobs",
}


def health_check(settings: RobotSettings) -> dict[str, Any]:
    """Check only the current research control plane and its storage."""

    result: dict[str, Any] = {
        "ok": True,
        "checked_at": int(time.time()),
        "service_scope": "wallet_discovery_research",
        "db_path": str(settings.db_path),
        "archive_dir": str(settings.archive_dir),
        "checks": {},
    }
    try:
        conn = connect_readonly(settings.db_path)
        try:
            conn.execute("SELECT 1").fetchone()
            tables = _table_names(conn)
            missing = sorted(REQUIRED_RESEARCH_TABLES - tables)
            if missing:
                raise RuntimeError(f"missing tables: {missing}")
            pending = pending_migration_versions(conn)
            if pending:
                raise RuntimeError(f"pending migrations: {pending}")
            result["checks"]["sqlite"] = "ok"
            result["pipeline"] = _pipeline_freshness(conn)
            runtime_readiness = _runtime_heartbeat_readiness(
                result["pipeline"],
                required=settings.required_runtime_heartbeats,
                max_age_seconds=settings.runtime_heartbeat_max_age_seconds,
                max_age_overrides=dict(settings.runtime_heartbeat_max_age_overrides),
                now=int(time.time()),
            )
            result["runtime_readiness"] = runtime_readiness
            if not runtime_readiness["ready"]:
                result["ok"] = False
                result["checks"]["runtime_heartbeats"] = runtime_readiness["reason"]
            else:
                result["checks"]["runtime_heartbeats"] = "ok"
            result["research_readiness"] = _research_readiness(conn)
            if not result["research_readiness"]["ready"]:
                result["ok"] = False
                result["checks"]["research_funnel"] = "; ".join(
                    result["research_readiness"]["blockers"]
                )
            else:
                result["checks"]["research_funnel"] = "ok"
            result["api_requests_1h"] = (
                api_request_summary(conn, since_seconds=3600)
                if "api_request_log" in tables
                else {"available": False}
            )
            if settings.rate_limit_db_path is not None:
                result["upstream_request_budget"] = api_rate_limit_summary_from_path(
                    settings.rate_limit_db_path
                )
                result["upstream_request_budget"]["storage"] = "dedicated"
            elif "api_rate_limit_state" in tables:
                result["upstream_request_budget"] = api_rate_limit_summary(conn)
                result["upstream_request_budget"]["storage"] = "main"
                result["upstream_request_budget"]["available"] = True
            result["storage"] = storage_report(settings, conn=conn)
        finally:
            conn.close()
    except Exception as exc:
        result["ok"] = False
        result["checks"]["sqlite"] = str(exc)

    for name, path in (
        ("db_parent", settings.db_path.parent),
        ("archive_dir", settings.archive_dir),
        ("log_dir", settings.log_dir),
        ("backup_dir", settings.backup_dir),
    ):
        try:
            if not path.is_dir():
                raise FileNotFoundError(path)
            if not os.access(path, os.W_OK | os.X_OK):
                raise PermissionError(f"directory is not writable: {path}")
            result["checks"][name] = "ok"
        except Exception as exc:
            result["ok"] = False
            result["checks"][name] = str(exc)
    return result


def _pipeline_freshness(conn: sqlite3.Connection) -> dict[str, Any]:
    placeholders = ", ".join("?" for _ in ACTIVE_RESEARCH_RUNTIME_EVENTS)
    rows = conn.execute(
        f"""
        SELECT name, status, started_at, finished_at, error
        FROM runtime_heartbeats
        WHERE name IN ({placeholders})
          AND heartbeat_id IN (
              SELECT MAX(heartbeat_id)
              FROM runtime_heartbeats
              WHERE name IN ({placeholders})
              GROUP BY name
          )
        ORDER BY name
        """,
        (*ACTIVE_RESEARCH_RUNTIME_EVENTS, *ACTIVE_RESEARCH_RUNTIME_EVENTS),
    ).fetchall()
    return {
        str(row["name"]): {
            "status": str(row["status"] or ""),
            "started_at": int(row["started_at"] or 0),
            "finished_at": int(row["finished_at"] or 0),
            "error": str(row["error"] or ""),
        }
        for row in rows
    }


def _runtime_heartbeat_readiness(
    pipeline: dict[str, Any],
    *,
    required: tuple[str, ...],
    max_age_seconds: int,
    max_age_overrides: dict[str, int] | None = None,
    now: int,
) -> dict[str, Any]:
    """Evaluate only explicitly required runtime loops."""

    required_names = tuple(dict.fromkeys(required))
    if not required_names:
        return {
            "ready": True,
            "required": [],
            "missing": [],
            "stale": [],
            "failed": [],
            "reason": "not_enforced",
        }
    max_age = max(60, int(max_age_seconds))
    age_limits = {
        name: max(60, int((max_age_overrides or {}).get(name, max_age)))
        for name in required_names
    }
    missing = [name for name in required_names if name not in pipeline]
    stale = [
        name
        for name in required_names
        if name in pipeline
        and int(pipeline[name].get("finished_at") or 0) < int(now) - age_limits[name]
    ]
    failed = [
        name
        for name in required_names
        if name in pipeline and str(pipeline[name].get("status") or "") != "ok"
    ]
    ready = not (missing or stale or failed)
    reason = "ok" if ready else f"missing={missing}; stale={stale}; failed={failed}"
    return {
        "ready": ready,
        "required": list(required_names),
        "max_age_seconds": max_age,
        "max_age_seconds_by_name": age_limits,
        "missing": missing,
        "stale": stale,
        "failed": failed,
        "reason": reason,
    }


def _research_readiness(conn: sqlite3.Connection) -> dict[str, Any]:
    levels = {
        str(row["level"]): int(row["count"] or 0)
        for row in conn.execute(
            "SELECT level, COUNT(*) AS count FROM wallet_levels GROUP BY level ORDER BY level"
        ).fetchall()
    }
    placeholders = ", ".join("?" for _ in ACTIVE_PIPELINE_JOB_TYPES)
    jobs = {
        f"{row['job_type']}:{row['status']}": int(row["count"] or 0)
        for row in conn.execute(
            f"""
            SELECT job_type, status, COUNT(*) AS count
            FROM pipeline_jobs
            WHERE job_type IN ({placeholders})
            GROUP BY job_type, status
            ORDER BY job_type, status
            """,
            ACTIVE_PIPELINE_JOB_TYPES,
        ).fetchall()
    }
    ingress_invariants = {
        "candidate_without_observation": _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM candidate_wallets AS candidate
            LEFT JOIN observed_wallets AS observed
              ON observed.wallet = candidate.address
            WHERE observed.wallet IS NULL
            """,
        ),
        "candidate_without_level": _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM candidate_wallets AS candidate
            LEFT JOIN wallet_levels AS levels
              ON levels.wallet = candidate.address
            WHERE levels.wallet IS NULL
            """,
        ),
        "observation_without_level": _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM observed_wallets AS observed
            LEFT JOIN wallet_levels AS levels
              ON levels.wallet = observed.wallet
            WHERE levels.wallet IS NULL
            """,
        ),
    }
    blockers = [
        f"{name}={count}"
        for name, count in ingress_invariants.items()
        if count > 0
    ]
    metrics = {
        "observed_wallets": _scalar(conn, "SELECT COUNT(*) FROM observed_wallets"),
        "candidate_wallets": _scalar(conn, "SELECT COUNT(*) FROM candidate_wallets"),
        "screened_wallets": _scalar(
            conn,
            "SELECT COUNT(*) FROM wallet_screen_summaries WHERE screen_complete = 1",
        ),
        "history_wallets": _scalar(conn, "SELECT COUNT(*) FROM wallet_history_summaries"),
        "active_history_artifacts": _scalar(
            conn,
            "SELECT COUNT(*) FROM wallet_history_artifacts WHERE status = 'active' AND purged_at IS NULL",
        ),
        "hard_risk_blocks": _scalar(
            conn,
            "SELECT COUNT(*) FROM wallet_levels WHERE hard_risk_block = 1",
        ),
        "fresh_elite_wallets": current_elite_wallet_count(conn, now=int(time.time())),
        "verified_l6_wallets": current_verified_l6_wallet_count(conn, now=int(time.time())),
        "ingress_invariants": ingress_invariants,
        "levels": {f"l{level}": levels.get(f"l{level}", 0) for level in range(7)},
        "jobs": jobs,
    }
    return {
        "ready": not blockers,
        "blockers": blockers,
        "metrics": metrics,
        "elite_wallets_available": metrics["fresh_elite_wallets"] > 0,
    }


def write_health(settings: RobotSettings, output_path: Path | None = None) -> dict[str, Any]:
    data = health_check(settings)
    output = output_path or (settings.log_dir / "health.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomically(output, data)
    return data


def _write_json_atomically(output: Path, data: dict[str, Any]) -> None:
    """Replace a JSON snapshot only after one complete, durable write."""

    payload = json.dumps(data, ensure_ascii=False, indent=2)
    partial: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            partial = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            mode = output.stat().st_mode & 0o777 if output.exists() else 0o644
            os.fchmod(handle.fileno(), mode)
        os.replace(partial, output)
        partial = None
    finally:
        if partial is not None:
            partial.unlink(missing_ok=True)


def verify_backup_database(path: Path, *, full_check: bool = False) -> dict[str, Any]:
    """Verify a manually created SQLite restore point."""

    resolved = path.resolve()
    uri = f"file:{quote(str(resolved), safe='/')}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        required = {"schema_migrations", *REQUIRED_RESEARCH_TABLES}
        missing = sorted(required - tables)
        migration_count = (
            int(conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0])
            if "schema_migrations" in tables
            else 0
        )
        expected_size = page_size * page_count
        actual_size = resolved.stat().st_size
        if page_size <= 0 or page_count <= 0 or expected_size != actual_size:
            raise RuntimeError(
                "backup page layout check failed: "
                f"page_size={page_size} page_count={page_count} "
                f"expected_size={expected_size} actual_size={actual_size}"
            )
        if missing or migration_count <= 0:
            raise RuntimeError(
                f"backup schema check failed: missing_tables={missing} "
                f"migration_count={migration_count}"
            )
        quick_check = "not_run"
        if full_check:
            row = conn.execute("PRAGMA quick_check").fetchone()
            quick_check = str(row[0]).lower() if row else "missing_result"
            if quick_check != "ok":
                raise RuntimeError(f"backup integrity check failed: {row}")
        return {
            "page_size": page_size,
            "page_count": page_count,
            "file_size": actual_size,
            "table_count": len(tables),
            "migration_count": migration_count,
            "full_check": bool(full_check),
            "quick_check": quick_check,
        }
    finally:
        conn.close()


def backup_database(settings: RobotSettings, *, full_check: bool = False) -> Path:
    """Create one explicit, verified backup; no scheduler calls this automatically."""

    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    if not settings.db_path.exists():
        raise FileNotFoundError(settings.db_path)
    timestamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    output = settings.backup_dir / f"pm_robot-{timestamp}.sqlite"
    partial = output.with_suffix(f"{output.suffix}.partial")
    partial.unlink(missing_ok=True)
    try:
        source = sqlite3.connect(settings.db_path)
        try:
            destination = sqlite3.connect(partial)
            try:
                source.backup(destination)
            finally:
                destination.close()
        finally:
            source.close()
        verify_backup_database(partial, full_check=full_check)
        partial.replace(output)
    except Exception:
        partial.unlink(missing_ok=True)
        raise
    latest = settings.backup_dir / "pm_robot-latest.sqlite"
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        try:
            os.link(output, latest)
        except OSError:
            latest.symlink_to(output.name)
    except OSError:
        pass
    return output


def dump_database_sql(settings: RobotSettings, output: BinaryIO) -> None:
    """Stream one consistent manual SQL dump."""

    if not settings.db_path.exists():
        raise FileNotFoundError(settings.db_path)
    uri = f"file:{settings.db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.execute("PRAGMA query_only = ON")
        conn.execute("BEGIN")
        try:
            for line in conn.iterdump():
                output.write(f"{line}\n".encode("utf-8"))
        finally:
            conn.execute("ROLLBACK")
    finally:
        conn.close()


def status(settings: RobotSettings) -> dict[str, Any]:
    data = health_check(settings)
    data["backup_dir"] = str(settings.backup_dir)
    data["log_dir"] = str(settings.log_dir)
    return data


def maintenance(
    settings: RobotSettings,
    *,
    api_log_days: int = 7,
    heartbeat_days: int = 30,
    pipeline_job_days: int = 30,
    keep_backups: int = 2,
    dry_run: bool = False,
    vacuum: bool = False,
    wal_checkpoint: str = "none",
    skip_cleanup: bool = False,
    cleanup_batch_limit: int = 10_000,
    reset_stale_jobs: bool = False,
    failed_job_cooldown_seconds: int = DEFAULT_FAILED_JOB_COOLDOWN_SECONDS,
    reset_stale_heartbeats: bool = False,
    stale_heartbeat_seconds: int = 21_600,
) -> dict[str, Any]:
    """Bound metadata growth and disable every retired queue type."""

    checkpoint_mode = wal_checkpoint.lower()
    if checkpoint_mode not in WAL_CHECKPOINT_MODES:
        raise ValueError(f"wal_checkpoint must be one of: {', '.join(WAL_CHECKPOINT_MODES)}")
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    storage_before = storage_report(settings)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        legacy_jobs = _disable_legacy_pipeline_jobs(conn, execute=not dry_run)
        stale_jobs = _recover_stale_pipeline_jobs(
            conn,
            execute=bool(reset_stale_jobs and not dry_run),
            failed_job_cooldown_seconds=failed_job_cooldown_seconds,
        )
        stale_heartbeats = _close_stale_runtime_heartbeats(
            conn,
            execute=bool(reset_stale_heartbeats and not dry_run),
            stale_seconds=stale_heartbeat_seconds,
        )
        deleted = (
            {}
            if skip_cleanup
            else _cleanup_metadata(
                conn,
                api_log_days=api_log_days,
                heartbeat_days=heartbeat_days,
                pipeline_job_days=pipeline_job_days,
                batch_limit=cleanup_batch_limit,
                execute=not dry_run,
            )
        )
        if not dry_run:
            conn.commit()
            if not skip_cleanup:
                conn.execute("PRAGMA optimize")
            if vacuum:
                conn.execute("VACUUM")
        checkpoint = _wal_checkpoint(conn, mode=checkpoint_mode, dry_run=dry_run)
    finally:
        conn.close()
    backup_cleanup = cleanup_backups(settings.backup_dir, keep=keep_backups, dry_run=dry_run)
    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "cleanup_skipped": bool(skip_cleanup),
        "legacy_jobs_disabled": legacy_jobs,
        "stale_jobs": stale_jobs,
        "stale_heartbeats": stale_heartbeats,
        "deleted": deleted,
        "wal_checkpoint": checkpoint,
        "vacuum": bool(vacuum and not dry_run),
        "backup_cleanup": backup_cleanup,
        "storage_before": storage_before,
        "storage": storage_report(settings),
    }


def _disable_legacy_pipeline_jobs(
    conn: sqlite3.Connection,
    *,
    execute: bool,
) -> dict[str, Any]:
    placeholders = ", ".join("?" for _ in ACTIVE_PIPELINE_JOB_TYPES)
    rows = conn.execute(
        f"""
        SELECT job_type, status, COUNT(*) AS count
        FROM pipeline_jobs
        WHERE job_type NOT IN ({placeholders})
          AND status IN ('queued', 'running')
        GROUP BY job_type, status
        ORDER BY job_type, status
        """,
        ACTIVE_PIPELINE_JOB_TYPES,
    ).fetchall()
    total = sum(int(row["count"] or 0) for row in rows)
    if execute and total:
        now = int(time.time())
        conn.execute(
            f"""
            UPDATE pipeline_jobs
            SET status = 'cancelled', lease_owner = NULL, lease_until = 0,
                completed_at = ?, updated_at = ?,
                last_error = 'retired_job_type_disabled_by_research_runtime'
            WHERE job_type NOT IN ({placeholders})
              AND status IN ('queued', 'running')
            """,
            (now, now, *ACTIVE_PIPELINE_JOB_TYPES),
        )
        conn.commit()
    return {
        "executed": bool(execute),
        "total": total,
        "by_job_type": [dict(row) for row in rows],
    }


def _recover_stale_pipeline_jobs(
    conn: sqlite3.Connection,
    *,
    execute: bool,
    failed_job_cooldown_seconds: int,
) -> dict[str, Any]:
    now = int(time.time())
    placeholders = ", ".join("?" for _ in ACTIVE_PIPELINE_JOB_TYPES)
    rows = conn.execute(
        f"""
        SELECT job_type,
               COUNT(*) AS count,
               SUM(CASE WHEN attempts < max_attempts THEN 1 ELSE 0 END) AS requeue_count,
               SUM(CASE WHEN attempts >= max_attempts THEN 1 ELSE 0 END) AS fail_count
        FROM pipeline_jobs
        WHERE job_type IN ({placeholders})
          AND status = 'running' AND lease_until <= ?
        GROUP BY job_type
        ORDER BY job_type
        """,
        (*ACTIVE_PIPELINE_JOB_TYPES, now),
    ).fetchall()
    queued_exhausted = _scalar(
        conn,
        f"""
        SELECT COUNT(*) FROM pipeline_jobs
        WHERE job_type IN ({placeholders})
          AND status = 'queued' AND attempts >= max_attempts
        """,
        ACTIVE_PIPELINE_JOB_TYPES,
    )
    expired = sum(int(row["count"] or 0) for row in rows)
    if execute and (expired or queued_exhausted):
        failed_retry_at = now + max(0, int(failed_job_cooldown_seconds))
        conn.execute(
            f"""
            UPDATE pipeline_jobs
            SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
                lease_owner = NULL, lease_until = 0,
                next_attempt_at = CASE
                    WHEN attempts >= max_attempts THEN MAX(next_attempt_at, ?)
                    ELSE 0
                END,
                last_error = CASE
                    WHEN attempts >= max_attempts THEN 'attempt_budget_exhausted_by_maintenance'
                    ELSE 'expired_lease_requeued_by_maintenance'
                END,
                updated_at = ?
            WHERE job_type IN ({placeholders})
              AND (
                  (status = 'running' AND lease_until <= ?)
                  OR (status = 'queued' AND attempts >= max_attempts)
              )
            """,
            (failed_retry_at, now, *ACTIVE_PIPELINE_JOB_TYPES, now),
        )
        conn.commit()
    return {
        "executed": bool(execute),
        "expired_running": expired,
        "queued_exhausted": queued_exhausted,
        "by_job_type": [dict(row) for row in rows],
    }


def _close_stale_runtime_heartbeats(
    conn: sqlite3.Connection,
    *,
    execute: bool,
    stale_seconds: int,
) -> dict[str, Any]:
    seconds = max(60, int(stale_seconds))
    cutoff = int(time.time()) - seconds
    placeholders = ", ".join("?" for _ in ACTIVE_RESEARCH_RUNTIME_EVENTS)
    rows = conn.execute(
        f"""
        SELECT name, COUNT(*) AS count
        FROM runtime_heartbeats
        WHERE name IN ({placeholders})
          AND status = 'running' AND started_at <= ?
        GROUP BY name
        ORDER BY name
        """,
        (*ACTIVE_RESEARCH_RUNTIME_EVENTS, cutoff),
    ).fetchall()
    total = sum(int(row["count"] or 0) for row in rows)
    if execute and total:
        conn.execute(
            f"""
            UPDATE runtime_heartbeats
            SET status = 'interrupted', finished_at = ?,
                error = CASE WHEN error = ''
                    THEN 'stale_heartbeat_closed_by_maintenance' ELSE error END
            WHERE name IN ({placeholders})
              AND status = 'running' AND started_at <= ?
            """,
            (int(time.time()), *ACTIVE_RESEARCH_RUNTIME_EVENTS, cutoff),
        )
        conn.commit()
    return {
        "executed": bool(execute),
        "stale_seconds": seconds,
        "total": total,
        "by_name": [dict(row) for row in rows],
    }


def _cleanup_metadata(
    conn: sqlite3.Connection,
    *,
    api_log_days: int,
    heartbeat_days: int,
    pipeline_job_days: int,
    batch_limit: int,
    execute: bool,
) -> dict[str, int]:
    now = int(time.time())
    limit = max(1, int(batch_limit))
    specs = (
        (
            "api_request_log",
            "ts < ?",
            (now - max(0, int(api_log_days)) * DAY_SECONDS,),
        ),
        (
            "runtime_heartbeats",
            "finished_at IS NOT NULL AND finished_at < ?",
            (now - max(0, int(heartbeat_days)) * DAY_SECONDS,),
        ),
        (
            "pipeline_jobs",
            "status IN ('done', 'failed', 'cancelled', 'superseded') AND updated_at < ?",
            (now - max(0, int(pipeline_job_days)) * DAY_SECONDS,),
        ),
    )
    deleted: dict[str, int] = {}
    for table, where, params in specs:
        matched = _scalar(
            conn,
            f"SELECT COUNT(*) FROM (SELECT 1 FROM {table} WHERE {where} LIMIT ?)",
            (*params, limit),
        )
        if execute and matched:
            deleted[table] = _delete_metadata_batch(
                conn,
                table=table,
                where=where,
                params=params,
                limit=limit,
            )
        else:
            deleted[table] = matched
    return deleted


def _delete_metadata_batch(
    conn: sqlite3.Connection,
    *,
    table: str,
    where: str,
    params: tuple[Any, ...],
    limit: int,
) -> int:
    """Commit one bounded cleanup batch and retry only transient writer contention."""

    def delete() -> int:
        cursor = conn.execute(
            f"""
            DELETE FROM {table}
            WHERE rowid IN (
                SELECT rowid FROM {table} WHERE {where} LIMIT ?
            )
            """,
            (*params, limit),
        )
        conn.commit()
        return max(0, int(cursor.rowcount))

    return retry_sqlite_locked(
        delete,
        rollback=conn.rollback,
        attempts=4,
        sleep_seconds=2.0,
    )


def storage_report(
    settings: RobotSettings,
    *,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    db_size = _path_size(settings.db_path)
    wal_size = _path_size(settings.db_path.with_name(settings.db_path.name + "-wal"))
    shm_size = _path_size(settings.db_path.with_name(settings.db_path.name + "-shm"))
    backup_files = sorted(
        path
        for path in settings.backup_dir.glob("pm_robot-*.sqlite")
        if path.name != "pm_robot-latest.sqlite"
    )
    backup_size = sum(_path_size(path) for path in backup_files)
    usage_root = settings.db_path.parent if settings.db_path.parent.exists() else Path(".")
    usage = shutil.disk_usage(usage_root)
    report: dict[str, Any] = {
        "db_size_mb": round(db_size / 1_048_576, 2),
        "db_wal_mb": round(wal_size / 1_048_576, 2),
        "db_shm_mb": round(shm_size / 1_048_576, 2),
        "backup_count": len(backup_files),
        "backup_size_mb": round(backup_size / 1_048_576, 2),
        "archive_dir": str(settings.archive_dir),
        "free_disk_gb": round(usage.free / 1_073_741_824, 2),
        "total_disk_gb": round(usage.total / 1_073_741_824, 2),
    }
    own_conn = conn is None and settings.db_path.exists()
    catalog_conn = connect_readonly(settings.db_path) if own_conn else conn
    try:
        if catalog_conn is not None and "wallet_history_artifacts" in _table_names(catalog_conn):
            row = catalog_conn.execute(
                """
                SELECT COUNT(*) AS artifact_count,
                       SUM(CASE WHEN status = 'active' AND purged_at IS NULL THEN 1 ELSE 0 END) AS active_count,
                       COALESCE(SUM(CASE WHEN purged_at IS NULL THEN byte_size ELSE 0 END), 0) AS catalog_bytes
                FROM wallet_history_artifacts
                """
            ).fetchone()
            report["parquet_artifacts"] = int(row["artifact_count"] or 0)
            report["parquet_active_artifacts"] = int(row["active_count"] or 0)
            report["parquet_catalog_size_mb"] = round(int(row["catalog_bytes"] or 0) / 1_048_576, 2)
    finally:
        if own_conn and catalog_conn is not None:
            catalog_conn.close()
    return report


def cleanup_backups(backup_dir: Path, *, keep: int, dry_run: bool = False) -> dict[str, Any]:
    backups = sorted(
        (
            path
            for path in backup_dir.glob("pm_robot-*.sqlite")
            if path.name != "pm_robot-latest.sqlite"
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    to_delete = backups[max(0, int(keep)) :]
    bytes_to_delete = sum(_path_size(path) for path in to_delete)
    if not dry_run:
        for path in to_delete:
            path.unlink(missing_ok=True)
    return {
        "kept": min(len(backups), max(0, int(keep))),
        "deleted": len(to_delete),
        "deleted_mb": round(bytes_to_delete / 1_048_576, 2),
    }


def _wal_checkpoint(
    conn: sqlite3.Connection,
    *,
    mode: str,
    dry_run: bool,
) -> dict[str, Any]:
    if mode == "none":
        return {"mode": mode, "executed": False, "skipped_reason": "not_requested"}
    if dry_run:
        return {"mode": mode, "executed": False, "skipped_reason": "dry_run"}
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        row = conn.execute(f"PRAGMA wal_checkpoint({mode.upper()})").fetchone()
    except sqlite3.OperationalError as exc:
        if not is_sqlite_locked_error(exc):
            raise
        return {
            "mode": mode,
            "executed": False,
            "skipped_reason": "sqlite_locked",
            "error": str(exc),
        }
    return {
        "mode": mode,
        "executed": True,
        "skipped_reason": "",
        "busy": int(row[0]),
        "log_frames": int(row[1]),
        "checkpointed_frames": int(row[2]),
    }


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _scalar(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] = (),
) -> int:
    return int(conn.execute(sql, params).fetchone()[0])


def _path_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except FileNotFoundError:
        return 0
