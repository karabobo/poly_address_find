"""Server operations: health checks, status, and backups."""

from __future__ import annotations

import csv
import json
import os
import shutil
import sqlite3
import time
from urllib.parse import quote
from pathlib import Path
from typing import Any, BinaryIO

from pm_robot.config import RobotSettings
from pm_robot.risk.eligibility import winner_library_eligibility_status
from pm_robot.storage.api_rate_limit import api_rate_limit_summary
from pm_robot.storage.db import connect, connect_readonly, is_sqlite_locked_error, run_migrations
from pm_robot.storage.evidence_archive import (
    EVIDENCE_TABLE_SPECS,
    capture_archive_scope,
    create_archive_run,
    drop_prune_temp_tables,
    ensure_archive_backend,
    export_evidence_archive,
    prepare_prune_temp_tables,
    register_archive_manifest,
    resumable_archive_run,
    set_archive_run_status,
    verify_archive_manifest,
)
from pm_robot.storage.repository import api_request_summary

DAY_SECONDS = 86_400
WAL_CHECKPOINT_MODES = ("none", "passive", "truncate")
DEFAULT_FAILED_JOB_COOLDOWN_SECONDS = 21_600
WALLET_REGISTRY_TABLE_COLUMNS = [
    "address",
    "candidate_stage",
    "registry_status",
    "raw_retention_tier",
    "leader_score",
    "review_stage",
    "review_reason",
    "policy_version",
    "scored_at",
    "total_volume_usdc",
    "recent_30d_volume_usdc",
    "net_pnl_usdc",
    "event_win_rate",
    "trade_win_rate",
    "copy_stream_roi",
    "copy_backtest_net_pnl_usdc",
    "edge_retention_pct",
    "walk_forward_consistency_pct",
    "hygiene_status",
    "primary_category",
    "evidence_stage",
    "activity_count",
    "oldest_activity_ts",
    "newest_activity_ts",
    "paper_orders",
    "paper_settled_positions",
    "paper_total_roi",
    "paper_settled_roi",
    "production_ready",
    "tags_json",
    "blockers_json",
    "source_json",
    "feature_json",
    "score_json",
    "evidence_json",
    "paper_json",
    "summary_json",
    "last_evaluated_at",
    "updated_at",
]

WALLET_REGISTRY_EXPORT_COLUMNS = [
    "address",
    "candidate_stage",
    "registry_status",
    "raw_retention_tier",
    "leader_score",
    "review_reason",
    "total_volume_usdc",
    "recent_30d_volume_usdc",
    "net_pnl_usdc",
    "copy_backtest_net_pnl_usdc",
    "paper_total_roi",
    "production_ready",
    "hygiene_status",
    "evidence_stage",
    "activity_count",
    "oldest_activity_ts",
    "newest_activity_ts",
    "tags_json",
    "blockers_json",
    "source_json",
    "summary_json",
]


def build_wallet_registry(
    settings: RobotSettings,
    *,
    limit: int = 0,
    stages: tuple[str, ...] = (),
    csv_output_path: Path | None = None,
    json_output_path: Path | None = None,
) -> dict[str, Any]:
    """Materialize a compact wallet library and optionally export it."""

    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        rows = _materialize_wallet_registry(conn, limit=limit, stages=stages)
        conn.commit()
    finally:
        conn.close()

    if csv_output_path:
        _write_wallet_registry_csv(csv_output_path, rows)
    if json_output_path:
        _write_wallet_registry_json(json_output_path, rows)

    return {
        "ok": True,
        "wallet_count": len(rows),
        "stage_counts": _count_by(rows, "candidate_stage"),
        "status_counts": _count_by(rows, "registry_status"),
        "retention_counts": _count_by(rows, "raw_retention_tier"),
        "csv_output_path": str(csv_output_path) if csv_output_path else "",
        "json_output_path": str(json_output_path) if json_output_path else "",
        "storage": storage_report(settings),
    }


def build_winner_library(
    settings: RobotSettings,
    *,
    limit: int = 0,
    stages: tuple[str, ...] = (),
    csv_output_path: Path | None = None,
    json_output_path: Path | None = None,
) -> dict[str, Any]:
    """Materialize the broad registry, then export only eligible copyable winners."""

    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        rows = _materialize_wallet_registry(conn, limit=limit, stages=stages)
        eligible_rows = [
            row
            for row in rows
            if winner_library_eligibility_status(conn, str(row.get("address") or "")).eligible
        ]
        conn.commit()
    finally:
        conn.close()

    if csv_output_path:
        _write_wallet_registry_csv(csv_output_path, eligible_rows)
    if json_output_path:
        _write_wallet_registry_json(json_output_path, eligible_rows)

    return {
        "ok": True,
        "wallet_count": len(eligible_rows),
        "broad_registry_wallet_count": len(rows),
        "stage_counts": _count_by(eligible_rows, "candidate_stage"),
        "status_counts": _count_by(eligible_rows, "registry_status"),
        "retention_counts": _count_by(eligible_rows, "raw_retention_tier"),
        "winner_library_filtered": True,
        "csv_output_path": str(csv_output_path) if csv_output_path else "",
        "json_output_path": str(json_output_path) if json_output_path else "",
        "storage": storage_report(settings),
    }


def health_check(settings: RobotSettings) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "checked_at": int(time.time()),
        "mode": settings.execution_mode,
        "db_path": str(settings.db_path),
        "checks": {},
    }
    try:
        settings.assert_safe()
        result["checks"]["mode_guard"] = "ok"
    except Exception as exc:
        result["ok"] = False
        result["checks"]["mode_guard"] = str(exc)

    try:
        conn = connect_readonly(settings.db_path)
        try:
            conn.execute("SELECT 1").fetchone()
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            required = {"candidate_wallets", "wallet_features", "leader_scores", "paper_orders"}
            missing = sorted(required - tables)
            if missing:
                raise RuntimeError(f"missing tables: {missing}")
            result["checks"]["sqlite"] = "ok"
            result["pipeline"] = _pipeline_freshness(conn, tables)
            result["production_readiness"] = _production_readiness(conn, tables)
            if "api_request_log" in tables:
                result["api_requests_1h"] = api_request_summary(conn, since_seconds=3600)
            if "api_rate_limit_state" in tables:
                result["upstream_request_budget"] = api_rate_limit_summary(conn)
            result["storage"] = storage_report(settings)
        finally:
            conn.close()
    except Exception as exc:
        result["ok"] = False
        result["checks"]["sqlite"] = str(exc)

    for path_name, path in (
        ("log_dir", settings.log_dir),
        ("backup_dir", settings.backup_dir),
        ("db_parent", settings.db_path.parent),
    ):
        try:
            if not path.is_dir():
                raise FileNotFoundError(path)
            if not os.access(path, os.W_OK | os.X_OK):
                raise PermissionError(f"directory is not writable: {path}")
            result["checks"][path_name] = "ok"
        except Exception as exc:
            result["ok"] = False
            result["checks"][path_name] = str(exc)
    return result


def _pipeline_freshness(conn: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    """Return the latest run per ingest type without scanning large fact tables."""
    if "ingest_runs" not in tables:
        return {}
    rows = conn.execute(
        """
        SELECT ingest_type, status, started_at, finished_at, error
        FROM ingest_runs
        WHERE run_id IN (
            SELECT MAX(run_id)
            FROM ingest_runs
            GROUP BY ingest_type
        )
        ORDER BY ingest_type
        """
    ).fetchall()
    return {
        str(row["ingest_type"]): {
            "status": str(row["status"] or ""),
            "started_at": int(row["started_at"] or 0),
            "finished_at": int(row["finished_at"] or 0),
            "error": str(row["error"] or ""),
        }
        for row in rows
    }


def _production_readiness(conn: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    required = {
        "wallet_features",
        "copy_pair_stats",
        "copy_leader_performance",
        "paper_wallet_quality",
        "candidate_wallets",
        "leader_publish",
    }
    if not required.issubset(tables):
        return {"closed": False, "blockers": ["production_schema_incomplete"]}
    metrics = {
        "verified_hygiene_wallets": _scalar(
            conn,
            """
            SELECT COUNT(*) FROM wallet_features
            WHERE lower(hygiene_status) IN ('clean', 'screened')
              AND maker_fraction IS NOT NULL
              AND COALESCE(json_extract(extra_json, '$.maker_fraction_source'), '')
                  != 'public_activity_no_maker_flags_observed'
            """,
        ),
        "qualified_copy_pairs": _scalar(
            conn, "SELECT COUNT(*) FROM copy_pair_stats WHERE qualifies = 1"
        ),
        "validated_copy_leaders": _scalar(
            conn,
            """
            SELECT COUNT(*) FROM copy_leader_performance
            WHERE edge_retention_pct >= 60
              AND walk_forward_consistency_pct >= 55
            """,
        ),
        "formal_paper_wallets": _scalar(
            conn,
            """
            SELECT COUNT(DISTINCT wallet) FROM paper_fills
            WHERE validation_cohort = 'validation'
            """,
        ) if "paper_fills" in tables else 0,
        "production_ready_wallets": _scalar(
            conn, "SELECT COUNT(*) FROM paper_wallet_quality WHERE production_ready = 1"
        ),
        "live_eligible_wallets": _scalar(
            conn, "SELECT COUNT(*) FROM candidate_wallets WHERE candidate_stage = 'live_eligible'"
        ),
        "active_published_leaders": _scalar(
            conn,
            """
            SELECT COUNT(*) FROM leader_publish
            WHERE revoked_at IS NULL AND expires_at > strftime('%s','now')
            """,
        ),
    }
    blockers = [
        name
        for name, value in (
            ("no_verified_hygiene_wallets", metrics["verified_hygiene_wallets"]),
            ("no_qualified_copy_pairs", metrics["qualified_copy_pairs"]),
            ("no_validated_copy_leaders", metrics["validated_copy_leaders"]),
            ("no_formal_paper_wallets", metrics["formal_paper_wallets"]),
            ("no_production_ready_wallets", metrics["production_ready_wallets"]),
            ("no_live_eligible_wallets", metrics["live_eligible_wallets"]),
            ("no_active_published_leaders", metrics["active_published_leaders"]),
        )
        if value <= 0
    ]
    return {"closed": not blockers, "blockers": blockers, "metrics": metrics}


def _scalar(conn: sqlite3.Connection, sql: str) -> int:
    return int(conn.execute(sql).fetchone()[0])


def write_health(settings: RobotSettings, output_path: Path | None = None) -> dict[str, Any]:
    data = health_check(settings)
    output = output_path or (settings.log_dir / "health.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def verify_backup_database(path: Path, *, full_check: bool = False) -> dict[str, Any]:
    """Verify backup structure quickly, with an optional full SQLite scan."""
    resolved = path.resolve()
    uri = f"file:{quote(str(resolved), safe='/')}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        required_tables = {
            "schema_migrations",
            "candidate_wallets",
            "wallet_processing_state",
            "pipeline_jobs",
            "wallet_activity",
            "leader_scores",
        }
        missing_tables = sorted(required_tables - tables)
        migration_count = (
            int(conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0])
            if "schema_migrations" in tables
            else 0
        )
        expected_size = page_size * page_count
        actual_size = resolved.stat().st_size
        if page_size <= 0 or page_count <= 0 or actual_size != expected_size:
            raise RuntimeError(
                "backup page layout check failed: "
                f"page_size={page_size} page_count={page_count} "
                f"expected_size={expected_size} actual_size={actual_size}"
            )
        if missing_tables or migration_count <= 0:
            raise RuntimeError(
                "backup schema check failed: "
                f"missing_tables={missing_tables} migration_count={migration_count}"
            )
        quick_check = "not_run"
        if full_check:
            check = conn.execute("PRAGMA quick_check").fetchone()
            quick_check = str(check[0]).lower() if check else "missing_result"
            if quick_check != "ok":
                raise RuntimeError(f"backup integrity check failed: {check}")
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
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    if not settings.db_path.exists():
        raise FileNotFoundError(settings.db_path)
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    out = settings.backup_dir / f"pm_robot-{ts}.sqlite"
    partial = out.with_suffix(f"{out.suffix}.partial")
    partial.unlink(missing_ok=True)
    # SQLite backup API is safer than raw copy with WAL; fallback copy is unnecessary.
    try:
        src = sqlite3.connect(settings.db_path)
        try:
            dst = sqlite3.connect(partial)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
        verify_backup_database(partial, full_check=full_check)
        partial.replace(out)
    except Exception:
        partial.unlink(missing_ok=True)
        raise
    if not out.exists():
        raise RuntimeError("backup file was not created")
    latest = settings.backup_dir / "pm_robot-latest.sqlite"
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        try:
            os.link(out, latest)
        except OSError:
            latest.symlink_to(out.name)
    except OSError:
        pass
    return out


def next_backup_delay_seconds(
    backup_dir: Path,
    *,
    interval_seconds: int,
    start_delay_seconds: int,
    now: float | None = None,
) -> int:
    """Return the restart delay without postponing an already-due backup."""
    interval = max(0, int(interval_seconds))
    start_delay = max(0, int(start_delay_seconds))
    latest = backup_dir / "pm_robot-latest.sqlite"
    try:
        latest_mtime = latest.stat().st_mtime
    except FileNotFoundError:
        # Timestamped files without the verified marker may be interrupted backups.
        try:
            latest_is_symlink = latest.is_symlink()
        except OSError:
            latest_is_symlink = False
        has_unverified_artifact = (
            latest_is_symlink
            or any(
                path.name != latest.name
                for path in backup_dir.glob("pm_robot-*.sqlite")
            )
            or any(backup_dir.glob("pm_robot-*.sqlite.partial"))
        )
        return 0 if has_unverified_artifact else start_delay
    current_time = time.time() if now is None else float(now)
    age = max(0, int(current_time - latest_mtime))
    return max(0, interval - age)


def dump_database_sql(settings: RobotSettings, output: BinaryIO) -> None:
    """Stream a consistent SQL dump without creating a large local backup file."""
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
    positions_days: int = 14,
    scores_days: int = 30,
    review_events_days: int = 30,
    ingest_runs_days: int = 30,
    runtime_heartbeat_days: int = 30,
    keep_backups: int = 2,
    dry_run: bool = False,
    vacuum: bool = False,
    wal_checkpoint: str = "none",
    skip_cleanup: bool = False,
    reset_stale_jobs: bool = False,
    failed_job_cooldown_seconds: int = DEFAULT_FAILED_JOB_COOLDOWN_SECONDS,
    reset_stale_ingest_runs: bool = False,
    stale_ingest_run_seconds: int = 21_600,
) -> dict[str, Any]:
    wal_checkpoint = wal_checkpoint.lower()
    if wal_checkpoint not in WAL_CHECKPOINT_MODES:
        raise ValueError(f"wal_checkpoint must be one of: {', '.join(WAL_CHECKPOINT_MODES)}")
    storage_before = storage_report(settings)
    conn = connect(settings.db_path)
    try:
        if skip_cleanup:
            deleted: dict[str, int] = {}
            runtime_heartbeat_cleanup = _cleanup_runtime_heartbeats(
                conn,
                days=runtime_heartbeat_days,
                dry_run=dry_run,
            )
        else:
            run_migrations(conn)
            deleted = _cleanup_database(
                conn,
                api_log_days=api_log_days,
                positions_days=positions_days,
                scores_days=scores_days,
                review_events_days=review_events_days,
                ingest_runs_days=ingest_runs_days,
                dry_run=dry_run,
            )
            runtime_heartbeat_cleanup = _cleanup_runtime_heartbeats(
                conn,
                days=runtime_heartbeat_days,
                dry_run=dry_run,
            )
        if not dry_run:
            if not skip_cleanup:
                conn.execute("PRAGMA optimize")
            if vacuum:
                conn.execute("VACUUM")
            conn.commit()
        stale_jobs = _reset_stale_pipeline_jobs(
            conn,
            execute=reset_stale_jobs and not dry_run,
            failed_job_cooldown_seconds=failed_job_cooldown_seconds,
        )
        duplicate_running_jobs = _reset_duplicate_running_pipeline_jobs(
            conn,
            execute=reset_stale_jobs and not dry_run,
            failed_job_cooldown_seconds=failed_job_cooldown_seconds,
        )
        exhausted_queued_jobs = _fail_exhausted_queued_pipeline_jobs(
            conn,
            execute=reset_stale_jobs and not dry_run,
            failed_job_cooldown_seconds=failed_job_cooldown_seconds,
        )
        stale_ingest_runs = _reset_stale_ingest_runs(
            conn,
            execute=reset_stale_ingest_runs and not dry_run,
            stale_seconds=stale_ingest_run_seconds,
        )
        wal_checkpoint_report = _wal_checkpoint(conn, mode=wal_checkpoint, dry_run=dry_run)
    finally:
        conn.close()
    backup_cleanup = cleanup_backups(settings.backup_dir, keep=keep_backups, dry_run=dry_run)
    return {
        "ok": True,
        "dry_run": dry_run,
        "vacuum": vacuum and not dry_run,
        "cleanup_skipped": skip_cleanup,
        "failed_job_cooldown_seconds": max(0, int(failed_job_cooldown_seconds)),
        "wal_checkpoint": wal_checkpoint_report,
        "stale_jobs": stale_jobs,
        "duplicate_running_jobs": duplicate_running_jobs,
        "exhausted_queued_jobs": exhausted_queued_jobs,
        "stale_ingest_runs": stale_ingest_runs,
        "runtime_heartbeat_cleanup": runtime_heartbeat_cleanup,
        "deleted": deleted,
        "backup_cleanup": backup_cleanup,
        "storage_before": storage_before,
        "storage": storage_report(settings),
    }


def _cleanup_runtime_heartbeats(
    conn: sqlite3.Connection,
    *,
    days: int,
    dry_run: bool,
) -> dict[str, Any]:
    """Bound loop heartbeat retention without scanning business evidence tables."""

    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ingest_runs'"
    ).fetchone()
    retention_days = max(0, int(days))
    if not table_exists:
        return {
            "available": False,
            "executed": False,
            "days": retention_days,
            "matched": 0,
            "deleted": 0,
        }
    cutoff = int(time.time()) - retention_days * DAY_SECONDS
    matched = int(
        conn.execute(
            "SELECT COUNT(*) FROM ingest_runs WHERE ingest_type GLOB 'loop_*' AND started_at < ?",
            (cutoff,),
        ).fetchone()[0]
    )
    deleted = 0
    if not dry_run and matched:
        cur = conn.execute(
            "DELETE FROM ingest_runs WHERE ingest_type GLOB 'loop_*' AND started_at < ?",
            (cutoff,),
        )
        deleted = max(0, int(cur.rowcount))
        conn.commit()
    return {
        "available": True,
        "executed": not dry_run,
        "days": retention_days,
        "matched": matched,
        "deleted": deleted,
    }


def _reset_stale_pipeline_jobs(
    conn: sqlite3.Connection,
    *,
    execute: bool,
    failed_job_cooldown_seconds: int,
) -> dict[str, Any]:
    """Recover expired leases, failing jobs that already exhausted their attempts."""
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "pipeline_jobs" not in tables:
        return {
            "available": False,
            "reset": False,
            "total": 0,
            "requeued_count": 0,
            "failed_count": 0,
            "by_job_type": [],
        }
    now = int(time.time())
    failed_retry_at = now + max(0, int(failed_job_cooldown_seconds))
    rows = conn.execute(
        """
        SELECT
            job_type,
            COUNT(*) AS count,
            SUM(CASE WHEN attempts < max_attempts THEN 1 ELSE 0 END) AS requeued_count,
            SUM(CASE WHEN attempts >= max_attempts THEN 1 ELSE 0 END) AS failed_count
        FROM pipeline_jobs
        WHERE status = 'running'
          AND lease_until <= ?
        GROUP BY job_type
        ORDER BY job_type ASC
        """,
        (now,),
    ).fetchall()
    total = sum(int(row["count"] or 0) for row in rows)
    requeued_count = sum(int(row["requeued_count"] or 0) for row in rows)
    failed_count = sum(int(row["failed_count"] or 0) for row in rows)
    if execute and total:
        conn.execute(
            """
            UPDATE pipeline_jobs
            SET status = CASE
                    WHEN attempts >= max_attempts THEN 'failed'
                    ELSE 'queued'
                END,
                lease_owner = NULL,
                lease_until = 0,
                next_attempt_at = CASE
                    WHEN attempts >= max_attempts THEN MAX(next_attempt_at, ?)
                    ELSE 0
                END,
                last_error = CASE
                    WHEN last_error != '' THEN last_error
                    WHEN attempts >= max_attempts THEN 'expired_lease_attempts_exhausted_by_maintenance'
                    ELSE 'expired_lease_requeued_by_maintenance'
                END,
                updated_at = ?
            WHERE status = 'running'
              AND lease_until <= ?
            """,
            (failed_retry_at, now, now),
        )
        conn.commit()
    return {
        "available": True,
        "reset": bool(execute),
        "total": total,
        "requeued_count": requeued_count,
        "failed_count": failed_count,
        "by_job_type": [
            {"job_type": str(row["job_type"]), "count": int(row["count"] or 0)}
            for row in rows
        ],
    }


def _reset_duplicate_running_pipeline_jobs(
    conn: sqlite3.Connection,
    *,
    execute: bool,
    failed_job_cooldown_seconds: int,
) -> dict[str, Any]:
    """Recover duplicate worker leases without reviving exhausted jobs."""

    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "pipeline_jobs" not in tables:
        return {
            "available": False,
            "reset": False,
            "total": 0,
            "requeued_count": 0,
            "failed_count": 0,
            "by_job_type": [],
        }
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                job_id,
                job_type,
                attempts,
                max_attempts,
                ROW_NUMBER() OVER (
                    PARTITION BY job_type, lease_owner
                    ORDER BY updated_at DESC, job_id DESC
                ) AS owner_rank
            FROM pipeline_jobs
            WHERE status = 'running'
              AND lease_owner IS NOT NULL
              AND lease_owner != ''
        )
        SELECT
            job_type,
            COUNT(*) AS count,
            SUM(CASE WHEN attempts < max_attempts THEN 1 ELSE 0 END) AS requeued_count,
            SUM(CASE WHEN attempts >= max_attempts THEN 1 ELSE 0 END) AS failed_count
        FROM ranked
        WHERE owner_rank > 1
        GROUP BY job_type
        ORDER BY job_type ASC
        """
    ).fetchall()
    total = sum(int(row["count"] or 0) for row in rows)
    requeued_count = sum(int(row["requeued_count"] or 0) for row in rows)
    failed_count = sum(int(row["failed_count"] or 0) for row in rows)
    if execute and total:
        now = int(time.time())
        failed_retry_at = now + max(0, int(failed_job_cooldown_seconds))
        conn.execute(
            """
            WITH ranked AS (
                SELECT
                    job_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY job_type, lease_owner
                        ORDER BY updated_at DESC, job_id DESC
                    ) AS owner_rank
                FROM pipeline_jobs
                WHERE status = 'running'
                  AND lease_owner IS NOT NULL
                  AND lease_owner != ''
            )
            UPDATE pipeline_jobs
            SET status = CASE
                    WHEN attempts >= max_attempts THEN 'failed'
                    ELSE 'queued'
                END,
                lease_owner = NULL,
                lease_until = 0,
                next_attempt_at = CASE
                    WHEN attempts >= max_attempts THEN MAX(next_attempt_at, ?)
                    ELSE 0
                END,
                last_error = CASE
                    WHEN last_error != '' THEN last_error
                    WHEN attempts >= max_attempts THEN 'duplicate_running_owner_attempts_exhausted_by_maintenance'
                    ELSE 'duplicate_running_owner_requeued_by_maintenance'
                END,
                updated_at = ?
            WHERE job_id IN (
                SELECT job_id
                FROM ranked
                WHERE owner_rank > 1
            )
            """,
            (failed_retry_at, now),
        )
        conn.commit()
    return {
        "available": True,
        "reset": bool(execute),
        "total": total,
        "requeued_count": requeued_count,
        "failed_count": failed_count,
        "by_job_type": [
            {"job_type": str(row["job_type"]), "count": int(row["count"] or 0)}
            for row in rows
        ],
    }


def _fail_exhausted_queued_pipeline_jobs(
    conn: sqlite3.Connection,
    *,
    execute: bool,
    failed_job_cooldown_seconds: int,
) -> dict[str, Any]:
    """Close legacy queued jobs that no worker can claim and release queue capacity."""

    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "pipeline_jobs" not in tables:
        return {
            "available": False,
            "reset": False,
            "total": 0,
            "failed_count": 0,
            "by_job_type": [],
        }
    rows = conn.execute(
        """
        SELECT job_type, COUNT(*) AS count
        FROM pipeline_jobs
        WHERE status = 'queued'
          AND attempts >= max_attempts
        GROUP BY job_type
        ORDER BY job_type ASC
        """
    ).fetchall()
    total = sum(int(row["count"] or 0) for row in rows)
    if execute and total:
        now = int(time.time())
        failed_retry_at = now + max(0, int(failed_job_cooldown_seconds))
        conn.execute(
            """
            UPDATE pipeline_jobs
            SET status = 'failed',
                lease_owner = NULL,
                lease_until = 0,
                next_attempt_at = MAX(next_attempt_at, ?),
                last_error = CASE
                    WHEN last_error = '' THEN 'attempts_exhausted_marked_failed_by_maintenance'
                    ELSE last_error
                END,
                updated_at = ?
            WHERE status = 'queued'
              AND attempts >= max_attempts
            """,
            (failed_retry_at, now),
        )
        conn.commit()
    return {
        "available": True,
        "reset": bool(execute),
        "total": total,
        "failed_count": total,
        "by_job_type": [dict(row) for row in rows],
    }


def _reset_stale_ingest_runs(
    conn: sqlite3.Connection,
    *,
    execute: bool,
    stale_seconds: int,
) -> dict[str, Any]:
    """Close stale run audit rows without changing any work queue state."""

    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "ingest_runs" not in tables:
        return {
            "available": False,
            "reset": False,
            "total": 0,
            "stale_seconds": max(0, int(stale_seconds)),
            "by_ingest_type": [],
        }
    seconds = max(60, int(stale_seconds))
    now = int(time.time())
    cutoff = now - seconds
    rows = conn.execute(
        """
        SELECT ingest_type, COUNT(*) AS count
        FROM ingest_runs
        WHERE status = 'running'
          AND started_at <= ?
        GROUP BY ingest_type
        ORDER BY ingest_type ASC
        """,
        (cutoff,),
    ).fetchall()
    total = sum(int(row["count"] or 0) for row in rows)
    if execute and total:
        conn.execute(
            """
            UPDATE ingest_runs
            SET status = 'interrupted',
                finished_at = started_at + ?,
                error = CASE
                    WHEN error = '' THEN 'stale_running_marked_interrupted_by_maintenance'
                    ELSE error
                END
            WHERE status = 'running'
              AND started_at <= ?
            """,
            (seconds, cutoff),
        )
        conn.commit()
    return {
        "available": True,
        "reset": bool(execute),
        "total": total,
        "stale_seconds": seconds,
        "by_ingest_type": [dict(row) for row in rows],
    }


def prune_low_value_evidence(
    settings: RobotSettings,
    *,
    limit: int = 20,
    keep_recent_activity: int = 0,
    dry_run: bool = True,
    vacuum: bool = False,
    archive: bool = False,
    archive_dir: Path | None = None,
) -> dict[str, Any]:
    """Archive and prune raw evidence after a wallet decision is summarized.

    Candidate, feature, latest-score, source, and registry summary rows remain the
    durable control-plane record. When archive is enabled, verified Parquet files
    are committed before any raw event, backtest, or redundant score row is removed.
    """

    archive_root = archive_dir or settings.archive_dir
    archive_run: dict[str, Any] | None = None
    effective_keep_recent_activity = max(0, int(keep_recent_activity))
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        if archive and not dry_run:
            ensure_archive_backend()
            archive_run = resumable_archive_run(conn)
        wallets = (
            list(archive_run["wallets"])
            if archive_run is not None
            else _low_value_prune_wallets(conn, limit=limit)
        )
        if archive and not dry_run and archive_run is None and wallets:
            _materialize_wallet_registry(conn, limit=0, stages=(), addresses=tuple(wallets))
            archive_run = create_archive_run(
                conn,
                wallets,
                keep_recent_activity=effective_keep_recent_activity,
            )
            _stage_wallet_registry_archive(conn, wallets, run_id=str(archive_run["run_id"]))
            _cancel_wallet_evidence_backfill(conn, wallets)
            capture_archive_scope(
                conn,
                str(archive_run["run_id"]),
                wallets,
                keep_recent_activity=effective_keep_recent_activity,
            )
            conn.commit()
        elif not archive and not dry_run:
            _materialize_wallet_registry(conn, limit=0, stages=(), addresses=tuple(wallets))
            conn.commit()
    finally:
        conn.close()
    if archive_run is not None:
        effective_keep_recent_activity = int(archive_run.get("keep_recent_activity") or 0)

    archive_result: dict[str, Any] = {
        "enabled": archive,
        "status": "planned" if dry_run and archive else "disabled",
        "run_id": "",
        "manifest_path": "",
        "row_count": 0,
        "file_count": 0,
        "byte_size": 0,
    }
    if archive and not dry_run and archive_run is not None:
        archive_result = _complete_evidence_archive(
            settings,
            archive_root=archive_root,
            archive_run=archive_run,
            keep_recent_activity=effective_keep_recent_activity,
        )
        if not archive_result["ok"]:
            return {
                "ok": False,
                "dry_run": False,
                "vacuum": False,
                "wallets": wallets,
                "wallet_count": len(wallets),
                "deleted": _empty_evidence_delete_counts(),
                "archive": archive_result,
                "storage": storage_report(settings),
            }

    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        conn.execute("PRAGMA foreign_keys = OFF")
        deleted = _prune_wallet_evidence_batch(
            conn,
            wallets,
            keep_recent_activity=effective_keep_recent_activity,
            dry_run=dry_run,
            archive_run_id=str(archive_result.get("run_id") or ""),
        )
        if not dry_run:
            raw_artifact_uri = ""
            archive_run_id = ""
            if archive_result.get("status") == "verified":
                archive_run_id = str(archive_result["run_id"])
                raw_artifact_uri = f"parquet://{archive_result['manifest_path']}"
            residual = _remaining_wallet_evidence_counts(
                conn,
                wallets,
                keep_recent_activity=effective_keep_recent_activity,
            )
            if not any(residual.values()):
                _mark_wallet_registry_pruned(
                    conn,
                    wallets,
                    archive_run_id=archive_run_id,
                )
            _cancel_wallet_evidence_backfill(
                conn,
                wallets,
                raw_artifact_uri_prefix="parquet-wallet://" if raw_artifact_uri else "",
            )
            if archive_run_id:
                final_archive_status = "pruned_partial" if any(residual.values()) else "pruned"
                set_archive_run_status(conn, archive_run_id, final_archive_status)
                archive_result["status"] = final_archive_status
                archive_result["residual"] = residual
            conn.commit()
        else:
            conn.rollback()
        conn.execute("PRAGMA foreign_keys = ON")
        if vacuum and not dry_run:
            conn.execute("VACUUM")
    finally:
        conn.close()
    return {
        "ok": True,
        "dry_run": dry_run,
        "vacuum": vacuum and not dry_run,
        "wallets": wallets,
        "wallet_count": len(wallets),
        "deleted": deleted,
        "archive": archive_result,
        "storage": storage_report(settings),
    }


def _complete_evidence_archive(
    settings: RobotSettings,
    *,
    archive_root: Path,
    archive_run: dict[str, Any],
    keep_recent_activity: int,
) -> dict[str, Any]:
    """Export or resume one archive run; a failure leaves SQLite evidence untouched."""

    run_id = str(archive_run["run_id"])
    try:
        if str(archive_run.get("status") or "") == "verified" and archive_run.get("manifest_path"):
            manifest = verify_archive_manifest(archive_root, str(archive_run["manifest_path"]))
            manifest["manifest_path"] = str(archive_run["manifest_path"])
        else:
            conn = connect(settings.db_path)
            try:
                set_archive_run_status(conn, run_id, "exporting")
                conn.commit()
            finally:
                conn.close()
            manifest = export_evidence_archive(
                settings.db_path,
                archive_root,
                run_id=run_id,
                archive_path=str(archive_run["archive_path"]),
                wallets=list(archive_run["wallets"]),
                keep_recent_activity=keep_recent_activity,
            )
            conn = connect(settings.db_path)
            try:
                register_archive_manifest(conn, manifest)
                conn.commit()
            finally:
                conn.close()
        return {
            "ok": True,
            "enabled": True,
            "status": "verified",
            "run_id": run_id,
            "manifest_path": str(manifest["manifest_path"]),
            "row_count": int(manifest["row_count"]),
            "file_count": int(manifest["file_count"]),
            "byte_size": int(manifest["byte_size"]),
        }
    except Exception as exc:
        conn = connect(settings.db_path)
        try:
            set_archive_run_status(conn, run_id, "failed", error=str(exc))
            conn.commit()
        finally:
            conn.close()
        return {
            "ok": False,
            "enabled": True,
            "status": "failed",
            "run_id": run_id,
            "manifest_path": "",
            "row_count": 0,
            "file_count": 0,
            "byte_size": 0,
            "error": str(exc),
        }


def _materialize_wallet_registry(
    conn: sqlite3.Connection,
    *,
    limit: int = 0,
    stages: tuple[str, ...] = (),
    addresses: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    now = int(time.time())
    source_rows = _wallet_registry_source_rows(
        conn,
        limit=limit,
        stages=stages,
        addresses=addresses,
    )
    archived_rows = {
        str(row["address"]): dict(row)
        for row in conn.execute(
            "SELECT * FROM wallet_registry WHERE registry_status = 'archived_raw_pruned'"
        ).fetchall()
    }
    rows = []
    for source_row in source_rows:
        row = dict(source_row)
        archived = archived_rows.get(str(row["address"]))
        rows.append(archived if archived is not None else _wallet_registry_row(row, now=now))
    if not rows:
        return []
    placeholders = ", ".join("?" for _ in WALLET_REGISTRY_TABLE_COLUMNS)
    assignments = ", ".join(
        f"{column} = excluded.{column}"
        for column in WALLET_REGISTRY_TABLE_COLUMNS
        if column != "address"
    )
    conn.executemany(
        f"""
        INSERT INTO wallet_registry({", ".join(WALLET_REGISTRY_TABLE_COLUMNS)})
        VALUES ({placeholders})
        ON CONFLICT(address) DO UPDATE SET {assignments}
        """,
        [
            tuple(row.get(column) for column in WALLET_REGISTRY_TABLE_COLUMNS)
            for row in rows
        ],
    )
    return rows


def _wallet_registry_source_rows(
    conn: sqlite3.Connection,
    *,
    limit: int,
    stages: tuple[str, ...],
    addresses: tuple[str, ...] = (),
) -> list[sqlite3.Row]:
    predicates: list[str] = []
    params: list[Any] = []
    if stages:
        predicates.append(f"cw.candidate_stage IN ({', '.join('?' for _ in stages)})")
        params.extend(stages)
    if addresses:
        predicates.append(f"cw.address IN ({', '.join('?' for _ in addresses)})")
        params.extend(address.lower() for address in addresses)
    where = f"WHERE {' AND '.join(predicates)}" if predicates else ""
    limit_sql = ""
    if limit > 0:
        limit_sql = "LIMIT ?"
        params.append(limit)
    return conn.execute(
        f"""
        SELECT
            cw.address,
            cw.sources,
            cw.labels,
            cw.notes,
            cw.links,
            cw.status,
            cw.candidate_stage,
            cw.first_seen_at,
            cw.updated_at AS candidate_updated_at,
            wf.cumulative_win_rate,
            wf.recent_30d_volume_usdc,
            wf.net_pnl_usdc,
            wf.total_volume_usdc,
            wf.event_win_rate,
            wf.trade_win_rate,
            wf.avg_dca_entries,
            wf.sell_pct,
            wf.bot_score,
            wf.trades_per_day,
            wf.median_gap_sec,
            wf.maker_fraction,
            wf.leader_in_degree,
            wf.copy_event_count,
            wf.copy_market_count,
            wf.containment_pct_median,
            wf.copy_stream_roi,
            wf.edge_retention_pct,
            wf.walk_forward_consistency_pct,
            wf.survival_score,
            wf.single_market_pnl_share,
            wf.net_to_gross_exposure,
            COALESCE(wf.hygiene_status, '') AS hygiene_status,
            COALESCE(wf.primary_category, '') AS primary_category,
            wf.last_active_days_ago,
            COALESCE(wf.extra_json, '{{}}') AS feature_extra_json,
            wf.updated_at AS feature_updated_at,
            COALESCE(ls.leader_score, 0) AS leader_score,
            COALESCE(ls.review_stage, '') AS review_stage,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(ls.policy_version, '') AS policy_version,
            COALESCE(ls.scored_at, 0) AS scored_at,
            COALESCE(ls.components_json, '{{}}') AS components_json,
            COALESCE(ls.penalties_json, '{{}}') AS penalties_json,
            COALESCE(ebb.stage, '') AS evidence_stage,
            COALESCE(ebb.target_depth, 0) AS evidence_target_depth,
            COALESCE(ebb.current_depth, 0) AS evidence_current_depth,
            COALESCE(ebb.stop_reason, '') AS evidence_stop_reason,
            COALESCE(ebb.error_count, 0) AS evidence_error_count,
            COALESCE(ebb.evidence_json, '{{}}') AS evidence_backfill_json,
            ebb.updated_at AS evidence_updated_at,
            COALESCE(clp.backtest_trade_count, 0) AS copy_backtest_trade_count,
            COALESCE(clp.copied_market_count, 0) AS copy_backtest_market_count,
            clp.total_stake_usdc AS copy_backtest_stake_usdc,
            clp.gross_pnl_usdc AS copy_backtest_gross_pnl_usdc,
            clp.net_pnl_usdc AS copy_backtest_net_pnl_usdc,
            clp.gross_roi AS copy_backtest_gross_roi,
            clp.net_roi AS copy_backtest_net_roi,
            clp.win_rate AS copy_backtest_win_rate,
            clp.median_lag_seconds AS copy_backtest_median_lag_seconds,
            clp.last_backtest_trade_at AS copy_backtest_last_trade_at,
            clp.updated_at AS copy_backtest_updated_at,
            COALESCE(pwq.orders, 0) AS paper_orders,
            COALESCE(pwq.open_positions, 0) AS paper_open_positions,
            COALESCE(pwq.settled_positions, 0) AS paper_settled_positions,
            pwq.mark_coverage AS paper_mark_coverage,
            pwq.settled_cost_usd AS paper_settled_cost_usd,
            pwq.settled_pnl_usd AS paper_settled_pnl_usd,
            pwq.settled_roi AS paper_settled_roi,
            pwq.total_pnl_usd AS paper_total_pnl_usd,
            pwq.total_roi AS paper_total_roi,
            COALESCE(pwq.production_ready, 0) AS production_ready,
            COALESCE(pwq.blockers_json, '[]') AS paper_blockers_json,
            pwq.updated_at AS paper_updated_at,
            COALESCE(lp.status, '') AS publish_status,
            COALESCE(lp.publish_stage, '') AS publish_stage,
            lp.published_at AS published_at,
            lp.expires_at AS publish_expires_at,
            COALESCE(existing_wr.registry_status, '') AS existing_registry_status,
            COALESCE(existing_wr.raw_retention_tier, '') AS existing_raw_retention_tier,
            (
                SELECT COUNT(*)
                FROM wallet_activity wa
                WHERE wa.address = cw.address
            ) AS activity_count,
            (
                SELECT MIN(wa.timestamp)
                FROM wallet_activity wa
                WHERE wa.address = cw.address
            ) AS oldest_activity_ts,
            (
                SELECT MAX(wa.timestamp)
                FROM wallet_activity wa
                WHERE wa.address = cw.address
            ) AS newest_activity_ts,
            (
                SELECT COUNT(*)
                FROM wallet_episodes we
                WHERE we.address = cw.address
            ) AS episode_count,
            (
                SELECT COUNT(*)
                FROM candidate_source_events cse
                WHERE cse.address = cw.address
            ) AS source_event_count,
            (
                SELECT MIN(cse.observed_at)
                FROM candidate_source_events cse
                WHERE cse.address = cw.address
            ) AS first_source_observed_at,
            (
                SELECT MAX(cse.observed_at)
                FROM candidate_source_events cse
                WHERE cse.address = cw.address
            ) AS latest_source_observed_at
        FROM candidate_wallets cw
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN leader_scores ls
          ON ls.score_id = (
              SELECT score_id
              FROM leader_scores
              WHERE address = cw.address
              ORDER BY scored_at DESC, score_id DESC
              LIMIT 1
          )
        LEFT JOIN evidence_backfill_budget ebb
          ON ebb.wallet = cw.address
        LEFT JOIN copy_leader_performance clp
          ON clp.leader_wallet = cw.address
        LEFT JOIN paper_wallet_quality pwq
          ON pwq.wallet = cw.address
        LEFT JOIN leader_publish lp
          ON lp.wallet = cw.address
         AND lp.revoked_at IS NULL
         AND lp.expires_at > strftime('%s','now')
        LEFT JOIN wallet_registry existing_wr
          ON existing_wr.address = cw.address
        {where}
        ORDER BY
            CASE cw.candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                WHEN 'needs_manual_review' THEN 3
                WHEN 'needs_data' THEN 4
                ELSE 5
            END ASC,
            COALESCE(ls.leader_score, 0) DESC,
            cw.updated_at DESC,
            cw.address ASC
        {limit_sql}
        """,
        params,
    ).fetchall()


def _wallet_registry_row(row: dict[str, Any], *, now: int) -> dict[str, Any]:
    feature_extra = _json_dict(row.get("feature_extra_json"))
    components = _json_dict(row.get("components_json"))
    penalties = _json_dict(row.get("penalties_json"))
    paper_blockers = _json_list(row.get("paper_blockers_json"))
    source = {
        "sources": row.get("sources") or "",
        "labels": row.get("labels") or "",
        "notes": row.get("notes") or "",
        "links": row.get("links") or "",
        "status": row.get("status") or "",
        "source_event_count": int(row.get("source_event_count") or 0),
        "first_source_observed_at": row.get("first_source_observed_at"),
        "latest_source_observed_at": row.get("latest_source_observed_at"),
    }
    feature = {
        "cumulative_win_rate": row.get("cumulative_win_rate"),
        "recent_30d_volume_usdc": row.get("recent_30d_volume_usdc"),
        "net_pnl_usdc": row.get("net_pnl_usdc"),
        "total_volume_usdc": row.get("total_volume_usdc"),
        "event_win_rate": row.get("event_win_rate"),
        "trade_win_rate": row.get("trade_win_rate"),
        "avg_dca_entries": row.get("avg_dca_entries"),
        "sell_pct": row.get("sell_pct"),
        "bot_score": row.get("bot_score"),
        "trades_per_day": row.get("trades_per_day"),
        "median_gap_sec": row.get("median_gap_sec"),
        "maker_fraction": row.get("maker_fraction"),
        "leader_in_degree": row.get("leader_in_degree"),
        "copy_event_count": row.get("copy_event_count"),
        "copy_market_count": row.get("copy_market_count"),
        "containment_pct_median": row.get("containment_pct_median"),
        "copy_stream_roi": row.get("copy_stream_roi"),
        "edge_retention_pct": row.get("edge_retention_pct"),
        "walk_forward_consistency_pct": row.get("walk_forward_consistency_pct"),
        "survival_score": row.get("survival_score"),
        "single_market_pnl_share": row.get("single_market_pnl_share"),
        "net_to_gross_exposure": row.get("net_to_gross_exposure"),
        "hygiene_status": row.get("hygiene_status") or "",
        "primary_category": row.get("primary_category") or "",
        "last_active_days_ago": row.get("last_active_days_ago"),
        "extra": feature_extra,
    }
    score = {
        "leader_score": float(row.get("leader_score") or 0),
        "review_stage": row.get("review_stage") or "",
        "review_reason": row.get("review_reason") or "",
        "policy_version": row.get("policy_version") or "",
        "scored_at": int(row.get("scored_at") or 0),
        "components": components,
        "penalties": penalties,
    }
    evidence = {
        "stage": row.get("evidence_stage") or "",
        "target_depth": int(row.get("evidence_target_depth") or 0),
        "current_depth": int(row.get("evidence_current_depth") or 0),
        "stop_reason": row.get("evidence_stop_reason") or "",
        "error_count": int(row.get("evidence_error_count") or 0),
        "activity_count": int(row.get("activity_count") or 0),
        "episode_count": int(row.get("episode_count") or 0),
        "oldest_activity_ts": row.get("oldest_activity_ts"),
        "newest_activity_ts": row.get("newest_activity_ts"),
        "backfill": _json_dict(row.get("evidence_backfill_json")),
        "updated_at": row.get("evidence_updated_at"),
    }
    paper = {
        "orders": int(row.get("paper_orders") or 0),
        "open_positions": int(row.get("paper_open_positions") or 0),
        "settled_positions": int(row.get("paper_settled_positions") or 0),
        "mark_coverage": row.get("paper_mark_coverage"),
        "settled_cost_usd": row.get("paper_settled_cost_usd"),
        "settled_pnl_usd": row.get("paper_settled_pnl_usd"),
        "settled_roi": row.get("paper_settled_roi"),
        "total_pnl_usd": row.get("paper_total_pnl_usd"),
        "total_roi": row.get("paper_total_roi"),
        "production_ready": bool(int(row.get("production_ready") or 0)),
        "blockers": paper_blockers,
        "updated_at": row.get("paper_updated_at"),
    }
    copy_backtest = {
        "trade_count": int(row.get("copy_backtest_trade_count") or 0),
        "market_count": int(row.get("copy_backtest_market_count") or 0),
        "stake_usdc": row.get("copy_backtest_stake_usdc"),
        "gross_pnl_usdc": row.get("copy_backtest_gross_pnl_usdc"),
        "net_pnl_usdc": row.get("copy_backtest_net_pnl_usdc"),
        "gross_roi": row.get("copy_backtest_gross_roi"),
        "net_roi": row.get("copy_backtest_net_roi"),
        "win_rate": row.get("copy_backtest_win_rate"),
        "median_lag_seconds": row.get("copy_backtest_median_lag_seconds"),
        "last_trade_at": row.get("copy_backtest_last_trade_at"),
        "updated_at": row.get("copy_backtest_updated_at"),
    }
    tags = _wallet_registry_tags(row, feature=feature, score=score, evidence=evidence, paper=paper)
    registry_status, retention_tier = _wallet_registry_status(
        row,
        feature=feature,
        score=score,
        evidence=evidence,
        paper=paper,
        tags=tags,
    )
    blockers = _wallet_registry_blockers(row, score=score, paper=paper)
    summary = {
        "action": registry_status,
        "raw_retention_tier": retention_tier,
        "generated_at": now,
        "publish_status": row.get("publish_status") or "",
        "publish_stage": row.get("publish_stage") or "",
        "published_at": row.get("published_at"),
        "publish_expires_at": row.get("publish_expires_at"),
    }
    return {
        "address": row["address"],
        "candidate_stage": row.get("candidate_stage") or "",
        "registry_status": registry_status,
        "raw_retention_tier": retention_tier,
        "leader_score": float(row.get("leader_score") or 0),
        "review_stage": row.get("review_stage") or "",
        "review_reason": row.get("review_reason") or "",
        "policy_version": row.get("policy_version") or "",
        "scored_at": int(row.get("scored_at") or 0),
        "total_volume_usdc": row.get("total_volume_usdc"),
        "recent_30d_volume_usdc": row.get("recent_30d_volume_usdc"),
        "net_pnl_usdc": row.get("net_pnl_usdc"),
        "event_win_rate": row.get("event_win_rate"),
        "trade_win_rate": row.get("trade_win_rate"),
        "copy_stream_roi": row.get("copy_stream_roi"),
        "copy_backtest_net_pnl_usdc": row.get("copy_backtest_net_pnl_usdc"),
        "edge_retention_pct": row.get("edge_retention_pct"),
        "walk_forward_consistency_pct": row.get("walk_forward_consistency_pct"),
        "hygiene_status": row.get("hygiene_status") or "",
        "primary_category": row.get("primary_category") or "",
        "evidence_stage": row.get("evidence_stage") or "",
        "activity_count": int(row.get("activity_count") or 0),
        "oldest_activity_ts": row.get("oldest_activity_ts"),
        "newest_activity_ts": row.get("newest_activity_ts"),
        "paper_orders": int(row.get("paper_orders") or 0),
        "paper_settled_positions": int(row.get("paper_settled_positions") or 0),
        "paper_total_roi": row.get("paper_total_roi"),
        "paper_settled_roi": row.get("paper_settled_roi"),
        "production_ready": int(row.get("production_ready") or 0),
        "tags_json": _json_dump(tags),
        "blockers_json": _json_dump(blockers),
        "source_json": _json_dump(source),
        "feature_json": _json_dump(feature),
        "score_json": _json_dump(score),
        "evidence_json": _json_dump({**evidence, "copy_backtest": copy_backtest}),
        "paper_json": _json_dump(paper),
        "summary_json": _json_dump(summary),
        "last_evaluated_at": now,
        "updated_at": now,
    }


def _wallet_registry_status(
    row: dict[str, Any],
    *,
    feature: dict[str, Any],
    score: dict[str, Any],
    evidence: dict[str, Any],
    paper: dict[str, Any],
    tags: list[str],
) -> tuple[str, str]:
    existing_status = str(row.get("existing_registry_status") or "")
    if existing_status in {"archive_pending", "archived_raw_pruned"}:
        return existing_status, "summary_only"
    stage = str(row.get("candidate_stage") or "")
    review_reason = str(score.get("review_reason") or "")
    leader_score = float(score.get("leader_score") or 0)
    publish_status = str(row.get("publish_status") or "")
    feature_extra = feature.get("extra") if isinstance(feature.get("extra"), dict) else {}
    feature_materialized = "feature_materializer_version" in feature_extra
    if publish_status:
        return "published_or_exported", "keep_full"
    if stage == "live_eligible":
        return "ready_for_external_validation", "keep_full"
    if stage in {"paper_approved", "paper_candidate"}:
        return "paper_follow_validation", "keep_full"
    if stage == "needs_manual_review":
        return "manual_review", "keep_full" if leader_score >= 50 else "summary_and_recent"
    if stage == "blocked_hygiene":
        return "blocked_hygiene", "summary_only"
    if stage == "blocked_copyability":
        return "blocked_copyability", "summary_only"
    if stage == "rejected":
        return "rejected", "summary_only"
    if stage == "needs_data":
        if leader_score <= 0 and feature_materialized and evidence.get("stage") not in {
            "light_pending",
            "medium_pending",
            "deep_pending",
        }:
            return "archive_low_value", "summary_only"
        if evidence.get("stage") in {"light_pending", "medium_pending", "deep_pending"}:
            return "needs_evidence_backfill", "summary_and_recent"
        if "low_profit" in tags or "low_volume" in tags:
            return "archive_low_value", "summary_only"
        return "needs_more_scoring_data", "summary_and_recent"
    if paper.get("production_ready"):
        return "ready_for_external_validation", "keep_full"
    return "observe", "summary_and_recent"


def _wallet_registry_tags(
    row: dict[str, Any],
    *,
    feature: dict[str, Any],
    score: dict[str, Any],
    evidence: dict[str, Any],
    paper: dict[str, Any],
) -> list[str]:
    tags: list[str] = []
    stage = str(row.get("candidate_stage") or "")
    if stage:
        tags.append(stage)
    leader_score = float(score.get("leader_score") or 0)
    if leader_score >= 60:
        tags.append("high_score")
    elif leader_score >= 50:
        tags.append("watchlist_score")
    elif leader_score >= 40:
        tags.append("borderline_score")
    net_pnl = _float_or_none(feature.get("net_pnl_usdc"))
    total_volume = _float_or_none(feature.get("total_volume_usdc"))
    recent_volume = _float_or_none(feature.get("recent_30d_volume_usdc"))
    if net_pnl is not None and net_pnl < 50:
        tags.append("low_profit")
    if total_volume is not None and total_volume < 1000:
        tags.append("low_volume")
    if recent_volume is not None and recent_volume < 500:
        tags.append("low_recent_volume")
    hygiene = str(feature.get("hygiene_status") or "").lower()
    if hygiene in {"clean", "screened"}:
        tags.append("hygiene_clean")
    elif hygiene:
        tags.append(f"hygiene_{hygiene}")
    evidence_stage = str(evidence.get("stage") or "")
    if evidence_stage:
        tags.append(f"evidence_{evidence_stage}")
    copy_backtest_pnl = _float_or_none(row.get("copy_backtest_net_pnl_usdc"))
    if copy_backtest_pnl is not None and copy_backtest_pnl > 0:
        tags.append("positive_copy_backtest")
    if paper.get("production_ready"):
        tags.append("paper_production_ready")
    paper_roi = _float_or_none(paper.get("total_roi"))
    if paper_roi is not None and paper_roi > 0:
        tags.append("positive_paper_roi")
    if evidence_stage == "paused_fast_market_specialist":
        tags.append("fast_market_specialist")
    return sorted(dict.fromkeys(tag for tag in tags if tag))


def _wallet_registry_blockers(
    row: dict[str, Any],
    *,
    score: dict[str, Any],
    paper: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    reason = str(score.get("review_reason") or "")
    if reason and reason not in {"watchlist_score", "borderline_score"}:
        blockers.append(f"score:{reason}")
    blockers.extend(f"paper:{item}" for item in paper.get("blockers", []) if item)
    stage = str(row.get("candidate_stage") or "")
    if stage.startswith("blocked_"):
        blockers.append(stage)
    return sorted(dict.fromkeys(blockers))


def _write_wallet_registry_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=WALLET_REGISTRY_EXPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in WALLET_REGISTRY_EXPORT_COLUMNS})


def _write_wallet_registry_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "")
        out[value] = out.get(value, 0) + 1
    return out


def _wallet_registry_refresh_needed(
    conn: sqlite3.Connection,
    *,
    max_age_seconds: int = 0,
) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM wallet_registry
        """
    ).fetchone()
    if row is None or int(row["count"] or 0) == 0:
        return True
    return False


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def storage_report(settings: RobotSettings) -> dict[str, Any]:
    db_size = _path_size(settings.db_path)
    wal_size = _path_size(settings.db_path.with_name(settings.db_path.name + "-wal"))
    shm_size = _path_size(settings.db_path.with_name(settings.db_path.name + "-shm"))
    # The latest path is a hard link or symlink to a timestamped restore point.
    backup_files = sorted(
        path
        for path in settings.backup_dir.glob("pm_robot-*.sqlite")
        if path.name != "pm_robot-latest.sqlite"
    )
    backup_size = sum(_path_size(path) for path in backup_files)
    usage = shutil.disk_usage(settings.db_path.parent if settings.db_path.parent.exists() else Path("."))
    return {
        "db_size_mb": round(db_size / 1_048_576, 2),
        "db_wal_mb": round(wal_size / 1_048_576, 2),
        "db_shm_mb": round(shm_size / 1_048_576, 2),
        "backup_count": len(backup_files),
        "backup_size_mb": round(backup_size / 1_048_576, 2),
        "free_disk_gb": round(usage.free / 1_073_741_824, 2),
        "total_disk_gb": round(usage.total / 1_073_741_824, 2),
    }


def _wal_checkpoint(
    conn: sqlite3.Connection,
    *,
    mode: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Optionally checkpoint the SQLite WAL without changing cleanup semantics."""

    if mode == "none":
        return {
            "mode": mode,
            "executed": False,
            "skipped_reason": "not_requested",
            "busy": None,
            "log_frames": None,
            "checkpointed_frames": None,
        }
    if dry_run:
        return {
            "mode": mode,
            "executed": False,
            "skipped_reason": "dry_run",
            "busy": None,
            "log_frames": None,
            "checkpointed_frames": None,
        }
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
            "busy": None,
            "log_frames": None,
            "checkpointed_frames": None,
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


def cleanup_backups(backup_dir: Path, *, keep: int, dry_run: bool = False) -> dict[str, Any]:
    backups = sorted(
        (path for path in backup_dir.glob("pm_robot-*.sqlite") if path.name != "pm_robot-latest.sqlite"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    to_delete = backups[max(keep, 0):]
    bytes_to_delete = sum(_path_size(path) for path in to_delete)
    if not dry_run:
        for path in to_delete:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
    return {
        "kept": min(len(backups), max(keep, 0)),
        "deleted": len(to_delete),
        "deleted_mb": round(bytes_to_delete / 1_048_576, 2),
    }


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _cleanup_database(
    conn: sqlite3.Connection,
    *,
    api_log_days: int,
    positions_days: int,
    scores_days: int,
    review_events_days: int,
    ingest_runs_days: int,
    dry_run: bool,
) -> dict[str, int]:
    now = int(time.time())
    specs = {
        "api_request_log": ("ts", now - api_log_days * DAY_SECONDS),
        "wallet_positions": ("captured_at", now - positions_days * DAY_SECONDS),
        "leader_scores": ("scored_at", now - scores_days * DAY_SECONDS),
        "review_events": ("created_at", now - review_events_days * DAY_SECONDS),
        "ingest_runs": ("started_at", now - ingest_runs_days * DAY_SECONDS),
        "paper_marks": ("marked_at", now - 30 * DAY_SECONDS),
        "paper_readiness_observations": ("observed_at", now - 90 * DAY_SECONDS),
    }
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    deleted: dict[str, int] = {}
    for table, (column, cutoff) in specs.items():
        if table not in tables:
            deleted[table] = 0
            continue
        count = int(
            conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} < ?", (cutoff,)).fetchone()[0]
        )
        deleted[table] = count
        if count and not dry_run:
            conn.execute(f"DELETE FROM {table} WHERE {column} < ?", (cutoff,))
    if not dry_run:
        conn.commit()
    return deleted


def _low_value_prune_wallets(conn: sqlite3.Connection, *, limit: int) -> list[str]:
    if limit <= 0:
        return []
    terminal_rows = conn.execute(
        """
        SELECT cw.address
        FROM candidate_wallets cw
        LEFT JOIN wallet_registry wr
          ON wr.address = cw.address
        WHERE cw.candidate_stage IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND wr.raw_pruned_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM paper_wallet_quality pwq
              WHERE pwq.wallet = cw.address
                AND pwq.production_ready = 1
          )
          AND NOT EXISTS (
              SELECT 1
              FROM leader_publish lp
              WHERE lp.wallet = cw.address
                AND lp.revoked_at IS NULL
                AND lp.expires_at > strftime('%s','now')
          )
        ORDER BY
          cw.updated_at ASC,
          cw.address ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    wallets = [str(row["address"]) for row in terminal_rows]
    remaining = limit - len(wallets)
    if remaining <= 0:
        return wallets
    needs_data_rows = conn.execute(
        """
        SELECT cw.address
        FROM candidate_wallets cw
        JOIN wallet_features wf
          ON wf.address = cw.address
        JOIN leader_latest_scores ls
          ON ls.address = cw.address
        LEFT JOIN evidence_backfill_budget ebb
          ON ebb.wallet = cw.address
        LEFT JOIN wallet_registry wr
          ON wr.address = cw.address
        WHERE cw.candidate_stage = 'needs_data'
          AND wr.raw_pruned_at IS NULL
          AND ls.review_stage = 'needs_data'
          AND ls.leader_score = 0
          AND instr(COALESCE(wf.extra_json, '{}'), 'feature_materializer_version') > 0
          AND COALESCE(ebb.stage, '') IN (
              '',
              'light_done',
              'medium_done',
              'paused_fast_market_specialist',
              'raw_pruned'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM paper_wallet_quality pwq
              WHERE pwq.wallet = cw.address
                AND (pwq.production_ready = 1 OR pwq.total_roi > 0)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM leader_publish lp
              WHERE lp.wallet = cw.address
                AND lp.revoked_at IS NULL
                AND lp.expires_at > strftime('%s','now')
          )
        ORDER BY
          CASE COALESCE(ebb.stage, '')
              WHEN 'paused_fast_market_specialist' THEN 0
              WHEN 'light_done' THEN 1
              ELSE 2
          END ASC,
          cw.updated_at ASC,
          cw.address ASC
        LIMIT ?
        """,
        (remaining,),
    ).fetchall()
    wallets.extend(str(row["address"]) for row in needs_data_rows)
    return wallets


def _prune_wallet_evidence_batch(
    conn: sqlite3.Connection,
    wallets: list[str],
    *,
    keep_recent_activity: int,
    dry_run: bool,
    archive_run_id: str = "",
) -> dict[str, int]:
    deleted = _empty_evidence_delete_counts()
    if not wallets:
        return deleted

    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    prepare_prune_temp_tables(
        conn,
        wallets,
        keep_recent_activity=keep_recent_activity,
    )
    for spec in EVIDENCE_TABLE_SPECS:
        if spec.table not in tables:
            continue
        if dry_run:
            deleted[spec.table] = int(
                conn.execute(
                    f'SELECT COUNT(*) FROM "{spec.table}" WHERE {spec.where_sql}'
                ).fetchone()[0]
            )
            continue
        changes_before = conn.total_changes
        if archive_run_id:
            conn.execute(
                f"""
                DELETE FROM "{spec.table}"
                WHERE rowid IN (
                    SELECT row_id
                    FROM evidence_archive_scope
                    WHERE run_id = ? AND table_name = ?
                )
                """,
                (archive_run_id, spec.table),
            )
        else:
            conn.execute(f'DELETE FROM "{spec.table}" WHERE {spec.where_sql}')
        deleted[spec.table] = conn.total_changes - changes_before
    drop_prune_temp_tables(conn)
    return deleted


def _empty_evidence_delete_counts() -> dict[str, int]:
    return {spec.table: 0 for spec in EVIDENCE_TABLE_SPECS}


def _remaining_wallet_evidence_counts(
    conn: sqlite3.Connection,
    wallets: list[str],
    *,
    keep_recent_activity: int,
) -> dict[str, int]:
    """Detect rows that arrived after scope capture so they can be archived later."""

    return _prune_wallet_evidence_batch(
        conn,
        wallets,
        keep_recent_activity=keep_recent_activity,
        dry_run=True,
    )


def _stage_wallet_registry_archive(
    conn: sqlite3.Connection,
    wallets: list[str],
    *,
    run_id: str,
) -> None:
    """Freeze raw ingestion for an archive run without changing candidate stage."""

    if not wallets:
        return
    now = int(time.time())
    conn.executemany(
        """
        UPDATE wallet_registry
        SET registry_status = 'archive_pending',
            raw_retention_tier = 'summary_only',
            raw_archive_run_id = ?,
            updated_at = ?,
            last_evaluated_at = ?
        WHERE address = ?
        """,
        ((run_id, now, now, wallet.lower()) for wallet in wallets),
    )


def _mark_wallet_registry_pruned(
    conn: sqlite3.Connection,
    wallets: list[str],
    *,
    archive_run_id: str = "",
) -> None:
    if not wallets:
        return
    now = int(time.time())
    conn.executemany(
        """
        UPDATE wallet_registry
        SET registry_status = 'archived_raw_pruned',
            raw_retention_tier = 'summary_only',
            raw_prune_version = ?,
            raw_pruned_at = ?,
            raw_archive_run_id = ?,
            raw_archived_at = CASE WHEN ? != '' THEN ? ELSE raw_archived_at END,
            raw_archive_locator = CASE
                WHEN ? != '' THEN 'parquet-wallet://' || address
                ELSE raw_archive_locator
            END,
            updated_at = ?,
            last_evaluated_at = ?
        WHERE address = ?
        """,
        (
            (
                "v3_parquet_archive" if archive_run_id else "v2_zero_raw",
                now,
                archive_run_id,
                archive_run_id,
                now,
                archive_run_id,
                now,
                now,
                wallet.lower(),
            )
            for wallet in wallets
        ),
    )


def _cancel_wallet_evidence_backfill(
    conn: sqlite3.Connection,
    wallets: list[str],
    *,
    raw_artifact_uri_prefix: str = "",
) -> None:
    if not wallets:
        return
    now = int(time.time())
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_pruned_wallets(wallet TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM temp_pruned_wallets")
    conn.executemany(
        "INSERT OR IGNORE INTO temp_pruned_wallets(wallet) VALUES (?)",
        ((wallet.lower(),) for wallet in wallets),
    )
    if "evidence_backfill_budget" in tables:
        conn.execute(
            """
            UPDATE evidence_backfill_budget
            SET stage = 'raw_pruned',
                next_attempt_at = 2147483647,
                stop_reason = 'raw_evidence_pruned_after_wallet_registry_archive',
                updated_at = ?
            WHERE wallet IN (SELECT wallet FROM temp_pruned_wallets)
            """,
            (now,),
        )
    if "evidence_backfill_jobs" in tables:
        conn.execute(
            """
            UPDATE evidence_backfill_jobs
            SET status = 'canceled',
                lease_owner = NULL,
                lease_until = 0,
                next_attempt_at = 2147483647,
                last_error = 'raw_evidence_pruned_after_wallet_registry_archive',
                updated_at = ?
            WHERE wallet IN (SELECT wallet FROM temp_pruned_wallets)
              AND status IN ('queued', 'running', 'failed')
            """,
            (now,),
        )
    if "pipeline_jobs" in tables:
        conn.execute(
            """
            UPDATE pipeline_jobs
            SET status = 'done',
                lease_owner = NULL,
                lease_until = 0,
                next_attempt_at = 2147483647,
                output_json = '{"archived":true,"reason":"raw_evidence_pruned"}',
                last_error = '',
                updated_at = ?,
                completed_at = ?
            WHERE wallet IN (SELECT wallet FROM temp_pruned_wallets)
              AND status IN ('queued', 'running', 'failed')
            """,
            (now, now),
        )
    if "wallet_processing_state" in tables:
        conn.execute(
            """
            UPDATE wallet_processing_state
            SET evidence_status = 'summary_ready',
                next_action = '',
                next_action_at = 2147483647,
                raw_artifact_uri = CASE
                    WHEN ? != '' THEN ? || wallet
                    ELSE ''
                END,
                updated_at = ?
            WHERE wallet IN (SELECT wallet FROM temp_pruned_wallets)
            """,
            (raw_artifact_uri_prefix, raw_artifact_uri_prefix, now),
        )
    conn.execute("DROP TABLE IF EXISTS temp_pruned_wallets")


def _prune_wallet_evidence(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    keep_recent_activity: int,
    dry_run: bool,
) -> dict[str, int]:
    wallet = wallet.lower()
    activity_ids_to_keep = {
        int(row["activity_id"])
        for row in conn.execute(
            """
            SELECT activity_id
            FROM wallet_activity
            WHERE address = ?
            ORDER BY timestamp DESC, activity_id DESC
            LIMIT ?
            """,
            (wallet, max(keep_recent_activity, 0)),
        ).fetchall()
    }
    if activity_ids_to_keep:
        placeholders = ",".join("?" for _ in activity_ids_to_keep)
        activity_where = f"address = ? AND activity_id NOT IN ({placeholders})"
        activity_params: tuple[Any, ...] = (wallet, *activity_ids_to_keep)
    else:
        activity_where = "address = ?"
        activity_params = (wallet,)
    specs: list[tuple[str, str, tuple[Any, ...]]] = [
        ("wallet_activity", activity_where, activity_params),
        ("wallet_episodes", "address = ?", (wallet,)),
        ("wallet_positions", "address = ?", (wallet,)),
        ("copy_trade_links", "leader_wallet = ? OR follower_wallet = ?", (wallet, wallet)),
        ("copy_pair_stats", "leader_wallet = ? OR follower_wallet = ?", (wallet, wallet)),
        ("paper_orders", "wallet = ?", (wallet,)),
        ("paper_fills", "wallet = ?", (wallet,)),
        ("paper_positions", "wallet = ?", (wallet,)),
        ("paper_settlements", "wallet = ?", (wallet,)),
        ("paper_marks", "wallet = ?", (wallet,)),
    ]
    deleted: dict[str, int] = {}
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    for table, where, params in specs:
        if table not in tables:
            deleted[table] = 0
            continue
        count = int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", params).fetchone()[0])
        deleted[table] = count
        if count and not dry_run:
            conn.execute(f"DELETE FROM {table} WHERE {where}", params)
    return deleted


def _path_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0
