"""Command line interface for wallet discovery, ranking, and data operations."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pm_robot.config import RobotSettings
from pm_robot.storage.db import (
    connect,
    initialize_database,
    run_migrations,
)
from pm_robot.orchestration.wallet_imports import import_candidates_from_csv, import_polydata_json

DEFAULT_FAILED_JOB_COOLDOWN_SECONDS = 21_600
DEFAULT_RESCREEN_AFTER_SECONDS = 7 * 86_400
DEFAULT_LIGHT_REFRESH_SECONDS = 30 * 86_400
DEFAULT_DEEP_REFRESH_SECONDS = 7 * 86_400
DEFAULT_L6_VALIDATION_REFRESH_SECONDS = 14 * 86_400
DEFAULT_GC_MIN_AGE_SECONDS = 30 * 86_400
DEFAULT_GC_KEEP_PER_WALLET = 1


def run_rtds_activity_discovery(*_args, **_kwargs):
    from pm_robot.orchestration.rtds_discovery import run_rtds_activity_discovery as impl

    return impl(*_args, **_kwargs)


def main() -> int:
    parser = argparse.ArgumentParser(prog="pm-robot")
    parser.add_argument("--env", default=".env", help="Environment file path")
    parser.add_argument("--db", default=None, help="SQLite database path")
    sub = parser.add_subparsers(dest="command", required=True)

    migrate_cmd = sub.add_parser("migrate", help="Apply SQLite migrations")
    migrate_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    runtime_heartbeat_cmd = sub.add_parser("runtime-heartbeat", help="Record a lightweight runtime loop heartbeat")
    runtime_heartbeat_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    runtime_heartbeat_cmd.add_argument(
        "--name",
        required=True,
        help="Stable heartbeat name, for example loop_wallet_screen_worker_0",
    )
    runtime_heartbeat_cmd.add_argument("--status", choices=["ok", "partial", "failed"], default="ok")
    runtime_heartbeat_cmd.add_argument("--rows-written", type=int, default=0)
    runtime_heartbeat_cmd.add_argument("--error", default="")

    import_addresses = sub.add_parser("import-addresses", help="Import candidate addresses CSV into SQLite")
    import_addresses.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    import_addresses.add_argument("--addresses", default="data/candidate_addresses.csv")

    import_polydata = sub.add_parser("import-polydata", help="Import Polydata trader JSON into SQLite")
    import_polydata.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    import_polydata.add_argument("--polydata-json", required=True)

    discover_leaderboard = sub.add_parser("discover-leaderboard", help="Discover active candidates from Polymarket leaderboards")
    discover_leaderboard.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    discover_leaderboard.add_argument("--metrics", default="profit,volume", help="Comma-separated leaderboard metrics")
    discover_leaderboard.add_argument("--windows", default="1d,7d,30d", help="Comma-separated leaderboard windows")
    discover_leaderboard.add_argument(
        "--categories",
        default="OVERALL,POLITICS,SPORTS,CRYPTO,ECONOMICS,TECH,FINANCE",
        help="Comma-separated official leaderboard categories for /v1/leaderboard",
    )
    discover_leaderboard.add_argument(
        "--time-periods",
        default="DAY,WEEK,MONTH,ALL",
        help="Comma-separated official leaderboard time periods",
    )
    discover_leaderboard.add_argument(
        "--order-bys",
        default="PNL,VOL",
        help="Comma-separated official leaderboard order fields",
    )
    discover_leaderboard.add_argument("--v1-limit", type=int, default=50)
    discover_leaderboard.add_argument("--v1-pages", type=int, default=2)

    discover_activity = sub.add_parser("discover-activity", help="Discover seed candidates from recent public activity")
    discover_activity.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    discover_activity.add_argument("--pages", type=int, default=5)
    discover_activity.add_argument("--page-limit", type=int, default=100)
    discover_activity.add_argument(
        "--min-trade-filter-usdc",
        type=float,
        default=0.0,
        help="Ask the Polymarket trades API to return only trades at or above this cash size",
    )
    discover_activity.add_argument("--max-candidates", type=int, default=200)
    discover_activity.add_argument("--sleep", type=float, default=0.25)

    discover_rtds = sub.add_parser("discover-rtds", help="Discover seed candidates from RTDS real-time activity trades")
    discover_rtds.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    discover_rtds.add_argument("--endpoint", default="wss://ws-live-data.polymarket.com")
    discover_rtds.add_argument("--min-trade-usdc", type=float, default=1.0)
    discover_rtds.add_argument("--batch-size", type=int, default=25)
    discover_rtds.add_argument("--flush-interval", type=float, default=10.0)
    discover_rtds.add_argument("--ping-interval", type=float, default=5.0)
    discover_rtds.add_argument("--receive-timeout", type=float, default=1.0)
    discover_rtds.add_argument(
        "--max-idle-seconds",
        type=float,
        default=300.0,
        help="Reconnect when RTDS delivers no non-heartbeat JSON messages for this long; 0 disables",
    )
    discover_rtds.add_argument("--reconnect-sleep", type=float, default=5.0)
    discover_rtds.add_argument("--max-runtime-seconds", type=float, default=0.0)
    discover_rtds.add_argument("--max-messages", type=int, default=0)
    discover_rtds.add_argument("--max-reconnects", type=int, default=0)

    wallet_screen_plan_cmd = sub.add_parser(
        "wallet-screen-plan",
        help="Queue bounded recent-trade screens for L1 wallets",
    )
    wallet_screen_plan_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_screen_plan_cmd.add_argument(
        "--limit", type=int, default=24, help="Maximum new screen jobs per planning pass"
    )
    wallet_screen_plan_cmd.add_argument(
        "--max-active-jobs",
        type=int,
        default=72,
        help="Queued/running screen job waterline; 0 disables",
    )
    wallet_screen_plan_cmd.add_argument("--shard-count", type=int, default=3)
    wallet_screen_plan_cmd.add_argument(
        "--rescreen-after-seconds",
        type=int,
        default=DEFAULT_RESCREEN_AFTER_SECONDS,
        help="Retry a failed L1 screen after this cooldown, only after a new sighting",
    )

    wallet_screen_worker_cmd = sub.add_parser(
        "wallet-screen-worker",
        help="Run one bounded recent-trade screen shard worker",
    )
    wallet_screen_worker_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_screen_worker_cmd.add_argument("--shard-index", type=int, required=True)
    wallet_screen_worker_cmd.add_argument("--shard-count", type=int, default=3)
    wallet_screen_worker_cmd.add_argument("--limit", type=int, default=2)
    wallet_screen_worker_cmd.add_argument("--lease-seconds", type=int, default=600)
    wallet_screen_worker_cmd.add_argument("--worker-id", default="")

    wallet_history_plan_cmd = sub.add_parser(
        "wallet-history-plan",
        help="Queue direct-to-Parquet light/deep history jobs for eligible wallets",
    )
    wallet_history_plan_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_history_plan_cmd.add_argument(
        "--limit", type=int, default=12, help="Maximum new history jobs per planning pass"
    )
    wallet_history_plan_cmd.add_argument(
        "--max-active-jobs",
        type=int,
        default=36,
        help="Queued/running history job waterline; 0 disables",
    )
    wallet_history_plan_cmd.add_argument("--shard-count", type=int, default=3)
    wallet_history_plan_cmd.add_argument(
        "--light-refresh-seconds",
        type=int,
        default=DEFAULT_LIGHT_REFRESH_SECONDS,
        help="Refresh L2 light history after this age, only after a new sighting",
    )
    wallet_history_plan_cmd.add_argument(
        "--deep-refresh-seconds",
        type=int,
        default=DEFAULT_DEEP_REFRESH_SECONDS,
        help="Refresh L3-L6 deep history after this age, only after a new sighting",
    )

    wallet_history_worker_cmd = sub.add_parser(
        "wallet-history-worker",
        help="Run one direct-to-Parquet wallet history shard worker",
    )
    wallet_history_worker_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_history_worker_cmd.add_argument(
        "--archive-dir",
        default="",
        help="Parquet root; defaults to PM_ROBOT_ARCHIVE_DIR",
    )
    wallet_history_worker_cmd.add_argument("--shard-index", type=int, required=True)
    wallet_history_worker_cmd.add_argument("--shard-count", type=int, default=3)
    wallet_history_worker_cmd.add_argument("--limit", type=int, default=1)
    wallet_history_worker_cmd.add_argument("--lease-seconds", type=int, default=1_800)
    wallet_history_worker_cmd.add_argument("--sleep", type=float, default=0.05)
    wallet_history_worker_cmd.add_argument("--worker-id", default="")

    wallet_history_gc_cmd = sub.add_parser(
        "wallet-history-gc",
        help="Prune old superseded Parquet snapshots while retaining audit metadata",
    )
    wallet_history_gc_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_history_gc_cmd.add_argument(
        "--archive-dir",
        default="",
        help="Parquet root; defaults to PM_ROBOT_ARCHIVE_DIR",
    )
    wallet_history_gc_cmd.add_argument(
        "--min-age-seconds", type=int, default=DEFAULT_GC_MIN_AGE_SECONDS
    )
    wallet_history_gc_cmd.add_argument(
        "--keep-per-wallet", type=int, default=DEFAULT_GC_KEEP_PER_WALLET
    )
    wallet_history_gc_cmd.add_argument("--limit", type=int, default=500)
    wallet_history_gc_cmd.add_argument(
        "--execute", action="store_true", help="Delete selected files; default is dry-run"
    )

    wallet_history_audit_cmd = sub.add_parser(
        "wallet-history-audit",
        help="Reconcile the Parquet tree with the SQLite artifact catalog",
    )
    wallet_history_audit_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_history_audit_cmd.add_argument(
        "--archive-dir",
        default="",
        help="Parquet root; defaults to PM_ROBOT_ARCHIVE_DIR",
    )
    wallet_history_audit_cmd.add_argument(
        "--verify-checksums",
        action="store_true",
        help="Read every expected file and verify SHA-256",
    )
    wallet_history_audit_cmd.add_argument(
        "--orphan-min-age-seconds",
        type=int,
        default=7 * 86_400,
        help="Only old uncatalogued files are eligible for removal",
    )
    wallet_history_audit_cmd.add_argument("--orphan-limit", type=int, default=500)
    wallet_history_audit_cmd.add_argument(
        "--delete-orphans",
        action="store_true",
        help="Delete bounded old uncatalogued files after path validation",
    )

    wallet_level_select_cmd = sub.add_parser(
        "wallet-level-select",
        help="Apply policy-versioned relative L3/L4/L5 cohort selection",
    )
    wallet_level_select_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_level_select_cmd.add_argument("--min-cohort-size", type=int, default=20)
    wallet_level_select_cmd.add_argument(
        "--timeout-min-cohort-size",
        type=int,
        default=5,
        help="Minimum cohort size eligible for timeout-based selection",
    )
    wallet_level_select_cmd.add_argument("--max-wait-seconds", type=int, default=3_600)
    wallet_level_select_cmd.add_argument("--l3-fraction", type=float, default=0.25)
    wallet_level_select_cmd.add_argument("--l4-fraction", type=float, default=0.20)
    wallet_level_select_cmd.add_argument("--l5-fraction", type=float, default=0.10)
    wallet_level_select_cmd.add_argument("--l3-max-promotions", type=int, default=12)
    wallet_level_select_cmd.add_argument("--l4-max-promotions", type=int, default=6)
    wallet_level_select_cmd.add_argument("--l5-max-promotions", type=int, default=2)

    wallet_l6_plan_cmd = sub.add_parser(
        "wallet-l6-plan",
        help="Queue low-volume independent validation for current L5/L6 wallets",
    )
    wallet_l6_plan_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_l6_plan_cmd.add_argument("--limit", type=int, default=5)
    wallet_l6_plan_cmd.add_argument("--max-active-jobs", type=int, default=10)
    wallet_l6_plan_cmd.add_argument("--shard-count", type=int, default=1)
    wallet_l6_plan_cmd.add_argument(
        "--refresh-seconds",
        type=int,
        default=DEFAULT_L6_VALIDATION_REFRESH_SECONDS,
    )

    wallet_l6_worker_cmd = sub.add_parser(
        "wallet-l6-worker",
        help="Run one bounded independent L6 validation worker",
    )
    wallet_l6_worker_cmd.add_argument(
        "--db", dest="command_db", default=None, help="SQLite database path"
    )
    wallet_l6_worker_cmd.add_argument(
        "--archive-dir",
        default="",
        help="Parquet root; defaults to PM_ROBOT_ARCHIVE_DIR",
    )
    wallet_l6_worker_cmd.add_argument("--shard-index", type=int, default=0)
    wallet_l6_worker_cmd.add_argument("--shard-count", type=int, default=1)
    wallet_l6_worker_cmd.add_argument("--limit", type=int, default=1)
    wallet_l6_worker_cmd.add_argument("--lease-seconds", type=int, default=1_800)
    wallet_l6_worker_cmd.add_argument("--sleep", type=float, default=0.05)
    wallet_l6_worker_cmd.add_argument("--worker-id", default="")

    pipeline_jobs_cmd = sub.add_parser("pipeline-jobs", help="Print wallet discovery job status")
    pipeline_jobs_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    pipeline_jobs_cmd.add_argument("--job-type", default="", help="Optional job type filter")

    health_cmd = sub.add_parser("health", help="Run health check and write logs/health.json")
    health_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    health_cmd.add_argument("--out", default="", help="Optional health JSON output path")

    status_cmd = sub.add_parser("status", help="Print JSON status")
    status_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    backup_cmd = sub.add_parser("backup", help="Create a SQLite backup")
    backup_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    backup_cmd.add_argument(
        "--full-check",
        action="store_true",
        help="Run a full SQLite quick_check after the fast structural verification",
    )

    backup_dump_cmd = sub.add_parser("backup-sql-dump", help="Stream a consistent SQL dump to stdout")
    backup_dump_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    maintenance_cmd = sub.add_parser("maintenance", help="Cleanup old rows, old backups, and optimize SQLite")
    maintenance_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    maintenance_cmd.add_argument("--api-log-days", type=int, default=7)
    maintenance_cmd.add_argument("--heartbeat-days", type=int, default=30)
    maintenance_cmd.add_argument("--pipeline-job-days", type=int, default=30)
    maintenance_cmd.add_argument("--keep-backups", type=int, default=2)
    maintenance_cmd.add_argument("--dry-run", action="store_true")
    maintenance_cmd.add_argument("--vacuum", action="store_true")
    maintenance_cmd.add_argument(
        "--skip-cleanup",
        action="store_true",
        help="Skip cleanup scans; useful for lightweight storage/WAL maintenance",
    )
    maintenance_cmd.add_argument(
        "--cleanup-batch-limit",
        type=int,
        default=10_000,
        help="Maximum expired metadata rows deleted per table per run",
    )
    maintenance_cmd.add_argument(
        "--wal-checkpoint",
        choices=("none", "passive", "truncate"),
        default="none",
        help="Optionally checkpoint the SQLite WAL; truncate is explicit because it can wait on readers",
    )
    maintenance_cmd.add_argument(
        "--reset-stale-jobs",
        action="store_true",
        help="Recover expired/duplicate leases and fail exhausted queued jobs; live leases are untouched",
    )
    maintenance_cmd.add_argument(
        "--failed-job-cooldown-seconds",
        type=int,
        default=DEFAULT_FAILED_JOB_COOLDOWN_SECONDS,
        help="Cooldown before a failed exhausted job may receive a fresh attempt budget",
    )
    maintenance_cmd.add_argument(
        "--reset-stale-heartbeats",
        action="store_true",
        help="Mark old running heartbeat rows interrupted; work queues are left untouched",
    )
    maintenance_cmd.add_argument(
        "--stale-heartbeat-seconds",
        type=int,
        default=21_600,
        help="Age threshold for stale running heartbeat rows",
    )

    web_cmd = sub.add_parser("web", help="Run the read-only research web console")
    web_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    web_cmd.add_argument("--host", default="127.0.0.1")
    web_cmd.add_argument("--port", type=int, default=8787)
    web_cmd.add_argument("--token", default="", help="Access token; defaults to PM_ROBOT_UI_TOKEN")
    web_cmd.add_argument("--no-auth", action="store_true", help="Disable token auth for local development only")

    args = parser.parse_args()
    settings = RobotSettings.load(Path(args.env))
    if args.db:
        settings = _replace_settings(settings, db_path=Path(args.db))
    if getattr(args, "command_db", None):
        settings = _replace_settings(settings, db_path=Path(args.command_db))
    db_path = settings.db_path
    if args.command == "migrate":
        initialize_database(db_path)
        conn = connect(db_path)
        try:
            applied = run_migrations(conn)
        finally:
            conn.close()
        print(f"applied migrations: {applied}" if applied else "database is up to date")
        return 0
    if args.command == "runtime-heartbeat":
        from pm_robot.storage.repository import record_runtime_heartbeat

        conn = connect(db_path)
        try:
            run_migrations(conn)
            heartbeat_id = record_runtime_heartbeat(
                conn,
                args.name,
                status=args.status,
                rows_written=args.rows_written,
                error=args.error,
            )
        finally:
            conn.close()
        print(
            json.dumps(
                {
                    "heartbeat_id": heartbeat_id,
                    "name": args.name,
                    "status": args.status,
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "import-addresses":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            count = import_candidates_from_csv(
                conn,
                addresses_path=Path(args.addresses),
            )
        finally:
            conn.close()
        print(f"imported {count} candidate addresses into {db_path}")
        return 0
    if args.command == "import-polydata":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            counts = import_polydata_json(conn, polydata_path=Path(args.polydata_json))
        finally:
            conn.close()
        print(
            f"imported {counts['candidates']} Polydata candidates and "
            f"{counts['features']} feature rows into {db_path}"
        )
        return 0
    if args.command == "discover-leaderboard":
        from pm_robot.orchestration.leaderboard_discovery import discover_leaderboard_candidates

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = discover_leaderboard_candidates(
                conn,
                metrics=_csv_tuple(args.metrics),
                windows=_csv_tuple(args.windows),
                categories=_csv_tuple(args.categories),
                time_periods=_csv_tuple(args.time_periods),
                order_bys=_csv_tuple(args.order_bys),
                v1_limit=args.v1_limit,
                v1_pages=args.v1_pages,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "discover-activity":
        from pm_robot.orchestration.activity_discovery import discover_activity_candidates

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = discover_activity_candidates(
                conn,
                pages=args.pages,
                page_limit=args.page_limit,
                min_trade_filter_usdc=args.min_trade_filter_usdc,
                max_candidates=args.max_candidates,
                sleep_seconds=args.sleep,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "limited"} else 1
    if args.command == "discover-rtds":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_rtds_activity_discovery(
                conn,
                endpoint=args.endpoint,
                min_trade_usdc=args.min_trade_usdc,
                batch_size=args.batch_size,
                flush_interval=args.flush_interval,
                ping_interval=args.ping_interval,
                receive_timeout=args.receive_timeout,
                max_idle_seconds=args.max_idle_seconds,
                reconnect_sleep=args.reconnect_sleep,
                max_runtime_seconds=args.max_runtime_seconds,
                max_messages=args.max_messages,
                max_reconnects=args.max_reconnects,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "wallet-screen-plan":
        from pm_robot.orchestration.wallet_screening import plan_wallet_screen_jobs

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = plan_wallet_screen_jobs(
                conn,
                limit=args.limit,
                max_active_jobs=args.max_active_jobs,
                shard_count=args.shard_count,
                rescreen_after_seconds=args.rescreen_after_seconds,
            )
            conn.commit()
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "wallet-screen-worker":
        from pm_robot.orchestration.wallet_screening import run_wallet_screen_worker

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_wallet_screen_worker(
                conn,
                shard_index=args.shard_index,
                shard_count=args.shard_count,
                limit=args.limit,
                lease_seconds=args.lease_seconds,
                worker_id=args.worker_id,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "wallet-history-plan":
        from pm_robot.orchestration.wallet_history_pipeline import plan_wallet_history_jobs

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = plan_wallet_history_jobs(
                conn,
                limit=args.limit,
                max_active_jobs=args.max_active_jobs,
                shard_count=args.shard_count,
                light_refresh_seconds=args.light_refresh_seconds,
                deep_refresh_seconds=args.deep_refresh_seconds,
            )
            conn.commit()
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "wallet-history-worker":
        from pm_robot.orchestration.wallet_history_pipeline import run_wallet_history_worker

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_wallet_history_worker(
                conn,
                archive_dir=Path(args.archive_dir) if args.archive_dir else settings.archive_dir,
                shard_index=args.shard_index,
                shard_count=args.shard_count,
                limit=args.limit,
                lease_seconds=args.lease_seconds,
                sleep_seconds=args.sleep,
                worker_id=args.worker_id,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "wallet-history-gc":
        from pm_robot.storage.wallet_history_store import prune_superseded_wallet_history_artifacts

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = prune_superseded_wallet_history_artifacts(
                conn,
                archive_dir=Path(args.archive_dir) if args.archive_dir else settings.archive_dir,
                min_age_seconds=args.min_age_seconds,
                keep_per_wallet=args.keep_per_wallet,
                limit=args.limit,
                dry_run=not args.execute,
            )
            if args.execute:
                conn.commit()
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "wallet-history-audit":
        from pm_robot.storage.wallet_history_store import audit_wallet_history_artifacts

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = audit_wallet_history_artifacts(
                conn,
                archive_dir=Path(args.archive_dir) if args.archive_dir else settings.archive_dir,
                verify_checksums=args.verify_checksums,
                orphan_min_age_seconds=args.orphan_min_age_seconds,
                orphan_limit=args.orphan_limit,
                delete_orphans=args.delete_orphans,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "wallet-level-select":
        from pm_robot.orchestration.wallet_level_selection import reconcile_wallet_level_selections

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = reconcile_wallet_level_selections(
                conn,
                min_cohort_size=args.min_cohort_size,
                timeout_min_cohort_size=args.timeout_min_cohort_size,
                max_wait_seconds=args.max_wait_seconds,
                l3_fraction=args.l3_fraction,
                l4_fraction=args.l4_fraction,
                l5_fraction=args.l5_fraction,
                l3_max_promotions=args.l3_max_promotions,
                l4_max_promotions=args.l4_max_promotions,
                l5_max_promotions=args.l5_max_promotions,
            )
            conn.commit()
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "wallet-l6-plan":
        from pm_robot.orchestration.l6_validation_pipeline import plan_l6_validation_jobs

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = plan_l6_validation_jobs(
                conn,
                limit=args.limit,
                max_active_jobs=args.max_active_jobs,
                shard_count=args.shard_count,
                refresh_seconds=args.refresh_seconds,
            )
            conn.commit()
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "wallet-l6-worker":
        from pm_robot.orchestration.l6_validation_pipeline import run_l6_validation_worker

        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_l6_validation_worker(
                conn,
                archive_dir=Path(args.archive_dir) if args.archive_dir else settings.archive_dir,
                shard_index=args.shard_index,
                shard_count=args.shard_count,
                limit=args.limit,
                lease_seconds=args.lease_seconds,
                sleep_seconds=args.sleep,
                worker_id=args.worker_id,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "pipeline-jobs":
        from pm_robot.storage.repository import pipeline_job_summary

        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = pipeline_job_summary(conn, job_type=args.job_type)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "health":
        from pm_robot.ops import write_health

        out = Path(args.out) if args.out else None
        data = write_health(settings, out)
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0 if data["ok"] else 1
    if args.command == "status":
        from pm_robot.ops import status

        print(json.dumps(status(settings), ensure_ascii=False, indent=2))
        return 0
    if args.command == "backup":
        from pm_robot.ops import backup_database

        out = backup_database(settings, full_check=args.full_check)
        print(f"backup written to {out}")
        return 0
    if args.command == "backup-sql-dump":
        from pm_robot.ops import dump_database_sql

        dump_database_sql(settings, sys.stdout.buffer)
        return 0
    if args.command == "maintenance":
        from pm_robot.ops import maintenance

        data = maintenance(
            settings,
            api_log_days=args.api_log_days,
            heartbeat_days=args.heartbeat_days,
            pipeline_job_days=args.pipeline_job_days,
            keep_backups=args.keep_backups,
            dry_run=args.dry_run,
            vacuum=args.vacuum,
            wal_checkpoint=args.wal_checkpoint,
            skip_cleanup=args.skip_cleanup,
            cleanup_batch_limit=args.cleanup_batch_limit,
            reset_stale_jobs=args.reset_stale_jobs,
            failed_job_cooldown_seconds=args.failed_job_cooldown_seconds,
            reset_stale_heartbeats=args.reset_stale_heartbeats,
            stale_heartbeat_seconds=args.stale_heartbeat_seconds,
        )
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0 if data["ok"] else 1
    if args.command == "web":
        from pm_robot.web import WebConsoleConfig, run_web_console

        run_web_console(
            WebConsoleConfig(
                settings=settings,
                host=args.host,
                port=args.port,
                token=args.token or os.environ.get("PM_ROBOT_UI_TOKEN", ""),
                auth_required=not args.no_auth,
            )
        )
        return 0
    return 1


def _replace_settings(settings: RobotSettings, **kwargs) -> RobotSettings:
    values = settings.__dict__.copy()
    values.update(kwargs)
    return RobotSettings(**values)


def _csv_tuple(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


if __name__ == "__main__":
    raise SystemExit(main())
