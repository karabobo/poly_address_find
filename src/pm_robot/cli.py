"""Command line interface for the integrated robot framework."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pm_robot.config import DEFAULT_POLICY_PATH, RobotSettings, load_policy
from pm_robot.ops import (
    DEFAULT_FAILED_JOB_COOLDOWN_SECONDS,
    backup_database,
    build_winner_library,
    build_wallet_registry,
    dump_database_sql,
    maintenance,
    prune_low_value_evidence,
    status,
    write_health,
)
from pm_robot.orchestration.review_pipeline import (
    build_review_queue,
    import_candidates_from_csv,
    import_features_from_csv,
    import_polydata_json,
    score_database,
)
from pm_robot.orchestration.positions_ingestor import ingest_positions
from pm_robot.orchestration.activity_ingestor import ingest_activity
from pm_robot.orchestration.activity_discovery import discover_activity_candidates
from pm_robot.orchestration.copyability_evidence import (
    copyability_evidence_job_status,
    plan_copyability_evidence_jobs,
    run_copyability_evidence_worker,
)
from pm_robot.orchestration.evidence_backfill import (
    plan_queued_evidence_backfill,
    prioritize_backfill_from_scores,
    queued_evidence_backfill_status,
    run_evidence_backfill,
    run_queued_evidence_backfill_worker,
)
from pm_robot.orchestration.eligibility_repair import prepare_eligibility_repairs
from pm_robot.orchestration.feature_materializer import materialize_wallet_features
from pm_robot.orchestration.gamma_ingestor import ingest_gamma_markets
from pm_robot.orchestration.leaderboard_discovery import discover_leaderboard_candidates
from pm_robot.orchestration.paper_runner import evaluate_paper_observer, preview_paper_observer, run_paper
from pm_robot.orchestration.pipeline_cycle import PipelineCycleOptions, run_pipeline_cycle
from pm_robot.orchestration.pipeline_audit import pipeline_audit_report
from pm_robot.orchestration.pipeline_smoothness import pipeline_smoothness_report
from pm_robot.orchestration.rtds_discovery import run_rtds_activity_discovery
from pm_robot.orchestration.trade_role_ingestor import ingest_trade_role_evidence
from pm_robot.orchestration.wallet_pipeline import (
    DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS,
    DEFAULT_PIPELINE_STAGE_WEIGHTS,
    plan_wallet_pipeline_jobs,
    run_wallet_pipeline_worker,
    wallet_pipeline_job_status,
)
from pm_robot.pipeline_terms import EvidenceJobStage
from pm_robot.execution.paper_portfolio import paper_readiness_rows, settle_paper_portfolio
from pm_robot.execution.preflight import (
    DEFAULT_EXECUTION_SERVICES,
    DEFAULT_ACTIVITY_LOOKBACK_SEC,
    DEFAULT_MAX_SIGNAL_AGE_SEC,
    DEFAULT_RTDS_WATCH_MIN_SCORE,
    execution_preflight_status,
    paper_realtime_audit_status,
    rtds_watch_audit_status,
    parse_compose_rows,
)
from pm_robot.research.copy_backtest import backtest_copy_stream
from pm_robot.research.copy_graph import mine_copy_graph
from pm_robot.research.publish import active_published_leaders, publish_leaders
from pm_robot.storage.db import connect, connect_readonly, initialize_database, run_migrations
from pm_robot.storage.repository import (
    activity_coverage,
    activity_coverage_summary,
    evidence_backfill_summary,
    gamma_market_cache_summary,
    materialize_wallet_processing_state,
    pipeline_job_summary,
    record_runtime_heartbeat,
    wallet_processing_state_summary,
)
from pm_robot.web import WebConsoleConfig, paper_handoff_csv, paper_handoff_data, run_web_console


def main() -> int:
    parser = argparse.ArgumentParser(prog="pm-robot")
    parser.add_argument("--env", default=".env", help="Environment file path")
    parser.add_argument("--db", default=None, help="SQLite database path")
    sub = parser.add_subparsers(dest="command", required=True)

    migrate_cmd = sub.add_parser("migrate", help="Apply SQLite migrations")
    migrate_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    runtime_heartbeat_cmd = sub.add_parser("runtime-heartbeat", help="Record a lightweight runtime loop heartbeat")
    runtime_heartbeat_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    runtime_heartbeat_cmd.add_argument("--name", required=True, help="Stable heartbeat name, for example loop_score_review")
    runtime_heartbeat_cmd.add_argument("--status", choices=["ok", "partial", "failed"], default="ok")
    runtime_heartbeat_cmd.add_argument("--rows-written", type=int, default=0)
    runtime_heartbeat_cmd.add_argument("--error", default="")

    import_addresses = sub.add_parser("import-addresses", help="Import candidate addresses CSV into SQLite")
    import_addresses.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    import_addresses.add_argument("--addresses", default="data/candidate_addresses.csv")
    import_addresses.add_argument(
        "--source-event-mode",
        choices=["upsert-source", "append"],
        default="upsert-source",
        help="Use upsert-source for curated lists so each wallet/source has one provenance row",
    )

    import_features = sub.add_parser("import-features", help="Import wallet feature CSV into SQLite")
    import_features.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    import_features.add_argument("--features", required=True)

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
    discover_activity.add_argument("--min-trades", type=int, default=2)
    discover_activity.add_argument("--min-usdc-volume", type=float, default=20.0)
    discover_activity.add_argument(
        "--min-trade-filter-usdc",
        type=float,
        default=0.0,
        help="Ask the Polymarket trades API to return only trades at or above this cash size",
    )
    discover_activity.add_argument("--max-candidates", type=int, default=200)
    discover_activity.add_argument("--sleep", type=float, default=0.25)
    discover_activity.add_argument("--out", default="", help="Optional candidate export JSON path")
    discover_activity.add_argument("--no-db-write", action="store_true", help="Only export discovered candidates; do not write SQLite")

    discover_rtds = sub.add_parser("discover-rtds", help="Discover seed candidates from RTDS real-time activity trades")
    discover_rtds.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    discover_rtds.add_argument("--endpoint", default="wss://ws-live-data.polymarket.com")
    discover_rtds.add_argument("--min-trade-usdc", type=float, default=500.0)
    discover_rtds.add_argument(
        "--paper-min-trade-usdc",
        type=float,
        default=0.0,
        help="Separate RTDS threshold for persisting already-approved paper-stage wallet activity",
    )
    discover_rtds.add_argument(
        "--watch-min-score",
        type=float,
        default=65.0,
        help="Persist RTDS activity for needs_manual_review wallets at or above this score without paper approval",
    )
    discover_rtds.add_argument("--batch-size", type=int, default=25)
    discover_rtds.add_argument("--flush-interval", type=float, default=10.0)
    discover_rtds.add_argument("--ping-interval", type=float, default=5.0)
    discover_rtds.add_argument("--receive-timeout", type=float, default=1.0)
    discover_rtds.add_argument("--reconnect-sleep", type=float, default=5.0)
    discover_rtds.add_argument("--max-runtime-seconds", type=float, default=0.0)
    discover_rtds.add_argument("--max-messages", type=int, default=0)
    discover_rtds.add_argument("--max-reconnects", type=int, default=0)

    ingest_positions_cmd = sub.add_parser("ingest-positions", help="Ingest current Polymarket positions")
    ingest_positions_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    ingest_positions_cmd.add_argument("--limit", type=int, default=25)
    ingest_positions_cmd.add_argument("--size-threshold", type=float, default=0.0)
    ingest_positions_cmd.add_argument("--sleep", type=float, default=0.25)

    ingest_activity_cmd = sub.add_parser("ingest-activity", help="Ingest wallet activity and rebuild episodes")
    ingest_activity_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    ingest_activity_cmd.add_argument("--wallet-limit", type=int, default=10)
    ingest_activity_cmd.add_argument("--page-limit", type=int, default=100)
    ingest_activity_cmd.add_argument("--max-events-per-wallet", type=int, default=200)
    ingest_activity_cmd.add_argument(
        "--target-events-per-wallet",
        type=int,
        default=0,
        help="Backfill mode: only target wallets with fewer than this many stored activity events",
    )
    ingest_activity_cmd.add_argument(
        "--paper-stage-only",
        action="store_true",
        help="Refresh only paper-stage research wallets for observation exports",
    )
    ingest_activity_cmd.add_argument("--sleep", type=float, default=0.25)

    ingest_gamma_cmd = sub.add_parser("ingest-gamma-markets", help="Ingest Gamma market metadata cache")
    ingest_gamma_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    ingest_gamma_cmd.add_argument("--limit", type=int, default=50)
    ingest_gamma_cmd.add_argument("--ttl-seconds", type=int, default=21_600)
    ingest_gamma_cmd.add_argument("--failure-ttl-seconds", type=int, default=604_800)
    ingest_gamma_cmd.add_argument("--sleep", type=float, default=0.1)
    ingest_gamma_cmd.add_argument("--paper-only", action="store_true", help="Only backfill markets referenced by paper fills")

    copy_graph_cmd = sub.add_parser("mine-copy-graph", help="Mine leader/follower copy relationships")
    copy_graph_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    copy_graph_cmd.add_argument("--policy", default=str(DEFAULT_POLICY_PATH))

    copy_backtest_cmd = sub.add_parser("backtest-copy-stream", help="Backtest copy stream ROI")
    copy_backtest_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    copy_backtest_cmd.add_argument("--policy", default=str(DEFAULT_POLICY_PATH))

    copyability_plan_cmd = sub.add_parser("copyability-plan", help="Plan dedicated copyability evidence jobs")
    copyability_plan_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    copyability_plan_cmd.add_argument("--limit", type=int, default=50, help="Maximum new jobs per planning pass")
    copyability_plan_cmd.add_argument(
        "--max-active-jobs",
        type=int,
        default=50,
        help="Queued/running copyability job waterline; 0 disables",
    )
    copyability_plan_cmd.add_argument("--min-score", type=float, default=40.0)
    copyability_plan_cmd.add_argument("--min-activity-events", type=int, default=25)
    copyability_plan_cmd.add_argument("--shard-count", type=int, default=3)
    copyability_plan_cmd.add_argument("--rescan-seconds", type=int, default=21_600)

    copyability_worker_cmd = sub.add_parser("copyability-worker", help="Run one copyability evidence shard worker")
    copyability_worker_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    copyability_worker_cmd.add_argument("--policy", default=str(DEFAULT_POLICY_PATH))
    copyability_worker_cmd.add_argument("--shard-index", type=int, required=True)
    copyability_worker_cmd.add_argument("--shard-count", type=int, default=3)
    copyability_worker_cmd.add_argument("--limit", type=int, default=4)
    copyability_worker_cmd.add_argument("--lease-seconds", type=int, default=7_200)
    copyability_worker_cmd.add_argument("--worker-id", default="")
    copyability_worker_cmd.add_argument("--max-leader-events", type=int, default=3_000)
    copyability_worker_cmd.add_argument("--max-followers-per-event", type=int, default=200)
    copyability_worker_cmd.add_argument("--prefer-scan-mode", default="")

    copyability_jobs_cmd = sub.add_parser("copyability-jobs", help="Print copyability evidence queue status")
    copyability_jobs_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    coverage_cmd = sub.add_parser("activity-coverage", help="Print wallet activity coverage")
    coverage_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    coverage_cmd.add_argument("--limit", type=int, default=25)
    coverage_cmd.add_argument("--summary", action="store_true", help="Print aggregate coverage only")

    evidence_backfill_cmd = sub.add_parser("evidence-backfill", help="Budgeted historical evidence backfill")
    evidence_backfill_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    evidence_backfill_cmd.add_argument("--light-limit", type=int, default=10)
    evidence_backfill_cmd.add_argument("--medium-limit", type=int, default=3)
    evidence_backfill_cmd.add_argument("--deep-limit", type=int, default=1)
    evidence_backfill_cmd.add_argument("--page-limit", type=int, default=100)
    evidence_backfill_cmd.add_argument("--sleep", type=float, default=0.25)

    evidence_backfill_status_cmd = sub.add_parser("evidence-backfill-status", help="Print evidence backfill queue status")
    evidence_backfill_status_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    evidence_plan_cmd = sub.add_parser("evidence-backfill-plan", help="Plan sharded evidence backfill jobs")
    evidence_plan_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    evidence_plan_cmd.add_argument("--light-limit", type=int, default=30)
    evidence_plan_cmd.add_argument("--medium-limit", type=int, default=20)
    evidence_plan_cmd.add_argument("--deep-limit", type=int, default=3)
    evidence_plan_cmd.add_argument("--shard-count", type=int, default=3)

    evidence_worker_cmd = sub.add_parser("evidence-backfill-worker", help="Run one sharded evidence backfill worker")
    evidence_worker_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    evidence_worker_cmd.add_argument("--shard-index", type=int, required=True)
    evidence_worker_cmd.add_argument("--shard-count", type=int, default=3)
    evidence_worker_cmd.add_argument("--limit", type=int, default=8)
    evidence_worker_cmd.add_argument("--page-limit", type=int, default=200)
    evidence_worker_cmd.add_argument("--sleep", type=float, default=0.02)
    evidence_worker_cmd.add_argument("--lease-seconds", type=int, default=900)
    evidence_worker_cmd.add_argument("--max-attempts", type=int, default=3)
    evidence_worker_cmd.add_argument("--worker-id", default="")

    evidence_jobs_cmd = sub.add_parser("evidence-backfill-jobs", help="Print sharded evidence backfill job status")
    evidence_jobs_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    wallet_pipeline_cmd = sub.add_parser("wallet-pipeline-state", help="Print or materialize v2 wallet pipeline state")
    wallet_pipeline_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    wallet_pipeline_cmd.add_argument("--materialize", action="store_true", help="Refresh summaries before printing")
    wallet_pipeline_cmd.add_argument("--limit", type=int, default=0, help="Wallets to materialize; 0 means all")
    wallet_pipeline_cmd.add_argument("--commit-every", type=int, default=100, help="Commit every N wallets while materializing")
    wallet_pipeline_cmd.add_argument(
        "--stale-only",
        action="store_true",
        help="Materialize only wallets whose candidate, budget, or activity watermark changed",
    )

    wallet_pipeline_plan_cmd = sub.add_parser("wallet-pipeline-plan", help="Plan v2 wallet evidence jobs from pipeline state")
    wallet_pipeline_plan_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    wallet_pipeline_plan_cmd.add_argument(
        "--light-limit",
        type=int,
        default=DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.LIGHT_PENDING.value],
    )
    wallet_pipeline_plan_cmd.add_argument(
        "--medium-limit",
        type=int,
        default=DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.MEDIUM_PENDING.value],
    )
    wallet_pipeline_plan_cmd.add_argument(
        "--deep-limit",
        type=int,
        default=DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.DEEP_PENDING.value],
    )
    wallet_pipeline_plan_cmd.add_argument("--shard-count", type=int, default=3)
    wallet_pipeline_plan_cmd.add_argument(
        "--max-active-jobs",
        type=int,
        default=240,
        help="Do not enqueue wallet evidence jobs when queued/running jobs are at this waterline; 0 disables",
    )

    wallet_pipeline_worker_cmd = sub.add_parser("wallet-pipeline-worker", help="Run one v2 wallet evidence shard worker")
    wallet_pipeline_worker_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    wallet_pipeline_worker_cmd.add_argument("--shard-index", type=int, required=True)
    wallet_pipeline_worker_cmd.add_argument("--shard-count", type=int, default=3)
    wallet_pipeline_worker_cmd.add_argument("--limit", type=int, default=8)
    wallet_pipeline_worker_cmd.add_argument("--page-limit", type=int, default=200)
    wallet_pipeline_worker_cmd.add_argument("--sleep", type=float, default=0.02)
    wallet_pipeline_worker_cmd.add_argument("--lease-seconds", type=int, default=900)
    wallet_pipeline_worker_cmd.add_argument(
        "--priority-aging-seconds",
        type=int,
        default=DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS,
        help="Claim the oldest queued job after this wait even when its numeric priority is lower; 0 disables",
    )
    wallet_pipeline_worker_cmd.add_argument("--worker-id", default="")

    wallet_pipeline_jobs_cmd = sub.add_parser("wallet-pipeline-jobs", help="Print v2 wallet evidence job status")
    wallet_pipeline_jobs_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    pipeline_jobs_cmd = sub.add_parser("pipeline-jobs", help="Print generic v2 pipeline job status")
    pipeline_jobs_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    pipeline_jobs_cmd.add_argument("--job-type", default="", help="Optional job type filter")

    eligibility_repair_cmd = sub.add_parser(
        "eligibility-repair-plan",
        help="Prepare evidence/copyability repairs for the canonical queue planners",
    )
    eligibility_repair_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    eligibility_repair_cmd.add_argument("--limit", type=int, default=100)
    eligibility_repair_cmd.add_argument("--min-score", type=float, default=40.0)
    eligibility_repair_cmd.add_argument("--min-copyability-activity-events", type=int, default=25)
    eligibility_repair_cmd.add_argument("--dry-run", action="store_true")

    smoothness_cmd = sub.add_parser(
        "pipeline-smoothness",
        help="Print a read-only pipeline smoothness report with eligibility blockers and queue backlog",
    )
    smoothness_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    smoothness_cmd.add_argument("--top", type=int, default=20, help="Number of stuck wallets to include")
    smoothness_cmd.add_argument("--min-score", type=float, default=0.0)
    smoothness_cmd.add_argument("--min-copyability-activity-events", type=int, default=25)

    audit_cmd = sub.add_parser(
        "pipeline-audit",
        help="Print a read-only end-to-end wallet discovery pipeline audit",
    )
    audit_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    audit_cmd.add_argument("--top", type=int, default=20)
    audit_cmd.add_argument("--min-score", type=float, default=40.0, help="Manual-review triage score floor")
    audit_cmd.add_argument(
        "--paper-min-score",
        type=float,
        default=None,
        help="Paper score floor; defaults to the active scoring policy",
    )

    cycle_cmd = sub.add_parser(
        "pipeline-cycle",
        help="Preview or execute one local no-worker pipeline smoothness cycle",
    )
    cycle_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    cycle_cmd.add_argument("--execute-plan", action="store_true", help="Mutate local DB by materializing state and queueing jobs")
    cycle_cmd.add_argument("--top", type=int, default=20)
    cycle_cmd.add_argument("--min-score", type=float, default=40.0)
    cycle_cmd.add_argument("--state-limit", type=int, default=0)
    cycle_cmd.add_argument("--repair-limit", type=int, default=100)
    cycle_cmd.add_argument("--shard-count", type=int, default=3)
    cycle_cmd.add_argument("--wallet-light-limit", type=int, default=30)
    cycle_cmd.add_argument("--wallet-medium-limit", type=int, default=20)
    cycle_cmd.add_argument("--wallet-deep-limit", type=int, default=5)
    cycle_cmd.add_argument("--copyability-limit", type=int, default=50)
    cycle_cmd.add_argument("--copyability-max-active-jobs", type=int, default=50)
    cycle_cmd.add_argument("--copyability-min-activity-events", type=int, default=25)
    cycle_cmd.add_argument("--feature-limit", type=int, default=10)
    cycle_cmd.add_argument("--feature-min-activity-events", type=int, default=25)
    cycle_cmd.add_argument("--score-limit", type=int, default=0)
    cycle_cmd.add_argument("--policy", default=str(DEFAULT_POLICY_PATH))
    cycle_cmd.add_argument("--no-score", action="store_true", help="Skip local incremental scoring in execute mode")

    prioritize_backfill_cmd = sub.add_parser("prioritize-backfill", help="Promote scored review wallets into the evidence backfill queue")
    prioritize_backfill_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    prioritize_backfill_cmd.add_argument("--min-score", type=float, default=40.0)
    prioritize_backfill_cmd.add_argument("--limit", type=int, default=50)
    prioritize_backfill_cmd.add_argument("--target-depth", type=int, default=1000)
    prioritize_backfill_cmd.add_argument("--priority", type=int, default=10)
    prioritize_backfill_cmd.add_argument("--source", default="score_priority")

    materialize_features_cmd = sub.add_parser("materialize-features", help="Materialize derived wallet scoring fields in small batches")
    materialize_features_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    materialize_features_cmd.add_argument("--limit", type=int, default=5)
    materialize_features_cmd.add_argument("--min-activity-events", type=int, default=25)

    trade_roles_cmd = sub.add_parser(
        "ingest-trade-roles",
        help="Collect official maker/taker evidence and update hygiene screening",
    )
    trade_roles_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    trade_roles_cmd.add_argument("--limit", type=int, default=5)

    gamma_cache_cmd = sub.add_parser("gamma-cache", help="Print Gamma market cache coverage")
    gamma_cache_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    review = sub.add_parser("build-review", help="Build candidate review queue")
    review.add_argument("--addresses", default="data/candidate_addresses.csv")
    review.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    review.add_argument("--features", default="")
    review.add_argument("--policy", default=str(DEFAULT_POLICY_PATH))
    review.add_argument("--out", default="reports/candidate_review_queue.csv")
    review.add_argument(
        "--incremental",
        action="store_true",
        help="Score only wallets whose evidence/features changed or are ready for scoring",
    )
    review.add_argument("--limit", type=int, default=0, help="Maximum wallets to consider in this run")
    review.add_argument(
        "--no-import-csv",
        action="store_true",
        help="Skip legacy CSV candidate import before database scoring",
    )
    review.add_argument(
        "--source-event-mode",
        choices=["upsert-source", "append"],
        default="upsert-source",
        help="Source event handling for the optional CSV import step",
    )
    review.add_argument(
        "--csv-only",
        action="store_true",
        help="Legacy mode: read CSV files directly without using SQLite",
    )

    health_cmd = sub.add_parser("health", help="Run health check and write logs/health.json")
    health_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    health_cmd.add_argument("--out", default="", help="Optional health JSON output path")

    status_cmd = sub.add_parser("status", help="Print JSON status")
    status_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    backup_cmd = sub.add_parser("backup", help="Create a SQLite backup")
    backup_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    backup_dump_cmd = sub.add_parser("backup-sql-dump", help="Stream a consistent SQL dump to stdout")
    backup_dump_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    paper_cmd = sub.add_parser("paper-run", help="Record paper orders for approved wallet-copy signals")
    paper_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    paper_cmd.add_argument("--limit", type=int, default=50)
    paper_cmd.add_argument("--max-stake-usd", type=float, default=40.0)
    paper_cmd.add_argument(
        "--max-signal-age-sec",
        type=int,
        default=21_600,
        help="Only paper-trade recent wallet activity; use 0 to disable the age gate",
    )
    paper_cmd.add_argument(
        "--include-watchlist-min-score",
        type=float,
        default=None,
        help="Deprecated compatibility flag; review/watchlist wallets still must pass paper eligibility",
    )
    paper_cmd.add_argument(
        "--include-review-min-score",
        type=float,
        default=None,
        help="Deprecated compatibility flag; needs_manual_review wallets still must pass paper eligibility",
    )
    paper_cmd.add_argument("--no-jsonl", action="store_true", help="Only write SQLite paper_orders")

    paper_observer_cmd = sub.add_parser(
        "paper-observer-preview",
        help="Export eligible recent paper signals without writing orders",
    )
    paper_observer_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    paper_observer_cmd.add_argument("--limit", type=int, default=50)
    paper_observer_cmd.add_argument(
        "--max-signal-age-sec",
        type=int,
        default=21_600,
        help="Only include recent approved wallet activity; use 0 to disable the age gate",
    )
    paper_observer_cmd.add_argument("--out", default="", help="Optional JSON export path")

    paper_observer_eval_cmd = sub.add_parser(
        "paper-observer-evaluate",
        help="Quote eligible recent paper signals without writing orders",
    )
    paper_observer_eval_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    paper_observer_eval_cmd.add_argument("--limit", type=int, default=50)
    paper_observer_eval_cmd.add_argument("--max-stake-usd", type=float, default=40.0)
    paper_observer_eval_cmd.add_argument(
        "--max-signal-age-sec",
        type=int,
        default=21_600,
        help="Only quote recent approved wallet activity; use 0 to disable the age gate",
    )
    paper_observer_eval_cmd.add_argument(
        "--max-actionable-signal-age-sec",
        type=int,
        default=300,
        help="Signals older than this remain historical quote checks but are not actionable",
    )
    paper_observer_eval_cmd.add_argument(
        "--persist",
        action="store_true",
        help="Persist quoteability evidence to paper_signal_evaluations without writing paper_orders",
    )
    paper_observer_eval_cmd.add_argument("--out", default="", help="Optional JSON export path")

    paper_handoff_cmd = sub.add_parser(
        "paper-handoff-export",
        help="Export research-approved paper handoff wallets without writing database rows",
    )
    paper_handoff_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    paper_handoff_cmd.add_argument("--limit", type=int, default=250)
    paper_handoff_cmd.add_argument("--out", default="reports/paper_handoff.json", help="JSON export path")
    paper_handoff_cmd.add_argument("--csv-out", default="reports/paper_handoff.csv", help="CSV export path")
    paper_handoff_cmd.add_argument("--no-csv", action="store_true", help="Do not write the CSV export")

    execution_preflight_cmd = sub.add_parser(
        "execution-preflight",
        help="Print the read-only start gate for the opt-in paper execution profile",
    )
    execution_preflight_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    execution_preflight_cmd.add_argument(
        "--max-signal-age-sec",
        type=int,
        default=DEFAULT_MAX_SIGNAL_AGE_SEC,
        help="Recent BUY/actionable-signal window used before starting execution",
    )
    execution_preflight_cmd.add_argument(
        "--execution-services",
        default=" ".join(DEFAULT_EXECUTION_SERVICES),
        help="Space- or comma-separated execution service names",
    )
    execution_preflight_cmd.add_argument("--compose-ps-json", default="", help="Optional Docker Compose ps JSON or JSONL output")
    execution_preflight_cmd.add_argument("--compose-error", default="", help="Optional Docker Compose ps error text")

    paper_realtime_audit_cmd = sub.add_parser(
        "paper-realtime-audit",
        help="Print per-wallet realtime blockers for paper-stage wallets without writing execution rows",
    )
    paper_realtime_audit_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    paper_realtime_audit_cmd.add_argument(
        "--max-signal-age-sec",
        type=int,
        default=DEFAULT_MAX_SIGNAL_AGE_SEC,
        help="Recent BUY/actionable-signal window used by the realtime audit",
    )
    paper_realtime_audit_cmd.add_argument(
        "--lookback-sec",
        type=int,
        default=DEFAULT_ACTIVITY_LOOKBACK_SEC,
        help="Paper wallet activity lookback window",
    )
    paper_realtime_audit_cmd.add_argument("--limit", type=int, default=50)

    rtds_watch_audit_cmd = sub.add_parser(
        "rtds-watch-audit",
        help="Print near-paper RTDS watch wallets without changing candidate stages",
    )
    rtds_watch_audit_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    rtds_watch_audit_cmd.add_argument("--min-score", type=float, default=DEFAULT_RTDS_WATCH_MIN_SCORE)
    rtds_watch_audit_cmd.add_argument("--lookback-sec", type=int, default=DEFAULT_ACTIVITY_LOOKBACK_SEC)
    rtds_watch_audit_cmd.add_argument("--limit", type=int, default=50)

    paper_settle_cmd = sub.add_parser("paper-settle", help="Settle paper fills, positions, marks, and PnL")
    paper_settle_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    paper_readiness_cmd = sub.add_parser("paper-readiness", help="Print paper wallet production-readiness gates")
    paper_readiness_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    publish_cmd = sub.add_parser("publish-leaders", help="Refresh publishable research wallet output")
    publish_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    publish_cmd.add_argument("--ttl-seconds", type=int, default=86_400)
    publish_cmd.add_argument("--out", default="", help="Optional JSON export path")

    published_cmd = sub.add_parser("published-leaders", help="Print active, unexpired published leaders")
    published_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")

    wallet_registry_cmd = sub.add_parser(
        "wallet-registry",
        help="Materialize and export the compact wallet library",
    )
    wallet_registry_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    wallet_registry_cmd.add_argument("--out", default="reports/wallet_registry.csv")
    wallet_registry_cmd.add_argument("--json-out", default="reports/wallet_registry.json")
    wallet_registry_cmd.add_argument("--limit", type=int, default=0)
    wallet_registry_cmd.add_argument(
        "--stages",
        default="",
        help="Optional comma-separated stages to export; default exports every wallet",
    )
    wallet_registry_cmd.add_argument(
        "--winner-only",
        action="store_true",
        help="Export only wallets that pass the filtered winner-library eligibility contract",
    )
    wallet_registry_cmd.add_argument("--no-csv", action="store_true")
    wallet_registry_cmd.add_argument("--no-json", action="store_true")

    maintenance_cmd = sub.add_parser("maintenance", help="Cleanup old rows, old backups, and optimize SQLite")
    maintenance_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    maintenance_cmd.add_argument("--api-log-days", type=int, default=7)
    maintenance_cmd.add_argument("--positions-days", type=int, default=14)
    maintenance_cmd.add_argument("--scores-days", type=int, default=30)
    maintenance_cmd.add_argument("--review-events-days", type=int, default=30)
    maintenance_cmd.add_argument("--ingest-runs-days", type=int, default=30)
    maintenance_cmd.add_argument("--keep-backups", type=int, default=2)
    maintenance_cmd.add_argument("--dry-run", action="store_true")
    maintenance_cmd.add_argument("--vacuum", action="store_true")
    maintenance_cmd.add_argument(
        "--skip-cleanup",
        action="store_true",
        help="Skip cleanup scans; useful for lightweight storage/WAL maintenance",
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
        "--reset-stale-ingest-runs",
        action="store_true",
        help="Mark old running ingest audit rows interrupted; work queues are left untouched",
    )
    maintenance_cmd.add_argument(
        "--stale-ingest-run-seconds",
        type=int,
        default=21_600,
        help="Age threshold for stale running ingest audit rows",
    )

    prune_cmd = sub.add_parser("prune-evidence", help="Prune low-value raw wallet evidence after materialization")
    prune_cmd.add_argument("--db", dest="command_db", default=None, help="SQLite database path")
    prune_cmd.add_argument("--limit", type=int, default=20)
    prune_cmd.add_argument("--keep-recent-activity", type=int, default=100)
    prune_cmd.add_argument("--execute", action="store_true", help="Actually delete rows; default is dry-run")
    prune_cmd.add_argument("--vacuum", action="store_true")

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
    if args.command not in {"health", "status"}:
        settings.assert_safe()
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
        conn = connect(db_path)
        try:
            run_migrations(conn)
            run_id = record_runtime_heartbeat(
                conn,
                args.name,
                status=args.status,
                rows_written=args.rows_written,
                error=args.error,
            )
        finally:
            conn.close()
        print(json.dumps({"run_id": run_id, "ingest_type": args.name, "status": args.status}, ensure_ascii=False))
        return 0
    if args.command == "import-addresses":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            count = import_candidates_from_csv(
                conn,
                addresses_path=Path(args.addresses),
                source_event_mode=_source_event_mode_arg(args.source_event_mode),
            )
        finally:
            conn.close()
        print(f"imported {count} candidate addresses into {db_path}")
        return 0
    if args.command == "import-features":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            count = import_features_from_csv(conn, features_path=Path(args.features))
        finally:
            conn.close()
        print(f"imported {count} wallet feature rows into {db_path}")
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
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = discover_activity_candidates(
                conn,
                pages=args.pages,
                page_limit=args.page_limit,
                min_trades=args.min_trades,
                min_usdc_volume=args.min_usdc_volume,
                min_trade_filter_usdc=args.min_trade_filter_usdc,
                max_candidates=args.max_candidates,
                sleep_seconds=args.sleep,
                output_path=Path(args.out) if args.out else None,
                write_db=not args.no_db_write,
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
                paper_min_trade_usdc=args.paper_min_trade_usdc,
                batch_size=args.batch_size,
                flush_interval=args.flush_interval,
                ping_interval=args.ping_interval,
                receive_timeout=args.receive_timeout,
                reconnect_sleep=args.reconnect_sleep,
                max_runtime_seconds=args.max_runtime_seconds,
                max_messages=args.max_messages,
                max_reconnects=args.max_reconnects,
                watch_min_score=args.watch_min_score,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "ingest-positions":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = ingest_positions(
                conn,
                limit=args.limit,
                size_threshold=args.size_threshold,
                sleep_seconds=args.sleep,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "backlog_active"} else 1
    if args.command == "ingest-activity":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = ingest_activity(
                conn,
                wallet_limit=args.wallet_limit,
                page_limit=args.page_limit,
                max_events_per_wallet=args.max_events_per_wallet,
                target_events_per_wallet=args.target_events_per_wallet,
                paper_stage_only=args.paper_stage_only,
                sleep_seconds=args.sleep,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "ingest-gamma-markets":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = ingest_gamma_markets(
                conn,
                limit=args.limit,
                ttl_seconds=args.ttl_seconds,
                failure_ttl_seconds=args.failure_ttl_seconds,
                sleep_seconds=args.sleep,
                paper_only=args.paper_only,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "mine-copy-graph":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = mine_copy_graph(conn, load_policy(Path(args.policy)))
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0
    if args.command == "backtest-copy-stream":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = backtest_copy_stream(conn, load_policy(Path(args.policy)))
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0
    if args.command == "copyability-plan":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = plan_copyability_evidence_jobs(
                conn,
                limit=args.limit,
                max_active_jobs=args.max_active_jobs,
                min_score=args.min_score,
                min_activity_events=args.min_activity_events,
                shard_count=args.shard_count,
                rescan_seconds=args.rescan_seconds,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "backlog_active"} else 1
    if args.command == "copyability-worker":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_copyability_evidence_worker(
                conn,
                shard_index=args.shard_index,
                shard_count=args.shard_count,
                limit=args.limit,
                lease_seconds=args.lease_seconds,
                worker_id=args.worker_id,
                policy_path=args.policy,
                max_leader_events=args.max_leader_events,
                max_followers_per_event=args.max_followers_per_event,
                prefer_scan_mode=args.prefer_scan_mode,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "copyability-jobs":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = copyability_evidence_job_status(conn)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "activity-coverage":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = activity_coverage_summary(conn) if args.summary else activity_coverage(conn, limit=args.limit)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "evidence-backfill":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_evidence_backfill(
                conn,
                light_limit=args.light_limit,
                medium_limit=args.medium_limit,
                deep_limit=args.deep_limit,
                page_limit=args.page_limit,
                sleep_seconds=args.sleep,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "evidence-backfill-status":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = evidence_backfill_summary(conn)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "evidence-backfill-plan":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = plan_queued_evidence_backfill(
                conn,
                light_limit=args.light_limit,
                medium_limit=args.medium_limit,
                deep_limit=args.deep_limit,
                shard_count=args.shard_count,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "evidence-backfill-worker":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_queued_evidence_backfill_worker(
                conn,
                shard_index=args.shard_index,
                shard_count=args.shard_count,
                limit=args.limit,
                page_limit=args.page_limit,
                sleep_seconds=args.sleep,
                lease_seconds=args.lease_seconds,
                max_attempts=args.max_attempts,
                worker_id=args.worker_id,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "evidence-backfill-jobs":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = queued_evidence_backfill_status(conn)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "wallet-pipeline-state":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = (
                materialize_wallet_processing_state(
                    conn,
                    limit=args.limit,
                    commit_every=args.commit_every,
                    stale_only=args.stale_only,
                )
                if args.materialize
                else wallet_processing_state_summary(conn)
            )
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "wallet-pipeline-plan":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = plan_wallet_pipeline_jobs(
                conn,
                light_limit=args.light_limit,
                medium_limit=args.medium_limit,
                deep_limit=args.deep_limit,
                shard_count=args.shard_count,
                max_active_jobs=args.max_active_jobs,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "wallet-pipeline-worker":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_wallet_pipeline_worker(
                conn,
                shard_index=args.shard_index,
                shard_count=args.shard_count,
                limit=args.limit,
                page_limit=args.page_limit,
                sleep_seconds=args.sleep,
                lease_seconds=args.lease_seconds,
                priority_aging_seconds=args.priority_aging_seconds,
                worker_id=args.worker_id,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "wallet-pipeline-jobs":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = wallet_pipeline_job_status(
                conn,
                priority_aging_seconds=_env_int(
                    "PM_ROBOT_PIPELINE_PRIORITY_AGING_SECONDS",
                    DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS,
                ),
                stage_weights={
                    EvidenceJobStage.LIGHT_PENDING.value: _env_int(
                        "PM_ROBOT_PIPELINE_PLANNER_LIGHT_LIMIT",
                        DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.LIGHT_PENDING.value],
                    ),
                    EvidenceJobStage.MEDIUM_PENDING.value: _env_int(
                        "PM_ROBOT_PIPELINE_PLANNER_MEDIUM_LIMIT",
                        DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.MEDIUM_PENDING.value],
                    ),
                    EvidenceJobStage.DEEP_PENDING.value: _env_int(
                        "PM_ROBOT_PIPELINE_PLANNER_DEEP_LIMIT",
                        DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.DEEP_PENDING.value],
                    ),
                },
            )
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "pipeline-jobs":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = pipeline_job_summary(conn, job_type=args.job_type)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "eligibility-repair-plan":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = prepare_eligibility_repairs(
                conn,
                limit=args.limit,
                min_score=args.min_score,
                min_copyability_activity_events=args.min_copyability_activity_events,
                dry_run=args.dry_run,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status == "ok" else 1
    if args.command == "pipeline-smoothness":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            report = pipeline_smoothness_report(
                conn,
                top=args.top,
                min_score=args.min_score,
                min_copyability_activity_events=args.min_copyability_activity_events,
            )
        finally:
            conn.close()
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1
    if args.command == "pipeline-audit":
        paper_min_score = args.paper_min_score
        if paper_min_score is None:
            policy = load_policy(settings.policy_path)
            paper_min_score = (policy.get("review_bands") or {}).get("paper_candidate")
            if paper_min_score is None:
                raise ValueError("active scoring policy is missing review_bands.paper_candidate")
        conn = connect_readonly(db_path)
        try:
            report = pipeline_audit_report(
                conn,
                top=args.top,
                min_score=args.min_score,
                paper_min_score=float(paper_min_score),
            )
        finally:
            conn.close()
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1
    if args.command == "pipeline-cycle":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            report = run_pipeline_cycle(
                conn,
                PipelineCycleOptions(
                    execute_plan=args.execute_plan,
                    top=args.top,
                    min_score=args.min_score,
                    state_limit=args.state_limit,
                    repair_limit=args.repair_limit,
                    shard_count=args.shard_count,
                    wallet_light_limit=args.wallet_light_limit,
                    wallet_medium_limit=args.wallet_medium_limit,
                    wallet_deep_limit=args.wallet_deep_limit,
                    copyability_limit=args.copyability_limit,
                    copyability_max_active_jobs=args.copyability_max_active_jobs,
                    copyability_min_activity_events=args.copyability_min_activity_events,
                    feature_limit=args.feature_limit,
                    feature_min_activity_events=args.feature_min_activity_events,
                    score_limit=args.score_limit,
                    run_scoring=not args.no_score,
                    policy_path=Path(args.policy),
                ),
            )
        finally:
            conn.close()
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1
    if args.command == "prioritize-backfill":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = prioritize_backfill_from_scores(
                conn,
                min_score=args.min_score,
                limit=args.limit,
                target_depth=args.target_depth,
                priority=args.priority,
                source=args.source,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0
    if args.command == "materialize-features":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = materialize_wallet_features(
                conn,
                limit=args.limit,
                min_activity_events=args.min_activity_events,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "ingest-trade-roles":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = ingest_trade_role_evidence(conn, limit=args.limit)
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0 if summary.status in {"ok", "partial"} else 1
    if args.command == "gamma-cache":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = gamma_market_cache_summary(conn)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "build-review" and args.csv_only:
        counts = build_review_queue(
            addresses_path=Path(args.addresses),
            features_path=Path(args.features) if args.features else None,
            policy_path=Path(args.policy),
            out_path=Path(args.out),
        )
        total = sum(counts.values())
        print(f"wrote {total} review rows to {args.out}")
        for stage, count in sorted(counts.items()):
            print(f"{stage}: {count}")
        return 0
    if args.command == "build-review":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            # Import/update CSV sources before scoring so existing workflows keep working.
            if not args.no_import_csv:
                import_candidates_from_csv(
                    conn,
                    addresses_path=Path(args.addresses),
                    source_event_mode=_source_event_mode_arg(args.source_event_mode),
                )
            if args.features:
                import_features_from_csv(conn, features_path=Path(args.features))
            counts = score_database(
                conn,
                policy_path=Path(args.policy),
                export_path=Path(args.out),
                incremental=args.incremental,
                limit=args.limit,
            )
        finally:
            conn.close()
        operational_keys = {
            "score_candidates_considered",
            "scores_written",
            "incomplete_rescore_skipped",
            "unchanged_score_skipped",
            "masked_valid_scores_restored",
            "paper_quality_blocked_this_run",
            "copyability_no_signal_blocked_this_run",
        }
        stage_total = sum(count for stage, count in counts.items() if stage not in operational_keys)
        considered = counts.get("score_candidates_considered", stage_total)
        written = counts.get("scores_written", stage_total)
        print(f"reviewed {considered} score candidates in {db_path}")
        print(f"wrote {written} new score rows")
        print(f"exported review report to {args.out}")
        for stage, count in sorted(counts.items()):
            print(f"{stage}: {count}")
        return 0
    if args.command == "health":
        out = Path(args.out) if args.out else None
        data = write_health(settings, out)
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0 if data["ok"] else 1
    if args.command == "status":
        print(json.dumps(status(settings), ensure_ascii=False, indent=2))
        return 0
    if args.command == "backup":
        out = backup_database(settings)
        print(f"backup written to {out}")
        return 0
    if args.command == "backup-sql-dump":
        dump_database_sql(settings, sys.stdout.buffer)
        return 0
    if args.command == "paper-run":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = run_paper(
                conn,
                ledger_path=None if args.no_jsonl else settings.paper_ledger_path,
                limit=args.limit,
                max_stake_usd=args.max_stake_usd,
                max_signal_age_sec=args.max_signal_age_sec,
                include_watchlist_min_score=args.include_watchlist_min_score,
                include_review_min_score=args.include_review_min_score,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0
    if args.command == "paper-observer-preview":
        conn = connect_readonly(db_path)
        try:
            summary = preview_paper_observer(
                conn,
                limit=args.limit,
                max_signal_age_sec=args.max_signal_age_sec,
            )
        finally:
            conn.close()
        payload = summary.__dict__
        if args.out:
            out = Path(args.out)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if args.command == "paper-observer-evaluate":
        conn = connect(db_path) if args.persist else connect_readonly(db_path)
        try:
            if args.persist:
                run_migrations(conn)
            summary = evaluate_paper_observer(
                conn,
                limit=args.limit,
                max_stake_usd=args.max_stake_usd,
                max_signal_age_sec=args.max_signal_age_sec,
                max_actionable_signal_age_sec=args.max_actionable_signal_age_sec,
                persist=args.persist,
            )
        finally:
            conn.close()
        payload = summary.__dict__
        if args.out:
            out = Path(args.out)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if args.command == "paper-handoff-export":
        payload = paper_handoff_data(settings, limit=args.limit)
        if args.out:
            out = Path(args.out)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        if not args.no_csv and args.csv_out:
            csv_out = Path(args.csv_out)
            csv_out.parent.mkdir(parents=True, exist_ok=True)
            csv_out.write_text(paper_handoff_csv(settings, limit=args.limit), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if args.command == "execution-preflight":
        conn = connect_readonly(db_path)
        try:
            payload = execution_preflight_status(
                conn,
                max_signal_age_sec=args.max_signal_age_sec,
                execution_services=_space_or_csv_tuple(args.execution_services) or DEFAULT_EXECUTION_SERVICES,
                compose_rows=parse_compose_rows(args.compose_ps_json),
                compose_error=args.compose_error,
            )
        finally:
            conn.close()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if args.command == "paper-realtime-audit":
        conn = connect_readonly(db_path)
        try:
            payload = paper_realtime_audit_status(
                conn,
                max_signal_age_sec=args.max_signal_age_sec,
                lookback_sec=args.lookback_sec,
                limit=args.limit,
            )
        finally:
            conn.close()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if args.command == "rtds-watch-audit":
        conn = connect_readonly(db_path)
        try:
            payload = rtds_watch_audit_status(
                conn,
                min_score=args.min_score,
                lookback_sec=args.lookback_sec,
                limit=args.limit,
            )
        finally:
            conn.close()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if args.command == "paper-settle":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = settle_paper_portfolio(conn)
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0
    if args.command == "paper-readiness":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = paper_readiness_rows(conn)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "publish-leaders":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            summary = publish_leaders(
                conn,
                ttl_seconds=args.ttl_seconds,
                output_path=Path(args.out) if args.out else None,
            )
        finally:
            conn.close()
        print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2))
        return 0
    if args.command == "published-leaders":
        conn = connect(db_path)
        try:
            run_migrations(conn)
            rows = active_published_leaders(conn)
        finally:
            conn.close()
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    if args.command == "wallet-registry":
        builder = build_winner_library if args.winner_only else build_wallet_registry
        data = builder(
            settings,
            limit=args.limit,
            stages=_csv_tuple(args.stages),
            csv_output_path=None if args.no_csv else Path(args.out),
            json_output_path=None if args.no_json else Path(args.json_out),
        )
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0 if data["ok"] else 1
    if args.command == "maintenance":
        data = maintenance(
            settings,
            api_log_days=args.api_log_days,
            positions_days=args.positions_days,
            scores_days=args.scores_days,
            review_events_days=args.review_events_days,
            ingest_runs_days=args.ingest_runs_days,
            keep_backups=args.keep_backups,
            dry_run=args.dry_run,
            vacuum=args.vacuum,
            wal_checkpoint=args.wal_checkpoint,
            skip_cleanup=args.skip_cleanup,
            reset_stale_jobs=args.reset_stale_jobs,
            failed_job_cooldown_seconds=args.failed_job_cooldown_seconds,
            reset_stale_ingest_runs=args.reset_stale_ingest_runs,
            stale_ingest_run_seconds=args.stale_ingest_run_seconds,
        )
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0 if data["ok"] else 1
    if args.command == "prune-evidence":
        data = prune_low_value_evidence(
            settings,
            limit=args.limit,
            keep_recent_activity=args.keep_recent_activity,
            dry_run=not args.execute,
            vacuum=args.vacuum,
        )
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0 if data["ok"] else 1
    if args.command == "web":
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


def _space_or_csv_tuple(value: str) -> tuple[str, ...]:
    text = value.replace(",", " ")
    return tuple(part.strip() for part in text.split() if part.strip())


def _source_event_mode_arg(value: str) -> str:
    if value == "upsert-source":
        return "upsert_source"
    return value


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name) or default)
    except (TypeError, ValueError):
        return int(default)


if __name__ == "__main__":
    raise SystemExit(main())
