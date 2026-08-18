import ast
import re
from pathlib import Path

from pm_robot.storage.db import connect, run_migrations


RETIRED_TABLES = {
    "copy_backtest_trades",
    "copy_leader_performance",
    "copy_leader_stats",
    "copy_pair_stats",
    "copy_trade_links",
    "evidence_backfill_budget",
    "ingest_runs",
    "wallet_activity",
    "wallet_activity_watermarks",
    "wallet_registry",
}

CURRENT_RESEARCH_TABLES = {
    "api_rate_limit_state",
    "api_request_log",
    "candidate_source_events",
    "candidate_wallets",
    "observed_wallets",
    "pipeline_jobs",
    "runtime_heartbeats",
    "schema_migrations",
    "wallet_features",
    "wallet_history_artifacts",
    "wallet_history_planner_dirty",
    "wallet_history_planner_sighting_dirty",
    "wallet_history_planner_state",
    "wallet_history_summaries",
    "wallet_level_events",
    "wallet_level_review_state",
    "wallet_level_selections",
    "wallet_levels",
    "wallet_l6_validations",
    "wallet_pnl_summaries",
    "wallet_screen_summaries",
}

SQL_NUMERIC_SEPARATOR = re.compile(
    r"(?<![A-Za-z0-9])\d[\d_]*_\d[\d_]*(?![A-Za-z0-9])"
)


def _columns(conn, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _index_columns(conn, index: str) -> list[str]:
    return [row["name"] for row in conn.execute(f"PRAGMA index_info({index})")]


def test_python_sql_strings_use_legacy_sqlite_compatible_numeric_literals():
    violations = []
    for root in (Path("src"), Path("tests")):
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                    continue
                values = sorted(set(SQL_NUMERIC_SEPARATOR.findall(node.value)))
                if values:
                    violations.append(f"{path}:{node.lineno}: {', '.join(values)}")

    assert violations == []


def test_final_research_schema_exposes_only_current_control_plane(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        candidate_columns = _columns(conn, "candidate_wallets")
        feature_columns = _columns(conn, "wallet_features")
        job_columns = _columns(conn, "pipeline_jobs")
        heartbeat_columns = _columns(conn, "runtime_heartbeats")
        planner_dirty_columns = _columns(conn, "wallet_history_planner_dirty")
        planner_sighting_columns = _columns(
            conn, "wallet_history_planner_sighting_dirty"
        )
        planner_bootstrap_columns = _index_columns(
            conn, "idx_wallet_levels_history_planner_bootstrap"
        )
        type_claim_columns = _index_columns(conn, "idx_pipeline_jobs_type_claim")
    finally:
        conn.close()

    assert tables == CURRENT_RESEARCH_TABLES
    assert RETIRED_TABLES.isdisjoint(tables)
    assert {"runtime_heartbeats", "pipeline_jobs"}.issubset(tables)
    assert {"job_action", "job_scope"}.issubset(job_columns)
    assert {"subject_key", "tier"}.isdisjoint(job_columns)
    assert "candidate_stage" not in candidate_columns
    assert {"copy_event_count", "copy_stream_roi"}.isdisjoint(feature_columns)
    assert {"name", "started_at", "finished_at", "status"}.issubset(
        heartbeat_columns
    )
    assert "dirty_generation" in planner_dirty_columns
    assert {
        "wallet",
        "last_seen_at",
        "dirty_at",
        "dirty_generation",
    }.issubset(planner_sighting_columns)
    assert planner_bootstrap_columns == ["level", "wallet"]
    assert type_claim_columns == [
        "job_type",
        "status",
        "shard",
        "next_attempt_at",
        "priority",
        "updated_at",
    ]
