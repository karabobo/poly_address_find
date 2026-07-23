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
    "wallet_history_summaries",
    "wallet_level_events",
    "wallet_level_selections",
    "wallet_levels",
    "wallet_l6_validations",
    "wallet_pnl_summaries",
    "wallet_screen_summaries",
}


def _columns(conn, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


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
