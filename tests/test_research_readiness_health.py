from pm_robot.config import RobotSettings
import json
from concurrent.futures import ThreadPoolExecutor

from pm_robot.ops import _write_json_atomically, health_check
from pm_robot.storage.api_rate_limit import RateLimitScope, SharedApiRateLimiter
from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_levels import advance_wallet_level, ensure_wallet_level
from pm_robot.wallet_levels import WalletLevel


def _settings(tmp_path, db_path, *, rate_limit_db_path=None):
    settings = RobotSettings(
        db_path=db_path,
        rate_limit_db_path=rate_limit_db_path,
        log_dir=tmp_path / "logs",
        backup_dir=tmp_path / "backups",
        archive_dir=tmp_path / "parquet",
    )
    settings.log_dir.mkdir()
    settings.backup_dir.mkdir()
    settings.archive_dir.mkdir()
    return settings


def test_health_snapshot_writes_remain_valid_under_concurrent_replacement(tmp_path):
    output = tmp_path / "health.json"

    def write_snapshot(writer: int) -> None:
        for sequence in range(10):
            _write_json_atomically(
                output,
                {
                    "writer": writer,
                    "sequence": sequence,
                    "payload": str(writer) * 100_000,
                },
            )

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(write_snapshot, range(4)))

    snapshot = json.loads(output.read_text(encoding="utf-8"))
    assert snapshot["writer"] in range(4)
    assert snapshot["sequence"] == 9
    assert len(snapshot["payload"]) == 100_000
    assert list(tmp_path.glob(".health.json.*.tmp")) == []


def test_health_reports_wallet_funnel_control_plane_separately_from_outputs(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()

    result = health_check(_settings(tmp_path, db_path))

    assert result["ok"] is True
    assert result["upstream_request_budget"]["scope_count"] == 0
    assert result["research_readiness"]["ready"] is True
    assert result["research_readiness"]["blockers"] == []
    assert result["research_readiness"]["elite_wallets_available"] is False
    assert "production_readiness" not in result


def test_health_reads_dedicated_rate_limit_database(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    rate_limit_db_path = tmp_path / "api_rate_limits.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()
    SharedApiRateLimiter(rate_limit_db_path, initialize_schema=True).reserve(
        [RateLimitScope("data:/activity", capacity=30, window_seconds=10)],
        now=100,
    )

    result = health_check(
        _settings(tmp_path, db_path, rate_limit_db_path=rate_limit_db_path)
    )

    assert result["ok"] is True
    assert result["upstream_request_budget"]["storage"] == "dedicated"
    assert result["upstream_request_budget"]["scope_count"] == 1


def test_health_pipeline_omits_retired_runtime_events(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO runtime_heartbeats(
                name, started_at, finished_at, status, error
            ) VALUES (?, 100, 101, 'ok', '')
            """,
            (
                ("loop_discovery_leaderboard",),
                ("loop_wallet_level_control",),
                ("loop_retention_prune",),
                ("loop_paper_observer_activity",),
                ("copyability_evidence_worker_0",),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    result = health_check(_settings(tmp_path, db_path))

    assert set(result["pipeline"]) == {
        "loop_discovery_leaderboard",
        "loop_wallet_level_control",
    }


def test_health_uses_loop_specific_heartbeat_freshness_windows(tmp_path, monkeypatch):
    db_path = tmp_path / "robot.sqlite"
    now = 10_000
    conn = connect(db_path)
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO runtime_heartbeats(
                name, started_at, finished_at, status, error
            ) VALUES (?, ?, ?, 'ok', '')
            """,
            (
                ("loop_discovery_leaderboard", now - 1_800, now - 1_800),
                ("loop_wallet_history_worker_0", now - 1_800, now - 1_800),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    settings = _settings(tmp_path, db_path)
    settings = RobotSettings(
        **{
            **settings.__dict__,
            "required_runtime_heartbeats": (
                "loop_discovery_leaderboard",
                "loop_wallet_history_worker_0",
            ),
            "runtime_heartbeat_max_age_seconds": 900,
            "runtime_heartbeat_max_age_overrides": (
                ("loop_discovery_leaderboard", 7_200),
            ),
        }
    )
    monkeypatch.setattr("pm_robot.ops.time.time", lambda: now)

    readiness = health_check(settings)["runtime_readiness"]

    assert readiness["ready"] is False
    assert readiness["stale"] == ["loop_wallet_history_worker_0"]
    assert readiness["max_age_seconds_by_name"] == {
        "loop_discovery_leaderboard": 7_200,
        "loop_wallet_history_worker_0": 900,
    }


def test_health_fails_closed_on_wallet_ingress_drift(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "9" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, first_seen_at, updated_at
            ) VALUES (?, 'broken-test-ingress', 100, 100)
            """,
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    result = health_check(_settings(tmp_path, db_path))

    assert result["ok"] is False
    assert result["research_readiness"]["ready"] is False
    assert result["research_readiness"]["blockers"] == [
        "candidate_without_observation=1",
        "candidate_without_level=1",
    ]
    assert result["research_readiness"]["metrics"]["ingress_invariants"] == {
        "candidate_without_observation": 1,
        "candidate_without_level": 1,
        "observation_without_level": 0,
    }


def test_settings_parse_heartbeat_freshness_overrides(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "PM_ROBOT_RUNTIME_HEARTBEAT_MAX_AGE_OVERRIDES",
        "loop_discovery_leaderboard:7200,loop_discovery_activity:7200",
    )

    settings = RobotSettings.load(tmp_path / "missing.env")

    assert settings.runtime_heartbeat_max_age_overrides == (
        ("loop_discovery_leaderboard", 7_200),
        ("loop_discovery_activity", 7_200),
    )


def test_health_counts_current_wallet_levels_without_execution_tables(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    elite = "0x" + "1" * 40
    deep = "0x" + "2" * 40
    discovered = "0x" + "3" * 40
    try:
        run_migrations(conn)
        for wallet in (elite, deep, discovered):
            ensure_wallet_level(conn, wallet, reason="test", now=100)
        for level in (
            WalletLevel.L1,
            WalletLevel.L2,
            WalletLevel.L3,
            WalletLevel.L4,
            WalletLevel.L5,
        ):
            advance_wallet_level(conn, elite, to_level=level, reason="test", now=100)
        for level in (WalletLevel.L1, WalletLevel.L2, WalletLevel.L3):
            advance_wallet_level(conn, deep, to_level=level, reason="test", now=100)
        conn.commit()
    finally:
        conn.close()

    result = health_check(_settings(tmp_path, db_path))
    readiness = result["research_readiness"]

    assert readiness["metrics"]["levels"] == {
        "l0": 1,
        "l1": 0,
        "l2": 0,
        "l3": 1,
        "l4": 0,
        "l5": 1,
        "l6": 0,
    }
    assert readiness["metrics"]["fresh_elite_wallets"] == 0
    assert readiness["elite_wallets_available"] is False


def test_health_only_exposes_l5_with_recent_deep_evidence_as_current_elite(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    wallet = "0x" + "4" * 40
    now = 2_000_000
    try:
        run_migrations(conn)
        ensure_wallet_level(conn, wallet, reason="test", now=100)
        for level in (
            WalletLevel.L1,
            WalletLevel.L2,
            WalletLevel.L3,
            WalletLevel.L4,
            WalletLevel.L5,
        ):
            advance_wallet_level(conn, wallet, to_level=level, reason="test", now=200)
        conn.execute(
            """
            INSERT INTO wallet_history_summaries(
                wallet, artifact_id, history_depth, activity_count,
                distinct_markets, total_volume_usdc, strategy_tags_json,
                risk_flags_json, research_score, score_components_json,
                methodology_version, computed_at, updated_at
            ) VALUES (?, 'artifact-fresh', 'deep', 200, 10, 5000,
                      '[]', '[]', 80, '{}', 'wallet_history_summary_v2', ?, ?)
            """,
            (wallet, now - 1_000, now - 1_000),
        )
        conn.execute(
            """
            INSERT INTO wallet_level_selections(
                wallet, target_level, evidence_artifact_id, policy_version,
                selected, rank_in_cohort, cohort_size, source_bucket,
                strategy_bucket, reason, decided_at, updated_at
            ) VALUES (?, 'l5', 'artifact-fresh', ?,
                      1, 1, 20, 'stream', 'general',
                      'relative_rank_selected', ?, ?)
            """,
            (wallet, SELECTION_POLICY_VERSION, now - 900, now - 900),
        )
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setattr("pm_robot.ops.time.time", lambda: now)

    readiness = health_check(_settings(tmp_path, db_path))["research_readiness"]

    assert readiness["metrics"]["fresh_elite_wallets"] == 1
    assert readiness["elite_wallets_available"] is True
