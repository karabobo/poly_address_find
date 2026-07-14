from pm_robot.ops import health_check
from pm_robot.config import RobotSettings
from pm_robot.storage.api_rate_limit import RateLimitScope, SharedApiRateLimiter
from pm_robot.storage.db import connect, run_migrations
from pm_robot.models import CandidateAddress
from pm_robot.storage.repository import upsert_candidate


def test_health_reports_business_closure_separately_from_operational_health(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()
    settings = RobotSettings(
        db_path=db_path,
        log_dir=tmp_path / "logs",
        backup_dir=tmp_path / "backups",
    )
    settings.log_dir.mkdir()
    settings.backup_dir.mkdir()

    result = health_check(settings)

    assert result["ok"] is True
    assert result["upstream_request_budget"]["scope_count"] == 0
    assert result["upstream_request_budget"]["active_cooldowns"] == 0
    assert result["production_readiness"]["closed"] is False
    assert "no_active_published_leaders" in result["production_readiness"]["blockers"]


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
    settings = RobotSettings(
        db_path=db_path,
        rate_limit_db_path=rate_limit_db_path,
        log_dir=tmp_path / "logs",
        backup_dir=tmp_path / "backups",
    )
    settings.log_dir.mkdir()
    settings.backup_dir.mkdir()

    result = health_check(settings)

    assert result["ok"] is True
    assert result["upstream_request_budget"]["storage"] == "dedicated"
    assert result["upstream_request_budget"]["scope_count"] == 1


def test_health_counts_only_copy_leaders_with_current_qualified_pairs(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    orphan = "0x" + "1" * 40
    valid = "0x" + "2" * 40
    follower = "0x" + "3" * 40
    try:
        run_migrations(conn)
        for wallet in (orphan, valid, follower):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        for wallet in (orphan, valid):
            conn.execute(
                """
                INSERT INTO copy_leader_performance(
                    leader_wallet, backtest_trade_count, copied_market_count,
                    total_stake_usdc, gross_pnl_usdc, net_pnl_usdc,
                    gross_roi, net_roi, win_rate, median_lag_seconds,
                    last_backtest_trade_at, updated_at, edge_retention_pct,
                    walk_forward_consistency_pct, max_drawdown_pct
                ) VALUES (?, 10, 5, 100, 20, 15, 0.2, 0.15, 0.7, 2,
                          200, 300, 80, 70, 0.1)
                """,
                (wallet,),
            )
        conn.execute(
            """
            INSERT INTO copy_pair_stats(
                leader_wallet, follower_wallet, copy_event_count, copy_market_count,
                follower_trade_count, containment_pct, leader_precedes_pct,
                median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
            ) VALUES (?, ?, 10, 5, 20, 0.3, 1.0, 2, 100, 200, 1, 300)
            """,
            (valid, follower),
        )
        conn.commit()
    finally:
        conn.close()

    settings = RobotSettings(
        db_path=db_path,
        log_dir=tmp_path / "logs",
        backup_dir=tmp_path / "backups",
    )
    settings.log_dir.mkdir()
    settings.backup_dir.mkdir()

    result = health_check(settings)

    assert result["production_readiness"]["metrics"]["qualified_copy_pairs"] == 1
    assert result["production_readiness"]["metrics"]["validated_copy_leaders"] == 1
