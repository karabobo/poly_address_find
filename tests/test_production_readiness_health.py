from pm_robot.ops import health_check
from pm_robot.config import RobotSettings
from pm_robot.storage.api_rate_limit import RateLimitScope, SharedApiRateLimiter
from pm_robot.storage.db import connect, run_migrations


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
