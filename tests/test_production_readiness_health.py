from pm_robot.ops import health_check
from pm_robot.config import RobotSettings
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
