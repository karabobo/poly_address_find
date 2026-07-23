import inspect
from pathlib import Path

import pm_robot.web as web_module
from pm_robot.config import RobotSettings
from pm_robot.storage.db import connect, run_migrations
from pm_robot.web import WebConsoleConfig, _render_dashboard, dashboard_data, run_web_console


FORBIDDEN_SURFACE_TERMS = (
    "paper",
    "paper_candidate",
    "paper_approved",
    "copyability",
    "observer",
    "publish",
    "execution",
    "needs_manual_review",
    "candidate_stage",
    "live_eligible",
)


def _settings(tmp_path: Path) -> RobotSettings:
    return RobotSettings(
        db_path=tmp_path / "pm_robot.sqlite",
        archive_dir=tmp_path / "parquet",
    )


def test_web_console_binds_before_starting_dashboard_prewarm(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()
    events = []

    class FakeServer:
        def __init__(self, address, handler):
            events.append(("bind", address, handler))

        def serve_forever(self):
            events.append(("serve",))

    monkeypatch.setattr(web_module, "ThreadingHTTPServer", FakeServer)
    monkeypatch.setattr(web_module, "_start_dashboard_cache_prewarm", lambda _settings: events.append(("prewarm",)))

    run_web_console(WebConsoleConfig(settings=settings, host="127.0.0.1", port=8787))

    assert [event[0] for event in events] == ["bind", "prewarm", "serve"]


def test_canonical_routes_are_wallet_research_only():
    source = inspect.getsource(web_module._handler_factory).lower()

    assert "/api/summary" in source
    assert "/api/wallet-levels" in source
    assert "/api/wallets" in source
    assert "/api/wallet/" in source
    assert all(term not in source for term in FORBIDDEN_SURFACE_TERMS)


def test_empty_dashboard_is_l0_l6_research_surface(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()
    monkeypatch.setenv("PM_ROBOT_WEB_DASHBOARD_CACHE_TTL_SEC", "0")

    data = dashboard_data(settings)
    html = _render_dashboard(settings)
    serialized = str(data).lower()

    assert data["schema_version"] == "wallet_research_v2"
    assert [row["level"] for row in data["level_counts"]] == [f"l{index}" for index in range(7)]
    assert [row["count"] for row in data["level_counts"]] == [0, 0, 0, 0, 0, 0, 0]
    assert [row["job_type"] for row in data["queues"]] == [
        "wallet_recent_screen",
        "wallet_history_collect",
        "wallet_l6_validate",
    ]
    assert "钱包研究分级" in html
    assert all(f"L{index}" in html for index in range(7))
    assert "快速初筛队列" in html
    assert "历史采集队列" in html
    assert "L6 独立复核队列" in html
    assert "高等级钱包" in html
    assert str(tmp_path) not in serialized
    assert str(tmp_path) not in html
    assert all(term not in serialized for term in FORBIDDEN_SURFACE_TERMS)
    assert all(term not in html.lower() for term in FORBIDDEN_SURFACE_TERMS)
