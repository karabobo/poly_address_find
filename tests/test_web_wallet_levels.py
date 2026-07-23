import json
import time
from pathlib import Path

from pm_robot.config import RobotSettings
from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.l6_validation import L6_VALIDATION_POLICY_VERSION
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.db import connect, run_migrations
from pm_robot.web import (
    _render_dashboard,
    _render_wallet_detail,
    dashboard_data,
    wallet_detail_data,
    wallet_table_rows,
)


WALLET = "0xabc0000000000000000000000000000000000001"
LEGACY_WALLET = "0xabc0000000000000000000000000000000000002"
FORBIDDEN_SURFACE_TERMS = (
    "paper",
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


def _seed_wallet_research_data(settings: RobotSettings) -> None:
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        now = 1_800_000_000
        conn.execute(
            """
            INSERT INTO observed_wallets(
                wallet, sources, labels, status, observed_trade_count,
                recent_trade_count, recent_usdc_total, recent_max_trade_usdc,
                first_seen_at, updated_at
            ) VALUES (?, 'rtds,polymarket_leaderboard', 'politics', 'active', 24,
                      10, 640, 180, ?, ?)
            """,
            (WALLET, now - 86_400, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_levels(
                wallet, level, level_reason, policy_version, first_seen_at,
                last_seen_at, level_updated_at, updated_at
            ) VALUES (?, 'l4', 'relative_rank_selected', 'levels-v2', ?, ?, ?, ?)
            """,
            (WALLET, now - 86_400, now, now - 120, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_screen_summaries(
                wallet, sample_limit, sample_trade_count, sample_volume_usdc,
                sample_market_count, latest_trade_at, screen_complete,
                screen_qualified, screen_reason, computed_at, updated_at
            ) VALUES (?, 10, 10, 640, 4, ?, 1, 1,
                      'sample_volume_at_least_100_usdc', ?, ?)
            """,
            (WALLET, now - 300, now - 200, now - 200),
        )
        conn.execute(
            """
            INSERT INTO wallet_pnl_summaries(
                wallet, current_position_value_usdc, open_estimated_pnl_usdc,
                closed_realized_pnl_usdc, total_estimated_pnl_usdc,
                capital_basis_usdc, cost_roi_estimate, open_position_count,
                closed_position_count, coverage, methodology_version,
                captured_at, updated_at
            ) VALUES (?, 2100, 180, 720, 900, 5000, 0.18, 6, 25,
                      'current_and_closed', 'pnl-v1', ?, ?)
            """,
            (WALLET, now - 180, now - 180),
        )
        conn.execute(
            """
            INSERT INTO wallet_history_artifacts(
                artifact_id, wallet, history_depth, storage_version,
                relative_path, row_count, byte_size, checksum, status,
                created_at, updated_at
            ) VALUES ('artifact-deep', ?, 'deep', 'parquet-v1',
                      'wallet_history/deep/test.parquet', 850, 8192, 'checksum',
                      'active', ?, ?)
            """,
            (WALLET, now - 150, now - 150),
        )
        conn.execute(
            """
            INSERT INTO wallet_history_summaries(
                wallet, artifact_id, history_depth, activity_count,
                distinct_markets, non_fast_trade_count, fast_market_share,
                total_volume_usdc, buy_count, sell_count, median_gap_sec,
                trades_per_day, market_volume_top_share, oldest_timestamp,
                latest_timestamp, strategy_tags_json, risk_flags_json,
                research_score, score_components_json, methodology_version,
                computed_at, updated_at
            ) VALUES (?, 'artifact-deep', 'deep', 850, 31, 520, 0.22,
                      48000, 510, 340, 95, 28, 0.14, ?, ?, ?, ?, 87.4,
                      '{"pnl": 20, "roi": 18, "breadth": 16}',
                      'history-v1', ?, ?)
            """,
            (
                WALLET,
                now - 2_592_000,
                now - 300,
                json.dumps(["multi_market", "high_frequency"]),
                json.dumps(["profit_concentration_watch"]),
                now - 140,
                now - 140,
            ),
        )
        conn.execute(
            """
            INSERT INTO wallet_level_selections(
                wallet, target_level, evidence_artifact_id, policy_version,
                selected, rank_in_cohort, cohort_size, source_bucket,
                strategy_bucket, reason, decided_at, updated_at
            ) VALUES (?, 'l4', 'artifact-deep', 'levels-v2', 1, 2, 80,
                      'leaderboard', 'multi_market', 'relative_rank_selected', ?, ?)
            """,
            (WALLET, now - 120, now - 120),
        )
        for job_type, job_action, job_scope, status, completed_at in (
            ("wallet_recent_screen", "screen_recent:v1", "sample", "done", now - 200),
            ("wallet_history_collect", "collect_deep_history:v1", "deep", "done", now - 140),
            ("copyability_evidence", "legacy", "legacy", "queued", None),
        ):
            conn.execute(
                """
                INSERT INTO pipeline_jobs(
                    job_type, wallet, job_action, job_scope, priority, shard,
                    status, attempts, max_attempts, created_at, updated_at,
                    completed_at
                ) VALUES (?, ?, ?, ?, 10, 0, ?, 1, 3, ?, ?, ?)
                """,
                (job_type, WALLET, job_action, job_scope, status, now - 500, now - 100, completed_at),
            )

        # Legacy rows deliberately contain retired product semantics. The new
        # wallet-level surface must neither query nor expose them.
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status, first_seen_at, updated_at
            ) VALUES (?, 'legacy', '', '', '', 'active', ?, ?)
            """,
            (LEGACY_WALLET, now - 1_000, now),
        )
        conn.commit()
    finally:
        conn.close()


def test_dashboard_uses_level_truth_and_ignores_legacy_stage_data(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    _seed_wallet_research_data(settings)
    monkeypatch.setenv("PM_ROBOT_WEB_DASHBOARD_CACHE_TTL_SEC", "0")

    data = dashboard_data(settings)
    counts = {row["level"]: row["count"] for row in data["level_counts"]}
    high = data["high_level_wallets"]
    queues = {row["job_type"]: row for row in data["queues"]}
    serialized = json.dumps(data, ensure_ascii=False, sort_keys=True).lower()
    html = _render_dashboard(settings).lower()

    assert counts == {"l0": 0, "l1": 0, "l2": 0, "l3": 0, "l4": 1, "l5": 0, "l6": 0}
    assert data["wallet_count"] == 1
    assert len(high) == 1
    assert high[0] == {
        "wallet": WALLET,
            "level": "l4",
            "level_reason": "relative_rank_selected",
            "current_elite": False,
            "verified_l6": False,
        "sources": "rtds,polymarket_leaderboard",
        "total_estimated_pnl_usdc": 900.0,
        "cost_roi_estimate": 0.18,
        "current_position_value_usdc": 2100.0,
        "history_depth": "deep",
        "activity_count": 850,
        "distinct_markets": 31,
        "research_score": 87.4,
        "strategy_tags": ["multi_market", "high_frequency"],
        "risk_flags": ["profit_concentration_watch"],
        "rank_in_cohort": 2,
        "cohort_size": 80,
        "selection_policy_version": "levels-v2",
        "l6_validation_decision": "",
        "l6_validation_reason": "",
        "l6_validated_at": 0,
        "updated_at": 1_799_999_860,
    }
    assert set(queues) == {
        "wallet_recent_screen",
        "wallet_history_collect",
        "wallet_l6_validate",
    }
    assert queues["wallet_recent_screen"]["status_counts"]["done"] == 1
    assert queues["wallet_history_collect"]["status_counts"]["done"] == 1
    assert all(term not in serialized for term in FORBIDDEN_SURFACE_TERMS)
    assert all(term not in html for term in FORBIDDEN_SURFACE_TERMS)
    assert str(tmp_path).lower() not in serialized
    assert str(tmp_path).lower() not in html


def test_dashboard_hides_stale_l5_but_directory_marks_historical_and_current_elite(tmp_path):
    settings = _settings(tmp_path)
    conn = connect(settings.db_path)
    current = "0x" + "1" * 40
    historical = "0x" + "2" * 40
    now = int(time.time())
    try:
        run_migrations(conn)
        for wallet, selected in ((current, 1), (historical, 0)):
            artifact_id = f"artifact-{wallet[-4:]}"
            conn.execute(
                "INSERT INTO observed_wallets(wallet, sources, first_seen_at, updated_at) "
                "VALUES (?, 'test', ?, ?)",
                (wallet, now - 100, now),
            )
            conn.execute(
                """
                INSERT INTO wallet_levels(
                    wallet, level, level_reason, policy_version,
                    first_seen_at, last_seen_at, level_updated_at, updated_at
                ) VALUES (?, 'l5', 'relative_rank_selected', ?, ?, ?, ?, ?)
                """,
                (wallet, SELECTION_POLICY_VERSION, now - 100, now, now - 50, now),
            )
            conn.execute(
                """
                INSERT INTO wallet_history_summaries(
                    wallet, artifact_id, history_depth, activity_count,
                    distinct_markets, total_volume_usdc, strategy_tags_json,
                    risk_flags_json, research_score, score_components_json,
                    methodology_version, computed_at, updated_at
                ) VALUES (?, ?, 'deep', 200, 10, 5000, '[]', '[]', 80,
                          '{}', ?, ?, ?)
                """,
                (wallet, artifact_id, METHODOLOGY_VERSION, now - 40, now - 40),
            )
            conn.execute(
                """
                INSERT INTO wallet_level_selections(
                    wallet, target_level, evidence_artifact_id, policy_version,
                    selected, rank_in_cohort, cohort_size, source_bucket,
                    strategy_bucket, reason, decided_at, updated_at, research_score
                ) VALUES (?, 'l5', ?, ?, ?, 1, 20, 'stream', 'general',
                          'relative_rank_selected', ?, ?, 80)
                """,
                (wallet, artifact_id, SELECTION_POLICY_VERSION, selected, now - 30, now - 30),
            )
        conn.commit()
    finally:
        conn.close()

    dashboard = dashboard_data(settings)
    directory = wallet_table_rows(settings, level="l5", limit=10)

    assert dashboard["current_elite_wallet_count"] == 1
    assert [row["wallet"] for row in dashboard["high_level_wallets"]] == [current]
    assert {row["wallet"]: row["current_elite"] for row in directory} == {
        current: True,
        historical: False,
    }


def test_wallet_list_and_detail_expose_research_evidence_only(tmp_path):
    settings = _settings(tmp_path)
    _seed_wallet_research_data(settings)

    rows = wallet_table_rows(settings, level="l4", query="abc", limit=25)
    detail = wallet_detail_data(settings, WALLET)
    detail_html = _render_wallet_detail(settings, WALLET)
    serialized = json.dumps(detail, ensure_ascii=False, sort_keys=True).lower()

    assert [row["wallet"] for row in rows] == [WALLET]
    assert detail["schema_version"] == "wallet_research_detail_v2"
    assert detail["found"] is True
    assert detail["level"]["level"] == "l4"
    assert detail["screen"]["sample_trade_count"] == 10
    assert detail["pnl"]["total_estimated_pnl_usdc"] == 900
    assert detail["history"]["history_depth"] == "deep"
    assert detail["history"]["strategy_tags"] == ["multi_market", "high_frequency"]
    assert detail["history"]["risk_flags"] == ["profit_concentration_watch"]
    assert [job["job_type"] for job in detail["pipeline_jobs"]] == [
        "wallet_history_collect",
        "wallet_recent_screen",
    ]
    assert detail["selections"][0]["target_level"] == "l4"
    assert "L4" in detail_html
    assert "18.0%" in detail_html
    assert all(term not in serialized for term in FORBIDDEN_SURFACE_TERMS)
    assert all(term not in detail_html.lower() for term in FORBIDDEN_SURFACE_TERMS)
    assert str(tmp_path) not in serialized
    assert str(tmp_path) not in detail_html


def test_web_exposes_only_fresh_independently_verified_l6(tmp_path):
    settings = _settings(tmp_path)
    wallet = "0x" + "6" * 40
    artifact_id = "artifact-l6-current"
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        conn.execute(
            "INSERT INTO observed_wallets(wallet, sources, first_seen_at, updated_at) "
            "VALUES (?, 'leaderboard', ?, ?)",
            (wallet, now - 1_000, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_levels(
                wallet, level, level_reason, policy_version,
                first_seen_at, last_seen_at, level_updated_at, updated_at
            ) VALUES (?, 'l6', 'independent_validation_passed', ?, ?, ?, ?, ?)
            """,
            (wallet, L6_VALIDATION_POLICY_VERSION, now - 1_000, now, now - 10, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_history_summaries(
                wallet, artifact_id, history_depth, activity_count,
                distinct_markets, total_volume_usdc, strategy_tags_json,
                risk_flags_json, research_score, score_components_json,
                methodology_version, computed_at, updated_at
            ) VALUES (?, ?, 'deep', 300, 12, 9000, '[]', '[]', 88,
                      '{}', ?, ?, ?)
            """,
            (wallet, artifact_id, METHODOLOGY_VERSION, now - 30, now - 30),
        )
        conn.execute(
            """
            INSERT INTO wallet_level_selections(
                wallet, target_level, evidence_artifact_id, policy_version,
                selected, rank_in_cohort, cohort_size, source_bucket,
                strategy_bucket, reason, decided_at, updated_at, research_score
            ) VALUES (?, 'l5', ?, ?, 1, 1, 20, 'leaderboard', 'general',
                      'relative_rank_selected', ?, ?, 88)
            """,
            (wallet, artifact_id, SELECTION_POLICY_VERSION, now - 20, now - 20),
        )
        conn.execute(
            """
            INSERT INTO wallet_l6_validations(
                validation_id, wallet, evidence_artifact_id, policy_version,
                decision, reason, coverage_start, coverage_end,
                closed_position_count, activity_count, active_weeks,
                positive_week_ratio, realized_pnl_usdc,
                recent_realized_pnl_usdc, top_market_profit_share,
                official_all_pnl_usdc, official_all_volume_usdc,
                official_profit_intensity,
                validated_at, updated_at
            ) VALUES ('validation-l6-current', ?, ?, ?, 'pass',
                      'independent_validation_passed', ?, ?, 18, 240, 8,
                      0.75, 320, 80, 0.25, 200000, 8000000, 0.025, ?, ?)
            """,
            (wallet, artifact_id, L6_VALIDATION_POLICY_VERSION,
             now - 90 * 86_400, now, now - 10, now - 10),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    detail = wallet_detail_data(settings, wallet)
    html = _render_wallet_detail(settings, wallet)

    assert data["verified_l6_wallet_count"] == 1
    assert data["high_level_wallets"][0]["wallet"] == wallet
    assert data["high_level_wallets"][0]["verified_l6"] is True
    assert detail["level"]["verified_l6"] is True
    assert detail["l6_validations"][0]["decision"] == "pass"
    assert "L6 独立复核" in html
    assert "官方全历史 PnL" in html
    assert "利润强度（非 ROI）" in html
    assert "+$200,000.00" in html
    assert "+$320.00" in html
    assert "75.0%" in html
