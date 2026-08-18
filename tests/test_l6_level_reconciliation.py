from __future__ import annotations

import time

from pm_robot.orchestration.l6_level_reconciliation import (
    L6_RECONCILIATION_POLICY_VERSION,
    reconcile_historical_l6_levels,
)
from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.l6_validation import L6_VALIDATION_POLICY_VERSION
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.db import connect, run_migrations


def _seed_l6(conn, wallet: str, *, now: int, current_l5_selection: bool) -> None:
    conn.execute(
        """
        INSERT INTO wallet_levels(
            wallet, level, level_reason, policy_version, first_seen_at,
            last_seen_at, level_updated_at, updated_at
        ) VALUES (?, 'l6', 'historical_l6', 'legacy', ?, ?, ?, ?)
        """,
        (wallet, now - 1_000, now, now, now),
    )
    if not current_l5_selection:
        return
    artifact_id = f"artifact-{wallet[-4:]}"
    conn.execute(
        """
        INSERT INTO wallet_history_summaries(
            wallet, artifact_id, history_depth, activity_count, distinct_markets,
            non_fast_trade_count, fast_market_share, total_volume_usdc,
            buy_count, sell_count, market_volume_top_share,
            strategy_tags_json, risk_flags_json, research_score,
            diagnostic_score, forward_selection_score,
            score_components_json, forward_score_components_json,
            methodology_version, computed_at, updated_at
        ) VALUES (?, ?, 'deep', 500, 20, 500, 0, 50000,
                  300, 200, 0.2, '[]', '[]', 90, 90, 90, '{}', '{}',
                  ?, ?, ?)
        """,
        (wallet, artifact_id, METHODOLOGY_VERSION, now, now),
    )
    conn.execute(
        """
        INSERT INTO wallet_level_selections(
            wallet, target_level, evidence_artifact_id, policy_version,
            selected, rank_in_cohort, cohort_size, source_bucket,
            strategy_bucket, reason, decided_at, updated_at,
            research_score, forward_selection_score, score_status
        ) VALUES (?, 'l5', ?, ?, 1, 1, 20, 'stream', 'general',
                  'relative_rank_selected', ?, ?, 90, 90, 'valid')
        """,
        (wallet, artifact_id, SELECTION_POLICY_VERSION, now, now),
    )


def _seed_passing_l5_validation(
    conn,
    wallet: str,
    *,
    now: int,
    high_frequency: bool = False,
    official_month_pnl: float = 100,
) -> None:
    _seed_l6(conn, wallet, now=now, current_l5_selection=True)
    artifact_id = f"artifact-{wallet[-4:]}"
    conn.execute("UPDATE wallet_levels SET level = 'l5' WHERE wallet = ?", (wallet,))
    if high_frequency:
        conn.execute(
            "UPDATE wallet_history_summaries SET strategy_tags_json = '[\"high_frequency\"]' "
            "WHERE wallet = ?",
            (wallet,),
        )
    conn.execute(
        """
        INSERT INTO wallet_l6_validations(
            validation_id, wallet, evidence_artifact_id, policy_version,
            decision, reason, active_weeks, positive_week_ratio,
            max_drawdown_ratio, top_market_profit_share, top_day_profit_share,
            official_all_pnl_usdc, official_all_volume_usdc,
            official_profit_intensity, official_month_pnl_usdc,
            official_week_pnl_usdc, evidence_metrics_json, validated_at, updated_at
        ) VALUES (?, ?, ?, ?, 'pass', 'independent_validation_passed',
                  10, 0.8, 0.2, 0.2, 0.2, 1000, 10000, 0.1, ?, 10,
                  '{"recent_active_days": 10, "last_trade_age_seconds": 60, "max_same_signal_trades_10_seconds": 2}',
                  ?, ?)
        """,
        (
            f"validation-{wallet[-4:]}",
            wallet,
            artifact_id,
            L6_VALIDATION_POLICY_VERSION,
            official_month_pnl,
            now,
            now,
        ),
    )


def test_reconciler_lowers_stale_historical_l6_to_defensible_levels(tmp_path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    current_candidate = "0x" + "1" * 40
    stale_history = "0x" + "2" * 40
    try:
        run_migrations(conn)
        _seed_l6(conn, current_candidate, now=now, current_l5_selection=True)
        _seed_l6(conn, stale_history, now=now, current_l5_selection=False)
        conn.commit()

        dry_run = reconcile_historical_l6_levels(conn, now=now, dry_run=True)
        assert dry_run.reclassified_l5 == 1
        assert dry_run.reclassified_l2 == 1
        assert conn.execute("SELECT COUNT(*) FROM wallet_levels WHERE level = 'l6'").fetchone()[0] == 2

        summary = reconcile_historical_l6_levels(conn, now=now)
        conn.commit()

        assert summary.historical_l6 == 2
        assert summary.current_valid_l6 == 0
        assert summary.reclassified_l5 == 1
        assert summary.reclassified_l2 == 1
        levels = dict(conn.execute("SELECT wallet, level FROM wallet_levels"))
        assert levels[current_candidate] == "l5"
        assert levels[stale_history] == "l2"
        events = conn.execute(
            "SELECT wallet, from_level, to_level, policy_version FROM wallet_level_events "
            "ORDER BY wallet"
        ).fetchall()
        assert [(row[0], row[1], row[2], row[3]) for row in events] == [
            (current_candidate, "l6", "l5", L6_RECONCILIATION_POLICY_VERSION),
            (stale_history, "l6", "l2", L6_RECONCILIATION_POLICY_VERSION),
        ]
    finally:
        conn.close()


def test_reconciler_restores_quality_eligible_l5_without_copyability_gate(tmp_path):
    now = int(time.time())
    conn = connect(tmp_path / "robot.sqlite")
    easy_wallet = "0x" + "3" * 40
    difficult_wallet = "0x" + "4" * 40
    quality_failure = "0x" + "5" * 40
    try:
        run_migrations(conn)
        _seed_passing_l5_validation(conn, easy_wallet, now=now)
        _seed_passing_l5_validation(conn, difficult_wallet, now=now, high_frequency=True)
        _seed_passing_l5_validation(conn, quality_failure, now=now, official_month_pnl=-200)
        conn.commit()

        dry_run = reconcile_historical_l6_levels(conn, now=now, dry_run=True)
        assert dry_run.promoted_l6 == 2
        assert conn.execute("SELECT COUNT(*) FROM wallet_levels WHERE level = 'l6'").fetchone()[0] == 0

        summary = reconcile_historical_l6_levels(conn, now=now)
        conn.commit()

        assert summary.promoted_l6 == 2
        levels = dict(conn.execute("SELECT wallet, level FROM wallet_levels"))
        assert levels[easy_wallet] == "l6"
        assert levels[difficult_wallet] == "l6"
        assert levels[quality_failure] == "l5"
        events = conn.execute(
            "SELECT wallet, from_level, to_level, reason FROM wallet_level_events "
            "WHERE to_level = 'l6' ORDER BY wallet"
        ).fetchall()
        assert [tuple(row) for row in events] == [
            (easy_wallet, "l5", "l6", "l6_current_quality_restored"),
            (difficult_wallet, "l5", "l6", "l6_current_quality_restored"),
        ]
    finally:
        conn.close()
