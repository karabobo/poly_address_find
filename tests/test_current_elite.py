from __future__ import annotations

import json
from pathlib import Path

from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.current_elite import (
    CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS,
    HIGH_CONFIDENCE_L6_POLICY_VERSION,
    HIGH_CONFIDENCE_L6_SCHEMA_VERSION,
    current_high_confidence_l6_candidates,
    current_score_candidate_wallets,
    current_elite_wallet_count,
    current_elite_wallets,
    current_high_confidence_l6_manifest_checksum,
    current_high_confidence_l6_snapshot,
    current_high_confidence_l6_wallet_count,
    current_high_confidence_l6_wallets,
    current_valid_l6_wallet_count,
    current_valid_l6_wallets,
    current_verified_l6_wallet_count,
    current_verified_l6_wallets,
)
from pm_robot.research.l6_validation import L6_VALIDATION_POLICY_VERSION
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.db import connect, run_migrations


def _seed_current_elite_candidate(
    conn,
    *,
    wallet: str,
    artifact_id: str,
    policy_version: str,
    now: int,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_levels(
            wallet, level, level_reason, policy_version,
            first_seen_at, last_seen_at, level_updated_at, updated_at
        ) VALUES (?, 'l5', 'relative_rank_selected', ?, ?, ?, ?, ?)
        """,
        (wallet, policy_version, now - 100, now, now - 50, now),
    )
    conn.execute(
        """
        INSERT INTO wallet_history_summaries(
            wallet, artifact_id, history_depth, activity_count,
            distinct_markets, total_volume_usdc, strategy_tags_json,
            risk_flags_json, research_score, diagnostic_score,
            forward_selection_score, score_components_json,
            forward_score_components_json, methodology_version, computed_at, updated_at
        ) VALUES (?, ?, 'deep', 200, 10, 5000, '[]', '[]', 80,
                  80, 70, '{}', '{}', ?, ?, ?)
        """,
        (wallet, artifact_id, METHODOLOGY_VERSION, now - 40, now - 40),
    )
    conn.execute(
        """
        INSERT INTO wallet_level_selections(
            wallet, target_level, evidence_artifact_id, policy_version,
            selected, rank_in_cohort, cohort_size, source_bucket,
            strategy_bucket, reason, decided_at, updated_at,
            research_score, forward_selection_score, score_status
        ) VALUES (?, 'l5', ?, ?, 1, 1, 20, 'stream', 'general',
                  'relative_rank_selected', ?, ?, 80, 70, 'valid')
        """,
        (wallet, artifact_id, policy_version, now - 30, now - 30),
    )


def _seed_l6_validation(
    conn,
    *,
    wallet: str,
    artifact_id: str,
    validation_id: str,
    now: int,
    official_week_pnl_usdc: float,
) -> None:
    conn.execute(
        "UPDATE wallet_levels SET level = 'l6', level_reason = ? WHERE wallet = ?",
        ("independent_validation_passed", wallet),
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
                  10, 0.8, 0.2, 0.2, 0.2,
                  1000, 10000, 0.1, 100, ?,
                  '{"recent_active_days": 10, "last_trade_age_seconds": 60, "max_same_signal_trades_10_seconds": 2}', ?, ?)
        """,
        (
            validation_id,
            wallet,
            artifact_id,
            L6_VALIDATION_POLICY_VERSION,
            official_week_pnl_usdc,
            now - 20,
            now - 20,
        ),
    )


def _seed_l6_validation_decision(
    conn,
    *,
    wallet: str,
    artifact_id: str,
    validation_id: str,
    now: int,
    decision: str,
    validated_at: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_l6_validations(
            validation_id, wallet, evidence_artifact_id, policy_version,
            decision, reason, active_weeks, positive_week_ratio,
            max_drawdown_ratio, top_market_profit_share, top_day_profit_share,
            official_all_pnl_usdc, official_all_volume_usdc,
            official_profit_intensity, official_month_pnl_usdc,
            official_week_pnl_usdc, evidence_metrics_json, validated_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?,
                  10, 0.8, 0.2, 0.2, 0.2,
                  1000, 10000, 0.1, 100, 10,
                  '{"recent_active_days": 10, "last_trade_age_seconds": 60, "max_same_signal_trades_10_seconds": 2}', ?, ?)
        """,
        (
            validation_id,
            wallet,
            artifact_id,
            L6_VALIDATION_POLICY_VERSION,
            decision,
            f"test_{decision}",
            now - 20 if validated_at is None else validated_at,
            now - 20 if validated_at is None else validated_at,
        ),
    )


def test_current_elite_accepts_non_default_policy_version(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    default_wallet = "0x" + "1" * 40
    runtime_wallet = "0x" + "2" * 40
    runtime_policy_version = "levels-runtime-v9"
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=default_wallet,
            artifact_id="artifact-default",
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        _seed_current_elite_candidate(
            conn,
            wallet=runtime_wallet,
            artifact_id="artifact-runtime",
            policy_version=runtime_policy_version,
            now=now,
        )
        conn.commit()

        assert current_elite_wallets(conn, now=now) == {default_wallet}
        assert current_elite_wallets(
            conn,
            now=now,
            policy_version=runtime_policy_version,
        ) == {runtime_wallet}
        assert (
            current_elite_wallets(
                conn,
                now=now,
                policy_version=runtime_policy_version,
                wallets=(default_wallet, runtime_wallet),
            )
            == {runtime_wallet}
        )
        assert current_elite_wallet_count(
            conn,
            now=now,
            policy_version=runtime_policy_version,
        ) == 1
    finally:
        conn.close()


def test_current_elite_is_legacy_alias_for_current_score_candidate(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id="artifact-score-candidate",
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        conn.commit()

        assert current_score_candidate_wallets(conn, now=now) == {wallet}
        assert current_elite_wallets(conn, now=now) == {wallet}
    finally:
        conn.close()


def test_verified_l6_requires_latest_pass_for_the_current_artifact(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    artifact_id = "artifact-l6"
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        conn.execute(
            "UPDATE wallet_levels SET level = 'l6', level_reason = ? WHERE wallet = ?",
            ("independent_validation_passed", wallet),
        )
        conn.execute(
            """
            INSERT INTO wallet_l6_validations(
                validation_id, wallet, evidence_artifact_id, policy_version,
                decision, reason, active_weeks, positive_week_ratio,
                max_drawdown_ratio, top_market_profit_share, top_day_profit_share,
                official_all_pnl_usdc, official_all_volume_usdc,
                official_profit_intensity, official_month_pnl_usdc, official_week_pnl_usdc,
                evidence_metrics_json, validated_at, updated_at
            ) VALUES ('validation-pass', ?, ?, ?, 'pass',
                      'independent_validation_passed', 10, 0.8,
                      0.2, 0.2, 0.2, 1000, 10000, 0.1, 100, 10,
                      '{"recent_active_days": 10, "last_trade_age_seconds": 60, "max_same_signal_trades_10_seconds": 2}', ?, ?)
            """,
            (wallet, artifact_id, L6_VALIDATION_POLICY_VERSION, now - 20, now - 20),
        )
        conn.commit()

        assert current_elite_wallets(conn, now=now) == {wallet}
        assert current_verified_l6_wallets(conn, now=now) == {wallet}
        assert current_valid_l6_wallets(conn, now=now) == {wallet}
        assert current_verified_l6_wallet_count(conn, now=now) == 1
        assert current_valid_l6_wallet_count(conn, now=now) == 1

        conn.execute(
            """
            INSERT INTO wallet_l6_validations(
                validation_id, wallet, evidence_artifact_id, policy_version,
                decision, reason, validated_at, updated_at
            ) VALUES ('validation-fail', ?, ?, ?, 'fail',
                      'negative_recent_realized_pnl', ?, ?)
            """,
            (wallet, artifact_id, L6_VALIDATION_POLICY_VERSION, now - 10, now - 10),
        )
        conn.commit()

        assert current_elite_wallets(conn, now=now) == set()
        assert current_verified_l6_wallets(conn, now=now) == set()
        assert current_valid_l6_wallets(conn, now=now) == set()
    finally:
        conn.close()


def test_current_valid_l6_rejects_warning_after_pass(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    artifact_id = "artifact-warning-after-pass"
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        conn.execute(
            "UPDATE wallet_levels SET level = 'l6', level_reason = ? WHERE wallet = ?",
            ("independent_validation_passed", wallet),
        )
        _seed_l6_validation_decision(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            validation_id="validation-pass",
            now=now,
            decision="pass",
            validated_at=now - 20,
        )
        _seed_l6_validation_decision(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            validation_id="validation-warning",
            now=now,
            decision="warning",
            validated_at=now - 10,
        )
        conn.commit()

        assert current_valid_l6_wallets(conn, now=now) == set()
        assert current_verified_l6_wallets(conn, now=now) == set()
    finally:
        conn.close()


def test_current_valid_l6_rejects_stale_validation(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "c" * 40
    artifact_id = "artifact-stale-validation"
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        conn.execute(
            "UPDATE wallet_levels SET level = 'l6', level_reason = ? WHERE wallet = ?",
            ("independent_validation_passed", wallet),
        )
        _seed_l6_validation_decision(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            validation_id="validation-stale-pass",
            now=now,
            decision="pass",
            validated_at=now - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS - 1,
        )
        conn.commit()

        assert current_valid_l6_wallets(conn, now=now) == set()
        assert current_verified_l6_wallets(conn, now=now) == set()
    finally:
        conn.close()


def test_current_valid_l6_latest_validation_overrides_historical_pass(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "d" * 40
    artifact_id = "artifact-latest-overrides"
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        conn.execute(
            "UPDATE wallet_levels SET level = 'l6', level_reason = ? WHERE wallet = ?",
            ("independent_validation_passed", wallet),
        )
        _seed_l6_validation_decision(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            validation_id="validation-historical-pass",
            now=now,
            decision="pass",
            validated_at=now - 20,
        )
        _seed_l6_validation_decision(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            validation_id="validation-latest-fail",
            now=now,
            decision="fail",
            validated_at=now - 10,
        )
        conn.commit()

        assert current_valid_l6_wallets(conn, now=now) == set()
        assert current_verified_l6_wallets(conn, now=now) == set()
    finally:
        conn.close()


def test_current_elite_ignores_old_artifact_fail_but_keeps_current_warning(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    artifact_id = "artifact-current"
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        conn.execute(
            """
            INSERT INTO wallet_l6_validations(
                validation_id, wallet, evidence_artifact_id, policy_version,
                decision, reason, validated_at, updated_at
            ) VALUES ('old-artifact-fail', ?, 'artifact-old', ?, 'fail',
                      'non_positive_official_all_time_pnl', ?, ?)
            """,
            (wallet, L6_VALIDATION_POLICY_VERSION, now - 20, now - 20),
        )
        conn.commit()
        assert current_elite_wallets(conn, now=now) == {wallet}

        conn.execute(
            """
            INSERT INTO wallet_l6_validations(
                validation_id, wallet, evidence_artifact_id, policy_version,
                decision, reason, validated_at, updated_at
            ) VALUES ('current-warning', ?, ?, ?, 'warning',
                      'limited_weekly_history', ?, ?)
            """,
            (wallet, artifact_id, L6_VALIDATION_POLICY_VERSION, now - 10, now - 10),
        )
        conn.commit()
        assert current_elite_wallets(conn, now=now) == {wallet}
    finally:
        conn.close()


def test_current_elite_ignores_same_artifact_failure_from_an_old_policy(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "8" * 40
    artifact_id = "artifact-current-policy"
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        conn.execute(
            """
            INSERT INTO wallet_l6_validations(
                validation_id, wallet, evidence_artifact_id, policy_version,
                decision, reason, validated_at, updated_at
            ) VALUES ('old-policy-fail', ?, ?, 'l6_independent_v2', 'fail',
                      'legacy_failure', ?, ?)
            """,
            (wallet, artifact_id, now - 20, now - 20),
        )
        conn.commit()
        assert current_elite_wallets(conn, now=now) == {wallet}

        conn.execute(
            """
            INSERT INTO wallet_l6_validations(
                validation_id, wallet, evidence_artifact_id, policy_version,
                decision, reason, validated_at, updated_at
            ) VALUES ('current-policy-fail', ?, ?, ?, 'fail',
                      'current_failure', ?, ?)
            """,
            (
                wallet,
                artifact_id,
                L6_VALIDATION_POLICY_VERSION,
                now - 10,
                now - 10,
            ),
        )
        conn.commit()
        assert current_elite_wallets(conn, now=now) == set()
    finally:
        conn.close()


def test_current_valid_l6_and_research_handoff_share_one_quality_contract(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    accepted_wallet = "0x" + "a" * 40
    negative_week_wallet = "0x" + "b" * 40
    now = 2_000_000
    try:
        run_migrations(conn)
        for wallet, artifact_id in (
            (accepted_wallet, "artifact-accepted"),
            (negative_week_wallet, "artifact-negative-week"),
        ):
            _seed_current_elite_candidate(
                conn,
                wallet=wallet,
                artifact_id=artifact_id,
                policy_version=SELECTION_POLICY_VERSION,
                now=now,
            )
        _seed_l6_validation(
            conn,
            wallet=accepted_wallet,
            artifact_id="artifact-accepted",
            validation_id="validation-accepted",
            now=now,
            official_week_pnl_usdc=10,
        )
        _seed_l6_validation(
            conn,
            wallet=negative_week_wallet,
            artifact_id="artifact-negative-week",
            validation_id="validation-negative-week",
            now=now,
            official_week_pnl_usdc=-100,
        )
        conn.commit()

        assert current_verified_l6_wallets(conn, now=now) == {accepted_wallet}
        assert current_high_confidence_l6_wallets(conn, now=now) == {accepted_wallet}
        assert current_high_confidence_l6_wallet_count(conn, now=now) == 1

        snapshot = current_high_confidence_l6_snapshot(conn, now=now)
        assert snapshot["selection_policy_version"] == SELECTION_POLICY_VERSION
        assert snapshot["validation_policy_version"] == L6_VALIDATION_POLICY_VERSION
        assert snapshot["methodology_version"] == METHODOLOGY_VERSION
        assert snapshot["high_confidence_policy"]["version"] == HIGH_CONFIDENCE_L6_POLICY_VERSION
        assert snapshot["schema_version"] == 3
        assert snapshot["automatic_trading_activation"] is False
        assert snapshot["research_only"] is True
        assert snapshot["not_for_trading"] is True
        source_version = snapshot["source_version"]
        assert snapshot["handoff_status"] == "degraded"
        assert snapshot["replace_active_set_allowed"] is False
        assert snapshot["handoff_readiness"] == {
            "runtime_ready": False,
            "research_ready": False,
            "planner_ready": False,
            "source": "not_evaluated",
        }
        assert source_version.startswith(f"hcl6:v{HIGH_CONFIDENCE_L6_SCHEMA_VERSION}:{now}:")
        assert len(source_version) <= 64
        assert snapshot["manifest_checksum"] == current_high_confidence_l6_manifest_checksum(snapshot)
        checksum_ignores_self = dict(snapshot, manifest_checksum="not-the-checksum")
        assert current_high_confidence_l6_manifest_checksum(checksum_ignores_self) == snapshot["manifest_checksum"]
        changed_manifest = dict(snapshot, candidate_count=2)
        assert current_high_confidence_l6_manifest_checksum(changed_manifest) != snapshot["manifest_checksum"]
        repeat_snapshot = current_high_confidence_l6_snapshot(conn, now=now)
        assert repeat_snapshot["source_version"] == source_version
        assert repeat_snapshot["manifest_checksum"] == snapshot["manifest_checksum"]
        assert "polyhermes_research_import" not in snapshot
        assert snapshot["candidate_count"] == 1
        assert snapshot["candidates"][0]["wallet"] == accepted_wallet
        assert snapshot["candidates"][0]["strategy_tags"] == []
        assert snapshot["candidates"][0]["risk_flags"] == []
        assert snapshot["candidates"][0]["evidence_artifact_id"] == "artifact-accepted"
        assert snapshot["candidates"][0]["quality_signals_passed"] == 8
        assert snapshot["candidates"][0]["quality_signals_required"] == 6
    finally:
        conn.close()


def test_current_high_confidence_l6_candidates_use_one_current_valid_detail_read(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    now = 2_000_000
    traced_statements: list[str] = []
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id="artifact-single-read",
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        _seed_l6_validation(
            conn,
            wallet=wallet,
            artifact_id="artifact-single-read",
            validation_id="validation-single-read",
            now=now,
            official_week_pnl_usdc=10,
        )
        conn.commit()

        conn.set_trace_callback(traced_statements.append)
        candidates = current_high_confidence_l6_candidates(conn, now=now)
        conn.set_trace_callback(None)

        assert [candidate["wallet"] for candidate in candidates] == [wallet]
        candidate_reads = [
            statement
            for statement in traced_statements
            if "FROM wallet_levels AS levels" in statement
            and "JOIN wallet_l6_validations AS validation" in statement
        ]
        assert len(candidate_reads) == 1
        assert "levels.level = 'l6'" in candidate_reads[0]
        assert "levels.hard_risk_block = 0" in candidate_reads[0]
        assert "summary.methodology_version" in candidate_reads[0]
        assert "summary.updated_at >=" in candidate_reads[0]
        assert "validation.decision = 'pass'" in candidate_reads[0]
        assert "validation.validated_at >=" in candidate_reads[0]
    finally:
        conn.set_trace_callback(None)
        conn.close()


def test_current_l6_quality_uses_hard_failures_and_soft_signal_quorum(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    mild_week_loss = "0x" + "6" * 40
    weak_quorum = "0x" + "7" * 40
    severe_month_loss = "0x" + "8" * 40
    now = 2_000_000
    try:
        run_migrations(conn)
        for wallet, artifact_id in (
            (mild_week_loss, "artifact-mild-week"),
            (weak_quorum, "artifact-weak-quorum"),
            (severe_month_loss, "artifact-severe-month"),
        ):
            _seed_current_elite_candidate(
                conn,
                wallet=wallet,
                artifact_id=artifact_id,
                policy_version=SELECTION_POLICY_VERSION,
                now=now,
            )
            _seed_l6_validation(
                conn,
                wallet=wallet,
                artifact_id=artifact_id,
                validation_id=f"validation-{wallet[-4:]}",
                now=now,
                official_week_pnl_usdc=-1 if wallet == mild_week_loss else 10,
            )
        conn.execute(
            """
            UPDATE wallet_l6_validations
            SET active_weeks = 6, positive_week_ratio = 0.6,
                official_profit_intensity = 0.003,
                official_month_pnl_usdc = 5, official_week_pnl_usdc = 0,
                max_drawdown_ratio = 0.3, top_market_profit_share = 0.4,
                top_day_profit_share = 0.4
            WHERE wallet = ?
            """,
            (weak_quorum,),
        )
        conn.execute(
            "UPDATE wallet_l6_validations SET official_month_pnl_usdc = -200 WHERE wallet = ?",
            (severe_month_loss,),
        )
        conn.commit()

        assert current_valid_l6_wallets(conn, now=now) == {mild_week_loss}
        candidate = current_high_confidence_l6_candidates(conn, now=now)[0]
        assert candidate["wallet"] == mild_week_loss
        assert candidate["quality_signals_passed"] == 7
        assert "positive_official_week_pnl" not in candidate["quality_signals"]
    finally:
        conn.close()


def test_current_high_confidence_l6_retains_execution_pattern_risk_as_profile(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id="artifact-execution-risk",
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        _seed_l6_validation(
            conn,
            wallet=wallet,
            artifact_id="artifact-execution-risk",
            validation_id="validation-execution-risk",
            now=now,
            official_week_pnl_usdc=10,
        )
        conn.execute(
            """
            UPDATE wallet_history_summaries
            SET fast_market_share = 0.5, trades_per_day = 25,
                market_volume_top_share = 0.2
            WHERE wallet = ?
            """,
            (wallet,),
        )
        conn.execute(
            """
            UPDATE wallet_l6_validations
            SET abnormal_flags_json = '["extreme_burst_frequency"]'
            WHERE validation_id = 'validation-execution-risk'
            """
        )
        conn.commit()

        assert current_valid_l6_wallets(conn, now=now) == {wallet}
        assert current_high_confidence_l6_wallets(conn, now=now) == {wallet}
        candidate = current_high_confidence_l6_candidates(conn, now=now)[0]
        assert candidate["activity_state"] == "active"
        assert candidate["execution_profile"] == "difficult"
        assert "execution_pattern_risk" in candidate["execution_flags"]
    finally:
        conn.close()


def test_current_high_confidence_l6_retains_non_copyable_strategy_as_profile(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "f" * 40
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id="artifact-high-frequency",
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        _seed_l6_validation(
            conn,
            wallet=wallet,
            artifact_id="artifact-high-frequency",
            validation_id="validation-high-frequency",
            now=now,
            official_week_pnl_usdc=10,
        )
        conn.execute(
            "UPDATE wallet_history_summaries SET strategy_tags_json = ? WHERE wallet = ?",
            ('["high_frequency"]', wallet),
        )
        conn.commit()

        assert current_elite_wallets(conn, now=now) == {wallet}
        assert current_valid_l6_wallets(conn, now=now) == {wallet}
        assert current_high_confidence_l6_wallets(conn, now=now) == {wallet}
        candidate = current_high_confidence_l6_candidates(conn, now=now)[0]
        assert candidate["execution_profile"] == "difficult"
        assert "non_copyable_strategy_shape" in candidate["execution_flags"]
    finally:
        conn.close()


def test_current_valid_l6_reports_execution_shape_without_rejecting_quality(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "c" * 40
    artifact_id = "artifact-execution-shape"
    now = 2_000_000
    execution_cases = (
        ("{}", "unknown", "not_assessed"),
        (
            '{"recent_active_days": 2, "last_trade_age_seconds": 60, "max_same_signal_trades_10_seconds": 2}',
            "inactive",
            "easy",
        ),
        (
            '{"recent_active_days": 10, "last_trade_age_seconds": 604801, "max_same_signal_trades_10_seconds": 2}',
            "inactive",
            "easy",
        ),
        (
            '{"recent_active_days": 10, "last_trade_age_seconds": 60, "max_same_signal_trades_10_seconds": 13}',
            "active",
            "difficult",
        ),
    )
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        _seed_l6_validation(
            conn,
            wallet=wallet,
            artifact_id=artifact_id,
            validation_id="validation-execution-shape",
            now=now,
            official_week_pnl_usdc=10,
        )
        conn.commit()

        assert current_valid_l6_wallets(conn, now=now) == {wallet}
        for evidence_metrics_json, activity_state, execution_profile in execution_cases:
            conn.execute(
                """
                UPDATE wallet_l6_validations
                SET evidence_metrics_json = ?
                WHERE validation_id = 'validation-execution-shape'
                """,
                (evidence_metrics_json,),
            )
            conn.commit()
            assert current_valid_l6_wallets(conn, now=now) == {wallet}
            assert current_high_confidence_l6_wallets(conn, now=now) == {wallet}
            candidate = current_high_confidence_l6_candidates(conn, now=now)[0]
            assert candidate["activity_state"] == activity_state
            assert candidate["execution_profile"] == execution_profile
    finally:
        conn.close()


def test_current_high_confidence_l6_snapshot_reports_handoff_statuses(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    now = 2_000_000
    wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet=wallet,
            artifact_id="artifact-accepted",
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        _seed_l6_validation(
            conn,
            wallet=wallet,
            artifact_id="artifact-accepted",
            validation_id="validation-accepted",
            now=now,
            official_week_pnl_usdc=10,
        )
        conn.execute("UPDATE wallet_levels SET level = 'l6' WHERE wallet = ?", (wallet,))
        conn.commit()

        readiness_ready = {
            "runtime_readiness": {"ready": True},
            "research_readiness": {"ready": True},
            "planner_ready": True,
        }
        readiness_warming = {
            "runtime_readiness": {"ready": True},
            "research_readiness": {"ready": True},
            "planner_ready": False,
        }
        readiness_degraded = {
            "runtime_readiness": {"ready": False},
            "research_readiness": {"ready": True},
            "planner_ready": False,
        }

        ready_snapshot = current_high_confidence_l6_snapshot(
            conn,
            now=now,
            readiness=readiness_ready,
        )
        assert ready_snapshot["handoff_status"] == "ready"
        assert ready_snapshot["replace_active_set_allowed"] is True

        warming_snapshot = current_high_confidence_l6_snapshot(
            conn,
            now=now,
            readiness=readiness_warming,
        )
        assert warming_snapshot["handoff_status"] == "warming"
        assert warming_snapshot["replace_active_set_allowed"] is False

        degraded_snapshot = current_high_confidence_l6_snapshot(
            conn,
            now=now,
            readiness=readiness_degraded,
        )
        assert degraded_snapshot["handoff_status"] == "degraded"
        assert degraded_snapshot["replace_active_set_allowed"] is False
    finally:
        conn.close()


def test_current_high_confidence_l6_empty_candidate_set_is_fail_closed_default(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    now = 2_000_000
    try:
        run_migrations(conn)
        snapshot = current_high_confidence_l6_snapshot(conn, now=now)
        assert snapshot["candidate_count"] == 0
        assert snapshot["handoff_status"] == "degraded"
        assert snapshot["replace_active_set_allowed"] is False
        assert snapshot["candidates"] == []
    finally:
        conn.close()


def test_current_high_confidence_l6_empty_candidate_set_revokes_ready_handoff(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    now = 2_000_000
    try:
        run_migrations(conn)

        snapshot = current_high_confidence_l6_snapshot(
            conn,
            now=now,
            readiness={
                "runtime_readiness": {"ready": True},
                "research_readiness": {"ready": True},
                "planner_ready": True,
            },
        )

        assert snapshot["candidate_count"] == 0
        assert snapshot["handoff_status"] == "degraded"
        assert snapshot["replace_active_set_allowed"] is False
        assert snapshot["handoff_readiness"]["source"] == "candidate_set_empty"
    finally:
        conn.close()


def test_current_high_confidence_l6_checksum_reacts_to_readiness_flags(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    now = 2_000_000
    try:
        run_migrations(conn)
        _seed_current_elite_candidate(
            conn,
            wallet="0x" + "1" * 40,
            artifact_id="artifact-accepted",
            policy_version=SELECTION_POLICY_VERSION,
            now=now,
        )
        _seed_l6_validation(
            conn,
            wallet="0x" + "1" + "1" * 39,
            artifact_id="artifact-accepted",
            validation_id="validation-accepted",
            now=now,
            official_week_pnl_usdc=10,
        )
        conn.execute("UPDATE wallet_levels SET level = 'l6' WHERE wallet = ?", ("0x" + "1" * 40,))
        conn.commit()

        snapshot_ready = current_high_confidence_l6_snapshot(
            conn,
            now=now,
            readiness={
                "runtime_readiness": {"ready": True},
                "research_readiness": {"ready": True},
                "planner_ready": True,
            },
        )
        snapshot_warming = current_high_confidence_l6_snapshot(
            conn,
            now=now,
            readiness={
                "runtime_readiness": {"ready": True},
                "research_readiness": {"ready": True},
                "planner_ready": False,
            },
        )
        assert current_high_confidence_l6_manifest_checksum(snapshot_ready) != current_high_confidence_l6_manifest_checksum(
            snapshot_warming
        )
    finally:
        conn.close()


def test_current_high_confidence_l6_fixture_has_a_cross_language_checksum() -> None:
    fixture = Path(__file__).parent / "fixtures" / "current_high_confidence_l6_snapshot.json"
    snapshot = json.loads(fixture.read_text())

    assert snapshot["schema_version"] == HIGH_CONFIDENCE_L6_SCHEMA_VERSION
    assert snapshot["manifest_checksum"] == "2dd4ccc54a2ae61ab27a664f4eb8d76eccf781e0c7edad02d7a3d0bc22ddac06"
    assert snapshot["manifest_checksum"] == current_high_confidence_l6_manifest_checksum(snapshot)
    tampered = json.loads(json.dumps(snapshot))
    tampered["candidates"][0]["max_drawdown_ratio"] = 0.000462
    assert current_high_confidence_l6_manifest_checksum(tampered) != snapshot["manifest_checksum"]
    assert len(snapshot["source_version"]) <= 64
    assert "polyhermes_research_import" not in snapshot
