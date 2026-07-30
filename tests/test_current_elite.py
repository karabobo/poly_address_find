from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.current_elite import (
    HIGH_CONFIDENCE_L6_POLICY_VERSION,
    current_elite_wallet_count,
    current_elite_wallets,
    current_high_confidence_l6_snapshot,
    current_high_confidence_l6_wallet_count,
    current_high_confidence_l6_wallets,
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
        ) VALUES (?, 'l5', ?, ?, 1, 1, 20, 'stream', 'general',
                  'relative_rank_selected', ?, ?, 80)
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
            official_week_pnl_usdc, validated_at, updated_at
        ) VALUES (?, ?, ?, ?, 'pass', 'independent_validation_passed',
                  10, 0.8, 0.2, 0.2, 0.2,
                  1000, 10000, 0.1, 100, ?, ?, ?)
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
                decision, reason, validated_at, updated_at
            ) VALUES ('validation-pass', ?, ?, ?, 'pass',
                      'independent_validation_passed', ?, ?)
            """,
            (wallet, artifact_id, L6_VALIDATION_POLICY_VERSION, now - 20, now - 20),
        )
        conn.commit()

        assert current_elite_wallets(conn, now=now) == {wallet}
        assert current_verified_l6_wallets(conn, now=now) == {wallet}
        assert current_verified_l6_wallet_count(conn, now=now) == 1

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


def test_high_confidence_l6_is_a_stricter_current_research_handoff(tmp_path):
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
            official_week_pnl_usdc=-1,
        )
        conn.commit()

        assert current_verified_l6_wallets(conn, now=now) == {
            accepted_wallet,
            negative_week_wallet,
        }
        assert current_high_confidence_l6_wallets(conn, now=now) == {accepted_wallet}
        assert current_high_confidence_l6_wallet_count(conn, now=now) == 1

        snapshot = current_high_confidence_l6_snapshot(conn, now=now)
        assert snapshot["high_confidence_policy"]["version"] == HIGH_CONFIDENCE_L6_POLICY_VERSION
        assert snapshot["automatic_trading_activation"] is False
        assert snapshot["candidate_count"] == 1
        assert snapshot["candidates"][0]["wallet"] == accepted_wallet
        assert snapshot["candidates"][0]["strategy_tags"] == []
        assert snapshot["candidates"][0]["risk_flags"] == []
    finally:
        conn.close()
