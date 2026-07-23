from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.current_elite import (
    current_elite_wallet_count,
    current_elite_wallets,
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

        assert current_elite_wallets(conn, now=now) == {wallet}
        assert current_verified_l6_wallets(conn, now=now) == set()
    finally:
        conn.close()
