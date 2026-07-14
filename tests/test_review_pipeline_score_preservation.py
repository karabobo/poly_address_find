import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Optional

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
import pm_robot.orchestration.review_pipeline as review_pipeline
from pm_robot.orchestration.review_pipeline import score_database, sync_candidate_stages_from_latest_scores
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    apply_copyability_no_signal_blocks,
    consume_fresh_score_actions,
    persist_score,
    sync_wallet_processing_state,
    upsert_candidate,
    upsert_wallet_feature as _upsert_wallet_feature,
)


POLICY_PATH = Path("config/leader_scoring_policy.json")


def _policy_version() -> str:
    return str(load_policy(POLICY_PATH).get("version", ""))


def _strong_features(address: str) -> WalletFeatures:
    return WalletFeatures(
        address=address,
        cumulative_win_rate=0.72,
        recent_30d_volume_usdc=750_000,
        net_pnl_usdc=250_000,
        total_volume_usdc=5_000_000,
        event_win_rate=0.88,
        trade_win_rate=0.58,
        avg_dca_entries=25,
        sell_pct=2,
        bot_score=45,
        maker_fraction=0.1,
        leader_in_degree=8,
        copy_event_count=40,
        copy_market_count=12,
        containment_pct_median=0.95,
        copy_stream_roi=0.025,
        edge_retention_pct=70,
        walk_forward_consistency_pct=60,
        survival_score=70,
        single_market_pnl_share=0.2,
        net_to_gross_exposure=0.7,
        hygiene_status="clean",
        primary_category="politics",
        extra={"paper_roi_after_slippage": 0.08},
    )


def upsert_wallet_feature(conn: sqlite3.Connection, feature: WalletFeatures) -> None:
    """Seed the qualified pair implied by positive copy scoring fields."""

    _upsert_wallet_feature(conn, feature)
    if not (
        float(feature.leader_in_degree or 0) > 0
        and float(feature.copy_event_count or 0) > 0
        and float(feature.copy_market_count or 0) > 0
    ):
        return
    conn.execute(
        """
        INSERT INTO copy_pair_stats(
            leader_wallet, follower_wallet, copy_event_count, copy_market_count,
            follower_trade_count, containment_pct, leader_precedes_pct,
            median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
        ) VALUES (?, ?, ?, ?, ?, 0.4, 1.0, 2, 100, 200, 1, 300)
        ON CONFLICT(leader_wallet, follower_wallet) DO UPDATE SET
            copy_event_count = excluded.copy_event_count,
            copy_market_count = excluded.copy_market_count,
            follower_trade_count = excluded.follower_trade_count,
            qualifies = 1,
            updated_at = excluded.updated_at
        """,
        (
            feature.address,
            feature.address,
            int(feature.copy_event_count or 0),
            int(feature.copy_market_count or 0),
            int(feature.copy_event_count or 0),
        ),
    )


def _latest_score(conn, address: str):
    return conn.execute(
        """
        SELECT *
        FROM leader_scores
        WHERE address = ?
        ORDER BY scored_at DESC, score_id DESC
        LIMIT 1
        """,
        (address,),
    ).fetchone()


def _insert_l3_state(conn, address: str, *, updated_at: int = 50) -> None:
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, updated_at
        ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done', 'score_wallet', 0, 100, ?)
        """,
        (address, updated_at),
    )


def _insert_l2_state(conn, address: str, *, updated_at: int = 50) -> None:
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, updated_at
        ) VALUES (?, 'l2_medium', 'summary_ready', 500, 0.8, 10, 'medium_done', 'score_wallet', 0, 500, ?)
        """,
        (address, updated_at),
    )


def _insert_pending_medium_state(conn, address: str, *, updated_at: int = 50) -> None:
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, updated_at
        ) VALUES (?, 'l1_light', 'queued', 200, 0.5, 10, 'light_done', 'medium_pending', 0, 200, ?)
        """,
        (address, updated_at),
    )


def _insert_bounded_deep_state(conn, address: str, *, updated_at: int = 50) -> None:
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, distinct_markets,
            non_fast_trade_count, last_deep_backfill_at, updated_at
        ) VALUES (?, 'l2_medium', 'summary_ready', 525, 0.9, 10, 'deep_done',
                  'score_wallet', 0, 525, 81, 524, ?, ?)
        """,
        (address, updated_at, updated_at),
    )


def _borderline_score(candidate: CandidateAddress) -> ScoreBreakdown:
    return ScoreBreakdown(
        address=candidate.address,
        leader_score=39.0,
        stage=CandidateStage.NEEDS_REVIEW,
        reason="borderline_score",
        components={"test": 39.0},
        penalties={},
    )


def test_fresh_score_action_consumption_does_not_touch_evidence_freshness(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "1" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _insert_l3_state(conn, address, updated_at=50)
        conn.commit()

        persist_score(
            conn,
            ScoreBreakdown(
                address=address,
                leader_score=55.0,
                stage=CandidateStage.NEEDS_REVIEW,
                reason="watchlist_score",
                components={"test": 55.0},
                penalties={},
            ),
            policy_version=_policy_version(),
        )
        state = conn.execute(
            "SELECT next_action, next_action_at, updated_at FROM wallet_processing_state WHERE wallet = ?",
            (address,),
        ).fetchone()

        assert state["next_action"] == "score_wallet"
        assert state["updated_at"] == 50

        consumed = consume_fresh_score_actions(conn, policy_version=_policy_version())
        state = conn.execute(
            "SELECT next_action, next_action_at, updated_at FROM wallet_processing_state WHERE wallet = ?",
            (address,),
        ).fetchone()

        assert consumed == 1
        assert state["next_action"] == ""
        assert state["next_action_at"] == 0
        assert state["updated_at"] == 50

        sync_wallet_processing_state(
            conn,
            address,
            {
                "activity_count": 1_200,
                "distinct_markets": 40,
                "non_fast_trade_count": 1_000,
                "fast_market_share": 0.1,
            },
            now=200,
        )
        refreshed = conn.execute(
            "SELECT next_action, updated_at FROM wallet_processing_state WHERE wallet = ?",
            (address,),
        ).fetchone()
        assert refreshed["next_action"] == "score_wallet"
        assert refreshed["updated_at"] == 200
    finally:
        conn.close()


def test_persist_score_does_not_consume_newer_evidence_action(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "8" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _insert_l3_state(conn, address, updated_at=9_999_999_999)

        persist_score(
            conn,
            ScoreBreakdown(
                address=address,
                leader_score=55.0,
                stage=CandidateStage.NEEDS_REVIEW,
                reason="watchlist_score",
                components={"test": 55.0},
                penalties={},
            ),
            policy_version=_policy_version(),
        )
        consumed = consume_fresh_score_actions(conn, policy_version=_policy_version())
        state = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (address,),
        ).fetchone()

        assert consumed == 0
        assert state["next_action"] == "score_wallet"
    finally:
        conn.close()


def test_consume_fresh_score_actions_only_clears_current_fresh_scores(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    fresh = "0x" + "2" * 40
    old_policy = "0x" + "3" * 40
    evidence_newer = "0x" + "4" * 40
    try:
        run_migrations(conn)
        for address in (fresh, old_policy, evidence_newer):
            upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
            conn.execute(
                "UPDATE candidate_wallets SET updated_at = 10 WHERE address = ?",
                (address,),
            )
            _insert_l3_state(conn, address, updated_at=50)
        conn.execute(
            "UPDATE wallet_processing_state SET updated_at = 200 WHERE wallet = ?",
            (evidence_newer,),
        )
        conn.executemany(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 55, 'needs_manual_review', 'watchlist_score', '{}', '{}', ?, 100)
            """,
            (
                (fresh, _policy_version()),
                (old_policy, "old-policy"),
                (evidence_newer, _policy_version()),
            ),
        )

        consumed = consume_fresh_score_actions(conn, policy_version=_policy_version())
        actions = {
            row["wallet"]: row["next_action"]
            for row in conn.execute(
                "SELECT wallet, next_action FROM wallet_processing_state ORDER BY wallet"
            ).fetchall()
        }

        assert consumed == 1
        assert actions[fresh] == ""
        assert actions[old_policy] == "score_wallet"
        assert actions[evidence_newer] == "score_wallet"
    finally:
        conn.close()


def test_score_database_rejects_terminal_thin_history_before_copyability(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "5" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        _insert_l3_state(conn, address)
        conn.execute(
            "UPDATE wallet_processing_state SET activity_count = 10 WHERE wallet = ?",
            (address,),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        state = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (address,),
        ).fetchone()

        assert counts["scores_written"] == 1
        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"] == "insufficient_directional_trades:10<100"
        assert state["next_action"] == ""
    finally:
        conn.close()


def test_score_database_keeps_thin_pending_history_in_refinement(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "6" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        _insert_pending_medium_state(conn, address)
        conn.execute(
            "UPDATE wallet_processing_state SET activity_count = 10 WHERE wallet = ?",
            (address,),
        )
        conn.commit()

        score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)

        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"] == "evidence_refinement_pending:medium_pending"
    finally:
        conn.close()


def test_directional_history_guard_preserves_explicit_low_value_reason(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "9" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(
            conn,
            replace(
                _strong_features(address),
                total_volume_usdc=100,
                recent_30d_volume_usdc=100,
                net_pnl_usdc=20,
            ),
        )
        _insert_l3_state(conn, address)
        conn.execute(
            "UPDATE wallet_processing_state SET activity_count = 10 WHERE wallet = ?",
            (address,),
        )
        conn.commit()

        score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)

        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"] == "insufficient_total_volume_usdc:100.00<1000.00"
    finally:
        conn.close()


def test_directional_history_guard_does_not_override_hard_hygiene_block(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "7" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        features = replace(_strong_features(address), hygiene_status="wash")
        upsert_wallet_feature(conn, features)
        _insert_l3_state(conn, address)
        conn.execute(
            "UPDATE wallet_processing_state SET activity_count = 10 WHERE wallet = ?",
            (address,),
        )
        conn.commit()

        score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)

        assert latest["review_stage"] == CandidateStage.BLOCKED_HYGIENE.value
        assert latest["review_reason"] == "hygiene_status=wash"
    finally:
        conn.close()


def test_score_database_keeps_pending_borderline_wallet_out_of_manual_review(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "5" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _insert_pending_medium_state(conn, address)
        conn.commit()
        monkeypatch.setattr(
            review_pipeline,
            "score_candidate",
            lambda candidate, _features, _policy: _borderline_score(candidate),
        )

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts["needs_data"] == 1
        assert latest["leader_score"] == 39.0
        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"] == "evidence_refinement_pending:medium_pending"
        assert stage == CandidateStage.NEEDS_DATA.value
    finally:
        conn.close()


def test_score_database_parks_terminal_borderline_wallet_outside_manual_review(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "6" * 40
    original_score_candidate = review_pipeline.score_candidate
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _insert_l2_state(conn, address)
        conn.commit()
        monkeypatch.setattr(
            review_pipeline,
            "score_candidate",
            lambda candidate, _features, _policy: _borderline_score(candidate),
        )

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts["needs_data"] == 1
        assert latest["leader_score"] == 39.0
        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"] == "score_below_watchlist_after_evidence"
        assert stage == CandidateStage.NEEDS_DATA.value

        monkeypatch.setattr(review_pipeline, "score_candidate", original_score_candidate)
        upsert_wallet_feature(conn, _strong_features(address))
        conn.execute(
            """
            UPDATE wallet_processing_state
            SET discovery_tier = 'l3_deep', current_stage = 'deep_done',
                updated_at = ?
            WHERE wallet = ?
            """,
            (int(latest["scored_at"]) + 10, address),
        )
        conn.execute(
            "UPDATE wallet_features SET updated_at = ? WHERE address = ?",
            (int(latest["scored_at"]) + 10, address),
        )
        conn.commit()

        promoted_counts = score_database(
            conn,
            policy_path=POLICY_PATH,
            incremental=True,
            limit=10,
        )
        promoted = _latest_score(conn, address)

        assert promoted_counts["score_candidates_considered"] == 1
        assert promoted["review_stage"] == CandidateStage.PAPER_APPROVED.value
    finally:
        conn.close()


def test_score_database_keeps_validated_near_threshold_wallet_in_review(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "4" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _insert_l2_state(conn, address)
        conn.commit()
        monkeypatch.setattr(
            review_pipeline,
            "score_candidate",
            lambda candidate, _features, _policy: ScoreBreakdown(
                address=candidate.address,
                leader_score=69.48,
                stage=CandidateStage.NEEDS_REVIEW,
                reason="validated_copy_stream_below_paper_score",
                components={"test": 69.48},
                penalties={},
            ),
        )

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)

        assert counts["needs_manual_review"] == 1
        assert latest["review_stage"] == CandidateStage.NEEDS_REVIEW.value
        assert latest["review_reason"] == "validated_copy_stream_below_paper_score"
    finally:
        conn.close()


def test_incremental_scoring_repairs_stale_manual_borderline_lifecycle(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "3" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _insert_pending_medium_state(conn, address)
        persist_score(conn, _borderline_score(CandidateAddress(address=address)), policy_version=_policy_version())
        conn.commit()
        monkeypatch.setattr(
            review_pipeline,
            "score_candidate",
            lambda candidate, _features, _policy: _borderline_score(candidate),
        )

        counts = score_database(
            conn,
            policy_path=POLICY_PATH,
            incremental=True,
            limit=10,
        )
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts["score_candidates_considered"] == 1
        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"] == "evidence_refinement_pending:medium_pending"
        assert stage == CandidateStage.NEEDS_DATA.value
    finally:
        conn.close()


def test_incremental_scoring_does_not_repeat_incomplete_borderline_repair_states(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    states = (
        ("0x" + "1" * 40, "summary_ready", "light_pending", ""),
        ("0x" + "2" * 40, "paused", "light_done", "score_wallet"),
    )
    try:
        run_migrations(conn)
        for address, evidence_status, current_stage, next_action in states:
            upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, evidence_depth,
                    evidence_confidence, priority, current_stage, next_action,
                    next_action_at, activity_count, updated_at
                ) VALUES (?, 'l1_light', ?, 100, 0.4, 10, ?, ?, 0, 100, 50)
                """,
                (address, evidence_status, current_stage, next_action),
            )
            persist_score(
                conn,
                _borderline_score(CandidateAddress(address=address)),
                policy_version=_policy_version(),
            )
        conn.commit()

        first = score_database(
            conn,
            policy_path=POLICY_PATH,
            incremental=True,
            limit=10,
        )
        second = score_database(
            conn,
            policy_path=POLICY_PATH,
            incremental=True,
            limit=10,
        )

        assert first["score_candidates_considered"] == 0
        assert second["score_candidates_considered"] == 0
        for address, *_ in states:
            latest = _latest_score(conn, address)
            assert latest["review_stage"] == CandidateStage.NEEDS_REVIEW.value
            assert latest["review_reason"] == "borderline_score"
    finally:
        conn.close()


def test_score_database_keeps_paper_score_in_manual_review_until_l3_ready(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "9" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        _insert_l2_state(conn, address)
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts["needs_manual_review"] == 1
        assert latest["leader_score"] >= 70
        assert latest["review_stage"] == CandidateStage.NEEDS_REVIEW.value
        assert latest["review_reason"] == "paper_evidence_tier_incomplete"
        assert stage == CandidateStage.NEEDS_REVIEW.value
    finally:
        conn.close()


def test_score_database_allows_bounded_deep_summary_for_paper_review(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "7" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        _insert_bounded_deep_state(conn, address)
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts["paper_approved"] == 1
        assert latest["leader_score"] >= 70
        assert latest["review_stage"] == CandidateStage.PAPER_APPROVED.value
        assert latest["review_reason"] != "paper_evidence_tier_incomplete"
        assert stage == CandidateStage.PAPER_APPROVED.value
    finally:
        conn.close()


def test_score_database_repairs_existing_paper_stage_without_l3(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "8" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        _insert_l2_state(conn, address)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_APPROVED.value, address),
        )
        persist_score(
            conn,
            ScoreBreakdown(
                address=address,
                leader_score=82.0,
                stage=CandidateStage.PAPER_APPROVED,
                reason="score_and_validation_present",
                components={"score": 82.0},
                penalties={},
            ),
            policy_version=_policy_version(),
        )
        conn.commit()

        repaired = review_pipeline.repair_paper_stage_evidence_incomplete(
            conn,
            policy_version=_policy_version(),
            now=123,
        )
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert repaired == 1
        assert latest["leader_score"] == 82.0
        assert latest["review_stage"] == CandidateStage.NEEDS_REVIEW.value
        assert latest["review_reason"] == "paper_evidence_tier_incomplete"
        assert stage == CandidateStage.NEEDS_REVIEW.value
    finally:
        conn.close()


def test_score_database_skips_incomplete_rescore_over_valid_latest_score(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "1" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        _insert_l3_state(conn, address)
        conn.commit()

        first_counts = score_database(conn, policy_path=POLICY_PATH)
        first = _latest_score(conn, address)
        assert first["leader_score"] > 0
        assert first["review_stage"] != CandidateStage.NEEDS_DATA.value

        conn.execute(
            """
            UPDATE wallet_features
            SET copy_event_count = NULL,
                copy_market_count = NULL,
                copy_stream_roi = NULL
            WHERE address = ?
            """,
            (address,),
        )
        conn.commit()

        second_counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)

        assert first_counts["paper_approved"] == 1
        assert second_counts["incomplete_rescore_skipped"] == 1
        assert latest["score_id"] == first["score_id"]
        assert latest["leader_score"] == first["leader_score"]
    finally:
        conn.close()


def test_score_database_does_not_preserve_valid_score_from_old_policy(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "a" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        old_valid = ScoreBreakdown(
            address=address,
            leader_score=72.0,
            stage=CandidateStage.PAPER_CANDIDATE,
            reason="score_above_paper_threshold",
            components={"old": 72.0},
            penalties={},
        )
        persist_score(conn, old_valid, policy_version="old-policy")
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_CANDIDATE.value, address),
        )
        conn.execute(
            """
            UPDATE wallet_features
            SET copy_event_count = NULL,
                copy_market_count = NULL
            WHERE address = ?
            """,
            (address,),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts.get("incomplete_rescore_skipped", 0) == 0
        assert counts.get("masked_valid_scores_restored", 0) == 0
        assert latest["policy_version"] == _policy_version()
        assert latest["review_stage"] == CandidateStage.NEEDS_DATA.value
        assert latest["review_reason"].startswith("missing_required_score_components:")
        assert stage == CandidateStage.NEEDS_DATA.value
    finally:
        conn.close()


def test_score_database_skips_unchanged_score_rows(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "9" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        conn.commit()

        first_counts = score_database(conn, policy_path=POLICY_PATH)
        first = _latest_score(conn, address)
        row_count = conn.execute("SELECT COUNT(*) AS n FROM leader_scores").fetchone()["n"]

        second_counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        latest_row_count = conn.execute("SELECT COUNT(*) AS n FROM leader_scores").fetchone()["n"]

        assert first_counts["scores_written"] == 1
        assert second_counts["scores_written"] == 0
        assert second_counts["unchanged_score_skipped"] == 1
        assert latest["score_id"] == first["score_id"]
        assert latest_row_count == row_count
    finally:
        conn.close()


def test_score_database_retries_locked_score_write(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "a" * 40
    attempts = {"locked": 0}
    original_persist_score = review_pipeline.persist_score

    def flaky_persist_score(conn_arg, score, *, policy_version=""):
        if attempts["locked"] == 0:
            attempts["locked"] += 1
            raise sqlite3.OperationalError("database is locked")
        original_persist_score(conn_arg, score, policy_version=policy_version)

    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        conn.commit()
        monkeypatch.setattr(review_pipeline, "persist_score", flaky_persist_score)

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)

        assert attempts["locked"] == 1
        assert counts["scores_written"] == 1
        assert latest is not None
    finally:
        conn.close()


def test_incremental_unchanged_score_advances_evaluation_watermark(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "c" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        _insert_l3_state(conn, address)
        conn.commit()

        score_database(conn, policy_path=POLICY_PATH)
        first = _latest_score(conn, address)
        conn.execute("UPDATE leader_scores SET scored_at = 10 WHERE score_id = ?", (first["score_id"],))
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH, incremental=True, limit=10)
        latest = _latest_score(conn, address)
        row_count = conn.execute("SELECT COUNT(*) AS n FROM leader_scores").fetchone()["n"]

        assert counts["score_candidates_considered"] == 1
        assert counts["scores_written"] == 0
        assert counts["unchanged_score_skipped"] == 1
        assert row_count == 1
        assert latest["score_id"] == first["score_id"]
        assert latest["scored_at"] > 50
    finally:
        conn.close()


def test_incremental_rescores_when_policy_version_changes_without_source_updates(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "d" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        old_score = ScoreBreakdown(
            address=address,
            leader_score=69.0,
            stage=CandidateStage.NEEDS_REVIEW,
            reason="watchlist_score",
            components={"old_policy": 69.0},
            penalties={},
        )
        persist_score(conn, old_score, policy_version="old-policy")
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.NEEDS_REVIEW.value, address),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH, incremental=True, limit=10)
        latest = _latest_score(conn, address)
        row_count = conn.execute("SELECT COUNT(*) AS n FROM leader_scores").fetchone()["n"]

        assert counts["score_candidates_considered"] == 1
        assert counts["scores_written"] == 1
        assert row_count == 2
        assert latest["policy_version"] == _policy_version()
        assert latest["components_json"] != '{"old_policy":69.0}'
    finally:
        conn.close()


def test_score_database_incremental_scores_ready_wallets_only(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    ready = "0x" + "a" * 40
    blank = "0x" + "b" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=ready, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=blank, sources="test"))
        upsert_wallet_feature(conn, _strong_features(ready))
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done', 'score_wallet', 0, 100, 50)
            """,
            (ready,),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH, incremental=True, limit=10)

        assert counts["score_candidates_considered"] == 1
        assert counts["scores_written"] == 1
        assert _latest_score(conn, ready) is not None
        assert _latest_score(conn, blank) is None
    finally:
        conn.close()


def test_score_database_does_not_treat_missing_maker_source_as_incomplete_rescore(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "7" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        conn.commit()

        score_database(conn, policy_path=POLICY_PATH)
        first = _latest_score(conn, address)
        assert first["leader_score"] > 0
        assert first["review_stage"] != CandidateStage.NEEDS_DATA.value

        conn.execute(
            """
            UPDATE wallet_features
            SET extra_json = ?
            WHERE address = ?
            """,
            ('{"maker_fraction_source":"public_activity_no_maker_flags_observed"}', address),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)

        assert counts.get("incomplete_rescore_skipped", 0) == 0
        assert latest["leader_score"] == first["leader_score"]
        assert latest["review_stage"] == first["review_stage"]
    finally:
        conn.close()


def test_score_database_preserves_non_restorable_stage_on_incomplete_rescore(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "8" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (address,),
        )
        conn.execute(
            """
            UPDATE wallet_features
            SET extra_json = ?
            WHERE address = ?
            """,
            ('{"maker_fraction_source":"public_activity_no_maker_flags_observed"}', address),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts.get("incomplete_rescore_skipped", 0) == 0
        assert latest is not None
        assert latest["review_stage"] != CandidateStage.NEEDS_DATA.value
        assert stage == CandidateStage.BLOCKED_HYGIENE.value
    finally:
        conn.close()


def test_incremental_score_processes_explicit_action_for_blocked_wallet(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "b" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        _insert_l3_state(conn, address)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_copyability' WHERE address = ?",
            (address,),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH, incremental=True, limit=10)
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts["score_candidates_considered"] == 1
        assert counts["scores_written"] == 1
        assert latest is not None
        assert stage == CandidateStage.BLOCKED_COPYABILITY.value
    finally:
        conn.close()


def test_score_database_removes_orphan_copy_credit_before_scoring(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "c" * 40
    follower = "0x" + "d" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        _upsert_wallet_feature(conn, _strong_features(address))
        _insert_l3_state(conn, address)
        conn.execute(
            "UPDATE wallet_processing_state SET next_action = '' WHERE wallet = ?",
            (address,),
        )
        conn.execute(
            """
            INSERT INTO copy_pair_stats(
                leader_wallet, follower_wallet, copy_event_count, copy_market_count,
                follower_trade_count, containment_pct, leader_precedes_pct,
                median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
            ) VALUES (?, ?, 12, 8, 40, 0.1, 1.0, 2, 100, 200, 0, 300)
            """,
            (address, follower),
        )
        persist_score(
            conn,
            ScoreBreakdown(
                address=address,
                leader_score=85.0,
                stage=CandidateStage.PAPER_APPROVED,
                reason="stale_copy_credit",
                components={"execution_copyability": 10.0},
                penalties={},
            ),
            policy_version=_policy_version(),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH, incremental=True, limit=10)
        latest = _latest_score(conn, address)
        feature = conn.execute(
            "SELECT * FROM wallet_features WHERE address = ?",
            (address,),
        ).fetchone()
        components = json.loads(latest["components_json"])

        assert counts["copyability_truth_reconciled"] == 1
        assert counts["score_candidates_considered"] == 1
        assert latest["review_stage"] not in {
            CandidateStage.PAPER_CANDIDATE.value,
            CandidateStage.PAPER_APPROVED.value,
            CandidateStage.LIVE_ELIGIBLE.value,
        }
        assert components["execution_copyability"] == 0
        assert components["copy_stream_roi"] == 0
        assert feature["leader_in_degree"] == 0
        assert feature["copy_event_count"] == 0
        assert feature["copy_market_count"] == 0
    finally:
        conn.close()


def test_score_database_restores_valid_score_masked_by_prior_incomplete_rescore(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "2" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        upsert_wallet_feature(conn, _strong_features(address))
        valid = ScoreBreakdown(
            address=address,
            leader_score=58.0,
            stage=CandidateStage.NEEDS_REVIEW,
            reason="watchlist_score",
            components={"test": 58.0},
            penalties={},
        )
        masked = ScoreBreakdown(
            address=address,
            leader_score=0.0,
            stage=CandidateStage.NEEDS_DATA,
            reason="missing_required_score_components:copy_event_count",
            components={},
            penalties={},
        )
        persist_score(conn, valid, policy_version=_policy_version())
        persist_score(conn, masked, policy_version=_policy_version())
        conn.execute(
            """
            UPDATE wallet_features
            SET copy_event_count = NULL,
                copy_market_count = NULL,
                copy_stream_roi = NULL
            WHERE address = ?
            """,
            (address,),
        )
        conn.commit()
        assert _latest_score(conn, address)["leader_score"] == 0.0

        counts = score_database(conn, policy_path=POLICY_PATH)
        latest = _latest_score(conn, address)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts["masked_valid_scores_restored"] == 1
        assert latest["leader_score"] == 58.0
        assert latest["review_stage"] == CandidateStage.NEEDS_REVIEW.value
        assert latest["policy_version"].endswith("+restored_after_incomplete_rescore")
        assert stage == CandidateStage.NEEDS_REVIEW.value
    finally:
        conn.close()


def test_score_database_repairs_stale_paper_stage_without_l3(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "3" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        valid = ScoreBreakdown(
            address=address,
            leader_score=58.0,
            stage=CandidateStage.NEEDS_REVIEW,
            reason="watchlist_score",
            components={"test": 58.0},
            penalties={},
        )
        persist_score(conn, valid, policy_version=_policy_version())
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_CANDIDATE.value, address),
        )
        conn.commit()

        counts = score_database(conn, policy_path=POLICY_PATH)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert counts["paper_evidence_incomplete_downgraded"] == 1
        assert "candidate_stage_synced_from_latest_score" not in counts
        assert stage == CandidateStage.NEEDS_REVIEW.value
    finally:
        conn.close()


def test_sync_candidate_stages_blocks_stale_paper_candidate_from_latest_score(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "4" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        blocked = ScoreBreakdown(
            address=address,
            leader_score=0.0,
            stage=CandidateStage.BLOCKED_HYGIENE,
            reason="hygiene_blacklist",
            components={},
            penalties={},
        )
        persist_score(conn, blocked, policy_version="test")
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_CANDIDATE.value, address),
        )
        conn.commit()

        synced = sync_candidate_stages_from_latest_scores(conn, now=123)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]
        event = conn.execute(
            "SELECT * FROM review_events WHERE address = ? ORDER BY event_id DESC LIMIT 1",
            (address,),
        ).fetchone()

        assert synced == 1
        assert stage == CandidateStage.BLOCKED_HYGIENE.value
        assert event["from_stage"] == CandidateStage.PAPER_CANDIDATE.value
        assert event["to_stage"] == CandidateStage.BLOCKED_HYGIENE.value
        assert event["reason"] == "sync_stage_from_latest_score"
    finally:
        conn.close()


def test_sync_candidate_stages_does_not_auto_unblock_copyability_block(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "5" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.BLOCKED_COPYABILITY.value, address),
        )
        missing = ScoreBreakdown(
            address=address,
            leader_score=0.0,
            stage=CandidateStage.NEEDS_DATA,
            reason="no_wallet_metrics_attached",
            components={},
            penalties={},
        )
        persist_score(conn, missing, policy_version="test")
        conn.commit()

        synced = sync_candidate_stages_from_latest_scores(conn, now=123)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert synced == 0
        assert stage == CandidateStage.BLOCKED_COPYABILITY.value
    finally:
        conn.close()


def _seed_manual_copyability_wallet(
    conn: sqlite3.Connection,
    address: str,
    *,
    job_status: str,
    scan_mode: Optional[str],
    candidate_pair_count: int,
    candidate_event_count: int,
    candidate_market_count: int,
    include_validated_pair_count: bool = True,
) -> None:
    upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
    extra = {
        "copy_candidate_pair_count": candidate_pair_count,
        "copy_candidate_event_count": candidate_event_count,
        "copy_candidate_market_count": candidate_market_count,
    }
    if include_validated_pair_count:
        extra["copy_validated_pair_count"] = 0
    feature = WalletFeatures(
        address=address,
        leader_in_degree=0,
        copy_event_count=0,
        copy_market_count=0,
        copy_stream_roi=0,
        hygiene_status="screened",
        extra=extra,
    )
    upsert_wallet_feature(conn, feature)
    persist_score(
        conn,
        ScoreBreakdown(
            address=address,
            leader_score=55.0,
            stage=CandidateStage.NEEDS_REVIEW,
            reason="watchlist_score",
            components={"watch": 1.0},
            penalties={},
        ),
        policy_version="test-policy",
    )
    input_data = {} if scan_mode is None else {"graph_scan_mode": scan_mode}
    conn.execute(
        """
        INSERT INTO pipeline_jobs(
            job_type, wallet, subject_key, tier, priority, shard, status,
            lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
            input_json, output_json, last_error, created_at, updated_at, completed_at
        ) VALUES (
            'copyability_evidence', ?, 'copyability', 'copyability', 10, 0, ?,
            NULL, 0, 1, 3, 0, ?, '{}', '', 100, 100, ?
        )
        """,
        (
            address,
            job_status,
            json.dumps(input_data),
            100 if job_status == "done" else None,
        ),
    )


def test_copyability_no_signal_blocks_only_completed_deep_zero_pair_wallets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    blocked = "0x" + "6" * 40
    pending = "0x" + "7" * 40
    weak_signal = "0x" + "8" * 40
    light_scan = "0x" + "9" * 40
    missing_validated_count = "0x" + "a" * 40
    try:
        run_migrations(conn)
        _seed_manual_copyability_wallet(
            conn,
            blocked,
            job_status="done",
            scan_mode=None,
            candidate_pair_count=0,
            candidate_event_count=0,
            candidate_market_count=0,
        )
        _seed_manual_copyability_wallet(
            conn,
            pending,
            job_status="queued",
            scan_mode=None,
            candidate_pair_count=0,
            candidate_event_count=0,
            candidate_market_count=0,
        )
        _seed_manual_copyability_wallet(
            conn,
            weak_signal,
            job_status="done",
            scan_mode=None,
            candidate_pair_count=2,
            candidate_event_count=20,
            candidate_market_count=4,
        )
        _seed_manual_copyability_wallet(
            conn,
            light_scan,
            job_status="done",
            scan_mode="light_missing_copyability",
            candidate_pair_count=0,
            candidate_event_count=0,
            candidate_market_count=0,
        )
        _seed_manual_copyability_wallet(
            conn,
            missing_validated_count,
            job_status="done",
            scan_mode=None,
            candidate_pair_count=0,
            candidate_event_count=0,
            candidate_market_count=0,
            include_validated_pair_count=False,
        )
        conn.commit()

        count = apply_copyability_no_signal_blocks(conn, now=123)
        rows = {
            row["address"]: row
            for row in conn.execute(
                """
                SELECT cw.address, cw.candidate_stage, ls.review_stage, ls.review_reason
                FROM candidate_wallets cw
                JOIN leader_scores ls
                  ON ls.score_id = (
                      SELECT score_id
                      FROM leader_scores
                      WHERE address = cw.address
                      ORDER BY score_id DESC
                      LIMIT 1
                  )
                WHERE cw.address IN (?, ?, ?, ?, ?)
                """,
                (blocked, pending, weak_signal, light_scan, missing_validated_count),
            )
        }
        event = conn.execute(
            """
            SELECT from_stage, to_stage, reason
            FROM review_events
            WHERE address = ?
            ORDER BY event_id DESC
            LIMIT 1
            """,
            (blocked,),
        ).fetchone()

        assert count == 2
        assert rows[blocked]["candidate_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert rows[blocked]["review_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert rows[blocked]["review_reason"] == "copyability_scan_no_signal"
        assert rows[missing_validated_count]["candidate_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert rows[missing_validated_count]["review_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert rows[missing_validated_count]["review_reason"] == "copyability_scan_no_signal"
        assert event["from_stage"] == CandidateStage.NEEDS_REVIEW.value
        assert event["to_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert event["reason"] == "copyability_scan_no_signal"
        assert rows[pending]["candidate_stage"] == CandidateStage.NEEDS_REVIEW.value
        assert rows[weak_signal]["candidate_stage"] == CandidateStage.NEEDS_REVIEW.value
        assert rows[light_scan]["candidate_stage"] == CandidateStage.NEEDS_REVIEW.value
    finally:
        conn.close()


def test_sync_candidate_stages_preserves_live_eligible_when_latest_score_still_paper_ready(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    address = "0x" + "6" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        approved = ScoreBreakdown(
            address=address,
            leader_score=75.0,
            stage=CandidateStage.PAPER_APPROVED,
            reason="score_and_validation_present",
            components={"test": 75.0},
            penalties={},
        )
        persist_score(conn, approved, policy_version="test")
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.LIVE_ELIGIBLE.value, address),
        )
        conn.commit()

        synced = sync_candidate_stages_from_latest_scores(conn, now=123)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (address,),
        ).fetchone()["candidate_stage"]

        assert synced == 0
        assert stage == CandidateStage.LIVE_ELIGIBLE.value
    finally:
        conn.close()
