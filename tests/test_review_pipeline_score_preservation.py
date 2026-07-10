import json
import sqlite3
from pathlib import Path
from typing import Optional

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
import pm_robot.orchestration.review_pipeline as review_pipeline
from pm_robot.orchestration.review_pipeline import score_database, sync_candidate_stages_from_latest_scores
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    apply_copyability_no_signal_blocks,
    persist_score,
    upsert_candidate,
    upsert_wallet_feature,
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


def test_score_database_syncs_stale_candidate_stage_from_latest_score(tmp_path):
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

        assert counts["candidate_stage_synced_from_latest_score"] == 1
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
