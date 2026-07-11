from pathlib import Path
import time

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.orchestration.evidence_backfill import summarize_wallet_evidence
from pm_robot.orchestration.evidence_promotion import promote_wallet_evidence
from pm_robot.orchestration.feature_materializer import MATERIALIZER_VERSION
from pm_robot.pipeline_terms import EvidenceJobStage, PipelineJobType
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    enqueue_pipeline_job,
    persist_score,
    persist_wallet_activity,
    seed_evidence_backfill_budget,
    sync_wallet_processing_state,
    upsert_candidate,
    upsert_wallet_feature,
)


POLICY_PATH = Path("config/leader_scoring_policy.json")
POLICY_VERSION = str(load_policy(POLICY_PATH)["version"])


def _events(wallet: str, count: int, *, market_count: int = 12) -> list[dict]:
    return [
        {
            "proxyWallet": wallet,
            "timestamp": 10_000 + idx,
            "conditionId": f"condition-{idx % market_count}",
            "eventSlug": f"event-{idx % market_count}",
            "slug": f"politics-market-{idx % market_count}",
            "asset": f"asset-{idx % market_count}",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY",
            "price": 0.5,
            "size": 20,
            "usdcSize": 10,
            "transactionHash": f"0x{idx:064x}",
        }
        for idx in range(count)
    ]


def _seed_ready_features(conn, wallet: str, activity_count: int, *, net_pnl: float = 250) -> None:
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            cumulative_win_rate=0.7,
            recent_30d_volume_usdc=2_000,
            net_pnl_usdc=net_pnl,
            total_volume_usdc=5_000,
            event_win_rate=0.65,
            trade_win_rate=0.6,
            avg_dca_entries=2,
            sell_pct=20,
            bot_score=10,
            trades_per_day=5,
            median_gap_sec=60,
            leader_in_degree=0,
            copy_event_count=0,
            copy_market_count=0,
            copy_stream_roi=0,
            single_market_pnl_share=0.2,
            net_to_gross_exposure=0.8,
            hygiene_status="screened",
            extra={
                "feature_materializer_version": MATERIALIZER_VERSION,
                "feature_materializer_activity_count": activity_count,
                "feature_materializer_fast_market_share": 0.0,
            },
        ),
    )


def _seed_stage(conn, wallet: str, *, stage: str, activity_count: int) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
    persist_wallet_activity(conn, wallet, _events(wallet, activity_count), ingested_at=20_000)
    seed_evidence_backfill_budget(conn, wallet, source="test", priority=10)
    conn.execute(
        """
        UPDATE evidence_backfill_budget
        SET stage = ?, target_depth = ?, current_depth = ?, stop_reason = 'legacy_transition'
        WHERE wallet = ?
        """,
        (
            stage,
            1_000 if stage in {"medium_done", "medium_pending"} else 200,
            activity_count,
            wallet,
        ),
    )
    evidence = summarize_wallet_evidence(conn, wallet)
    sync_wallet_processing_state(conn, wallet, evidence, source="test", now=30_000)


def test_light_done_requires_policy_materiality_before_medium(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    approved = "0x" + "1" * 40
    deferred = "0x" + "2" * 40
    try:
        run_migrations(conn)
        for wallet in (approved, deferred):
            _seed_stage(conn, wallet, stage=EvidenceJobStage.LIGHT_DONE.value, activity_count=80)
        _seed_ready_features(conn, approved, 80, net_pnl=250)
        _seed_ready_features(conn, deferred, 80, net_pnl=20)
        conn.execute(
            "UPDATE wallet_processing_state SET activity_count = 25 WHERE wallet = ?",
            (approved,),
        )
        conn.commit()

        summary = promote_wallet_evidence(conn, policy_path=POLICY_PATH, limit=10, now=40_000)
        rows = {
            row["wallet"]: row
            for row in conn.execute(
                "SELECT wallet, stage, stop_reason FROM evidence_backfill_budget"
            ).fetchall()
        }

        assert summary.medium_approved == 1
        assert summary.deferred == 1
        assert rows[approved]["stage"] == EvidenceJobStage.MEDIUM_PENDING.value
        assert rows[approved]["stop_reason"].startswith("promotion_approved:medium_pending:")
        assert rows[deferred]["stage"] == EvidenceJobStage.LIGHT_DONE.value
        assert rows[deferred]["stop_reason"].startswith("promotion_deferred:medium_pending:")
    finally:
        conn.close()


def test_medium_done_requires_fresh_score_before_deep(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    approved = "0x" + "3" * 40
    deferred = "0x" + "4" * 40
    try:
        run_migrations(conn)
        for wallet in (approved, deferred):
            _seed_stage(conn, wallet, stage=EvidenceJobStage.MEDIUM_DONE.value, activity_count=350)
            _seed_ready_features(conn, wallet, 350)
        conn.commit()
        for wallet, score in ((approved, 55), (deferred, 35)):
            persist_score(
                conn,
                ScoreBreakdown(
                    address=wallet,
                    leader_score=score,
                    stage=CandidateStage.NEEDS_REVIEW,
                    reason="test_score",
                    components={},
                    penalties={},
                ),
                policy_version=POLICY_VERSION,
            )
        conn.execute(
            "UPDATE leader_scores SET scored_at = (SELECT MAX(updated_at) + 1 FROM wallet_features)"
        )
        conn.commit()

        summary = promote_wallet_evidence(conn, policy_path=POLICY_PATH, limit=10, now=60_000)
        rows = {
            row["wallet"]: row
            for row in conn.execute(
                "SELECT wallet, stage, stop_reason FROM evidence_backfill_budget"
            ).fetchall()
        }

        assert summary.deep_approved == 1
        assert summary.deferred == 1
        assert rows[approved]["stage"] == EvidenceJobStage.DEEP_PENDING.value
        assert rows[deferred]["stage"] == EvidenceJobStage.MEDIUM_DONE.value
        assert "medium_score_below_40" in rows[deferred]["stop_reason"]
    finally:
        conn.close()


def test_unapproved_legacy_job_is_superseded_before_network_planning(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        _seed_stage(conn, wallet, stage=EvidenceJobStage.MEDIUM_PENDING.value, activity_count=80)
        assert enqueue_pipeline_job(
            conn,
            job_type=PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
            wallet=wallet,
            subject_key=EvidenceJobStage.MEDIUM_PENDING.value,
            tier="l1_light",
            priority=10,
            shard=0,
            now=30_000,
        )
        conn.commit()

        summary = promote_wallet_evidence(conn, policy_path=POLICY_PATH, limit=0, now=40_000)
        job = conn.execute(
            "SELECT status, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.queued_jobs_superseded == 1
        assert job["status"] == "superseded"
        assert job["last_error"] == "awaiting_policy_evidence_promotion"
    finally:
        conn.close()


def test_light_pending_never_skips_its_network_depth(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        _seed_stage(
            conn,
            wallet,
            stage=EvidenceJobStage.LIGHT_PENDING.value,
            activity_count=80,
        )
        _seed_ready_features(conn, wallet, 80)
        conn.commit()

        summary = promote_wallet_evidence(
            conn,
            policy_path=POLICY_PATH,
            limit=10,
            now=40_000,
        )
        budget = conn.execute(
            "SELECT stage, stop_reason FROM evidence_backfill_budget WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.targets_seen == 0
        assert budget["stage"] == EvidenceJobStage.LIGHT_PENDING.value
        assert budget["stop_reason"] == "legacy_transition"
    finally:
        conn.close()


def test_stale_approval_is_invalidated_and_pending_state_is_normalized(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    try:
        run_migrations(conn)
        _seed_stage(
            conn,
            wallet,
            stage=EvidenceJobStage.LIGHT_DONE.value,
            activity_count=80,
        )
        _seed_ready_features(conn, wallet, 80)
        conn.commit()
        first = promote_wallet_evidence(
            conn,
            policy_path=POLICY_PATH,
            limit=10,
            now=40_000,
        )
        assert first.medium_approved == 1
        assert enqueue_pipeline_job(
            conn,
            job_type=PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
            wallet=wallet,
            subject_key=EvidenceJobStage.MEDIUM_PENDING.value,
            tier="l1_light",
            priority=10,
            shard=0,
            now=40_001,
        )
        conn.execute(
            "UPDATE wallet_features SET updated_at = updated_at + 1 WHERE address = ?",
            (wallet,),
        )
        conn.commit()

        second = promote_wallet_evidence(
            conn,
            policy_path=POLICY_PATH,
            limit=0,
            now=40_002,
        )
        budget = conn.execute(
            "SELECT stage, stop_reason FROM evidence_backfill_budget WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        job = conn.execute(
            "SELECT status FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert second.stale_approvals_invalidated == 1
        assert second.queued_jobs_superseded == 1
        assert second.pending_states_normalized == 1
        assert budget["stage"] == EvidenceJobStage.LIGHT_DONE.value
        assert budget["stop_reason"].startswith(
            "promotion_recheck_required:medium_pending:"
        )
        assert job["status"] == "superseded"
    finally:
        conn.close()


def test_same_policy_deferral_waits_for_new_evidence(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        _seed_stage(
            conn,
            wallet,
            stage=EvidenceJobStage.LIGHT_DONE.value,
            activity_count=80,
        )
        _seed_ready_features(conn, wallet, 80, net_pnl=20)
        conn.commit()
        evaluation_at = int(time.time()) + 10

        first = promote_wallet_evidence(
            conn,
            policy_path=POLICY_PATH,
            limit=10,
            now=evaluation_at,
        )
        unchanged = promote_wallet_evidence(
            conn,
            policy_path=POLICY_PATH,
            limit=10,
            now=evaluation_at + 1,
        )
        persist_wallet_activity(
            conn,
            wallet,
            _events(wallet, 81),
            ingested_at=evaluation_at + 2,
        )
        _seed_ready_features(conn, wallet, 81, net_pnl=20)
        conn.commit()
        refreshed = promote_wallet_evidence(
            conn,
            policy_path=POLICY_PATH,
            limit=10,
            now=evaluation_at + 3,
        )

        assert first.deferred == 1
        assert unchanged.targets_seen == 0
        assert refreshed.targets_seen == 1
        assert refreshed.deferred == 1
    finally:
        conn.close()


def test_deep_promotion_rejects_score_older_than_features(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        _seed_stage(
            conn,
            wallet,
            stage=EvidenceJobStage.MEDIUM_DONE.value,
            activity_count=350,
        )
        _seed_ready_features(conn, wallet, 350)
        persist_score(
            conn,
            ScoreBreakdown(
                address=wallet,
                leader_score=55,
                stage=CandidateStage.NEEDS_REVIEW,
                reason="test_score",
                components={},
                penalties={},
            ),
            policy_version=POLICY_VERSION,
        )
        feature_updated_at = int(
            conn.execute(
                "SELECT updated_at FROM wallet_features WHERE address = ?",
                (wallet,),
            ).fetchone()[0]
        )
        conn.execute(
            "UPDATE leader_scores SET scored_at = ? WHERE address = ?",
            (feature_updated_at - 1, wallet),
        )
        conn.commit()

        summary = promote_wallet_evidence(
            conn,
            policy_path=POLICY_PATH,
            limit=10,
            now=60_000,
        )
        budget = conn.execute(
            "SELECT stage, stop_reason FROM evidence_backfill_budget WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.deep_approved == 0
        assert summary.deferred == 1
        assert budget["stage"] == EvidenceJobStage.MEDIUM_DONE.value
        assert "medium_score_not_fresh" in budget["stop_reason"]
    finally:
        conn.close()
