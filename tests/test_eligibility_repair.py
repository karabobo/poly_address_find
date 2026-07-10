from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.orchestration.eligibility_repair import prepare_eligibility_repairs
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import persist_score, persist_wallet_activity, upsert_candidate, upsert_wallet_feature


def _trade_events(wallet: str, count: int) -> list[dict]:
    return [
        {
            "proxyWallet": wallet,
            "timestamp": 10_000 + idx,
            "conditionId": f"condition-{idx % 5}",
            "eventSlug": f"event-{idx % 5}",
            "slug": f"market-{idx % 5}",
            "asset": f"asset-{idx % 5}",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY",
            "price": 0.55,
            "size": 20,
            "usdcSize": 11,
            "transactionHash": f"0x{idx:064x}",
        }
        for idx in range(count)
    ]


def _score(conn, wallet: str, *, score: float = 70.0, stage: CandidateStage = CandidateStage.NEEDS_REVIEW) -> None:
    persist_score(
        conn,
        ScoreBreakdown(
            address=wallet,
            leader_score=score,
            stage=stage,
            reason="watchlist_score",
            components={"score": score},
            penalties={},
        ),
        policy_version="test",
    )


def test_eligibility_repair_prepares_thin_review_wallet_for_wallet_planner(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        _score(conn, wallet, score=72)
        conn.commit()

        summary = prepare_eligibility_repairs(conn, limit=10, now=50_000)
        budget = conn.execute("SELECT * FROM evidence_backfill_budget WHERE wallet = ?", (wallet,)).fetchone()
        job = conn.execute(
            "SELECT * FROM pipeline_jobs WHERE wallet = ? AND job_type = 'wallet_evidence_backfill'",
            (wallet,),
        ).fetchone()
        assert summary.wallets_seen == 1
        assert summary.wallets_ineligible == 1
        assert summary.evidence_budgets_seeded == 1
        assert summary.wallet_repairs_prepared == 1
        assert summary.copyability_repairs_ready == 0
        assert summary.reason_counts["insufficient_trade_events"] == 1
        assert summary.action_counts["wallet_evidence_backfill"] == 1
        assert budget["source"] == "eligibility_repair"
        assert budget["target_depth"] == 200
        assert job is None
    finally:
        conn.close()


def test_eligibility_repair_leaves_copyability_admission_to_copyability_planner(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                hygiene_status="clean",
                copy_event_count=0,
                edge_retention_pct=80,
                walk_forward_consistency_pct=100,
            ),
        )
        _score(conn, wallet, score=72)
        persist_wallet_activity(conn, wallet, _trade_events(wallet, 120), ingested_at=20_000)
        conn.commit()

        summary = prepare_eligibility_repairs(conn, limit=10, now=50_000)
        wallet_job = conn.execute(
            "SELECT * FROM pipeline_jobs WHERE wallet = ? AND job_type = 'wallet_evidence_backfill'",
            (wallet,),
        ).fetchone()
        copy_job = conn.execute(
            "SELECT * FROM pipeline_jobs WHERE wallet = ? AND job_type = 'copyability_evidence'",
            (wallet,),
        ).fetchone()
        assert summary.wallets_seen == 1
        assert summary.wallets_ineligible == 1
        assert summary.wallet_repairs_prepared == 0
        assert summary.copyability_repairs_ready == 1
        assert summary.reason_counts["missing_copyability_evidence"] == 1
        assert summary.action_counts["copyability_evidence"] == 1
        assert wallet_job is None
        assert copy_job is None
    finally:
        conn.close()


def test_eligibility_repair_does_not_queue_paper_eligible_wallet(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                hygiene_status="clean",
                copy_event_count=2,
                edge_retention_pct=80,
                walk_forward_consistency_pct=100,
            ),
        )
        _score(conn, wallet, score=60, stage=CandidateStage.PAPER_CANDIDATE)
        persist_wallet_activity(conn, wallet, _trade_events(wallet, 100), ingested_at=20_000)
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done',
                      'score_wallet', 0, 1000, 20, 200, 20000)
            """,
            (wallet,),
        )
        conn.commit()

        summary = prepare_eligibility_repairs(conn, limit=10, now=50_000)
        jobs = conn.execute("SELECT * FROM pipeline_jobs WHERE wallet = ?", (wallet,)).fetchall()

        assert summary.wallets_seen == 1
        assert summary.wallets_ineligible == 0
        assert summary.reason_counts == {}
        assert jobs == []
    finally:
        conn.close()
