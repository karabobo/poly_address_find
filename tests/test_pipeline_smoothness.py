from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.orchestration.eligibility_repair import plan_eligibility_repair_jobs
from pm_robot.orchestration.pipeline_smoothness import pipeline_smoothness_report
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


def _score(conn, wallet: str, *, score: float, stage: CandidateStage, reason: str) -> None:
    persist_score(
        conn,
        ScoreBreakdown(
            address=wallet,
            leader_score=score,
            stage=stage,
            reason=reason,
            components={"score": score},
            penalties={},
        ),
        policy_version="test",
    )


def test_pipeline_smoothness_reports_eligibility_blockers_and_backlog(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    thin = "0x" + "1" * 40
    copy_blocked = "0x" + "2" * 40
    paper_ready = "0x" + "3" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=thin, sources="test_source"))
        _score(conn, thin, score=72, stage=CandidateStage.NEEDS_REVIEW, reason="watchlist_score")

        upsert_candidate(conn, CandidateAddress(address=copy_blocked, sources="test_source"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=copy_blocked,
                hygiene_status="clean",
                copy_event_count=0,
                edge_retention_pct=80,
                walk_forward_consistency_pct=100,
            ),
        )
        _score(conn, copy_blocked, score=68, stage=CandidateStage.NEEDS_REVIEW, reason="watchlist_score")
        persist_wallet_activity(conn, copy_blocked, _trade_events(copy_blocked, 120), ingested_at=20_000)

        upsert_candidate(conn, CandidateAddress(address=paper_ready, sources="test_source"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=paper_ready,
                hygiene_status="clean",
                copy_event_count=2,
                edge_retention_pct=80,
                walk_forward_consistency_pct=100,
            ),
        )
        _score(conn, paper_ready, score=60, stage=CandidateStage.PAPER_CANDIDATE, reason="paper_candidate")
        persist_wallet_activity(conn, paper_ready, _trade_events(paper_ready, 100), ingested_at=20_000)
        _insert_l3_evidence(conn, paper_ready)
        conn.commit()

        plan_eligibility_repair_jobs(conn, limit=10, shard_count=1, now=50_000)
        report = pipeline_smoothness_report(conn, top=10, now=50_100)

        assert report["ok"] is True
        assert report["stage_counts"][CandidateStage.NEEDS_REVIEW.value] == 2
        assert report["stage_counts"][CandidateStage.PAPER_CANDIDATE.value] == 1
        assert report["eligibility"]["wallets_scanned"] == 3
        assert report["eligibility"]["paper_eligible"] == 1
        assert report["eligibility"]["paper_ineligible"] == 2
        assert report["eligibility"]["reason_counts"]["provisional_review_stage"] == 2
        assert report["eligibility"]["action_counts"]["wallet_evidence_backfill"] == 1
        assert report["eligibility"]["action_counts"]["copyability_evidence"] == 1
        assert report["queues"]["wallet_pipeline"]["statuses"] == [
            {"job_type": "wallet_evidence_backfill", "status": "queued", "count": 1}
        ]
        assert report["queues"]["copyability"]["statuses"] == [
            {"job_type": "copyability_evidence", "status": "queued", "count": 1}
        ]
        stuck = {item["wallet"]: item for item in report["top_stuck_wallets"]}
        assert stuck[thin]["wallet_pipeline_status"] == "queued"
        assert stuck[copy_blocked]["copyability_status"] == "queued"
        assert paper_ready not in stuck
        assert any("wallet-pipeline" in step for step in report["next_steps"])
        assert any("copyability" in step for step in report["next_steps"])
    finally:
        conn.close()


def _insert_l3_evidence(conn, wallet: str) -> None:
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, distinct_markets,
            non_fast_trade_count, updated_at
        ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done',
                  'score_wallet', 0, 1000, 20, 200, 20_000)
        """,
        (wallet,),
    )
