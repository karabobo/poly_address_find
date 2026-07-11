from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.orchestration.copyability_evidence import plan_copyability_evidence_jobs
from pm_robot.orchestration.eligibility_repair import prepare_eligibility_repairs
from pm_robot.orchestration.pipeline_smoothness import pipeline_smoothness_report
from pm_robot.orchestration.wallet_pipeline import plan_wallet_pipeline_jobs
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    materialize_wallet_processing_state,
    persist_score,
    persist_wallet_activity,
    upsert_candidate,
    upsert_wallet_feature,
)


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

        prepare_eligibility_repairs(conn, limit=10, now=50_000)
        materialize_wallet_processing_state(conn, limit=2)
        plan_wallet_pipeline_jobs(conn, shard_count=1, now=50_000)
        plan_copyability_evidence_jobs(
            conn,
            limit=10,
            max_active_jobs=10,
            min_score=40,
            min_activity_events=25,
            shard_count=1,
            now=50_000,
        )
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
            {"job_type": "copyability_evidence", "status": "queued", "count": 2}
        ]
        stuck = {item["wallet"]: item for item in report["top_stuck_wallets"]}
        assert stuck[thin]["wallet_pipeline_status"] == "queued"
        assert stuck[copy_blocked]["copyability_status"] == "queued"
        assert paper_ready not in stuck
        assert any("wallet-pipeline-plan/worker" in step for step in report["next_steps"])
        assert any("copyability-plan/worker" in step for step in report["next_steps"])
    finally:
        conn.close()


def test_pipeline_smoothness_scopes_trade_counts_to_selected_wallets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        _score(conn, wallet, score=72, stage=CandidateStage.NEEDS_REVIEW, reason="watchlist_score")
        persist_wallet_activity(conn, wallet, _trade_events(wallet, 3), ingested_at=20_000)
        conn.commit()

        statements: list[str] = []
        conn.set_trace_callback(statements.append)
        report = pipeline_smoothness_report(conn, top=10, min_score=40, now=50_100)
        conn.set_trace_callback(None)

        row = next(item for item in report["top_stuck_wallets"] if item["wallet"] == wallet)
        assert row["trade_events"] == 3
        eligibility_query = next(
            statement
            for statement in statements
            if "WITH active_jobs AS" in statement and "AS trade_events" in statement
        )
        assert "LEFT JOIN leader_latest_scores" in eligibility_query
        assert "WHERE wa.address = cw.address" in eligibility_query
        assert "GROUP BY address" not in eligibility_query
    finally:
        conn.close()


def test_pipeline_smoothness_does_not_requeue_completed_deep_near_miss(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(address=wallet, hygiene_status="clean", copy_event_count=0),
        )
        _score(conn, wallet, score=68, stage=CandidateStage.NEEDS_REVIEW, reason="watchlist_score")
        persist_wallet_activity(conn, wallet, _trade_events(wallet, 120), ingested_at=20_000)
        _insert_l3_evidence(conn, wallet)
        conn.execute(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, 1, 12, 3, 0.4, 20, 0, 50000, 50000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 3, 0,
                      'done', NULL, 0, 1, 3, 0, '{"graph_scan_mode":"deep"}',
                      '{"graph_scan_mode":"deep"}', '', 50000, 50000, 50000)
            """,
            (wallet,),
        )
        conn.commit()

        report = pipeline_smoothness_report(conn, top=10, min_score=40, now=50_100)

        row = next(item for item in report["top_stuck_wallets"] if item["wallet"] == wallet)
        assert row["review_disposition"] == "copyability_near_miss"
        assert row["review_handling"] == "watch"
        assert row["operator_required"] is False
        assert "copyability_evidence" not in row["recommended_actions"]
        assert report["eligibility"]["disposition_counts"] == {"copyability_near_miss": 1}
        assert report["eligibility"]["action_counts"] == {}
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
                  'score_wallet', 0, 1000, 20, 200, 20000)
        """,
        (wallet,),
    )
