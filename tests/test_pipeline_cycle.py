from pathlib import Path

from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.orchestration.pipeline_cycle import PipelineCycleOptions, run_pipeline_cycle
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


def _score(conn, wallet: str, *, score: float, stage: CandidateStage = CandidateStage.NEEDS_REVIEW) -> None:
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


def test_pipeline_cycle_dry_run_is_read_only(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        _score(conn, wallet, score=72)
        conn.commit()

        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(execute_plan=False, shard_count=1, min_score=40),
        )
        jobs = conn.execute("SELECT * FROM pipeline_jobs").fetchall()
        budgets = conn.execute("SELECT * FROM evidence_backfill_budget").fetchall()
        states = conn.execute("SELECT * FROM wallet_processing_state").fetchall()

        assert report["ok"] is True
        assert report["dry_run"] is True
        assert report["executed"] is False
        assert report["steps"][0]["name"] == "eligibility_repair_preview"
        assert report["steps"][0]["data"]["wallets_ineligible"] == 1
        assert jobs == []
        assert budgets == []
        assert states == []
    finally:
        conn.close()


def test_pipeline_cycle_execute_queues_repair_without_workers_or_duplicate_light_job(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    thin = "0x" + "2" * 40
    copy_blocked = "0x" + "3" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=thin, sources="test_source"))
        _score(conn, thin, score=72)

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
        _score(conn, copy_blocked, score=68)
        persist_wallet_activity(conn, copy_blocked, _trade_events(copy_blocked, 120), ingested_at=20_000)
        conn.commit()

        report = run_pipeline_cycle(
            conn,
            PipelineCycleOptions(
                execute_plan=True,
                shard_count=1,
                min_score=40,
                feature_limit=0,
                run_scoring=False,
                policy_path=Path("config/leader_scoring_policy.json"),
            ),
        )
        thin_jobs = conn.execute(
            """
            SELECT * FROM pipeline_jobs
            WHERE wallet = ?
              AND job_type = 'wallet_evidence_backfill'
              AND subject_key = 'light_pending'
            """,
            (thin,),
        ).fetchall()
        copyability_jobs = conn.execute(
            "SELECT * FROM pipeline_jobs WHERE wallet = ? AND job_type = 'copyability_evidence'",
            (copy_blocked,),
        ).fetchall()
        runs = conn.execute("SELECT * FROM ingest_runs").fetchall()

        assert report["ok"] is True
        assert report["dry_run"] is False
        assert report["executed"] is True
        assert report["safety"]["runs_network_workers"] is False
        assert len(thin_jobs) == 1
        assert thin_jobs[0]["subject_key"] == "light_pending"
        assert thin_jobs[0]["tier"] == "eligibility_repair"
        assert len(copyability_jobs) == 1
        assert runs == []
        assert report["after"]["queues"]["wallet_pipeline"]["statuses"]
        assert report["after"]["queues"]["copyability"]["statuses"] == [
            {"job_type": "copyability_evidence", "status": "queued", "count": 1}
        ]
    finally:
        conn.close()
