import json
import time

from pm_robot.clients.http import HttpClientError
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown
from pm_robot.pipeline_terms import EvidenceJobStage
from pm_robot.orchestration.feature_materializer import MATERIALIZER_VERSION
from pm_robot.orchestration.evidence_backfill import (
    _fetch_activity_history,
    plan_queued_evidence_backfill,
    prioritize_backfill_from_scores,
    queued_evidence_backfill_status,
    run_evidence_backfill,
    run_queued_evidence_backfill_worker,
    summarize_wallet_evidence,
)
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    enqueue_evidence_backfill_job,
    persist_score,
    seed_evidence_backfill_budget,
    upsert_candidate,
)


class FakeEvidenceClient:
    def __init__(self, activity_by_wallet, positions_by_wallet=None):
        self.activity_by_wallet = activity_by_wallet
        self.positions_by_wallet = positions_by_wallet or {}
        self.activity_calls = []
        self.position_calls = []

    def activity(self, wallet, *, limit, offset):
        self.activity_calls.append((wallet, limit, offset))
        rows = self.activity_by_wallet.get(wallet, [])
        return rows[offset : offset + limit]

    def positions(self, wallet, *, size_threshold=0.0):
        self.position_calls.append((wallet, size_threshold))
        return self.positions_by_wallet.get(wallet, [])


class RateLimitedEvidenceClient:
    def activity(self, wallet, *, limit, offset):
        raise HttpClientError(
            "shared cooldown",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=60.0,
        )

    def positions(self, wallet, *, size_threshold=0.0):
        raise AssertionError("positions should not run after activity cooldown")


def _event(wallet: str, idx: int, *, market: str) -> dict:
    return {
        "proxyWallet": wallet,
        "timestamp": 1_000 + idx,
        "conditionId": f"condition-{idx % 20}",
        "eventSlug": f"event-{idx % 20}",
        "slug": market,
        "asset": f"asset-{idx % 20}",
        "outcome": "YES",
        "type": "TRADE",
        "side": "BUY" if idx % 3 else "SELL",
        "price": 0.5,
        "size": 10,
        "usdcSize": 5,
        "transactionHash": f"0x{idx:064x}",
    }


def test_activity_history_uses_one_request_for_light_depth():
    wallet = "0x" + "1" * 40
    rows = [_event(wallet, idx, market=f"politics-{idx % 4}") for idx in range(200)]
    client = FakeEvidenceClient({wallet: rows})

    result = _fetch_activity_history(
        client,
        wallet,
        page_limit=500,
        max_events=200,
        sleep_seconds=0,
    )

    assert len(result) == 200
    assert client.activity_calls == [(wallet, 200, 0)]


def test_activity_history_clamps_page_size_to_data_api_limit():
    wallet = "0x" + "2" * 40
    rows = [_event(wallet, idx, market=f"politics-{idx % 4}") for idx in range(1_000)]
    client = FakeEvidenceClient({wallet: rows})

    result = _fetch_activity_history(
        client,
        wallet,
        page_limit=1_000,
        max_events=1_000,
        sleep_seconds=0,
    )

    assert len(result) == 1_000
    assert client.activity_calls == [
        (wallet, 500, 0),
        (wallet, 500, 500),
    ]


def test_evidence_backfill_promotes_diverse_light_wallet_to_medium(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        seed_evidence_backfill_budget(conn, wallet, source="polymarket_trades_global", priority=20)
        conn.commit()
        activity = [_event(wallet, idx, market=f"politics-market-{idx % 8}") for idx in range(200)]
        client = FakeEvidenceClient(
            {wallet: activity},
            {wallet: [{"asset": "asset-open", "size": 10, "marketSlug": "politics-market-1"}]},
        )

        summary = run_evidence_backfill(
            conn,
            light_limit=1,
            medium_limit=0,
            deep_limit=0,
            page_limit=50,
            sleep_seconds=0,
            client=client,
        )
        budget = conn.execute("SELECT * FROM evidence_backfill_budget WHERE wallet = ?", (wallet,)).fetchone()
        evidence = summarize_wallet_evidence(conn, wallet)

        assert summary.status == "ok"
        assert summary.wallets_succeeded == 1
        assert summary.activity_events_written == 200
        assert summary.positions_written == 1
        assert budget["stage"] == "light_done"
        assert budget["target_depth"] == 200
        assert evidence["distinct_markets"] == 8
    finally:
        conn.close()


def test_evidence_backfill_pauses_fast_market_specialist(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        seed_evidence_backfill_budget(conn, wallet, source="polymarket_trades_global", priority=20)
        conn.commit()
        activity = [_event(wallet, idx, market=f"btc-updown-5m-{idx % 3}") for idx in range(100)]
        client = FakeEvidenceClient({wallet: activity})

        summary = run_evidence_backfill(
            conn,
            light_limit=1,
            medium_limit=0,
            deep_limit=0,
            page_limit=50,
            sleep_seconds=0,
            client=client,
        )
        budget = conn.execute("SELECT * FROM evidence_backfill_budget WHERE wallet = ?", (wallet,)).fetchone()
        evidence = summarize_wallet_evidence(conn, wallet)

        assert summary.status == "ok"
        assert budget["stage"] == "paused_fast_market_specialist"
        assert budget["stop_reason"] == "fast_market_specialist"
        assert evidence["fast_market_share"] == 1.0
    finally:
        conn.close()


def test_direct_evidence_backfill_reports_actual_attempts_on_shared_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        for suffix in ("6", "7"):
            wallet = "0x" + suffix * 40
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
            seed_evidence_backfill_budget(conn, wallet, source="test", priority=20)
        conn.commit()

        summary = run_evidence_backfill(
            conn,
            light_limit=2,
            medium_limit=0,
            deep_limit=0,
            sleep_seconds=0,
            client=RateLimitedEvidenceClient(),
        )

        assert summary.status == "partial"
        assert summary.wallets_attempted == 1
        assert summary.wallets_succeeded == 0
        run = conn.execute(
            "SELECT wallets_attempted FROM ingest_runs WHERE run_id = ?",
            (summary.run_id,),
        ).fetchone()
        assert run["wallets_attempted"] == 1
    finally:
        conn.close()


def test_prioritize_backfill_from_scores_promotes_review_wallets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "c" * 40
    done_wallet = "0x" + "d" * 40
    try:
        run_migrations(conn)
        for item in (wallet, done_wallet):
            upsert_candidate(conn, CandidateAddress(address=item, sources="test"))
        persist_score(
            conn,
            ScoreBreakdown(
                address=wallet,
                leader_score=51.0,
                stage=CandidateStage.NEEDS_REVIEW,
                reason="watchlist_score",
                components={},
                penalties={},
            ),
            policy_version="test",
        )
        persist_score(
            conn,
            ScoreBreakdown(
                address=done_wallet,
                leader_score=60.0,
                stage=CandidateStage.NEEDS_REVIEW,
                reason="watchlist_score",
                components={},
                penalties={},
            ),
            policy_version="test",
        )
        seed_evidence_backfill_budget(conn, done_wallet, source="test", priority=20, target_depth=1000)
        conn.execute(
            "UPDATE evidence_backfill_budget SET stage = 'medium_done', current_depth = 1000 WHERE wallet = ?",
            (done_wallet,),
        )
        conn.commit()

        summary = prioritize_backfill_from_scores(
            conn,
            min_score=40,
            limit=10,
            target_depth=1000,
            priority=7,
            now=50_000,
        )
        promoted = conn.execute("SELECT * FROM evidence_backfill_budget WHERE wallet = ?", (wallet,)).fetchone()
        unchanged = conn.execute("SELECT * FROM evidence_backfill_budget WHERE wallet = ?", (done_wallet,)).fetchone()

        assert summary.wallets_matched == 1
        assert summary.budgets_updated == 1
        assert promoted["stage"] == "medium_pending"
        assert promoted["priority"] == 7
        assert promoted["target_depth"] == 1000
        assert promoted["stop_reason"] == "promotion_requested:medium_pending:test"
        assert unchanged["stage"] == "medium_done"
    finally:
        conn.close()


def test_legacy_evidence_planner_requires_depth_approval(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        seed_evidence_backfill_budget(conn, wallet, source="test", priority=20)
        conn.execute(
            """
            UPDATE evidence_backfill_budget
            SET stage = 'medium_pending', target_depth = 1000,
                stop_reason = 'promotion_requested:medium_pending:test'
            WHERE wallet = ?
            """,
            (wallet,),
        )
        conn.commit()

        blocked = plan_queued_evidence_backfill(
            conn,
            policy_version="test",
            light_limit=0,
            medium_limit=1,
            deep_limit=0,
            shard_count=1,
            now=55_000,
        )
        conn.execute(
            """
            UPDATE evidence_backfill_budget
            SET stop_reason = 'promotion_approved:medium_pending:test:55001:0',
                evidence_json = ?
            WHERE wallet = ?
            """,
            (
                json.dumps(
                    {
                        "promotion": {
                            "approved": True,
                            "job_action": "medium_pending",
                            "policy_version": "test",
                            "feature_updated_at": 55_001,
                            "activity_count": 0,
                            "materializer_version": MATERIALIZER_VERSION,
                        }
                    }
                ),
                wallet,
            ),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(address, extra_json, updated_at)
            VALUES (?, ?, 55001)
            """,
            (
                wallet,
                json.dumps(
                    {
                        "feature_materializer_version": MATERIALIZER_VERSION,
                        "feature_materializer_activity_count": 0,
                    }
                ),
            ),
        )
        conn.commit()
        approved = plan_queued_evidence_backfill(
            conn,
            policy_version="test",
            light_limit=0,
            medium_limit=1,
            deep_limit=0,
            shard_count=1,
            now=55_001,
        )

        assert blocked.jobs_enqueued == 0
        assert approved.jobs_enqueued == 1
    finally:
        conn.close()


def test_queued_evidence_backfill_plans_and_processes_shards(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + "e" * 40, "0x" + "f" * 40]
    try:
        run_migrations(conn)
        for wallet in wallets:
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
            seed_evidence_backfill_budget(conn, wallet, source="polymarket_trades_global", priority=20)
        conn.commit()
        plan = plan_queued_evidence_backfill(
            conn,
            light_limit=2,
            medium_limit=0,
            deep_limit=0,
            shard_count=2,
            now=60_000,
        )
        before = queued_evidence_backfill_status(conn)
        client = FakeEvidenceClient(
            {
                wallet: [_event(wallet, idx, market=f"politics-market-{idx % 6}") for idx in range(80)]
                for wallet in wallets
            },
            {
                wallet: [{"asset": "asset-open", "size": 10, "marketSlug": "politics-market-1"}]
                for wallet in wallets
            },
        )

        summaries = [
            run_queued_evidence_backfill_worker(
                conn,
                shard_index=idx,
                shard_count=2,
                limit=5,
                page_limit=40,
                sleep_seconds=0,
                client=client,
            )
            for idx in range(2)
        ]
        after = queued_evidence_backfill_status(conn)
        budgets = [
            conn.execute("SELECT * FROM evidence_backfill_budget WHERE wallet = ?", (wallet,)).fetchone()
            for wallet in wallets
        ]

        assert plan.status == "ok"
        assert plan.jobs_enqueued == 2
        assert before["statuses"] == [{"status": "queued", "count": 2}]
        assert sum(summary.jobs_succeeded for summary in summaries) == 2
        assert sum(summary.activity_events_written for summary in summaries) == 160
        assert sum(summary.positions_written for summary in summaries) == 2
        assert after["statuses"] == [{"status": "done", "count": 2}]
        assert {budget["stage"] for budget in budgets} == {"light_done"}
    finally:
        conn.close()


def test_legacy_evidence_queue_deferral_does_not_consume_attempt(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        seed_evidence_backfill_budget(conn, wallet, source="test", priority=20)
        conn.commit()
        plan_queued_evidence_backfill(
            conn,
            light_limit=1,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
            now=int(time.time()),
        )

        summary = run_queued_evidence_backfill_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            sleep_seconds=0,
            client=RateLimitedEvidenceClient(),
        )
        job = conn.execute(
            "SELECT status, attempts, next_attempt_at FROM evidence_backfill_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.jobs_failed == 0
        assert job["status"] == "queued"
        assert job["attempts"] == 0
        assert job["next_attempt_at"] > int(time.time())
    finally:
        conn.close()


def test_legacy_worker_supersedes_unapproved_depth_without_network_calls(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    client = FakeEvidenceClient({wallet: []})
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        seed_evidence_backfill_budget(conn, wallet, source="test", priority=20)
        conn.execute(
            """
            UPDATE evidence_backfill_budget
            SET stage = 'medium_pending',
                target_depth = 1000,
                stop_reason = 'legacy_auto_promotion'
            WHERE wallet = ?
            """,
            (wallet,),
        )
        assert enqueue_evidence_backfill_job(
            conn,
            wallet=wallet,
            stage=EvidenceJobStage.MEDIUM_PENDING.value,
            target_depth=1_000,
            priority=20,
            shard=0,
            now=50_000,
        )
        conn.commit()

        summary = run_queued_evidence_backfill_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            sleep_seconds=0,
            client=client,
        )
        job = conn.execute(
            "SELECT status, last_error FROM evidence_backfill_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.jobs_attempted == 1
        assert summary.jobs_succeeded == 0
        assert summary.jobs_failed == 0
        assert client.activity_calls == []
        assert client.position_calls == []
        assert job["status"] == "superseded"
        assert job["last_error"] == "evidence_depth_not_approved"
    finally:
        conn.close()


def test_legacy_worker_supersedes_stale_policy_approval_without_network_calls(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    client = FakeEvidenceClient({wallet: []})
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        seed_evidence_backfill_budget(conn, wallet, source="test", priority=20)
        conn.execute(
            """
            UPDATE evidence_backfill_budget
            SET stage = 'medium_pending', target_depth = 1000,
                stop_reason = 'promotion_approved:medium_pending:old-policy:55001:0',
                evidence_json = ?
            WHERE wallet = ?
            """,
            (
                json.dumps(
                    {
                        "promotion": {
                            "approved": True,
                            "job_action": "medium_pending",
                            "policy_version": "old-policy",
                            "feature_updated_at": 55_001,
                            "activity_count": 0,
                            "materializer_version": MATERIALIZER_VERSION,
                        }
                    }
                ),
                wallet,
            ),
        )
        conn.execute(
            "INSERT INTO wallet_features(address, extra_json, updated_at) VALUES (?, ?, 55001)",
            (
                wallet,
                json.dumps(
                    {
                        "feature_materializer_version": MATERIALIZER_VERSION,
                        "feature_materializer_activity_count": 0,
                    }
                ),
            ),
        )
        assert enqueue_evidence_backfill_job(
            conn,
            wallet=wallet,
            stage=EvidenceJobStage.MEDIUM_PENDING.value,
            target_depth=1_000,
            priority=20,
            shard=0,
            now=55_001,
        )
        conn.commit()

        summary = run_queued_evidence_backfill_worker(
            conn,
            policy_version="current-policy",
            shard_index=0,
            shard_count=1,
            limit=1,
            sleep_seconds=0,
            client=client,
        )
        job = conn.execute(
            "SELECT status, last_error FROM evidence_backfill_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.jobs_attempted == 1
        assert summary.jobs_succeeded == 0
        assert summary.jobs_failed == 0
        assert client.activity_calls == []
        assert client.position_calls == []
        assert dict(job) == {
            "status": "superseded",
            "last_error": "evidence_depth_not_approved",
        }
    finally:
        conn.close()
