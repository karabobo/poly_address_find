import ast
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from pm_robot.clients.http import HttpClientError
from pm_robot.models import CandidateAddress, CandidateStage
from pm_robot.orchestration.copyability_evidence import JOB_TYPE as COPYABILITY_JOB_TYPE
from pm_robot.orchestration.evidence_backfill import summarize_wallet_evidence
from pm_robot.orchestration.wallet_pipeline import (
    JOB_TYPE as WALLET_EVIDENCE_JOB_TYPE,
    plan_wallet_pipeline_jobs,
    run_wallet_pipeline_worker,
    wallet_pipeline_job_status,
)
from pm_robot.pipeline_terms import (
    CANDIDATE_STAGES,
    COMPATIBLE_PIPELINE_STAGE_ORDER,
    DEFAULT_EVIDENCE_JOB_STAGE,
    EVIDENCE_JOB_STAGES,
    EVIDENCE_STATUSES,
    EVIDENCE_TIERS,
    PAPER_ELIGIBLE_CANDIDATE_STAGES,
    PAPER_READY_CANDIDATE_STAGES,
    PENDING_EVIDENCE_JOB_STAGES,
    PIPELINE_JOB_TYPES,
    PROVISIONAL_CANDIDATE_STAGES,
    PUBLISHABLE_CANDIDATE_STAGE,
    REVIEW_FUNNEL_CANDIDATE_STAGES,
    TERMINAL_EVIDENCE_JOB_STAGES,
    EvidenceJobStage,
    EvidenceStatus,
    EvidenceTier,
    PipelineJobType,
)
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    claim_pipeline_job,
    complete_pipeline_job,
    enqueue_pipeline_job,
    materialize_wallet_processing_state,
    persist_wallet_activity,
    pipeline_job_summary,
    renew_pipeline_job_lease,
    retry_pipeline_job,
    seed_evidence_backfill_budget,
    sync_wallet_processing_state,
    upsert_candidate,
    upsert_wallet_evidence_summary,
    wallet_pipeline_tier,
)


def test_wallet_pipeline_plan_cli_honors_active_job_waterline(tmp_path, monkeypatch, capsys):
    from pm_robot.cli import main

    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet="0x" + "1" * 40,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            priority=10,
            shard=0,
            now=10_000,
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "wallet-pipeline-plan",
            "--max-active-jobs",
            "1",
        ],
    )

    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["throttled"] is True
    assert payload["reason"] == "active_queue_waterline"
    assert payload["active_jobs"] == 1
    assert payload["max_active_jobs"] == 1


def test_legacy_evidence_backfill_plan_cli_does_not_require_wallet_waterline(
    tmp_path,
    monkeypatch,
    capsys,
):
    from pm_robot.cli import main

    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(tmp_path / "robot.sqlite"),
            "evidence-backfill-plan",
            "--light-limit",
            "0",
            "--medium-limit",
            "0",
            "--deep-limit",
            "0",
        ],
    )

    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"


def test_wallet_pipeline_jobs_cli_uses_runtime_scheduler_configuration(
    tmp_path,
    monkeypatch,
    capsys,
):
    from pm_robot.cli import main

    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()
    monkeypatch.setenv("PM_ROBOT_PIPELINE_PRIORITY_AGING_SECONDS", "900")
    monkeypatch.setenv("PM_ROBOT_PIPELINE_PLANNER_LIGHT_LIMIT", "9")
    monkeypatch.setenv("PM_ROBOT_PIPELINE_PLANNER_MEDIUM_LIMIT", "6")
    monkeypatch.setenv("PM_ROBOT_PIPELINE_PLANNER_DEEP_LIMIT", "3")
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "wallet-pipeline-jobs",
        ],
    )

    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    weights = {row["job_action"]: row["configured_weight"] for row in payload["stage_schedule"]}

    assert payload["priority_aging_seconds"] == 900
    assert weights == {
        EvidenceJobStage.LIGHT_PENDING.value: 9,
        EvidenceJobStage.MEDIUM_PENDING.value: 6,
        EvidenceJobStage.DEEP_PENDING.value: 3,
    }


def test_pipeline_job_completion_rejects_stale_lease_owner(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            shard=0,
            now=10_000,
        )
        conn.commit()
        job = claim_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            shard=0,
            worker_id="worker-current",
            lease_seconds=60,
            now=10_001,
        )
        assert job is not None

        assert complete_pipeline_job(
            conn,
            job_id=int(job["job_id"]),
            worker_id="worker-stale",
            output_data={"ok": False},
            now=10_010,
        ) is False
        row = conn.execute(
            "SELECT status, lease_owner, output_json FROM pipeline_jobs WHERE job_id = ?",
            (job["job_id"],),
        ).fetchone()
        assert dict(row) == {
            "status": "running",
            "lease_owner": "worker-current",
            "output_json": "{}",
        }
        assert retry_pipeline_job(
            conn,
            job_id=int(job["job_id"]),
            worker_id="worker-stale",
            error="stale worker failure",
            next_attempt_at=10_020,
            now=10_010,
        ) is False

        assert complete_pipeline_job(
            conn,
            job_id=int(job["job_id"]),
            worker_id="worker-current",
            output_data={"ok": True},
            now=10_011,
        ) is True
    finally:
        conn.close()


def test_pipeline_scheduler_deferral_does_not_consume_failure_attempt(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            shard=0,
            max_attempts=3,
            now=10_000,
        )
        conn.commit()
        job = claim_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            shard=0,
            worker_id="worker-rate-limit",
            lease_seconds=60,
            now=10_001,
        )
        assert job is not None
        assert job["attempts"] == 1

        assert retry_pipeline_job(
            conn,
            job_id=int(job["job_id"]),
            worker_id="worker-rate-limit",
            error="shared upstream cooldown",
            next_attempt_at=10_050,
            count_attempt=False,
            now=10_002,
        )
        row = conn.execute(
            "SELECT status, attempts, next_attempt_at FROM pipeline_jobs WHERE job_id = ?",
            (job["job_id"],),
        ).fetchone()
        assert dict(row) == {
            "status": "queued",
            "attempts": 0,
            "next_attempt_at": 10_050,
        }
    finally:
        conn.close()


def test_pipeline_claim_uses_numeric_priority_before_aging_threshold(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    low_priority_wallet = "0x" + "d" * 40
    high_priority_wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=low_priority_wallet,
            subject_key=EvidenceJobStage.DEEP_PENDING.value,
            tier=EvidenceTier.L2_MEDIUM.value,
            priority=100,
            shard=0,
            now=3_000,
        )
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=high_priority_wallet,
            subject_key=EvidenceJobStage.LIGHT_PENDING.value,
            tier=EvidenceTier.L0_DISCOVERED.value,
            priority=1,
            shard=0,
            now=3_500,
        )
        conn.commit()

        job = claim_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            shard=0,
            worker_id="worker-priority",
            lease_seconds=60,
            priority_aging_seconds=1_800,
            now=4_000,
        )

        assert job is not None
        assert job["wallet"] == high_priority_wallet
    finally:
        conn.close()


def test_pipeline_claim_promotes_old_job_after_aging_threshold(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    aged_wallet = "0x" + "f" * 40
    fresh_wallet = "0x" + "0" * 40
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=aged_wallet,
            subject_key=EvidenceJobStage.DEEP_PENDING.value,
            tier=EvidenceTier.L2_MEDIUM.value,
            priority=100,
            shard=0,
            now=1_000,
        )
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=fresh_wallet,
            subject_key=EvidenceJobStage.LIGHT_PENDING.value,
            tier=EvidenceTier.L0_DISCOVERED.value,
            priority=1,
            shard=0,
            now=3_500,
        )
        conn.commit()

        job = claim_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            shard=0,
            worker_id="worker-aging",
            lease_seconds=60,
            priority_aging_seconds=1_800,
            now=4_000,
        )

        assert job is not None
        assert job["wallet"] == aged_wallet
    finally:
        conn.close()


def test_wallet_pipeline_status_reports_stage_backlog_and_scheduler_cursor(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet="0x" + "1" * 40,
            subject_key=EvidenceJobStage.LIGHT_PENDING.value,
            tier=EvidenceTier.L0_DISCOVERED.value,
            priority=10,
            shard=0,
            now=1_000,
        )
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet="0x" + "2" * 40,
            subject_key=EvidenceJobStage.MEDIUM_PENDING.value,
            tier=EvidenceTier.L1_LIGHT.value,
            priority=20,
            shard=0,
            now=3_500,
        )
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet="0x" + "3" * 40,
            subject_key=EvidenceJobStage.DEEP_PENDING.value,
            tier=EvidenceTier.L2_MEDIUM.value,
            priority=30,
            shard=0,
            next_attempt_at=5_000,
            now=1_000,
        )
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet="0x" + "4" * 40,
            subject_key=EvidenceJobStage.DEEP_PENDING.value,
            tier=EvidenceTier.L2_MEDIUM.value,
            priority=40,
            shard=0,
            now=1_000,
        )
        conn.execute(
            "UPDATE pipeline_jobs SET attempts = max_attempts WHERE wallet = ?",
            ("0x" + "4" * 40,),
        )
        conn.execute(
            """
            UPDATE pipeline_jobs
            SET status = 'running', lease_owner = 'worker', lease_until = 5000
            WHERE subject_key = ?
            """,
            (EvidenceJobStage.MEDIUM_PENDING.value,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_scheduler_state(
                job_type, subject_key, current_weight, last_selected_at, updated_at
            ) VALUES (?, ?, -2, 3900, 3900)
            """,
            (WALLET_EVIDENCE_JOB_TYPE, EvidenceJobStage.LIGHT_PENDING.value),
        )
        conn.commit()

        status = wallet_pipeline_job_status(
            conn,
            now=4_000,
            priority_aging_seconds=1_800,
            stage_weights={
                EvidenceJobStage.LIGHT_PENDING.value: 3,
                EvidenceJobStage.MEDIUM_PENDING.value: 2,
                EvidenceJobStage.DEEP_PENDING.value: 1,
            },
        )
        stages = {row["job_action"]: row for row in status["stage_schedule"]}

        assert status["aged_queued_count"] == 1
        assert status["due_queued_count"] == 1
        assert status["deferred_queued_count"] == 1
        assert status["exhausted_queued_count"] == 1
        assert status["oldest_claimable_wait_seconds"] == 3_000
        assert stages[EvidenceJobStage.LIGHT_PENDING.value] == {
            "job_action": EvidenceJobStage.LIGHT_PENDING.value,
            "configured_weight": 3,
            "queued_count": 1,
            "due_queued_count": 1,
            "deferred_queued_count": 0,
            "exhausted_queued_count": 0,
            "running_count": 0,
            "active_count": 1,
            "active_per_weight": 0.3333,
            "aged_queued_count": 1,
            "oldest_claimable_queued_at": 1_000,
            "oldest_claimable_wait_seconds": 3_000,
            "current_weight": -2,
            "last_selected_at": 3_900,
            "scheduler_updated_at": 3_900,
        }
        assert stages[EvidenceJobStage.MEDIUM_PENDING.value]["running_count"] == 1
        assert stages[EvidenceJobStage.MEDIUM_PENDING.value]["active_per_weight"] == 0.5
        assert stages[EvidenceJobStage.DEEP_PENDING.value]["queued_count"] == 2
        assert stages[EvidenceJobStage.DEEP_PENDING.value]["due_queued_count"] == 0
        assert stages[EvidenceJobStage.DEEP_PENDING.value]["deferred_queued_count"] == 1
        assert stages[EvidenceJobStage.DEEP_PENDING.value]["exhausted_queued_count"] == 1
        assert stages[EvidenceJobStage.DEEP_PENDING.value]["aged_queued_count"] == 0
        assert stages[EvidenceJobStage.DEEP_PENDING.value]["oldest_claimable_wait_seconds"] == 0
        index_names = {row[1] for row in conn.execute("PRAGMA index_list('pipeline_jobs')")}
        assert "idx_pipeline_jobs_type_action_status_updated" in index_names
    finally:
        conn.close()


def test_concurrent_pipeline_claims_assign_one_job_once(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet="0x" + "3" * 40,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            shard=0,
            now=10_000,
        )
        conn.commit()
    finally:
        conn.close()

    barrier = Barrier(2)

    def claim(worker_id):
        worker_conn = connect(db_path)
        try:
            barrier.wait()
            return claim_pipeline_job(
                worker_conn,
                job_type=WALLET_EVIDENCE_JOB_TYPE,
                shard=0,
                worker_id=worker_id,
                lease_seconds=60,
                now=10_001,
            )
        finally:
            worker_conn.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        jobs = list(executor.map(claim, ("worker-a", "worker-b")))

    claimed = [job for job in jobs if job is not None]
    assert len(claimed) == 1
    assert claimed[0]["attempts"] == 1

    conn = connect(db_path)
    try:
        row = conn.execute(
            "SELECT status, attempts, lease_owner FROM pipeline_jobs"
        ).fetchone()
    finally:
        conn.close()
    assert row["status"] == "running"
    assert row["attempts"] == 1
    assert row["lease_owner"] in {"worker-a", "worker-b"}


def _event(wallet: str, idx: int, *, market: str) -> dict:
    return {
        "proxyWallet": wallet,
        "timestamp": 10_000 + idx,
        "conditionId": f"condition-{idx % 30}",
        "eventSlug": f"event-{idx % 30}",
        "slug": market,
        "asset": f"asset-{idx % 30}",
        "outcome": "YES",
        "type": "TRADE",
        "side": "BUY" if idx % 4 else "SELL",
        "price": 0.55,
        "size": 20,
        "usdcSize": 11,
        "transactionHash": f"0x{idx:064x}",
    }


class FakePipelineClient:
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


class RateLimitedPipelineClient:
    def __init__(self):
        self.activity_calls = 0

    def activity(self, wallet, *, limit, offset):
        self.activity_calls += 1
        raise HttpClientError(
            "shared cooldown",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=60.0,
        )

    def positions(self, wallet, *, size_threshold=0.0):
        raise AssertionError("positions should not run after activity cooldown")


def _seed_pending_state(
    conn,
    wallet: str,
    *,
    evidence_tier: str,
    evidence_status: str,
    evidence_job_stage: str,
    priority: int = 50,
    now: int = 30_000,
) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, distinct_markets,
            non_fast_trade_count, updated_at
        ) VALUES (?, ?, ?, 0, 0.0, ?, '', ?, 0, 0, 0, 0, ?)
        """,
        (wallet, evidence_tier, evidence_status, priority, evidence_job_stage, now),
    )


def _seed_light_pending_state(conn, wallet: str, *, priority: int = 50, now: int = 30_000) -> None:
    _seed_pending_state(
        conn,
        wallet,
        evidence_tier=EvidenceTier.L0_DISCOVERED.value,
        evidence_status=EvidenceStatus.NEEDS_LIGHT.value,
        evidence_job_stage=EvidenceJobStage.LIGHT_PENDING.value,
        priority=priority,
        now=now,
    )


def test_wallet_pipeline_stops_batch_and_preserves_attempts_on_shared_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + "a" * 40, "0x" + "b" * 40]
    client = RateLimitedPipelineClient()
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_light_pending_state(conn, wallet)
        conn.commit()
        plan = plan_wallet_pipeline_jobs(
            conn,
            light_limit=2,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
            now=40_000,
        )

        summary = run_wallet_pipeline_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=2,
            sleep_seconds=0,
            client=client,
        )
        rows = conn.execute(
            "SELECT status, attempts FROM pipeline_jobs ORDER BY job_id"
        ).fetchall()

        assert plan.jobs_enqueued == 2
        assert summary.jobs_attempted == 1
        assert summary.jobs_failed == 0
        assert client.activity_calls == 1
        assert [dict(row) for row in rows] == [
            {"status": "queued", "attempts": 0},
            {"status": "queued", "attempts": 0},
        ]
    finally:
        conn.close()


def test_wallet_pipeline_does_not_claim_job_during_existing_shared_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "c" * 40
    try:
        run_migrations(conn)
        _seed_light_pending_state(conn, wallet)
        conn.execute(
            """
            INSERT INTO api_rate_limit_state(
                scope, capacity, window_seconds, cooldown_until, updated_at
            ) VALUES ('data:*', 50, 10, ?, ?)
            """,
            (time.time() + 60, time.time()),
        )
        conn.commit()
        plan_wallet_pipeline_jobs(
            conn,
            light_limit=1,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
        )

        summary = run_wallet_pipeline_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            sleep_seconds=0,
            client=RateLimitedPipelineClient(),
        )
        job = conn.execute(
            "SELECT status, attempts, lease_owner FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.status == "partial"
        assert summary.jobs_attempted == 0
        assert summary.jobs_failed == 0
        assert "shared upstream cooldown active" in summary.error
        assert dict(job) == {"status": "queued", "attempts": 0, "lease_owner": None}
    finally:
        conn.close()


def test_wallet_pipeline_state_stale_only_materializes_changed_wallets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    changed_wallet = "0x" + "d" * 40
    unchanged_wallet = "0x" + "e" * 40
    try:
        run_migrations(conn)
        for wallet in (changed_wallet, unchanged_wallet):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.commit()
        materialize_wallet_processing_state(conn)
        conn.execute("UPDATE candidate_wallets SET updated_at = 1000")
        conn.execute("UPDATE wallet_processing_state SET updated_at = 1000")
        conn.execute(
            "UPDATE candidate_wallets SET updated_at = 1001 WHERE address = ?",
            (changed_wallet,),
        )
        conn.commit()

        summary = materialize_wallet_processing_state(conn, stale_only=True)

        assert summary["wallets_seen"] == 1
        assert summary["wallets_materialized"] == 1
        changed = conn.execute(
            "SELECT updated_at FROM wallet_processing_state WHERE wallet = ?",
            (changed_wallet,),
        ).fetchone()
        unchanged = conn.execute(
            "SELECT updated_at FROM wallet_processing_state WHERE wallet = ?",
            (unchanged_wallet,),
        ).fetchone()
        assert changed["updated_at"] > 1001
        assert unchanged["updated_at"] == 1000

        persist_wallet_activity(
            conn,
            changed_wallet,
            [_event(changed_wallet, 1, market="same-second-watermark")],
            ingested_at=int(changed["updated_at"]),
        )
        same_second = materialize_wallet_processing_state(conn, stale_only=True)
        assert same_second["wallets_materialized"] == 1
    finally:
        conn.close()


def test_wallet_pipeline_tier_thresholds():
    assert wallet_pipeline_tier(0, 0, 0, 0.0) == "l0_discovered"
    assert wallet_pipeline_tier(24, 2, 4, 0.0) == "l1_light"
    assert wallet_pipeline_tier(240, 6, 220, 0.0) == "l2_medium"
    assert wallet_pipeline_tier(1_000, 12, 100, 0.1) == "l3_deep"
    assert wallet_pipeline_tier(240, 6, 20, 0.9) == "l1_light"


def test_pipeline_terms_are_canonical_and_compatible():
    assert EVIDENCE_TIERS == (
        EvidenceTier.L0_DISCOVERED.value,
        EvidenceTier.L1_LIGHT.value,
        EvidenceTier.L2_MEDIUM.value,
        EvidenceTier.L3_DEEP.value,
    )
    assert all("l4" not in value for value in EVIDENCE_TIERS)
    assert EVIDENCE_JOB_STAGES == (
        EvidenceJobStage.LIGHT_PENDING.value,
        EvidenceJobStage.LIGHT_DONE.value,
        EvidenceJobStage.MEDIUM_PENDING.value,
        EvidenceJobStage.MEDIUM_DONE.value,
        EvidenceJobStage.DEEP_PENDING.value,
        EvidenceJobStage.DEEP_DONE.value,
    )
    assert PENDING_EVIDENCE_JOB_STAGES == (
        EvidenceJobStage.LIGHT_PENDING.value,
        EvidenceJobStage.MEDIUM_PENDING.value,
        EvidenceJobStage.DEEP_PENDING.value,
    )
    assert TERMINAL_EVIDENCE_JOB_STAGES == (
        EvidenceJobStage.LIGHT_DONE.value,
        EvidenceJobStage.MEDIUM_DONE.value,
        EvidenceJobStage.DEEP_DONE.value,
    )
    assert DEFAULT_EVIDENCE_JOB_STAGE == EvidenceJobStage.LIGHT_PENDING.value
    assert PIPELINE_JOB_TYPES == (
        PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
        PipelineJobType.COPYABILITY_EVIDENCE.value,
    )
    assert EVIDENCE_STATUSES == (
        EvidenceStatus.PENDING.value,
        EvidenceStatus.NEEDS_LIGHT.value,
        EvidenceStatus.NEEDS_MEDIUM.value,
        EvidenceStatus.NEEDS_DEEP.value,
        EvidenceStatus.QUEUED.value,
        EvidenceStatus.SUMMARY_READY.value,
        EvidenceStatus.PAUSED.value,
    )

    assert WALLET_EVIDENCE_JOB_TYPE == PipelineJobType.WALLET_EVIDENCE_BACKFILL.value
    assert COPYABILITY_JOB_TYPE == PipelineJobType.COPYABILITY_EVIDENCE.value
    assert CandidateStage.NEEDS_REVIEW.value in CANDIDATE_STAGES
    assert CandidateStage.PAPER_CANDIDATE.value in CANDIDATE_STAGES
    assert REVIEW_FUNNEL_CANDIDATE_STAGES == (
        CandidateStage.NEEDS_REVIEW.value,
        CandidateStage.PAPER_CANDIDATE.value,
        CandidateStage.PAPER_APPROVED.value,
        CandidateStage.LIVE_ELIGIBLE.value,
    )
    assert PAPER_ELIGIBLE_CANDIDATE_STAGES == (
        CandidateStage.PAPER_CANDIDATE.value,
        CandidateStage.PAPER_APPROVED.value,
        CandidateStage.LIVE_ELIGIBLE.value,
    )
    assert PAPER_READY_CANDIDATE_STAGES == PAPER_ELIGIBLE_CANDIDATE_STAGES
    assert PROVISIONAL_CANDIDATE_STAGES == (CandidateStage.NEEDS_REVIEW.value,)
    assert PUBLISHABLE_CANDIDATE_STAGE == CandidateStage.LIVE_ELIGIBLE.value
    assert COMPATIBLE_PIPELINE_STAGE_ORDER == (
        CandidateStage.LIVE_ELIGIBLE.value,
        CandidateStage.PAPER_APPROVED.value,
        CandidateStage.PAPER_CANDIDATE.value,
        CandidateStage.NEEDS_REVIEW.value,
    )


def test_pipeline_job_enqueue_calls_have_only_canonical_planner_owners():
    owners: set[str] = set()
    raw_sql_owners: set[str] = set()
    orchestration_dir = Path(__file__).resolve().parents[1] / "src/pm_robot/orchestration"
    for path in orchestration_dir.glob("*.py"):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        if any(
            isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Name) and node.func.id == "enqueue_pipeline_job")
                or (isinstance(node.func, ast.Attribute) and node.func.attr == "enqueue_pipeline_job")
            )
            for node in ast.walk(tree)
        ):
            owners.add(path.name)
        if "insert into pipeline_jobs" in " ".join(source.lower().split()):
            raw_sql_owners.add(path.name)

    assert owners == {"copyability_evidence.py", "wallet_pipeline.py"}
    assert raw_sql_owners == set()


def test_wallet_evidence_summary_and_state_are_idempotent(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        seed_evidence_backfill_budget(conn, wallet, source="polymarket_trades_global", priority=12)
        persist_wallet_activity(
            conn,
            wallet,
            [_event(wallet, idx, market=f"politics-market-{idx % 6}") for idx in range(240)],
            ingested_at=20_000,
        )
        evidence = summarize_wallet_evidence(conn, wallet)

        for _ in range(2):
            upsert_wallet_evidence_summary(
                conn,
                wallet,
                evidence,
                source_artifacts=[f"sqlite://wallet_activity/{wallet}"],
                computed_at=30_000,
            )
            state = sync_wallet_processing_state(
                conn,
                wallet,
                evidence,
                source="test",
                now=30_000,
            )
            conn.commit()

        summary = conn.execute(
            "SELECT * FROM wallet_evidence_summary WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        state_row = conn.execute(
            "SELECT * FROM wallet_processing_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        artifacts = conn.execute("SELECT * FROM data_artifacts").fetchall()
        copyability = json.loads(summary["copyability_json"])

        assert evidence["activity_count"] == 240
        assert state["discovery_tier"] == "l2_medium"
        assert summary["distinct_markets"] == 6
        assert copyability["usable_for_copyability"] is True
        assert "usable_for_paper" not in copyability
        assert state_row["priority"] == 12
        assert state_row["evidence_status"] == "needs_deep"
        assert state_row["next_action"] == "deep_pending"
        assert len(artifacts) == 1
    finally:
        conn.close()


def test_wallet_pipeline_plans_and_runs_v2_backfill_job(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        materialize_wallet_processing_state(conn, limit=10, source="test_seed")

        plan = plan_wallet_pipeline_jobs(
            conn,
            light_limit=5,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
            now=40_000,
        )
        before = wallet_pipeline_job_status(conn)
        client = FakePipelineClient(
            {wallet: [_event(wallet, idx, market=f"politics-market-{idx % 6}") for idx in range(80)]},
            {wallet: [{"asset": "asset-open", "size": 10, "marketSlug": "politics-market-1"}]},
        )

        summary = run_wallet_pipeline_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=2,
            page_limit=40,
            sleep_seconds=0,
            client=client,
        )
        after = wallet_pipeline_job_status(conn)
        job_row = conn.execute(
            "SELECT * FROM pipeline_jobs WHERE job_type = ? AND wallet = ?",
            (WALLET_EVIDENCE_JOB_TYPE, wallet),
        ).fetchone()
        budget = conn.execute("SELECT * FROM evidence_backfill_budget WHERE wallet = ?", (wallet,)).fetchone()
        state = conn.execute("SELECT * FROM wallet_processing_state WHERE wallet = ?", (wallet,)).fetchone()
        evidence = conn.execute("SELECT * FROM wallet_evidence_summary WHERE wallet = ?", (wallet,)).fetchone()

        assert plan.status == "ok"
        assert plan.jobs_enqueued == 1
        assert before["statuses"] == [
            {"job_type": "wallet_evidence_backfill", "status": "queued", "count": 1}
        ]
        assert job_row["tier"] == "l0_discovered"
        assert job_row["subject_key"] == DEFAULT_EVIDENCE_JOB_STAGE
        assert summary.status == "ok"
        assert summary.jobs_succeeded == 1
        assert summary.activity_events_written == 80
        assert summary.positions_written == 1
        assert budget["stage"] == "medium_pending"
        assert budget["target_depth"] == 1000
        assert state["next_action"] == "medium_pending"
        assert evidence["activity_count"] == 80
        assert after["statuses"] == [
            {"job_type": "wallet_evidence_backfill", "status": "done", "count": 1}
        ]
    finally:
        conn.close()


def test_wallet_pipeline_rolls_back_partial_evidence_before_retry(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        materialize_wallet_processing_state(conn, limit=10, source="test_seed")
        plan_wallet_pipeline_jobs(
            conn,
            light_limit=1,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
            now=40_000,
        )
        client = FakePipelineClient(
            {wallet: [_event(wallet, idx, market="rollback-market") for idx in range(5)]},
            {wallet: [{"asset": "asset-open", "size": 10, "marketSlug": "rollback-market"}]},
        )

        def fail_positions(*args, **kwargs):
            raise RuntimeError("position persistence failed")

        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_pipeline.persist_wallet_positions",
            fail_positions,
        )
        summary = run_wallet_pipeline_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            page_limit=20,
            sleep_seconds=0,
            client=client,
            worker_id="rollback-worker",
        )
        activity_count = conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0]
        job = conn.execute(
            "SELECT status, lease_owner, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.status == "partial"
        assert summary.jobs_failed == 1
        assert activity_count == 0
        assert job["status"] == "queued"
        assert job["lease_owner"] is None
        assert "position persistence failed" in job["last_error"]
    finally:
        conn.close()


def test_wallet_pipeline_rolls_back_when_completion_loses_lease(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        materialize_wallet_processing_state(conn, limit=10, source="test_seed")
        plan_wallet_pipeline_jobs(
            conn,
            light_limit=1,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
            now=40_000,
        )
        client = FakePipelineClient(
            {wallet: [_event(wallet, idx, market="lease-loss-market") for idx in range(5)]},
            {wallet: [{"asset": "asset-open", "size": 10, "marketSlug": "lease-loss-market"}]},
        )
        monkeypatch.setattr(
            "pm_robot.orchestration.wallet_pipeline.complete_pipeline_job",
            lambda *args, **kwargs: False,
        )

        summary = run_wallet_pipeline_worker(
            conn,
            shard_index=0,
            shard_count=1,
            limit=1,
            page_limit=20,
            sleep_seconds=0,
            client=client,
            worker_id="lease-loss-worker",
        )
        activity_count = conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0]
        job = conn.execute(
            "SELECT status, lease_owner, output_json FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.status == "partial"
        assert summary.jobs_failed == 1
        assert "lease was lost" in summary.error
        assert activity_count == 0
        assert job["status"] == "running"
        assert job["lease_owner"] == "lease-loss-worker"
        assert json.loads(job["output_json"]) == {}
    finally:
        conn.close()


def test_pipeline_job_dedupe_scope_includes_tier(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            priority=50,
            shard=0,
            input_data={"attempt": 1},
            now=10_000,
        )
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            priority=20,
            shard=1,
            input_data={"attempt": 2},
            now=10_010,
        )
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L2_MEDIUM.value,
            priority=30,
            shard=2,
            input_data={"attempt": 3},
            now=10_020,
        )
        rows = conn.execute(
            """
            SELECT tier, priority, shard, input_json
            FROM pipeline_jobs
            WHERE job_type = ? AND wallet = ? AND subject_key = ?
            ORDER BY tier
            """,
            (WALLET_EVIDENCE_JOB_TYPE, wallet, DEFAULT_EVIDENCE_JOB_STAGE),
        ).fetchall()

        assert len(rows) == 2
        assert rows[0]["tier"] == EvidenceTier.L1_LIGHT.value
        assert rows[0]["priority"] == 20
        assert rows[0]["shard"] == 1
        assert json.loads(rows[0]["input_json"]) == {"attempt": 2}
        assert rows[1]["tier"] == EvidenceTier.L2_MEDIUM.value
        assert rows[1]["priority"] == 30
    finally:
        conn.close()


def test_enqueue_pipeline_job_does_not_reopen_completed_job(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            priority=20,
            shard=0,
            input_data={"attempt": 1},
            now=10_000,
        )
        conn.commit()

        job = claim_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            shard=0,
            worker_id="worker-a",
            lease_seconds=60,
            now=10_001,
        )
        assert job is not None
        complete_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker-a",
            output_data={"ok": True},
            now=10_010,
        )
        conn.commit()

        assert not enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            priority=5,
            shard=0,
            input_data={"attempt": 2},
            now=10_020,
        )
        row = conn.execute(
            "SELECT status, priority, input_json, updated_at FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert row["status"] == "done"
        assert row["priority"] == 20
        assert json.loads(row["input_json"]) == {"attempt": 1}
        assert row["updated_at"] == 10_010
    finally:
        conn.close()


def test_enqueue_pipeline_job_reopens_failed_scope_only_after_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L0_DISCOVERED.value,
            priority=20,
            shard=0,
            now=1_000,
        )
        conn.execute(
            """
            UPDATE pipeline_jobs
            SET status = 'failed', attempts = max_attempts,
                next_attempt_at = 10000, last_error = 'persistent upstream failure'
            WHERE wallet = ?
            """,
            (wallet,),
        )
        conn.commit()

        assert not enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L0_DISCOVERED.value,
            priority=10,
            shard=0,
            now=9_999,
        )
        deferred = conn.execute(
            "SELECT status, attempts, next_attempt_at, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert dict(deferred) == {
            "status": "failed",
            "attempts": 3,
            "next_attempt_at": 10_000,
            "last_error": "persistent upstream failure",
        }

        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L0_DISCOVERED.value,
            priority=10,
            shard=0,
            now=10_000,
        )
        reopened = conn.execute(
            "SELECT status, attempts, next_attempt_at, priority, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert dict(reopened) == {
            "status": "queued",
            "attempts": 0,
            "next_attempt_at": 0,
            "priority": 10,
            "last_error": "persistent upstream failure",
        }
    finally:
        conn.close()


def test_wallet_pipeline_planner_skips_completed_exact_scope_jobs(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    completed_wallet = "0x" + "8" * 40
    fresh_wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        for wallet in (completed_wallet, fresh_wallet):
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, evidence_depth,
                    evidence_confidence, priority, current_stage, next_action,
                    next_action_at, activity_count, distinct_markets,
                    non_fast_trade_count, updated_at
                ) VALUES (?, 'l1_light', 'needs_light', 120, 0.7, ?,
                          'light_done', 'light_pending', 0, ?, 1, ?, ?)
                """,
                (wallet, 1 if wallet == completed_wallet else 50, 8 if wallet == completed_wallet else 6, 8, 30_000),
            )
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=completed_wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            priority=1,
            shard=0,
            input_data={"attempt": 1},
            now=30_000,
        )
        conn.commit()
        job = claim_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            shard=0,
            worker_id="worker-a",
            lease_seconds=60,
            now=30_001,
        )
        assert job is not None
        complete_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker-a",
            output_data={"ok": True},
            now=30_010,
        )
        conn.commit()

        plan = plan_wallet_pipeline_jobs(
            conn,
            light_limit=1,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
            now=40_000,
        )
        queued = conn.execute(
            "SELECT wallet, status FROM pipeline_jobs WHERE status = 'queued'"
        ).fetchall()

        assert plan.targets_seen == 1
        assert plan.jobs_enqueued == 1
        assert [row["wallet"] for row in queued] == [fresh_wallet]
    finally:
        conn.close()


def test_wallet_pipeline_planner_throttles_when_active_queue_is_full(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    active_wallet = "0x" + "b" * 40
    pending_wallet = "0x" + "c" * 40
    try:
        run_migrations(conn)
        _seed_light_pending_state(conn, pending_wallet, priority=10)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=active_wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L0_DISCOVERED.value,
            priority=1,
            shard=0,
            input_data={"stage": DEFAULT_EVIDENCE_JOB_STAGE},
            now=30_000,
        )
        conn.commit()

        plan = plan_wallet_pipeline_jobs(
            conn,
            light_limit=5,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
            max_active_jobs=1,
            now=40_000,
        )
        queued = conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE job_type = ? AND status = 'queued' ORDER BY wallet",
            (WALLET_EVIDENCE_JOB_TYPE,),
        ).fetchall()

        assert plan.status == "ok"
        assert plan.throttled is True
        assert plan.reason == "active_queue_waterline"
        assert plan.active_jobs == 1
        assert plan.max_active_jobs == 1
        assert plan.targets_seen == 0
        assert plan.jobs_enqueued == 0
        assert [row["wallet"] for row in queued] == [active_wallet]
    finally:
        conn.close()


def test_wallet_pipeline_planner_truncates_targets_to_queue_capacity(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    active_wallet = "0x" + "d" * 40
    first_wallet = "0x" + "e" * 40
    second_wallet = "0x" + "f" * 40
    try:
        run_migrations(conn)
        _seed_light_pending_state(conn, first_wallet, priority=10)
        _seed_light_pending_state(conn, second_wallet, priority=20)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=active_wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L0_DISCOVERED.value,
            priority=1,
            shard=0,
            input_data={"stage": DEFAULT_EVIDENCE_JOB_STAGE},
            now=30_000,
        )
        conn.commit()

        plan = plan_wallet_pipeline_jobs(
            conn,
            light_limit=5,
            medium_limit=0,
            deep_limit=0,
            shard_count=1,
            max_active_jobs=2,
            now=40_000,
        )
        queued = conn.execute(
            "SELECT wallet FROM pipeline_jobs WHERE job_type = ? AND status = 'queued' ORDER BY priority, wallet",
            (WALLET_EVIDENCE_JOB_TYPE,),
        ).fetchall()

        assert plan.status == "ok"
        assert plan.throttled is False
        assert plan.active_jobs == 1
        assert plan.max_active_jobs == 2
        assert plan.targets_seen == 1
        assert plan.jobs_enqueued == 1
        assert [row["wallet"] for row in queued] == [active_wallet, first_wallet]
    finally:
        conn.close()


def test_wallet_pipeline_planner_preserves_deep_capacity_at_high_waterline(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    active_light = "0x" + "1" * 40
    active_medium = "0x" + "2" * 40
    target_specs = (
        (
            "0x" + "3" * 40,
            EvidenceTier.L0_DISCOVERED.value,
            EvidenceStatus.NEEDS_LIGHT.value,
            EvidenceJobStage.LIGHT_PENDING.value,
        ),
        (
            "0x" + "4" * 40,
            EvidenceTier.L1_LIGHT.value,
            EvidenceStatus.NEEDS_MEDIUM.value,
            EvidenceJobStage.MEDIUM_PENDING.value,
        ),
        (
            "0x" + "5" * 40,
            EvidenceTier.L2_MEDIUM.value,
            EvidenceStatus.NEEDS_DEEP.value,
            EvidenceJobStage.DEEP_PENDING.value,
        ),
    )
    try:
        run_migrations(conn)
        for wallet, evidence_tier, evidence_status, evidence_job_stage in target_specs:
            _seed_pending_state(
                conn,
                wallet,
                evidence_tier=evidence_tier,
                evidence_status=evidence_status,
                evidence_job_stage=evidence_job_stage,
                priority=10,
            )
        for wallet, evidence_job_stage in (
            (active_light, EvidenceJobStage.LIGHT_PENDING.value),
            (active_medium, EvidenceJobStage.MEDIUM_PENDING.value),
        ):
            assert enqueue_pipeline_job(
                conn,
                job_type=WALLET_EVIDENCE_JOB_TYPE,
                wallet=wallet,
                subject_key=evidence_job_stage,
                tier="active",
                priority=1,
                shard=0,
                input_data={"stage": evidence_job_stage},
                now=30_000,
            )
        conn.commit()

        plan = plan_wallet_pipeline_jobs(
            conn,
            light_limit=1,
            medium_limit=1,
            deep_limit=1,
            shard_count=1,
            max_active_jobs=3,
            now=40_000,
        )
        new_job = conn.execute(
            """
            SELECT wallet, subject_key
            FROM pipeline_jobs
            WHERE tier != 'active'
            """
        ).fetchone()

        assert plan.active_jobs == 2
        assert plan.targets_seen == 1
        assert plan.jobs_enqueued == 1
        assert dict(new_job) == {
            "wallet": target_specs[2][0],
            "subject_key": EvidenceJobStage.DEEP_PENDING.value,
        }
    finally:
        conn.close()


def test_wallet_pipeline_planner_persists_fairness_across_drained_cycles(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    light_wallets = ["0x" + value * 40 for value in ("6", "7", "8")]
    medium_wallet = "0x" + "9" * 40
    deep_wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        for wallet in light_wallets:
            _seed_light_pending_state(conn, wallet, priority=1, now=10_000)
        _seed_pending_state(
            conn,
            medium_wallet,
            evidence_tier=EvidenceTier.L1_LIGHT.value,
            evidence_status=EvidenceStatus.NEEDS_MEDIUM.value,
            evidence_job_stage=EvidenceJobStage.MEDIUM_PENDING.value,
            priority=100,
            now=20_000,
        )
        _seed_pending_state(
            conn,
            deep_wallet,
            evidence_tier=EvidenceTier.L2_MEDIUM.value,
            evidence_status=EvidenceStatus.NEEDS_DEEP.value,
            evidence_job_stage=EvidenceJobStage.DEEP_PENDING.value,
            priority=100,
            now=20_000,
        )
        conn.commit()

        planned_stages = []
        for cycle in range(3):
            plan = plan_wallet_pipeline_jobs(
                conn,
                light_limit=1,
                medium_limit=1,
                deep_limit=1,
                shard_count=1,
                max_active_jobs=1,
                now=40_000 + cycle,
            )
            assert plan.jobs_enqueued == 1
            job = conn.execute(
                """
                SELECT job_id, subject_key
                FROM pipeline_jobs
                WHERE status = 'queued'
                """
            ).fetchone()
            planned_stages.append(str(job["subject_key"]))
            conn.execute(
                "UPDATE pipeline_jobs SET status = 'done' WHERE job_id = ?",
                (job["job_id"],),
            )
            conn.commit()

        assert planned_stages == [
            EvidenceJobStage.LIGHT_PENDING.value,
            EvidenceJobStage.MEDIUM_PENDING.value,
            EvidenceJobStage.DEEP_PENDING.value,
        ]
    finally:
        conn.close()


def test_concurrent_wallet_planners_do_not_oversubscribe_queue_capacity(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_light_pending_state(conn, "0x" + "b" * 40, priority=10)
        _seed_light_pending_state(conn, "0x" + "c" * 40, priority=20)
        conn.commit()
    finally:
        conn.close()

    barrier = Barrier(2)

    def plan_one_slot():
        worker_conn = connect(db_path)
        try:
            barrier.wait()
            return plan_wallet_pipeline_jobs(
                worker_conn,
                light_limit=2,
                medium_limit=0,
                deep_limit=0,
                shard_count=1,
                max_active_jobs=1,
                now=40_000,
            )
        finally:
            worker_conn.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        summaries = list(executor.map(lambda _index: plan_one_slot(), range(2)))

    conn = connect(db_path)
    try:
        active_jobs = conn.execute(
            """
            SELECT COUNT(*)
            FROM pipeline_jobs
            WHERE job_type = ? AND status IN ('queued', 'running')
            """,
            (WALLET_EVIDENCE_JOB_TYPE,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert sum(summary.jobs_enqueued for summary in summaries) == 1
    assert sum(1 for summary in summaries if summary.throttled) == 1
    assert active_jobs == 1


def test_wallet_processing_state_is_evidence_tier_source_of_truth(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        persist_wallet_activity(
            conn,
            wallet,
            [_event(wallet, idx, market=f"macro-market-{idx % 5}") for idx in range(240)],
            ingested_at=20_000,
        )
        result = materialize_wallet_processing_state(conn, limit=10, source="test_materialize")
        assert result["wallets_materialized"] == 1

        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier="manual_scope_not_evidence_tier",
            priority=10,
            shard=0,
            input_data={"stage": DEFAULT_EVIDENCE_JOB_STAGE},
            now=30_000,
        )
        state_row = conn.execute(
            "SELECT discovery_tier FROM wallet_processing_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        job_row = conn.execute(
            "SELECT tier FROM pipeline_jobs WHERE wallet = ? AND job_type = ?",
            (wallet, WALLET_EVIDENCE_JOB_TYPE),
        ).fetchone()

        assert state_row["discovery_tier"] == EvidenceTier.L2_MEDIUM.value
        assert job_row["tier"] == "manual_scope_not_evidence_tier"
    finally:
        conn.close()


def test_wallet_processing_state_does_not_keep_stale_pending_stage(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        seed_evidence_backfill_budget(conn, wallet, source="polymarket_trades_global", priority=10)
        conn.execute(
            """
            UPDATE evidence_backfill_budget
            SET stage = 'medium_pending', target_depth = 1000, current_depth = 1000
            WHERE wallet = ?
            """,
            (wallet,),
        )
        persist_wallet_activity(
            conn,
            wallet,
            [_event(wallet, idx, market=f"deep-market-{idx % 12}") for idx in range(1_050)],
            ingested_at=20_000,
        )
        conn.commit()

        result = materialize_wallet_processing_state(conn, limit=10, source="test_materialize")
        state_row = conn.execute(
            "SELECT discovery_tier, evidence_status, next_action FROM wallet_processing_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert result["wallets_materialized"] == 1
        assert state_row["discovery_tier"] == EvidenceTier.L3_DEEP.value
        assert state_row["evidence_status"] == "summary_ready"
        assert state_row["next_action"] == "score_wallet"
    finally:
        conn.close()


def test_materialize_wallet_processing_state_from_existing_activity(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
        persist_wallet_activity(
            conn,
            wallet,
            [_event(wallet, idx, market=f"news-market-{idx % 4}") for idx in range(80)],
            ingested_at=20_000,
        )

        result = materialize_wallet_processing_state(conn, limit=10, source="test_materialize")
        state_row = conn.execute(
            "SELECT * FROM wallet_processing_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert result["wallets_seen"] == 1
        assert result["wallets_materialized"] == 1
        assert state_row["discovery_tier"] == "l1_light"
        assert state_row["next_action"] == "medium_pending"
    finally:
        conn.close()


def test_pipeline_jobs_claim_complete_and_summarize(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    legacy_job_type = "wallet_evidence_l1"
    try:
        run_migrations(conn)
        assert legacy_job_type not in PIPELINE_JOB_TYPES
        assert enqueue_pipeline_job(
            conn,
            job_type=legacy_job_type,
            wallet=wallet,
            subject_key="activity",
            tier="l1_light",
            priority=5,
            shard=1,
            input_data={"target_depth": 200},
            now=10_000,
        )
        conn.commit()

        job = claim_pipeline_job(
            conn,
            job_type=legacy_job_type,
            shard=1,
            worker_id="worker-a",
            lease_seconds=60,
            now=10_001,
        )
        assert job is not None
        assert job["wallet"] == wallet
        assert job["attempts"] == 1

        complete_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker-a",
            output_data={"ok": True},
            now=10_010,
        )
        conn.commit()
        summary = pipeline_job_summary(conn, job_type=legacy_job_type)

        assert summary["statuses"] == [
            {"job_type": legacy_job_type, "status": "done", "count": 1}
        ]
    finally:
        conn.close()


def test_running_pipeline_job_lease_can_only_be_renewed_by_owner(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type="copyability_evidence",
            wallet=wallet,
            subject_key="copyability",
            tier="copyability",
            priority=5,
            shard=0,
            input_data={},
            now=20_000,
        )
        conn.commit()

        job = claim_pipeline_job(
            conn,
            job_type="copyability_evidence",
            shard=0,
            worker_id="worker-a",
            lease_seconds=60,
            now=20_001,
        )
        assert job is not None

        assert not renew_pipeline_job_lease(
            conn,
            job_id=int(job["job_id"]),
            worker_id="worker-b",
            lease_seconds=600,
            now=20_010,
        )
        assert renew_pipeline_job_lease(
            conn,
            job_id=int(job["job_id"]),
            worker_id="worker-a",
            lease_seconds=600,
            now=20_010,
        )
        conn.commit()

        row = conn.execute(
            "SELECT lease_owner, lease_until, updated_at FROM pipeline_jobs WHERE job_id = ?",
            (job["job_id"],),
        ).fetchone()
        assert row["lease_owner"] == "worker-a"
        assert row["lease_until"] == 20_610
        assert row["updated_at"] == 20_010
    finally:
        conn.close()
