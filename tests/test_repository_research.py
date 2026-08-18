import json

import pytest

from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    claim_pipeline_job,
    complete_pipeline_job,
    enqueue_pipeline_job,
    get_wallet_features,
    pipeline_job_summary,
    record_runtime_heartbeat,
    retry_pipeline_job,
    upsert_candidate,
    upsert_wallet_feature,
)


WALLET = "0x" + "a" * 40


def _column_names(conn, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def test_candidate_upsert_merges_provenance_without_legacy_stage_column(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        upsert_candidate(
            conn,
            CandidateAddress(address=WALLET, sources="manual", labels="seed"),
            now=100,
        )
        upsert_candidate(
            conn,
            CandidateAddress(address=WALLET, sources="manual", notes="verified"),
            now=200,
        )
        row = conn.execute(
            "SELECT sources, labels, notes FROM candidate_wallets WHERE address = ?",
            (WALLET,),
        ).fetchone()
        source_rows = conn.execute(
            "SELECT observed_at, recorded_at FROM candidate_source_events WHERE address = ?",
            (WALLET,),
        ).fetchall()
        candidate_columns = _column_names(conn, "candidate_wallets")
    finally:
        conn.close()

    assert dict(row) == {
        "sources": "manual",
        "labels": "seed",
        "notes": "verified",
    }
    assert "candidate_stage" not in candidate_columns
    assert [tuple(item) for item in source_rows] == [(100, 200)]


def test_wallet_feature_upsert_uses_only_current_research_model(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=WALLET, sources="manual"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=WALLET,
                net_pnl_usdc=20,
                total_volume_usdc=500,
                extra={"source": "manual"},
            ),
        )
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=WALLET,
                net_pnl_usdc=30,
                trade_win_rate=0.6,
                extra={"sample": 50},
            ),
        )
        feature = get_wallet_features(conn)[WALLET]
        feature_columns = _column_names(conn, "wallet_features")
    finally:
        conn.close()

    assert feature.net_pnl_usdc == 30
    assert feature.total_volume_usdc == 500
    assert feature.trade_win_rate == 0.6
    assert feature.extra == {"sample": 50, "source": "manual"}
    assert "copy_event_count" not in feature_columns
    assert "copy_stream_roi" not in feature_columns


def test_queue_accepts_only_current_job_types_and_requires_lease_owner(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        with pytest.raises(ValueError, match="unsupported pipeline job type"):
            enqueue_pipeline_job(
                conn,
                job_type="copyability_evidence",
                wallet=WALLET,
            )
        assert enqueue_pipeline_job(
            conn,
            job_type="wallet_recent_screen",
            wallet=WALLET,
            job_action="screen:v1",
            job_scope="sample",
            shard=1,
            now=100,
        )
        conn.commit()
        job = claim_pipeline_job(
            conn,
            job_type="wallet_recent_screen",
            shard=1,
            worker_id="worker-a",
            lease_seconds=60,
            now=101,
        )
        assert job is not None
        assert complete_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker-b",
            now=102,
        ) is False
        assert complete_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker-a",
            output_data={"qualified": True},
            now=102,
        ) is True
        conn.commit()
        summary = pipeline_job_summary(conn)
        stored = conn.execute(
            "SELECT job_action, job_scope FROM pipeline_jobs WHERE wallet = ?",
            (WALLET,),
        ).fetchone()
    finally:
        conn.close()

    assert dict(stored) == {"job_action": "screen:v1", "job_scope": "sample"}
    assert summary["statuses"] == [
        {"job_type": "wallet_recent_screen", "status": "done", "count": 1}
    ]


def test_retry_can_release_upstream_deferral_without_spending_attempt(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v1",
            job_scope="light",
            now=100,
        )
        conn.commit()
        job = claim_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            shard=0,
            worker_id="worker",
            lease_seconds=60,
            now=101,
        )
        assert retry_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker",
            error="upstream cooldown",
            next_attempt_at=200,
            count_attempt=False,
            now=102,
        )
        conn.commit()
        row = conn.execute(
            "SELECT status, attempts, next_attempt_at FROM pipeline_jobs"
        ).fetchone()
    finally:
        conn.close()

    assert tuple(row) == ("queued", 0, 200)


def test_claim_preserves_previous_last_error_until_success(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v1",
            job_scope="light",
            now=100,
        )
        conn.execute(
            "UPDATE pipeline_jobs SET last_error = 'previous failure' WHERE wallet = ?",
            (WALLET,),
        )
        conn.commit()

        job = claim_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            shard=0,
            worker_id="worker",
            lease_seconds=60,
            now=101,
        )
        claimed = conn.execute("SELECT last_error FROM pipeline_jobs").fetchone()[0]
        complete_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker",
            now=102,
        )
        completed = conn.execute("SELECT status, last_error FROM pipeline_jobs").fetchone()
    finally:
        conn.close()

    assert job["last_error"] == "previous failure"
    assert claimed == "previous failure"
    assert tuple(completed) == ("done", "")


def test_retry_can_fail_permanently_without_waiting_for_attempt_budget(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v1",
            job_scope="light",
            max_attempts=3,
            now=100,
        )
        conn.commit()
        job = claim_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            shard=0,
            worker_id="worker",
            lease_seconds=60,
            now=101,
        )
        assert retry_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker",
            error="incompatible history data: malformed local rows",
            next_attempt_at=200,
            count_attempt=False,
            fail_permanently=True,
            now=102,
        )
        conn.commit()
        row = conn.execute(
            "SELECT status, attempts, max_attempts, last_error, terminal_reason "
            "FROM pipeline_jobs"
        ).fetchone()
    finally:
        conn.close()

    assert tuple(row) == (
        "terminal_failed",
        3,
        3,
        "incompatible history data: malformed local rows",
        "permanent_failure",
    )


def test_terminal_pipeline_failure_is_not_requeued_by_generic_enqueue(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v1",
            job_scope="light",
            max_attempts=3,
            now=100,
        )
        conn.commit()
        job = claim_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            shard=0,
            worker_id="worker",
            lease_seconds=60,
            now=101,
        )
        assert retry_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker",
            error="incompatible history data: malformed local rows",
            next_attempt_at=200,
            count_attempt=False,
            fail_permanently=True,
            terminal_reason="wallet_history_data_quality",
            terminal_policy_version="wallet_history_summary_v4",
            now=102,
        )
        conn.commit()

        assert enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v1",
            job_scope="light",
            now=201,
        ) is False
        conn.commit()
        blocked = conn.execute(
            "SELECT status, attempts, next_attempt_at, terminal_reason, "
            "terminal_policy_version FROM pipeline_jobs WHERE wallet = ?",
            (WALLET,),
        ).fetchone()
        claimed = claim_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            shard=0,
            worker_id="worker",
            lease_seconds=60,
            now=202,
        )

        assert enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v2",
            job_scope="light",
            now=203,
        ) is True
        conn.commit()
        replacement = claim_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            shard=0,
            worker_id="worker",
            lease_seconds=60,
            now=204,
        )
    finally:
        conn.close()

    assert tuple(blocked) == (
        "terminal_failed",
        3,
        200,
        "wallet_history_data_quality",
        "wallet_history_summary_v4",
    )
    assert claimed is None
    assert replacement is not None
    assert replacement["job_action"] == "light:v2"


def test_transient_failed_pipeline_job_still_requeues_after_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v1",
            job_scope="light",
            max_attempts=1,
            now=100,
        )
        conn.commit()
        job = claim_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            shard=0,
            worker_id="worker",
            lease_seconds=60,
            now=101,
        )
        assert retry_pipeline_job(
            conn,
            job_id=job["job_id"],
            worker_id="worker",
            error="upstream gateway timeout",
            next_attempt_at=200,
            now=102,
        )
        conn.commit()

        assert enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v1",
            job_scope="light",
            now=199,
        ) is False
        assert enqueue_pipeline_job(
            conn,
            job_type="wallet_history_collect",
            wallet=WALLET,
            job_action="light:v1",
            job_scope="light",
            now=201,
        ) is True
        conn.commit()
        row = conn.execute(
            "SELECT status, attempts, max_attempts, next_attempt_at, terminal_reason "
            "FROM pipeline_jobs WHERE wallet = ?",
            (WALLET,),
        ).fetchone()
    finally:
        conn.close()

    assert tuple(row) == ("queued", 0, 3, 0, "")


def test_runtime_heartbeat_is_compact_ingest_audit(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        run_id = record_runtime_heartbeat(
            conn,
            "loop_rtds_discovery",
            rows_written=12,
            error="messages=120 wallets=40",
            now=500,
        )
        row = conn.execute(
            "SELECT status, rows_written, error FROM runtime_heartbeats WHERE heartbeat_id = ?",
            (run_id,),
        ).fetchone()
    finally:
        conn.close()

    assert tuple(row) == ("ok", 12, "messages=120 wallets=40")
