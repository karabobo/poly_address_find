import json

import pytest

from pm_robot.config import RobotSettings
from pm_robot.ops import maintenance
from pm_robot.orchestration.wallet_pipeline import JOB_TYPE as WALLET_EVIDENCE_JOB_TYPE
from pm_robot.pipeline_terms import DEFAULT_EVIDENCE_JOB_STAGE, EvidenceTier
from pm_robot.storage.db import connect, initialize_database, run_migrations
from pm_robot.storage.repository import (
    claim_pipeline_job,
    complete_pipeline_job,
    enqueue_pipeline_job,
)


def _settings(tmp_path):
    db_path = tmp_path / "data" / "robot.sqlite"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir(parents=True)
    return RobotSettings(db_path=db_path, backup_dir=backup_dir, execution_mode="research")


def _prepare_wal_database(settings):
    initialize_database(settings.db_path)
    conn = connect(settings.db_path)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS sample_rows(id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO sample_rows(value) VALUES ('x')")
        conn.commit()
    finally:
        conn.close()


def test_maintenance_dry_run_reports_but_skips_wal_checkpoint(tmp_path):
    settings = _settings(tmp_path)
    _prepare_wal_database(settings)

    result = maintenance(settings, dry_run=True, wal_checkpoint="passive")

    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["wal_checkpoint"] == {
        "mode": "passive",
        "executed": False,
        "skipped_reason": "dry_run",
        "busy": None,
        "log_frames": None,
        "checkpointed_frames": None,
    }
    assert "storage_before" in result
    assert "db_wal_mb" in result["storage"]


def test_maintenance_can_run_passive_wal_checkpoint(tmp_path):
    settings = _settings(tmp_path)
    _prepare_wal_database(settings)

    result = maintenance(settings, wal_checkpoint="passive")

    checkpoint = result["wal_checkpoint"]
    assert checkpoint["mode"] == "passive"
    assert checkpoint["executed"] is True
    assert checkpoint["skipped_reason"] == ""
    assert isinstance(checkpoint["busy"], int)
    assert isinstance(checkpoint["log_frames"], int)
    assert isinstance(checkpoint["checkpointed_frames"], int)


def test_maintenance_skip_cleanup_avoids_cleanup_scan(tmp_path, monkeypatch):
    settings = _settings(tmp_path)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("cleanup scan should be skipped")

    monkeypatch.setattr("pm_robot.ops._cleanup_database", fail_if_called)

    result = maintenance(settings, dry_run=True, skip_cleanup=True)

    assert result["cleanup_skipped"] is True
    assert result["deleted"] == {}


def test_maintenance_rejects_unknown_wal_checkpoint_mode(tmp_path):
    settings = _settings(tmp_path)
    _prepare_wal_database(settings)

    with pytest.raises(ValueError, match="wal_checkpoint"):
        maintenance(settings, wal_checkpoint="force")


def test_maintenance_can_requeue_only_expired_running_pipeline_jobs(tmp_path):
    settings = _settings(tmp_path)
    initialize_database(settings.db_path)
    now = 2_000
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 10, 0, ?, 'worker', ?, 1, 3, 0, '{}', '{}', '', ?, ?)
            """,
            [
                ("copyability_evidence", "0x" + "1" * 40, "copyability", "copyability", "running", 1, now, now),
                ("wallet_evidence_backfill", "0x" + "2" * 40, "light_pending", "l0_discovered", "running", 4_000_000_000, now, now),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    dry_run = maintenance(settings, skip_cleanup=True, dry_run=True, reset_stale_jobs=True)
    assert dry_run["stale_jobs"]["reset"] is False
    assert dry_run["stale_jobs"]["total"] == 1

    result = maintenance(settings, skip_cleanup=True, reset_stale_jobs=True)
    assert result["stale_jobs"]["reset"] is True
    assert result["stale_jobs"]["total"] == 1

    conn = connect(settings.db_path)
    try:
        expired = conn.execute(
            "SELECT status, lease_owner, lease_until, last_error FROM pipeline_jobs WHERE wallet = ?",
            ("0x" + "1" * 40,),
        ).fetchone()
        live = conn.execute(
            "SELECT status, lease_owner, lease_until, last_error FROM pipeline_jobs WHERE wallet = ?",
            ("0x" + "2" * 40,),
        ).fetchone()
    finally:
        conn.close()
    assert dict(expired) == {
        "status": "queued",
        "lease_owner": None,
        "lease_until": 0,
        "last_error": "expired_lease_requeued_by_maintenance",
    }
    assert live["status"] == "running"
    assert live["lease_owner"] == "worker"
    assert live["lease_until"] == 4_000_000_000


def test_expired_job_is_recovered_and_completed_by_replacement_worker(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    initialize_database(settings.db_path)
    wallet = "0x" + "9" * 40
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        assert enqueue_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            wallet=wallet,
            subject_key=DEFAULT_EVIDENCE_JOB_STAGE,
            tier=EvidenceTier.L1_LIGHT.value,
            shard=0,
            now=1_000,
        )
        conn.commit()
        abandoned = claim_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            shard=0,
            worker_id="worker-crashed",
            lease_seconds=10,
            now=1_001,
        )
        assert abandoned is not None
    finally:
        conn.close()

    monkeypatch.setattr("pm_robot.ops.time.time", lambda: 1_012)
    recovered = maintenance(settings, skip_cleanup=True, reset_stale_jobs=True)
    assert recovered["stale_jobs"]["total"] == 1

    conn = connect(settings.db_path)
    try:
        replacement = claim_pipeline_job(
            conn,
            job_type=WALLET_EVIDENCE_JOB_TYPE,
            shard=0,
            worker_id="worker-replacement",
            lease_seconds=60,
            now=1_013,
        )
        assert replacement is not None
        assert replacement["attempts"] == 2
        assert complete_pipeline_job(
            conn,
            job_id=int(replacement["job_id"]),
            worker_id="worker-replacement",
            output_data={"recovered": True},
            now=1_014,
        ) is True
        conn.commit()
        row = conn.execute(
            "SELECT status, attempts, lease_owner, output_json FROM pipeline_jobs WHERE job_id = ?",
            (replacement["job_id"],),
        ).fetchone()
    finally:
        conn.close()

    assert row["status"] == "done"
    assert row["attempts"] == 2
    assert row["lease_owner"] is None
    assert json.loads(row["output_json"]) == {"recovered": True}


def test_maintenance_requeues_older_duplicate_running_pipeline_leases(tmp_path):
    settings = _settings(tmp_path)
    initialize_database(settings.db_path)
    now = 2_000
    wallet_old = "0x" + "1" * 40
    wallet_new = "0x" + "2" * 40
    wallet_other = "0x" + "3" * 40
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 10, 0,
                'running', ?, 4000000000, 1, 3, 0, '{}', '{}', '', ?, ?)
            """,
            [
                (wallet_old, "copyability-worker-a", now, now),
                (wallet_new, "copyability-worker-a", now + 10, now + 10),
                (wallet_other, "copyability-worker-b", now, now),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    dry_run = maintenance(settings, skip_cleanup=True, dry_run=True, reset_stale_jobs=True)
    assert dry_run["duplicate_running_jobs"]["reset"] is False
    assert dry_run["duplicate_running_jobs"]["total"] == 1

    result = maintenance(settings, skip_cleanup=True, reset_stale_jobs=True)
    assert result["duplicate_running_jobs"]["reset"] is True
    assert result["duplicate_running_jobs"]["total"] == 1
    assert result["duplicate_running_jobs"]["by_job_type"] == [
        {"job_type": "copyability_evidence", "count": 1}
    ]

    conn = connect(settings.db_path)
    try:
        old = conn.execute(
            "SELECT status, lease_owner, lease_until, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet_old,),
        ).fetchone()
        new = conn.execute(
            "SELECT status, lease_owner, lease_until, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet_new,),
        ).fetchone()
        other = conn.execute(
            "SELECT status, lease_owner, lease_until, last_error FROM pipeline_jobs WHERE wallet = ?",
            (wallet_other,),
        ).fetchone()
    finally:
        conn.close()

    assert dict(old) == {
        "status": "queued",
        "lease_owner": None,
        "lease_until": 0,
        "last_error": "duplicate_running_owner_requeued_by_maintenance",
    }
    assert new["status"] == "running"
    assert new["lease_owner"] == "copyability-worker-a"
    assert new["lease_until"] == 4_000_000_000
    assert new["last_error"] == ""
    assert other["status"] == "running"
    assert other["lease_owner"] == "copyability-worker-b"
    assert other["lease_until"] == 4_000_000_000
    assert other["last_error"] == ""


def test_maintenance_marks_only_stale_running_ingest_runs_interrupted(tmp_path):
    settings = _settings(tmp_path)
    initialize_database(settings.db_path)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO ingest_runs(
                ingest_type, started_at, finished_at, status,
                wallets_attempted, wallets_succeeded, rows_written, error
            ) VALUES (?, ?, NULL, 'running', 0, 0, 0, '')
            """,
            [
                ("copyability_evidence_worker_legacy", 1_000),
                ("copyability_evidence_worker_live", 4_000_000_000),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    dry_run = maintenance(
        settings,
        skip_cleanup=True,
        dry_run=True,
        reset_stale_ingest_runs=True,
        stale_ingest_run_seconds=3_600,
    )
    assert dry_run["stale_ingest_runs"]["reset"] is False
    assert dry_run["stale_ingest_runs"]["total"] == 1

    result = maintenance(
        settings,
        skip_cleanup=True,
        reset_stale_ingest_runs=True,
        stale_ingest_run_seconds=3_600,
    )
    assert result["stale_ingest_runs"]["reset"] is True
    assert result["stale_ingest_runs"]["total"] == 1
    assert result["stale_ingest_runs"]["by_ingest_type"] == [
        {"ingest_type": "copyability_evidence_worker_legacy", "count": 1}
    ]

    conn = connect(settings.db_path)
    try:
        legacy = conn.execute(
            """
            SELECT status, started_at, finished_at, error
            FROM ingest_runs
            WHERE ingest_type = 'copyability_evidence_worker_legacy'
            """
        ).fetchone()
        live = conn.execute(
            """
            SELECT status, finished_at, error
            FROM ingest_runs
            WHERE ingest_type = 'copyability_evidence_worker_live'
            """
        ).fetchone()
    finally:
        conn.close()

    assert dict(legacy) == {
        "status": "interrupted",
        "started_at": 1_000,
        "finished_at": 4_600,
        "error": "stale_running_marked_interrupted_by_maintenance",
    }
    assert dict(live) == {"status": "running", "finished_at": None, "error": ""}
