import sqlite3
import time
from pathlib import Path

from pm_robot.config import RobotSettings
from pm_robot.ops import _delete_metadata_batch, maintenance
from pm_robot.storage.db import connect, run_migrations


def _settings(tmp_path: Path) -> RobotSettings:
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        log_dir=tmp_path / "logs",
        backup_dir=tmp_path / "backups",
        archive_dir=tmp_path / "parquet",
    )
    for path in (
        settings.db_path.parent,
        settings.log_dir,
        settings.backup_dir,
        settings.archive_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()
    return settings


def _insert_job(
    conn,
    *,
    job_type: str,
    wallet: str,
    status: str,
    attempts: int = 0,
    max_attempts: int = 3,
    lease_until: int = 0,
    updated_at: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO pipeline_jobs(
            job_type, wallet, job_action, job_scope, priority, shard, status,
            lease_owner, lease_until, attempts, max_attempts,
            next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, 'test', 'sample', 10, 0, ?, 'worker', ?, ?, ?, 0, ?, ?)
        """,
        (
            job_type,
            wallet,
            status,
            lease_until,
            attempts,
            max_attempts,
            updated_at,
            updated_at,
        ),
    )


def test_maintenance_dry_run_reports_legacy_jobs_without_changing_them(tmp_path):
    settings = _settings(tmp_path)
    conn = connect(settings.db_path)
    try:
        _insert_job(
            conn,
            job_type="copyability_evidence",
            wallet="0x" + "1" * 40,
            status="queued",
        )
        conn.commit()
    finally:
        conn.close()

    result = maintenance(settings, dry_run=True)

    assert result["legacy_jobs_disabled"]["total"] == 1
    assert result["legacy_jobs_disabled"]["executed"] is False
    conn = connect(settings.db_path)
    try:
        assert conn.execute(
            "SELECT status FROM pipeline_jobs WHERE job_type = 'copyability_evidence'"
        ).fetchone()[0] == "queued"
    finally:
        conn.close()


def test_maintenance_disables_legacy_jobs_and_bounds_metadata(tmp_path):
    settings = _settings(tmp_path)
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        _insert_job(
            conn,
            job_type="wallet_evidence_backfill",
            wallet="0x" + "2" * 40,
            status="running",
            lease_until=now + 60,
        )
        conn.execute(
            """
            INSERT INTO api_request_log(
                ts, base_url, endpoint, latency_ms, retry_count, error_type, ok
            ) VALUES (?, 'https://example.invalid', '/old', 1, 0, '', 1)
            """,
            (now - 10 * 86_400,),
        )
        conn.execute(
            """
            INSERT INTO runtime_heartbeats(
                name, started_at, finished_at, status
            ) VALUES ('loop_rtds_discovery', ?, ?, 'ok')
            """,
            (now - 40 * 86_400, now - 40 * 86_400 + 1),
        )
        conn.commit()
    finally:
        conn.close()

    result = maintenance(
        settings,
        api_log_days=7,
        heartbeat_days=30,
        cleanup_batch_limit=100,
    )

    assert result["legacy_jobs_disabled"]["total"] == 1
    assert result["deleted"]["api_request_log"] == 1
    assert result["deleted"]["runtime_heartbeats"] == 1
    conn = connect(settings.db_path)
    try:
        row = conn.execute(
            "SELECT status, lease_owner, last_error FROM pipeline_jobs "
            "WHERE job_type = 'wallet_evidence_backfill'"
        ).fetchone()
        assert tuple(row) == (
            "cancelled",
            None,
            "retired_job_type_disabled_by_research_runtime",
        )
    finally:
        conn.close()


def test_maintenance_recovers_only_expired_active_jobs(tmp_path):
    settings = _settings(tmp_path)
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet="0x" + "3" * 40,
            status="running",
            attempts=1,
            lease_until=now - 1,
        )
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "4" * 40,
            status="running",
            attempts=3,
            max_attempts=3,
            lease_until=now - 1,
        )
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet="0x" + "5" * 40,
            status="running",
            attempts=1,
            lease_until=now + 600,
        )
        conn.commit()
    finally:
        conn.close()

    result = maintenance(settings, skip_cleanup=True, reset_stale_jobs=True)

    assert result["stale_jobs"]["expired_running"] == 2
    conn = connect(settings.db_path)
    try:
        statuses = {
            row["wallet"]: row["status"]
            for row in conn.execute("SELECT wallet, status FROM pipeline_jobs")
        }
    finally:
        conn.close()
    assert statuses["0x" + "3" * 40] == "queued"
    assert statuses["0x" + "4" * 40] == "failed"
    assert statuses["0x" + "5" * 40] == "running"


def test_maintenance_closes_only_stale_active_runtime_runs(tmp_path):
    settings = _settings(tmp_path)
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        conn.executemany(
            """
            INSERT INTO runtime_heartbeats(name, started_at, finished_at, status)
            VALUES (?, ?, ?, 'running')
            """,
            (
                ("loop_wallet_screen_worker_0", now - 10_000, now - 10_000),
                ("loop_wallet_history_worker_0", now, now),
                ("retired_score_loop", now - 10_000, now - 10_000),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    result = maintenance(
        settings,
        skip_cleanup=True,
        reset_stale_heartbeats=True,
        stale_heartbeat_seconds=3_600,
    )

    assert result["stale_heartbeats"]["total"] == 1
    conn = connect(settings.db_path)
    try:
        statuses = {
            row["name"]: row["status"]
            for row in conn.execute("SELECT name, status FROM runtime_heartbeats")
        }
    finally:
        conn.close()
    assert statuses["loop_wallet_screen_worker_0"] == "interrupted"
    assert statuses["loop_wallet_history_worker_0"] == "running"
    assert statuses["retired_score_loop"] == "running"


def test_metadata_cleanup_retries_transient_sqlite_writer_lock(monkeypatch):
    class Cursor:
        rowcount = 3

    class LockOnceConnection:
        def __init__(self):
            self.execute_calls = 0
            self.commits = 0
            self.rollbacks = 0

        def execute(self, sql, params):
            del sql, params
            self.execute_calls += 1
            if self.execute_calls == 1:
                raise sqlite3.OperationalError("database is locked")
            return Cursor()

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    monkeypatch.setattr("pm_robot.storage.db.time.sleep", lambda _seconds: None)
    conn = LockOnceConnection()

    deleted = _delete_metadata_batch(
        conn,
        table="api_request_log",
        where="ts < ?",
        params=(123,),
        limit=500,
    )

    assert deleted == 3
    assert conn.execute_calls == 2
    assert conn.commits == 1
    assert conn.rollbacks == 1
