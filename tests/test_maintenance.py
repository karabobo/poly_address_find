import sqlite3
import time
from pathlib import Path

import pm_robot.ops as ops_module
from pm_robot.config import RobotSettings
from pm_robot.ops import _delete_metadata_batch, health_check, maintenance
from pm_robot.storage.db import connect, run_migrations


def _insert_observed_l0(
    conn,
    *,
    wallet: str,
    updated_at: int,
    recent_usdc_total: float,
    hard_risk_block: bool = False,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_levels(
            wallet, level, level_reason, hard_risk_block, first_seen_at,
            last_seen_at, level_updated_at, updated_at
        ) VALUES (?, 'l0', 'test', ?, ?, ?, ?, ?)
        """,
        (
            wallet,
            1 if hard_risk_block else 0,
            updated_at,
            updated_at,
            updated_at,
            updated_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO observed_wallets(
            wallet, sources, observed_trade_count, recent_trade_count,
            recent_usdc_total, recent_max_trade_usdc, recent_trades_json,
            first_seen_at, updated_at
        ) VALUES (?, 'polymarket_rtds_activity', 1, 1, ?, ?, '[]', ?, ?)
        """,
        (
            wallet,
            recent_usdc_total,
            recent_usdc_total,
            updated_at,
            updated_at,
        ),
    )


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
    last_error: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO pipeline_jobs(
            job_type, wallet, job_action, job_scope, priority, shard, status,
            lease_owner, lease_until, attempts, max_attempts,
            next_attempt_at, last_error, created_at, updated_at
        ) VALUES (?, ?, 'test', 'sample', 10, 0, ?, 'worker', ?, ?, ?, 0, ?, ?, ?)
        """,
        (
            job_type,
            wallet,
            status,
            lease_until,
            attempts,
            max_attempts,
            last_error,
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


def test_light_maintenance_reports_parquet_catalog_without_archive_walk(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    artifact_path = settings.archive_dir / "wallet_history" / "depth=light" / "artifact.parquet"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b"parquet placeholder")
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO wallet_history_artifacts(
                artifact_id, wallet, history_depth, storage_version, relative_path,
                row_count, byte_size, checksum, status, created_at, updated_at
            ) VALUES (
                'artifact-light', ?, 'light', 'test',
                'wallet_history/depth=light/artifact.parquet',
                10, ?, 'checksum', 'active', 1, 1
            )
            """,
            ("0x" + "1" * 40, 5 * 1_048_576),
        )
        conn.commit()
    finally:
        conn.close()

    original_rglob = Path.rglob

    def fail_archive_rglob(path, pattern):
        if path == settings.archive_dir or settings.archive_dir in path.parents:
            raise AssertionError(
                f"light maintenance must not scan archive parquet files: {path}/{pattern}"
            )
        return original_rglob(path, pattern)

    original_path_size = ops_module._path_size

    def fail_archive_path_size(path):
        if path == settings.archive_dir or settings.archive_dir in path.parents:
            raise AssertionError(
                f"light maintenance must not stat archive parquet files: {path}"
            )
        return original_path_size(path)

    monkeypatch.setattr(Path, "rglob", fail_archive_rglob)
    monkeypatch.setattr("pm_robot.ops._path_size", fail_archive_path_size)

    result = maintenance(settings, skip_cleanup=True)

    assert result["storage_before"]["parquet_artifacts"] == 1
    assert result["storage_before"]["parquet_active_artifacts"] == 1
    assert result["storage_before"]["parquet_catalog_size_mb"] == 5.0
    assert result["storage_before"]["parquet_storage_source"] == "catalog"
    assert result["storage"]["parquet_artifacts"] == 1
    assert result["storage"]["parquet_active_artifacts"] == 1
    assert result["storage"]["parquet_catalog_size_mb"] == 5.0
    assert result["storage"]["parquet_storage_source"] == "catalog"


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


def test_maintenance_preserves_existing_last_error_when_recovering_stale_jobs(tmp_path):
    settings = _settings(tmp_path)
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "9" * 40,
            status="running",
            attempts=3,
            max_attempts=3,
            lease_until=now - 1,
            last_error="original terminal data error",
        )
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet="0x" + "a" * 40,
            status="running",
            attempts=1,
            lease_until=now - 1,
        )
        conn.commit()
    finally:
        conn.close()

    maintenance(settings, skip_cleanup=True, reset_stale_jobs=True)

    conn = connect(settings.db_path)
    try:
        errors = {
            row["wallet"]: row["last_error"]
            for row in conn.execute("SELECT wallet, last_error FROM pipeline_jobs")
        }
    finally:
        conn.close()

    assert errors["0x" + "9" * 40] == "original terminal data error"
    assert errors["0x" + "a" * 40] == "expired_lease_requeued_by_maintenance"


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


def test_maintenance_prunes_only_stale_unqualified_discovery_only_l0(tmp_path):
    settings = _settings(tmp_path)
    now = int(time.time())
    stale = now - 8 * 86_400
    fresh = now - 60
    wallets = {
        name: "0x" + digit * 40
        for name, digit in (
            ("expired", "1"),
            ("fresh", "2"),
            ("qualified", "3"),
            ("candidate", "4"),
            ("blocked", "5"),
            ("active_job", "6"),
            ("has_evidence", "7"),
            ("terminal_job", "8"),
        )
    }
    conn = connect(settings.db_path)
    try:
        _insert_observed_l0(
            conn,
            wallet=wallets["expired"],
            updated_at=stale,
            recent_usdc_total=30,
        )
        _insert_observed_l0(
            conn,
            wallet=wallets["fresh"],
            updated_at=fresh,
            recent_usdc_total=30,
        )
        _insert_observed_l0(
            conn,
            wallet=wallets["qualified"],
            updated_at=stale,
            recent_usdc_total=100,
        )
        _insert_observed_l0(
            conn,
            wallet=wallets["candidate"],
            updated_at=stale,
            recent_usdc_total=30,
        )
        conn.execute(
            """
            INSERT INTO candidate_wallets(address, first_seen_at, updated_at)
            VALUES (?, ?, ?)
            """,
            (wallets["candidate"], stale, stale),
        )
        _insert_observed_l0(
            conn,
            wallet=wallets["blocked"],
            updated_at=stale,
            recent_usdc_total=30,
            hard_risk_block=True,
        )
        _insert_observed_l0(
            conn,
            wallet=wallets["active_job"],
            updated_at=stale,
            recent_usdc_total=30,
        )
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet=wallets["active_job"],
            status="queued",
            updated_at=stale,
        )
        _insert_observed_l0(
            conn,
            wallet=wallets["has_evidence"],
            updated_at=stale,
            recent_usdc_total=30,
        )
        conn.execute(
            """
            INSERT INTO wallet_pnl_summaries(wallet, updated_at)
            VALUES (?, ?)
            """,
            (wallets["has_evidence"], stale),
        )
        _insert_observed_l0(
            conn,
            wallet=wallets["terminal_job"],
            updated_at=stale,
            recent_usdc_total=30,
        )
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet=wallets["terminal_job"],
            status="failed",
            updated_at=stale,
        )
        conn.execute(
            """
            INSERT INTO wallet_level_events(
                wallet, from_level, to_level, reason, created_at
            ) VALUES (?, 'l0', 'l0', 'test_only', ?)
            """,
            (wallets["expired"], stale),
        )
        conn.execute(
            """
            INSERT INTO wallet_screen_summaries(wallet, updated_at)
            VALUES (?, ?)
            """,
            (wallets["expired"], stale),
        )
        conn.commit()
    finally:
        conn.close()

    preview = maintenance(
        settings,
        dry_run=True,
        l0_retention_days=7,
        l0_cleanup_batch_limit=100,
    )
    assert preview["l0_retention"]["eligible_wallets"] == 1
    assert preview["l0_retention"]["deleted_wallets"] == 0

    result = maintenance(
        settings,
        l0_retention_days=7,
        l0_cleanup_batch_limit=100,
    )

    assert result["l0_retention"]["eligible_wallets"] == 1
    assert result["l0_retention"]["deleted_wallets"] == 1
    assert result["l0_retention"]["deleted_rows"]["observed_wallets"] == 1
    assert result["l0_retention"]["deleted_rows"]["wallet_levels"] == 1
    assert result["l0_retention"]["deleted_rows"]["wallet_level_events"] == 1
    assert result["l0_retention"]["deleted_rows"]["wallet_screen_summaries"] == 1
    conn = connect(settings.db_path)
    try:
        remaining = {
            str(row["wallet"])
            for row in conn.execute("SELECT wallet FROM observed_wallets")
        }
        assert wallets["expired"] not in remaining
        assert remaining == set(wallets.values()) - {wallets["expired"]}
        assert conn.execute(
            "SELECT 1 FROM wallet_levels WHERE wallet = ?",
            (wallets["expired"],),
        ).fetchone() is None
        assert conn.execute(
            "SELECT status FROM pipeline_jobs WHERE wallet = ?",
            (wallets["terminal_job"],),
        ).fetchone()[0] == "failed"
    finally:
        conn.close()


def test_maintenance_bounds_l0_pruning_per_run(tmp_path):
    settings = _settings(tmp_path)
    stale = int(time.time()) - 8 * 86_400
    conn = connect(settings.db_path)
    try:
        for digit in ("a", "b", "c"):
            _insert_observed_l0(
                conn,
                wallet="0x" + digit * 40,
                updated_at=stale,
                recent_usdc_total=10,
            )
        conn.commit()
    finally:
        conn.close()

    result = maintenance(
        settings,
        l0_retention_days=7,
        l0_cleanup_batch_limit=2,
    )

    assert result["l0_retention"]["eligible_wallets"] == 2
    assert result["l0_retention"]["deleted_wallets"] == 2
    conn = connect(settings.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM observed_wallets").fetchone()[0] == 1
    finally:
        conn.close()


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


def test_health_check_exposes_pipeline_queue_health(tmp_path):
    settings = _settings(tmp_path)
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "b" * 40,
            status="running",
            attempts=1,
            lease_until=now - 1,
        )
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "c" * 40,
            status="queued",
            attempts=3,
            max_attempts=3,
        )
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet="0x" + "d" * 40,
            status="failed",
            attempts=3,
            max_attempts=3,
            last_error="",
        )
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "e" * 40,
            status="failed",
            attempts=3,
            max_attempts=3,
            last_error="incompatible history data",
        )
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet="0x" + "f" * 40,
            status="running",
            attempts=1,
            lease_until=now + 600,
        )
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "1" * 39 + "0",
            status="queued",
            attempts=0,
            max_attempts=3,
        )
        conn.commit()
    finally:
        conn.close()

    result = health_check(settings)
    queue_health = result["research_readiness"]["metrics"]["queue_health"]

    assert result["pipeline_queue_health"] == queue_health
    assert queue_health["expired_running"] == 1
    assert queue_health["queued_exhausted"] == 1
    assert queue_health["failed_missing_error"] == 1
    assert queue_health["queued"] == 2
    assert queue_health["running"] == 2
    assert queue_health["active"] == 2
    assert queue_health["wallet_history_screen_active"] == 2
    assert {row["job_type"] for row in queue_health["failed_by_job_type"]} == {
        "wallet_history_collect",
        "wallet_recent_screen",
    }


def test_pipeline_queue_health_distinguishes_stall_from_ordinary_backlog(tmp_path):
    settings = _settings(tmp_path)
    now = 2_000_000
    conn = connect(settings.db_path)
    try:
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "a" * 40,
            status="queued",
            updated_at=now - 3_601,
        )
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet="0x" + "b" * 40,
            status="queued",
            updated_at=now - 30,
        )
        conn.commit()

        queue_health = ops_module._pipeline_queue_health(
            conn,
            now=now,
            stall_seconds=3_600,
        )
    finally:
        conn.close()

    assert queue_health["stall_after_seconds"] == 3_600
    assert queue_health["stalled_by_job_type"] == [
        {
            "job_type": "wallet_history_collect",
            "due_queued": 1,
            "oldest_due_updated_at": now - 3_601,
            "last_completed_at": 0,
        }
    ]


def test_maintenance_preflight_uses_pipeline_job_index_only(tmp_path):
    settings = _settings(tmp_path)
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "a" * 40,
            status="queued",
            attempts=0,
            max_attempts=3,
        )
        _insert_job(
            conn,
            job_type="wallet_recent_screen",
            wallet="0x" + "b" * 40,
            status="running",
            attempts=1,
            lease_until=now + 60,
        )
        _insert_job(
            conn,
            job_type="wallet_history_collect",
            wallet="0x" + "c" * 40,
            status="running",
            attempts=1,
            lease_until=now - 60,
        )
        conn.commit()
        sql, params = ops_module._maintenance_preflight_active_queue_query(now)
        eqp = [
            row[3]
            for row in conn.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
        ]
    finally:
        conn.close()

    result = ops_module.maintenance_preflight(settings, now=now)
    query_text = " ".join([sql, *eqp]).lower()

    assert result["wallet_history_screen_active"] == 2
    assert result["defer_maintenance"] is True
    assert any("idx_pipeline_jobs_type_claim" in detail for detail in eqp)
    assert "pipeline_jobs" in query_text
    for forbidden in (
        "observed_wallets",
        "wallet_levels",
        "wallet_history_artifacts",
        "wallet_history_summaries",
        "parquet",
    ):
        assert forbidden not in query_text


def test_truncate_wal_dry_run_and_below_threshold_are_non_destructive(tmp_path):
    settings = _settings(tmp_path)

    dry = maintenance(settings, dry_run=True, wal_checkpoint="truncate")
    assert dry["wal_checkpoint"]["skipped_reason"] == "dry_run"

    result = maintenance(
        settings,
        skip_cleanup=True,
        wal_checkpoint="truncate",
        wal_truncate_threshold_mb=512,
        wal_truncate_allowed_hours="0-23",
        wal_checkpoint_timeout_ms=250,
    )

    assert result["wal_checkpoint"]["executed"] is False
    assert result["wal_checkpoint"]["skipped_reason"] == "below_threshold"
    assert result["wal_checkpoint"]["requested_mode"] == "truncate"


def test_truncate_wal_outside_allowed_window_skips_without_truncating(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    wal_path = settings.db_path.with_name(f"{settings.db_path.name}-wal")
    wal_path.touch()
    calls = []

    def fake_checkpoint(conn, *, mode, timeout_ms):
        del conn, timeout_ms
        calls.append(mode)
        assert mode == "passive"
        return {
            "mode": mode,
            "executed": True,
            "skipped_reason": "",
            "busy": 0,
            "log_frames": 10,
            "checkpointed_frames": 10,
        }

    class Noon:
        tm_hour = 12

    monkeypatch.setattr("pm_robot.ops._execute_wal_checkpoint", fake_checkpoint)
    monkeypatch.setattr("pm_robot.ops._main_database_path", lambda _conn: settings.db_path)
    monkeypatch.setattr("pm_robot.ops._path_size", lambda _path: 2 * 1_048_576)
    monkeypatch.setattr("pm_robot.ops.time.localtime", lambda: Noon())

    result = maintenance(
        settings,
        skip_cleanup=True,
        wal_checkpoint="truncate",
        wal_truncate_threshold_mb=1,
        wal_truncate_allowed_hours="0-6",
    )

    assert calls == ["passive"]
    assert result["wal_checkpoint"]["executed"] is False
    assert result["wal_checkpoint"]["skipped_reason"] == "outside_allowed_hours"
    assert result["wal_checkpoint"]["current_hour"] == 12
    assert result["wal_checkpoint"]["allowed_hours"] == list(range(7))


def test_truncate_wal_busy_readers_skip_without_truncating(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    wal_path = settings.db_path.with_name(f"{settings.db_path.name}-wal")
    wal_path.touch()
    calls = []

    def fake_checkpoint(conn, *, mode, timeout_ms):
        del conn, timeout_ms
        calls.append(mode)
        assert mode == "passive"
        return {
            "mode": mode,
            "executed": True,
            "skipped_reason": "",
            "busy": 2,
            "log_frames": 10,
            "checkpointed_frames": 8,
        }

    monkeypatch.setattr("pm_robot.ops._execute_wal_checkpoint", fake_checkpoint)
    monkeypatch.setattr("pm_robot.ops._main_database_path", lambda _conn: settings.db_path)
    monkeypatch.setattr("pm_robot.ops._path_size", lambda _path: 2 * 1_048_576)

    result = maintenance(
        settings,
        skip_cleanup=True,
        wal_checkpoint="truncate",
        wal_truncate_threshold_mb=1,
        wal_truncate_allowed_hours="0-23",
    )

    assert calls == ["passive"]
    assert result["wal_checkpoint"]["executed"] is False
    assert result["wal_checkpoint"]["skipped_reason"] == "busy_readers"
    assert result["wal_checkpoint"]["busy"] == 2


def test_truncate_wal_above_threshold_with_no_busy_readers_executes(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    wal_path = settings.db_path.with_name(f"{settings.db_path.name}-wal")
    wal_path.touch()
    calls = []

    def fake_checkpoint(conn, *, mode, timeout_ms):
        del conn, timeout_ms
        calls.append(mode)
        return {
            "mode": mode,
            "executed": True,
            "skipped_reason": "",
            "busy": 0,
            "log_frames": 10,
            "checkpointed_frames": 10,
        }

    class ThreeAm:
        tm_hour = 3

    monkeypatch.setattr("pm_robot.ops._execute_wal_checkpoint", fake_checkpoint)
    monkeypatch.setattr("pm_robot.ops._main_database_path", lambda _conn: settings.db_path)
    monkeypatch.setattr("pm_robot.ops._path_size", lambda _path: 2 * 1_048_576)
    monkeypatch.setattr("pm_robot.ops.time.localtime", lambda: ThreeAm())

    result = maintenance(
        settings,
        skip_cleanup=True,
        wal_checkpoint="truncate",
        wal_truncate_threshold_mb=1,
        wal_truncate_allowed_hours="0-6",
        wal_checkpoint_timeout_ms=250,
    )

    assert calls == ["passive", "truncate"]
    assert result["wal_checkpoint"]["mode"] == "truncate"
    assert result["wal_checkpoint"]["executed"] is True
    assert result["wal_checkpoint"]["skipped_reason"] == ""
    assert result["wal_checkpoint"]["wal_size_mb"] == 2.0
    assert result["wal_checkpoint"]["probe"]["probe_mode"] == "passive"
