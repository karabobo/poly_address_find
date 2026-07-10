import os
import sqlite3
from io import BytesIO
from pathlib import Path

from pm_robot.config import RobotSettings
from pm_robot.ops import (
    backup_database,
    dump_database_sql,
    next_backup_delay_seconds,
    storage_report,
)
from pm_robot.storage.db import connect, initialize_database, run_migrations


def test_backup_database_creates_verified_restoreable_sqlite(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
        execution_mode="research",
    )
    settings.db_path.parent.mkdir()
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()

    backup = backup_database(settings)

    restored = sqlite3.connect(backup)
    try:
        assert restored.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert restored.execute(
            "SELECT COUNT(*) FROM schema_migrations"
        ).fetchone()[0] > 0
    finally:
        restored.close()
    assert (settings.backup_dir / "pm_robot-latest.sqlite").exists()


def test_dump_database_sql_stream_is_restoreable(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
        execution_mode="research",
    )
    settings.db_path.parent.mkdir()
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()

    stream = BytesIO()
    dump_database_sql(settings, stream)

    restored_path = tmp_path / "restored.sqlite"
    restored = sqlite3.connect(restored_path)
    try:
        restored.executescript(stream.getvalue().decode("utf-8"))
        assert restored.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert restored.execute(
            "SELECT COUNT(*) FROM schema_migrations"
        ).fetchone()[0] > 0
    finally:
        restored.close()


def test_online_backup_captures_committed_wal_without_double_counting_latest(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
        execution_mode="research",
    )
    initialize_database(settings.db_path)
    writer = connect(settings.db_path)
    try:
        run_migrations(writer)
        writer.execute("CREATE TABLE backup_probe(value TEXT NOT NULL)")
        writer.executemany(
            "INSERT INTO backup_probe(value) VALUES (?)",
            [("a",), ("b",), ("c",)],
        )
        writer.commit()

        backup = backup_database(settings)
    finally:
        writer.close()

    restored = sqlite3.connect(backup)
    try:
        assert restored.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert restored.execute("SELECT COUNT(*) FROM backup_probe").fetchone()[0] == 3
    finally:
        restored.close()

    report = storage_report(settings)
    assert report["backup_count"] == 1


def test_backup_restart_delay_uses_start_delay_only_without_history(tmp_path):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()

    assert next_backup_delay_seconds(
        backup_dir,
        interval_seconds=86_400,
        start_delay_seconds=600,
        now=100_000,
    ) == 600


def test_backup_restart_delay_never_postpones_due_backup(tmp_path):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    backup = backup_dir / "pm_robot-20260710-000000.sqlite"
    backup.touch()

    backup_mtime = 100_000
    os.utime(backup, (backup_mtime, backup_mtime))
    os.link(backup, backup_dir / "pm_robot-latest.sqlite")
    assert next_backup_delay_seconds(
        backup_dir,
        interval_seconds=86_400,
        start_delay_seconds=600,
        now=backup_mtime + 86_399,
    ) == 1
    assert next_backup_delay_seconds(
        backup_dir,
        interval_seconds=86_400,
        start_delay_seconds=600,
        now=backup_mtime + 86_400,
    ) == 0
    assert next_backup_delay_seconds(
        backup_dir,
        interval_seconds=86_400,
        start_delay_seconds=600,
        now=backup_mtime + 90_000,
    ) == 0


def test_backup_restart_delay_runs_now_if_matched_backup_disappears(tmp_path, monkeypatch):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    backup = backup_dir / "pm_robot-20260710-000000.sqlite"
    backup.touch()
    latest = backup_dir / "pm_robot-latest.sqlite"
    os.link(backup, latest)
    original_stat = Path.stat

    def disappearing_stat(path, *args, **kwargs):
        if path == latest:
            raise FileNotFoundError(path)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", disappearing_stat)
    assert next_backup_delay_seconds(
        backup_dir,
        interval_seconds=86_400,
        start_delay_seconds=600,
        now=100_000,
    ) == 0


def test_backup_restart_delay_ignores_newer_unverified_partial_artifact(tmp_path):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    verified = backup_dir / "pm_robot-20260709-000000.sqlite"
    verified.touch()
    latest = backup_dir / "pm_robot-latest.sqlite"
    os.link(verified, latest)
    partial = backup_dir / "pm_robot-20260710-000000.sqlite"
    partial.touch()

    verified_mtime = 100_000
    os.utime(verified, (verified_mtime, verified_mtime))
    os.utime(partial, (verified_mtime + 86_399, verified_mtime + 86_399))
    assert next_backup_delay_seconds(
        backup_dir,
        interval_seconds=86_400,
        start_delay_seconds=600,
        now=verified_mtime + 86_400,
    ) == 0


def test_backup_restart_delay_runs_now_with_only_interrupted_partial(tmp_path):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    (backup_dir / "pm_robot-20260710-000000.sqlite.partial").touch()

    assert next_backup_delay_seconds(
        backup_dir,
        interval_seconds=86_400,
        start_delay_seconds=600,
        now=100_000,
    ) == 0


def test_backup_database_promotes_verified_partial_atomically(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
        execution_mode="research",
    )
    initialize_database(settings.db_path)

    backup = backup_database(settings)

    assert backup.exists()
    assert not list(settings.backup_dir.glob("*.partial"))
    assert (settings.backup_dir / "pm_robot-latest.sqlite").exists()
