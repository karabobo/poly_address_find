import sqlite3
from io import BytesIO

import pytest

from pm_robot.config import RobotSettings
from pm_robot.ops import (
    backup_database,
    dump_database_sql,
    storage_report,
    verify_backup_database,
)
from pm_robot.storage.db import connect, initialize_database, run_migrations


def test_backup_database_creates_verified_restoreable_sqlite(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
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
    verification = verify_backup_database(backup)
    assert verification["full_check"] is False
    assert verification["quick_check"] == "not_run"


def test_dump_database_sql_stream_is_restoreable(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
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


def test_backup_database_promotes_verified_partial_atomically(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
    )
    initialize_database(settings.db_path)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()

    backup = backup_database(settings)

    assert backup.exists()
    assert not list(settings.backup_dir.glob("*.partial"))
    assert (settings.backup_dir / "pm_robot-latest.sqlite").exists()


def test_backup_database_supports_optional_full_integrity_scan(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
    )
    initialize_database(settings.db_path)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()

    backup = backup_database(settings, full_check=True)
    verification = verify_backup_database(backup, full_check=True)

    assert verification["full_check"] is True
    assert verification["quick_check"] == "ok"


def test_failed_same_second_backup_preserves_previous_verified_file(
    tmp_path, monkeypatch
):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "robot.sqlite",
        backup_dir=tmp_path / "backups",
    )
    initialize_database(settings.db_path)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
    finally:
        conn.close()
    monkeypatch.setattr(
        "pm_robot.ops.time.strftime",
        lambda *_args, **_kwargs: "20260711-000000",
    )
    first = backup_database(settings)

    def fail_verification(*_args, **_kwargs):
        raise RuntimeError("verification failed")

    monkeypatch.setattr("pm_robot.ops.verify_backup_database", fail_verification)
    with pytest.raises(RuntimeError, match="verification failed"):
        backup_database(settings)

    assert first.exists()
    assert first.stat().st_size > 0
