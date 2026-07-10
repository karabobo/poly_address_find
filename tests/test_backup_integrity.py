import sqlite3
from io import BytesIO

from pm_robot.config import RobotSettings
from pm_robot.ops import backup_database, dump_database_sql, storage_report
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
