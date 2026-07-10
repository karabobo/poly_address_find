import sqlite3
from pathlib import Path

import pytest

from pm_robot.storage.db import (
    connect,
    connect_readonly,
    initialize_database,
    is_sqlite_locked_error,
    retry_sqlite_locked,
)


def test_initialize_database_enables_wal_and_writer_connects(tmp_path: Path):
    db_path = tmp_path / "robot.sqlite"
    initialize_database(db_path)

    conn = connect(db_path)
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        conn.execute("CREATE TABLE sample(value INTEGER)")
        conn.commit()
    finally:
        conn.close()


def test_readonly_connection_rejects_writes(tmp_path: Path):
    db_path = tmp_path / "robot.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE sample(value INTEGER)")
    conn.commit()
    conn.close()

    readonly = connect_readonly(db_path)
    try:
        assert readonly.execute("SELECT COUNT(*) FROM sample").fetchone()[0] == 0
        with pytest.raises(sqlite3.OperationalError):
            readonly.execute("INSERT INTO sample(value) VALUES (1)")
    finally:
        readonly.close()


def test_retry_sqlite_locked_rolls_back_and_retries():
    attempts = {"count": 0, "rollbacks": 0}

    def operation() -> str:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return "ok"

    assert is_sqlite_locked_error(sqlite3.OperationalError("database is locked"))
    assert (
        retry_sqlite_locked(
            operation,
            rollback=lambda: attempts.__setitem__("rollbacks", attempts["rollbacks"] + 1),
            attempts=2,
            sleep_seconds=0,
        )
        == "ok"
    )
    assert attempts == {"count": 2, "rollbacks": 1}
