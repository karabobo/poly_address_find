"""SQLite connection helpers and migration runner."""

from __future__ import annotations

import contextlib
import fcntl
import sqlite3
import time
from pathlib import Path
from typing import Callable, Iterator, TypeVar

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
T = TypeVar("T")


def connect(db_path: Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    """Open a writable application connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=120, check_same_thread=check_same_thread)
    conn.execute("PRAGMA busy_timeout = 120000")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def is_sqlite_locked_error(exc: BaseException) -> bool:
    """Return true for SQLite lock contention that can be safely retried."""

    if not isinstance(exc, sqlite3.OperationalError):
        return False
    text = str(exc).lower()
    return "database is locked" in text or "database table is locked" in text


def retry_sqlite_locked(
    operation: Callable[[], T],
    *,
    rollback: Callable[[], object] | None = None,
    attempts: int = 4,
    sleep_seconds: float = 5.0,
) -> T:
    """Retry a short SQLite write section after lock contention."""

    max_attempts = max(1, attempts)
    for attempt in range(max_attempts):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if not is_sqlite_locked_error(exc):
                raise
            if rollback is not None:
                try:
                    rollback()
                except sqlite3.Error:
                    pass
            if attempt >= max_attempts - 1:
                raise
            time.sleep(max(0.0, sleep_seconds) * (attempt + 1))
    raise RuntimeError("unreachable sqlite retry state")


def connect_readonly(
    db_path: Path,
    *,
    check_same_thread: bool = True,
    timeout_seconds: int = 5,
) -> sqlite3.Connection:
    """Open a query-only connection that cannot start write transactions."""
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(
        uri,
        uri=True,
        timeout=timeout_seconds,
        check_same_thread=check_same_thread,
    )
    conn.execute(f"PRAGMA busy_timeout = {max(timeout_seconds, 0) * 1000}")
    conn.execute("PRAGMA query_only = ON")
    conn.row_factory = sqlite3.Row
    return conn


def initialize_database(db_path: Path) -> None:
    """Apply persistent SQLite settings during install or maintenance."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("PRAGMA busy_timeout = 120000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    finally:
        conn.close()


def _migration_paths() -> list[tuple[int, Path]]:
    migrations: list[tuple[int, Path]] = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        version_text = path.name.split("_", 1)[0]
        if version_text.isdigit():
            migrations.append((int(version_text), path))
    return migrations


def _applied_migration_versions(conn: sqlite3.Connection) -> set[int] | None:
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if table_exists is None:
        return None
    return {int(row[0]) for row in conn.execute("SELECT version FROM schema_migrations")}


@contextlib.contextmanager
def _migration_lock(conn: sqlite3.Connection, *, timeout_seconds: float = 120.0) -> Iterator[None]:
    """Serialize schema changes across CLI processes sharing one database."""

    database_path = str(conn.execute("PRAGMA database_list").fetchone()[2] or "")
    if not database_path:
        yield
        return

    lock_path = Path(f"{database_path}.migrate.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out waiting for migration lock: {lock_path}")
                time.sleep(0.1)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def run_migrations(conn: sqlite3.Connection) -> list[int]:
    migrations = _migration_paths()
    expected_versions = {version for version, _path in migrations}
    applied = _applied_migration_versions(conn)

    # Normal service startup must stay read-only when the schema is current.
    if applied is not None and expected_versions.issubset(applied):
        return []

    with _migration_lock(conn):
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)"
        )
        conn.commit()
        applied = _applied_migration_versions(conn) or set()
        newly_applied: list[int] = []
        for version, path in migrations:
            if version in applied:
                continue
            conn.executescript(path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (version, int(time.time())),
            )
            conn.commit()
            applied.add(version)
            newly_applied.append(version)
        return newly_applied
