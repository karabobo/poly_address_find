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
DATABASE_ACCESS_LOCK_SUFFIX = ".access.lock"
CONTROL_PLANE_LOCK_SUFFIX = ".control-plane"
# Migration 62 establishes the research-only baseline used to recover missing
# historical migration markers. Later cleanup migrations remain idempotent.
RESEARCH_SCHEMA_BASELINE_VERSION = 62
MIGRATION_SCHEMA_POSTCONDITIONS = {
    50: ("wallet_activity_watermarks", "activity_count"),
    59: ("wallet_history_artifacts", "purged_at"),
    60: ("wallet_level_selections", "research_score"),
    61: ("wallet_history_artifacts", "purge_started_at"),
    62: ("runtime_heartbeats", "name"),
    66: ("wallet_l6_validations", "validation_id"),
    67: ("wallet_l6_validations", "official_all_pnl_usdc"),
}

MIGRATION_POSTCONDITION_REPAIRS = {
    59: """
        CREATE INDEX IF NOT EXISTS idx_wallet_history_artifacts_gc
        ON wallet_history_artifacts(status, purged_at, updated_at, wallet);
    """,
    60: """
        UPDATE wallet_level_selections
        SET research_score = (
            SELECT summary.research_score
            FROM wallet_history_summaries AS summary
            WHERE summary.wallet = wallet_level_selections.wallet
              AND summary.artifact_id = wallet_level_selections.evidence_artifact_id
        )
        WHERE research_score IS NULL;

        CREATE INDEX IF NOT EXISTS idx_wallet_level_selections_reference
        ON wallet_level_selections(target_level, policy_version, decided_at DESC, wallet);
    """,
    61: """
        CREATE INDEX IF NOT EXISTS idx_wallet_history_artifacts_purge
        ON wallet_history_artifacts(
            status, purged_at, purge_started_at, updated_at, wallet
        );
    """,
    62: """
        CREATE INDEX IF NOT EXISTS idx_runtime_heartbeats_name_time
        ON runtime_heartbeats(name, finished_at DESC, heartbeat_id DESC);
    """,
    66: """
        CREATE INDEX IF NOT EXISTS idx_wallet_l6_validations_latest
        ON wallet_l6_validations(wallet, validated_at DESC, validation_id DESC);
        CREATE INDEX IF NOT EXISTS idx_wallet_l6_validations_decision
        ON wallet_l6_validations(decision, validated_at DESC, wallet);
    """,
}


class _AccessLockedConnection(sqlite3.Connection):
    """SQLite connection that owns a shared database maintenance lock."""

    _pm_robot_access_lock: object | None = None

    def close(self) -> None:
        lock_file = self._pm_robot_access_lock
        self._pm_robot_access_lock = None
        try:
            super().close()
        finally:
            if lock_file is not None:
                _release_database_access_lock(lock_file)


def connect(db_path: Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    """Open a writable application connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    access_lock = _acquire_database_access_lock(db_path, exclusive=False)
    conn: _AccessLockedConnection | None = None
    try:
        conn = sqlite3.connect(
            db_path,
            timeout=120,
            check_same_thread=check_same_thread,
            factory=_AccessLockedConnection,
        )
        conn._pm_robot_access_lock = access_lock
        conn.execute("PRAGMA busy_timeout = 120000")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        return conn
    except BaseException:
        if conn is None:
            _release_database_access_lock(access_lock)
        else:
            conn.close()
        raise


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
    access_lock = _acquire_database_access_lock(
        db_path,
        exclusive=False,
        timeout_seconds=max(0, timeout_seconds),
    )
    conn: _AccessLockedConnection | None = None
    try:
        conn = sqlite3.connect(
            uri,
            uri=True,
            timeout=timeout_seconds,
            check_same_thread=check_same_thread,
            factory=_AccessLockedConnection,
        )
        conn._pm_robot_access_lock = access_lock
        conn.execute(f"PRAGMA busy_timeout = {max(timeout_seconds, 0) * 1000}")
        conn.execute("PRAGMA query_only = ON")
        conn.row_factory = sqlite3.Row
        return conn
    except BaseException:
        if conn is None:
            _release_database_access_lock(access_lock)
        else:
            conn.close()
        raise


def initialize_database(db_path: Path) -> None:
    """Apply persistent SQLite settings during install or maintenance."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with database_access_guard(db_path, exclusive=False):
        conn = sqlite3.connect(db_path, timeout=120)
        try:
            conn.execute("PRAGMA busy_timeout = 120000")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
        finally:
            conn.close()


@contextlib.contextmanager
def database_access_guard(
    db_path: Path,
    *,
    exclusive: bool,
    timeout_seconds: float = 120.0,
) -> Iterator[None]:
    """Coordinate ordinary connections with offline atomic database replacement."""

    lock_file = _acquire_database_access_lock(
        db_path,
        exclusive=exclusive,
        timeout_seconds=timeout_seconds,
    )
    try:
        yield
    finally:
        _release_database_access_lock(lock_file)


@contextlib.contextmanager
def database_control_plane_guard(
    db_path: Path,
    *,
    timeout_seconds: float = 120.0,
) -> Iterator[None]:
    """Give research-control priority over low-priority retention writes."""

    canonical_path = db_path.expanduser().resolve()
    lock_key = Path(f"{canonical_path}{CONTROL_PLANE_LOCK_SUFFIX}")
    with database_access_guard(
        lock_key,
        exclusive=True,
        timeout_seconds=timeout_seconds,
    ):
        yield


def _acquire_database_access_lock(
    db_path: Path,
    *,
    exclusive: bool,
    timeout_seconds: float = 120.0,
):
    lock_path = Path(f"{db_path}{DATABASE_ACCESS_LOCK_SUFFIX}")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("a+", encoding="utf-8")
    operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    try:
        while True:
            try:
                fcntl.flock(lock_file.fileno(), operation | fcntl.LOCK_NB)
                return lock_file
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    mode = "exclusive" if exclusive else "shared"
                    raise TimeoutError(
                        f"timed out waiting for {mode} database access lock: {lock_path}"
                    )
                time.sleep(0.1)
    except BaseException:
        lock_file.close()
        raise


def _release_database_access_lock(lock_file) -> None:
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        lock_file.close()


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


def pending_migration_versions(conn: sqlite3.Connection) -> list[int]:
    """Return unapplied migration versions without changing the database."""

    applied = _applied_migration_versions(conn) or set()
    return [version for version, _path in _migration_paths() if version not in applied]


def _migration_schema_postcondition_satisfied(
    conn: sqlite3.Connection,
    version: int,
) -> bool:
    """Recognize a completed non-idempotent schema step after an interrupted marker write."""

    postcondition = MIGRATION_SCHEMA_POSTCONDITIONS.get(version)
    if postcondition is None:
        return False
    table, column = postcondition
    return any(
        str(row[1]) == column
        for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    )


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

        # Migration 62 replaces the historical all-in-one schema. If its schema
        # is already present, missing older markers must not recreate retired
        # runtime tables during recovery from marker drift.
        if _migration_schema_postcondition_satisfied(
            conn,
            RESEARCH_SCHEMA_BASELINE_VERSION,
        ):
            superseded = [
                version
                for version, _path in migrations
                if version <= RESEARCH_SCHEMA_BASELINE_VERSION and version not in applied
            ]
            if superseded:
                applied_at = int(time.time())
                conn.executemany(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) "
                    "VALUES (?, ?)",
                    ((version, applied_at) for version in superseded),
                )
                repair_sql = MIGRATION_POSTCONDITION_REPAIRS.get(
                    RESEARCH_SCHEMA_BASELINE_VERSION,
                    "",
                ).strip()
                if repair_sql:
                    conn.executescript(repair_sql)
                conn.commit()
                applied.update(superseded)
                newly_applied.extend(superseded)

        for version, path in migrations:
            if version in applied:
                continue
            if _migration_schema_postcondition_satisfied(conn, version):
                repair_sql = MIGRATION_POSTCONDITION_REPAIRS.get(version, "").strip()
                if repair_sql:
                    conn.executescript(repair_sql)
                conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (version, int(time.time())),
                )
                conn.commit()
                applied.add(version)
                newly_applied.append(version)
                continue
            foreign_keys = int(conn.execute("PRAGMA foreign_keys").fetchone()[0])
            try:
                conn.executescript(path.read_text(encoding="utf-8"))
                conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (version, int(time.time())),
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                # A failed table-rebuild migration can stop before its final
                # PRAGMA. Restore the connection's original integrity mode.
                conn.execute(f"PRAGMA foreign_keys = {foreign_keys}")
            applied.add(version)
            newly_applied.append(version)
        return newly_applied
