"""Cross-process upstream request pacing backed by the shared SQLite database."""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Callable, Iterable


RATE_LIMIT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS api_rate_limit_state (
    scope TEXT PRIMARY KEY,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    window_seconds REAL NOT NULL CHECK (window_seconds > 0),
    next_permit_at REAL NOT NULL DEFAULT 0,
    cooldown_until REAL NOT NULL DEFAULT 0,
    last_status_code INTEGER,
    last_retry_after_seconds REAL NOT NULL DEFAULT 0,
    total_permits INTEGER NOT NULL DEFAULT 0,
    total_cooldowns INTEGER NOT NULL DEFAULT 0,
    last_cooldown_reason TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_api_rate_limit_state_cooldown
    ON api_rate_limit_state(cooldown_until DESC, updated_at DESC);
"""


@dataclass(frozen=True)
class RateLimitScope:
    """One shared upstream budget such as data:* or data:/activity."""

    name: str
    capacity: int
    window_seconds: float

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("rate-limit scope name is required")
        if self.capacity <= 0:
            raise ValueError("rate-limit capacity must be positive")
        if self.window_seconds <= 0:
            raise ValueError("rate-limit window must be positive")

    @property
    def interval_seconds(self) -> float:
        return float(self.window_seconds) / int(self.capacity)


@dataclass(frozen=True)
class RateLimitReservation:
    scheduled_at: float
    wait_seconds: float
    coordinated: bool
    deferred: bool = False
    error: str = ""


class SharedRateLimitDeferred(RuntimeError):
    """Signal that queue scheduling should handle a long shared wait."""

    def __init__(self, retry_after_seconds: float):
        super().__init__("shared upstream request budget is cooling down")
        self.retry_after_seconds = max(0.0, float(retry_after_seconds))


class SharedRateLimitUnavailable(RuntimeError):
    """Signal that shared coordination failed and network work must be deferred."""

    def __init__(self, error: str, *, retry_after_seconds: float = 5.0):
        super().__init__(error or "shared upstream request budget is unavailable")
        self.retry_after_seconds = max(1.0, float(retry_after_seconds))


class SharedApiRateLimiter:
    """Reserve smooth request slots across containers sharing one SQLite file."""

    def __init__(
        self,
        db_path: Path,
        *,
        lock_timeout_seconds: float = 2.0,
        max_block_seconds: float = 30.0,
        initialize_schema: bool = False,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.db_path = Path(db_path)
        self.lock_timeout_seconds = max(0.0, float(lock_timeout_seconds))
        self.max_block_seconds = max(0.0, float(max_block_seconds))
        self.initialize_schema = bool(initialize_schema)
        self._clock = clock
        self._sleep = sleeper
        self._schema_ready = False
        self._schema_lock = Lock()
        self.last_error = ""

    def wait(self, scopes: Iterable[RateLimitScope]) -> None:
        """Wait for a shared slot or defer before any uncoordinated network request."""
        normalized = _normalize_scopes(scopes)
        if not normalized:
            return
        while True:
            reservation = self.reserve(
                normalized,
                max_wait_seconds=self.max_block_seconds,
            )
            if not reservation.coordinated:
                raise SharedRateLimitUnavailable(reservation.error)
            if reservation.deferred:
                raise SharedRateLimitDeferred(reservation.wait_seconds)
            remaining = max(0.0, reservation.scheduled_at - self._clock())
            if remaining > 0:
                self._sleep(remaining)
            cooldown_wait = self.current_cooldown_wait(normalized)
            if cooldown_wait <= 0:
                return
            if cooldown_wait > self.max_block_seconds:
                raise SharedRateLimitDeferred(cooldown_wait)
            self._sleep(cooldown_wait)

    def reserve(
        self,
        scopes: Iterable[RateLimitScope],
        *,
        now: float | None = None,
        max_wait_seconds: float | None = None,
    ) -> RateLimitReservation:
        """Atomically reserve one permit for every requested scope."""
        normalized = _normalize_scopes(scopes)
        current = self._clock() if now is None else float(now)
        if not normalized:
            return RateLimitReservation(current, 0.0, True)
        conn: sqlite3.Connection | None = None
        stage = "connect"
        try:
            conn = self._connect()
            stage = "begin"
            conn.execute("BEGIN IMMEDIATE")
            stage = "read_state"
            placeholders = ",".join("?" for _ in normalized)
            rows = conn.execute(
                f"""
                SELECT scope, next_permit_at, cooldown_until
                FROM api_rate_limit_state
                WHERE scope IN ({placeholders})
                """,
                tuple(scope.name for scope in normalized),
            ).fetchall()
            state = {str(row["scope"]): row for row in rows}
            scheduled_at = current
            for scope in normalized:
                row = state.get(scope.name)
                if row is not None:
                    scheduled_at = max(
                        scheduled_at,
                        float(row["next_permit_at"] or 0),
                        float(row["cooldown_until"] or 0),
                    )
            wait_seconds = max(0.0, scheduled_at - current)
            if max_wait_seconds is not None and wait_seconds > max(0.0, float(max_wait_seconds)):
                conn.rollback()
                self.last_error = ""
                return RateLimitReservation(
                    scheduled_at=scheduled_at,
                    wait_seconds=wait_seconds,
                    coordinated=True,
                    deferred=True,
                )
            for scope in normalized:
                stage = "write_state"
                row = state.get(scope.name)
                next_permit_at = max(
                    scheduled_at,
                    float(row["next_permit_at"] or 0) if row is not None else 0.0,
                    float(row["cooldown_until"] or 0) if row is not None else 0.0,
                ) + scope.interval_seconds
                conn.execute(
                    """
                    INSERT INTO api_rate_limit_state(
                        scope, capacity, window_seconds, next_permit_at,
                        cooldown_until, total_permits, updated_at
                    ) VALUES (?, ?, ?, ?, 0, 1, ?)
                    ON CONFLICT(scope) DO UPDATE SET
                        capacity = excluded.capacity,
                        window_seconds = excluded.window_seconds,
                        next_permit_at = excluded.next_permit_at,
                        total_permits = api_rate_limit_state.total_permits + 1,
                        updated_at = excluded.updated_at
                    """,
                    (
                        scope.name,
                        scope.capacity,
                        scope.window_seconds,
                        next_permit_at,
                        current,
                    ),
                )
            stage = "commit"
            conn.commit()
            self.last_error = ""
            return RateLimitReservation(
                scheduled_at=scheduled_at,
                wait_seconds=wait_seconds,
                coordinated=True,
            )
        except sqlite3.Error as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
            self.last_error = f"reserve_{stage}: {type(exc).__name__}: {exc}"[:500]
            return RateLimitReservation(
                scheduled_at=current,
                wait_seconds=0.0,
                coordinated=False,
                error=self.last_error,
            )
        finally:
            if conn is not None:
                conn.close()

    def record_cooldown(
        self,
        scopes: Iterable[RateLimitScope],
        *,
        retry_after_seconds: float,
        status_code: int = 429,
        reason: str = "rate_limited",
        now: float | None = None,
    ) -> bool:
        """Propagate an upstream cooldown to every process using these scopes."""
        normalized = _normalize_scopes(scopes)
        if not normalized:
            return True
        current = self._clock() if now is None else float(now)
        retry_after = max(0.0, float(retry_after_seconds))
        cooldown_until = current + retry_after
        conn: sqlite3.Connection | None = None
        try:
            conn = self._connect()
            conn.execute("BEGIN IMMEDIATE")
            for scope in normalized:
                conn.execute(
                    """
                    INSERT INTO api_rate_limit_state(
                        scope, capacity, window_seconds, next_permit_at,
                        cooldown_until, last_status_code, last_retry_after_seconds,
                        total_cooldowns, last_cooldown_reason, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(scope) DO UPDATE SET
                        capacity = excluded.capacity,
                        window_seconds = excluded.window_seconds,
                        next_permit_at = MAX(api_rate_limit_state.next_permit_at, excluded.next_permit_at),
                        cooldown_until = MAX(api_rate_limit_state.cooldown_until, excluded.cooldown_until),
                        last_status_code = excluded.last_status_code,
                        last_retry_after_seconds = excluded.last_retry_after_seconds,
                        total_cooldowns = api_rate_limit_state.total_cooldowns + 1,
                        last_cooldown_reason = excluded.last_cooldown_reason,
                        updated_at = excluded.updated_at
                    """,
                    (
                        scope.name,
                        scope.capacity,
                        scope.window_seconds,
                        cooldown_until,
                        cooldown_until,
                        int(status_code),
                        retry_after,
                        reason[:500],
                        current,
                    ),
                )
            conn.commit()
            self.last_error = ""
            return True
        except sqlite3.Error as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
            self.last_error = f"{type(exc).__name__}: {exc}"[:500]
            return False
        finally:
            if conn is not None:
                conn.close()

    def current_cooldown_wait(
        self,
        scopes: Iterable[RateLimitScope],
        *,
        now: float | None = None,
    ) -> float:
        normalized = _normalize_scopes(scopes)
        if not normalized:
            return 0.0
        current = self._clock() if now is None else float(now)
        conn: sqlite3.Connection | None = None
        try:
            conn = self._connect()
            placeholders = ",".join("?" for _ in normalized)
            row = conn.execute(
                f"""
                SELECT MAX(cooldown_until) AS cooldown_until
                FROM api_rate_limit_state
                WHERE scope IN ({placeholders})
                """,
                tuple(scope.name for scope in normalized),
            ).fetchone()
            self.last_error = ""
            return max(0.0, float(row["cooldown_until"] or 0) - current)
        except sqlite3.Error as exc:
            self.last_error = f"cooldown_read: {type(exc).__name__}: {exc}"[:500]
            raise SharedRateLimitUnavailable(self.last_error) from exc
        finally:
            if conn is not None:
                conn.close()

    def _connect(self) -> sqlite3.Connection:
        if self.initialize_schema:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=self.lock_timeout_seconds)
        try:
            conn.execute(f"PRAGMA busy_timeout = {int(self.lock_timeout_seconds * 1000)}")
            conn.row_factory = sqlite3.Row
            if self.initialize_schema and not self._schema_ready:
                with self._schema_lock:
                    if not self._schema_ready:
                        schema_exists = conn.execute(
                            """
                            SELECT 1
                            FROM sqlite_master
                            WHERE type = 'table' AND name = 'api_rate_limit_state'
                            """
                        ).fetchone()
                        if schema_exists is None:
                            conn.execute("PRAGMA journal_mode = WAL")
                            conn.executescript(RATE_LIMIT_SCHEMA_SQL)
                            conn.commit()
                        self._schema_ready = True
            return conn
        except Exception:
            conn.close()
            raise


def sqlite_main_database_path(conn: sqlite3.Connection) -> Path | None:
    """Return the file-backed main database path, or None for in-memory databases."""
    try:
        rows = conn.execute("PRAGMA database_list").fetchall()
    except sqlite3.Error:
        return None
    for row in rows:
        if str(row[1]) != "main":
            continue
        value = str(row[2] or "")
        if not value or value == ":memory:":
            return None
        return Path(value)
    return None


def writable_sqlite_main_database_path(conn: sqlite3.Connection) -> Path | None:
    """Return the main database path only when the supplied connection is writable."""
    try:
        row = conn.execute("PRAGMA query_only").fetchone()
    except sqlite3.Error:
        return None
    if row is not None and int(row[0] or 0):
        return None
    return sqlite_main_database_path(conn)


def api_rate_limit_summary(
    conn: sqlite3.Connection,
    *,
    now: float | None = None,
) -> dict[str, object]:
    current = time.time() if now is None else float(now)
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
                scope, capacity, window_seconds, next_permit_at,
                cooldown_until, last_status_code, last_retry_after_seconds,
                total_permits, total_cooldowns, last_cooldown_reason, updated_at
            FROM api_rate_limit_state
            ORDER BY scope ASC
            """
        ).fetchall()
    ]
    active_cooldowns = 0
    for row in rows:
        cooldown_until = float(row.get("cooldown_until") or 0)
        next_permit_at = float(row.get("next_permit_at") or 0)
        remaining = max(0.0, cooldown_until - current)
        if remaining > 0:
            active_cooldowns += 1
            state = "cooldown"
        elif next_permit_at > current:
            state = "paced"
        else:
            state = "idle"
        row["state"] = state
        row["rate"] = f'{int(row.get("capacity") or 0)}/{float(row.get("window_seconds") or 0):g}s'
        row["next_permit_at"] = int(next_permit_at)
        row["cooldown_until"] = int(cooldown_until)
        row["cooldown_remaining_seconds"] = round(remaining, 3)
        row["updated_at"] = int(float(row.get("updated_at") or 0))
    return {
        "scope_count": len(rows),
        "active_cooldowns": active_cooldowns,
        "total_permits": sum(int(row.get("total_permits") or 0) for row in rows),
        "total_cooldowns": sum(int(row.get("total_cooldowns") or 0) for row in rows),
        "rows": rows,
    }


def api_rate_limit_summary_from_path(
    db_path: Path,
    *,
    now: float | None = None,
) -> dict[str, object]:
    """Read the dedicated coordination database without touching the main wallet DB."""

    path = Path(db_path)
    if not path.exists():
        summary = _empty_api_rate_limit_summary()
        summary["coordination_error"] = "coordination database is not initialized"
        return summary
    conn: sqlite3.Connection | None = None
    try:
        uri = f"file:{path.resolve().as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=0.5)
        conn.execute("PRAGMA busy_timeout = 500")
        conn.row_factory = sqlite3.Row
        summary = api_rate_limit_summary(conn, now=now)
        summary["available"] = True
        return summary
    except sqlite3.Error as exc:
        summary = _empty_api_rate_limit_summary()
        summary["coordination_error"] = f"{type(exc).__name__}: {exc}"[:500]
        return summary
    finally:
        if conn is not None:
            conn.close()


def configured_rate_limit_db_path() -> Path | None:
    value = os.environ.get("PM_ROBOT_RATE_LIMIT_DB_PATH", "").strip()
    return Path(value) if value else None


def configured_rate_limit_lock_timeout_seconds(default: float = 10.0) -> float:
    try:
        return max(0.1, float(os.environ.get("PM_ROBOT_RATE_LIMIT_LOCK_TIMEOUT_SECONDS", default)))
    except (TypeError, ValueError):
        return max(0.1, float(default))


def _empty_api_rate_limit_summary() -> dict[str, object]:
    return {
        "scope_count": 0,
        "active_cooldowns": 0,
        "total_permits": 0,
        "total_cooldowns": 0,
        "rows": [],
        "available": False,
    }


def api_rate_limit_cooldown_wait(
    conn: sqlite3.Connection,
    scope_names: Iterable[str],
    *,
    now: float | None = None,
) -> float:
    """Return the longest active cooldown for the requested shared scopes."""
    names = sorted({str(name).strip() for name in scope_names if str(name).strip()})
    if not names:
        return 0.0
    current = time.time() if now is None else float(now)
    placeholders = ",".join("?" for _ in names)
    row = conn.execute(
        f"""
        SELECT MAX(cooldown_until) AS cooldown_until
        FROM api_rate_limit_state
        WHERE scope IN ({placeholders})
        """,
        tuple(names),
    ).fetchone()
    return max(0.0, float(row["cooldown_until"] or 0) - current)


def _normalize_scopes(scopes: Iterable[RateLimitScope]) -> list[RateLimitScope]:
    by_name: dict[str, RateLimitScope] = {}
    for scope in scopes:
        existing = by_name.get(scope.name)
        if existing is None or scope.interval_seconds > existing.interval_seconds:
            by_name[scope.name] = scope
    return [by_name[name] for name in sorted(by_name)]
