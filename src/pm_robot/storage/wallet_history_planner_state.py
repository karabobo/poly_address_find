"""Bounded refresh helpers for compact wallet history planner state."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any


STATE_COLUMNS = (
    "wallet",
    "level",
    "hard_risk_block",
    "last_seen_at",
    "current_depth",
    "current_methodology_version",
    "methodology_stale",
    "current_pnl_methodology_version",
    "pnl_captured_at",
    "pnl_refresh_needed",
    "activity_refresh_needed",
    "research_score",
    "summary_updated_at",
    "sample_trade_count",
    "sample_volume_usdc",
    "sample_market_count",
    "target_depth",
    "refresh_lane",
    "urgency",
    "is_eligible",
    "next_refresh_at",
    "refreshed_at",
)

WALLET_HISTORY_PLANNER_MISSING_STATE_SQL = """
    SELECT levels.wallet, NULL AS dirty_generation
    FROM wallet_levels AS levels
    LEFT JOIN wallet_history_planner_state AS state
      ON state.wallet = levels.wallet
    WHERE state.wallet IS NULL
      AND levels.level IN ('l2', 'l3', 'l4', 'l5', 'l6')
    ORDER BY levels.level ASC, levels.wallet ASC
    LIMIT ?
"""

WALLET_HISTORY_PLANNER_DIRTY_CLAIM_SQL = """
    SELECT wallet, dirty_generation
    FROM wallet_history_planner_dirty
    ORDER BY dirty_at ASC, wallet ASC
    LIMIT ?
"""

WALLET_HISTORY_PLANNER_DIRTY_BACKLOG_SQL = """
    SELECT 1
    FROM wallet_history_planner_dirty
    LIMIT 1
"""

WALLET_HISTORY_PLANNER_SIGHTING_CLAIM_SQL = """
    SELECT sighting.wallet,
           sighting.last_seen_at,
           sighting.dirty_generation,
           state.level,
           state.current_depth,
           state.summary_updated_at,
           state.next_refresh_at
    FROM wallet_history_planner_sighting_dirty AS sighting
    JOIN wallet_history_planner_state AS state
      ON state.wallet = sighting.wallet
    LEFT JOIN wallet_history_planner_dirty AS full_dirty
      ON full_dirty.wallet = sighting.wallet
    WHERE full_dirty.wallet IS NULL
    ORDER BY sighting.dirty_at ASC, sighting.wallet ASC
    LIMIT ?
"""

WALLET_HISTORY_PLANNER_SIGHTING_BACKLOG_SQL = """
    SELECT 1
    FROM wallet_history_planner_sighting_dirty
    LIMIT 1
"""


@dataclass(frozen=True)
class WalletHistoryPlannerRefreshClaim:
    wallet: str
    dirty_generation: int | None


@dataclass(frozen=True)
class WalletHistoryPlannerSightingClaim:
    wallet: str
    last_seen_at: int
    dirty_generation: int
    level: str
    current_depth: str
    summary_updated_at: int
    next_refresh_at: int


def select_wallet_history_planner_refresh_wallets(
    conn: sqlite3.Connection,
    *,
    limit: int,
    now: int,
) -> list[WalletHistoryPlannerRefreshClaim]:
    """Return a bounded dirty/missing wallet set for state rebuild."""

    bounded = max(0, int(limit))
    if bounded == 0:
        return []
    claims: list[WalletHistoryPlannerRefreshClaim] = []
    seen: set[str] = set()

    def add_rows(rows) -> None:
        for row in rows:
            wallet = str(row["wallet"])
            if wallet in seen:
                continue
            seen.add(wallet)
            generation = row["dirty_generation"]
            claims.append(
                WalletHistoryPlannerRefreshClaim(
                    wallet=wallet,
                    dirty_generation=(
                        int(generation) if generation is not None else None
                    ),
                )
            )

    add_rows(
        conn.execute(
            WALLET_HISTORY_PLANNER_DIRTY_CLAIM_SQL,
            (bounded,),
        )
    )
    if len(claims) >= bounded:
        return claims

    add_rows(
        conn.execute(
            """
            SELECT state.wallet, NULL AS dirty_generation
            FROM wallet_history_planner_state AS state
            WHERE state.next_refresh_at > 0
              AND state.next_refresh_at <= ?
            ORDER BY state.next_refresh_at ASC, state.wallet ASC
            LIMIT ?
            """,
            (int(now), bounded - len(claims)),
        )
    )
    if len(claims) >= bounded:
        return claims

    add_rows(
        conn.execute(
            WALLET_HISTORY_PLANNER_MISSING_STATE_SQL,
            (bounded - len(claims),),
        )
    )
    return claims


def wallet_history_planner_bootstrap_complete(conn: sqlite3.Connection) -> bool:
    """True when every relevant L2-L6 wallet has compact planner state."""

    return (
        conn.execute(
            """
            SELECT 1
            FROM wallet_levels AS levels
            LEFT JOIN wallet_history_planner_state AS state
              ON state.wallet = levels.wallet
            WHERE levels.level IN ('l2', 'l3', 'l4', 'l5', 'l6')
              AND state.wallet IS NULL
            LIMIT 1
            """
        ).fetchone()
        is None
    )


def wallet_history_planner_dirty_backlog_empty(conn: sqlite3.Connection) -> bool:
    """True when no relevant dirty row remains to make state stale."""

    return (
        conn.execute(WALLET_HISTORY_PLANNER_DIRTY_BACKLOG_SQL).fetchone()
        is None
    )


def select_wallet_history_planner_sightings(
    conn: sqlite3.Connection,
    *,
    limit: int,
) -> list[WalletHistoryPlannerSightingClaim]:
    """Claim coalesced sighting changes without reading evidence side tables."""

    bounded = max(0, int(limit))
    if bounded == 0:
        return []
    return [
        WalletHistoryPlannerSightingClaim(
            wallet=str(row["wallet"]),
            last_seen_at=int(row["last_seen_at"] or 0),
            dirty_generation=int(row["dirty_generation"]),
            level=str(row["level"] or ""),
            current_depth=str(row["current_depth"] or ""),
            summary_updated_at=int(row["summary_updated_at"] or 0),
            next_refresh_at=int(row["next_refresh_at"] or 0),
        )
        for row in conn.execute(
            WALLET_HISTORY_PLANNER_SIGHTING_CLAIM_SQL,
            (bounded,),
        )
    ]


def wallet_history_planner_sighting_backlog_empty(
    conn: sqlite3.Connection,
) -> bool:
    """True when every coalesced sighting has reached compact state."""

    return (
        conn.execute(WALLET_HISTORY_PLANNER_SIGHTING_BACKLOG_SQL).fetchone()
        is None
    )


def apply_wallet_history_planner_sightings(
    conn: sqlite3.Connection,
    *,
    updates: list[tuple[int, int, str]],
    claims: list[WalletHistoryPlannerSightingClaim],
) -> None:
    """Apply state-only sighting updates and clear observed generations."""

    if not claims:
        return
    if updates:
        claim_generations = {
            claim.wallet: claim.dirty_generation for claim in claims
        }
        guarded_updates = [
            (last_seen_at, next_refresh_at, wallet, claim_generations[wallet])
            for last_seen_at, next_refresh_at, wallet in updates
        ]
        sql = """
            UPDATE wallet_history_planner_state
            SET last_seen_at = MAX(last_seen_at, ?),
                next_refresh_at = ?
            WHERE wallet = ?
              AND EXISTS (
                    SELECT 1
                    FROM wallet_history_planner_sighting_dirty AS sighting
                    WHERE sighting.wallet = wallet_history_planner_state.wallet
                      AND sighting.dirty_generation = ?
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM wallet_history_planner_dirty AS full_dirty
                    WHERE full_dirty.wallet = wallet_history_planner_state.wallet
              )
        """
        executemany = getattr(conn, "executemany", None)
        if executemany is None:
            for item in guarded_updates:
                conn.execute(sql, item)
        else:
            executemany(sql, guarded_updates)
    observed = [
        (claim.wallet, claim.dirty_generation)
        for claim in claims
    ]
    sql = (
        "DELETE FROM wallet_history_planner_sighting_dirty "
        "WHERE wallet = ? AND dirty_generation = ?"
    )
    executemany = getattr(conn, "executemany", None)
    if executemany is None:
        for item in observed:
            conn.execute(sql, item)
    else:
        executemany(sql, observed)


def load_wallet_history_planner_level_rows(
    conn: sqlite3.Connection,
    *,
    wallets: list[str],
) -> list[sqlite3.Row]:
    if not wallets:
        return []
    rows: list[sqlite3.Row] = []
    for chunk in _chunks(wallets, limit=_sqlite_variable_limit(conn)):
        placeholders = ", ".join("?" for _ in chunk)
        rows.extend(
            conn.execute(
                f"""
                SELECT wallet, level, hard_risk_block, last_seen_at
                FROM wallet_levels
                WHERE wallet IN ({placeholders})
                """,
                tuple(chunk),
            )
        )
    return rows


def replace_wallet_history_planner_state_rows(
    conn: sqlite3.Connection,
    *,
    claims: list[WalletHistoryPlannerRefreshClaim],
    rows: list[dict[str, Any]],
) -> None:
    """Atomically replace state and clear only the observed dirty generations."""

    if not claims:
        return
    wallets = [claim.wallet for claim in claims]
    chunks = _chunks(wallets, limit=_sqlite_variable_limit(conn))
    for chunk in chunks:
        placeholders = ", ".join("?" for _ in chunk)
        conn.execute(
            f"DELETE FROM wallet_history_planner_state WHERE wallet IN ({placeholders})",
            tuple(chunk),
        )
    if rows:
        column_list = ", ".join(STATE_COLUMNS)
        value_list = ", ".join("?" for _ in STATE_COLUMNS)
        sql = f"""
        INSERT INTO wallet_history_planner_state({column_list})
        VALUES ({value_list})
        """
        params = [tuple(row[column] for column in STATE_COLUMNS) for row in rows]
        executemany = getattr(conn, "executemany", None)
        if executemany is None:
            for item in params:
                conn.execute(sql, item)
        else:
            executemany(sql, params)
    observed = [
        (claim.wallet, claim.dirty_generation)
        for claim in claims
        if claim.dirty_generation is not None
    ]
    if observed:
        sql = (
            "DELETE FROM wallet_history_planner_dirty "
            "WHERE wallet = ? AND dirty_generation = ?"
        )
        executemany = getattr(conn, "executemany", None)
        if executemany is None:
            for item in observed:
                conn.execute(sql, item)
        else:
            executemany(sql, observed)


def _sqlite_variable_limit(conn: sqlite3.Connection) -> int:
    getlimit = getattr(conn, "getlimit", None)
    if getlimit is None:
        return 999
    try:
        limit = int(getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
    except (AttributeError, TypeError, ValueError, sqlite3.Error):
        return 999
    return max(1, limit)


def _chunks(items: list[Any], *, limit: int) -> list[list[Any]]:
    chunk_size = max(1, int(limit))
    return [
        items[index : index + chunk_size]
        for index in range(0, len(items), chunk_size)
    ]
