"""Persistence boundary for monotonic discovery wallet levels."""

from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.wallet_levels import LevelDecision, WalletLevel


ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")
_LEVEL_RANK = {level: index for index, level in enumerate(WalletLevel)}


@dataclass(frozen=True)
class WalletLevelRecord:
    wallet: str
    level: WalletLevel
    level_reason: str
    policy_version: str
    hard_risk_block: bool
    first_seen_at: int
    last_seen_at: int
    level_updated_at: int
    updated_at: int


def normalize_wallet(wallet: str) -> str:
    """Normalize and validate one EVM wallet address."""

    normalized = str(wallet or "").strip().lower()
    if not ADDRESS_RE.fullmatch(normalized):
        raise ValueError(f"invalid wallet address: {wallet!r}")
    return normalized


def get_wallet_level(conn: sqlite3.Connection, wallet: str) -> WalletLevelRecord:
    """Return the canonical level row for one wallet."""

    normalized = normalize_wallet(wallet)
    row = conn.execute("SELECT * FROM wallet_levels WHERE wallet = ?", (normalized,)).fetchone()
    if row is None:
        raise KeyError(normalized)
    return WalletLevelRecord(
        wallet=normalized,
        level=WalletLevel(str(row["level"])),
        level_reason=str(row["level_reason"] or ""),
        policy_version=str(row["policy_version"] or ""),
        hard_risk_block=bool(row["hard_risk_block"]),
        first_seen_at=int(row["first_seen_at"] or 0),
        last_seen_at=int(row["last_seen_at"] or 0),
        level_updated_at=int(row["level_updated_at"] or 0),
        updated_at=int(row["updated_at"] or 0),
    )


def ensure_wallet_level(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    reason: str,
    now: int | None = None,
) -> WalletLevelRecord:
    """Create an L0 row or touch an existing level without changing it."""

    normalized = normalize_wallet(wallet)
    ts = int(time.time()) if now is None else int(now)
    conn.execute(
        """
        INSERT INTO wallet_levels(
            wallet, level, level_reason, first_seen_at, last_seen_at,
            level_updated_at, updated_at
        ) VALUES (?, 'l0', ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            last_seen_at = MAX(wallet_levels.last_seen_at, excluded.last_seen_at),
            updated_at = excluded.updated_at
        """,
        (normalized, str(reason or "")[:500], ts, ts, ts, ts),
    )
    return get_wallet_level(conn, normalized)


def advance_wallet_level(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    to_level: WalletLevel,
    reason: str,
    policy_version: str = "",
    facts: dict[str, Any] | None = None,
    now: int | None = None,
) -> LevelDecision:
    """Advance exactly one level; queue state and history rows are untouched."""

    normalized = normalize_wallet(wallet)
    target = WalletLevel(to_level)
    ts = int(time.time()) if now is None else int(now)
    current = get_wallet_level(conn, normalized)
    current_rank = _LEVEL_RANK[current.level]
    target_rank = _LEVEL_RANK[target]

    if target_rank < current_rank:
        raise ValueError(f"wallet level downgrade is not automatic: {current.level.value}->{target.value}")
    if target_rank > current_rank + 1:
        raise ValueError(f"wallet level may advance only one level: {current.level.value}->{target.value}")
    if target_rank == current_rank or current.hard_risk_block:
        return LevelDecision(current.level, "hard_risk_block" if current.hard_risk_block else "unchanged")

    compact_reason = str(reason or "level_advanced")[:500]
    compact_policy = str(policy_version or "")[:200]
    conn.execute(
        """
        UPDATE wallet_levels
        SET level = ?, level_reason = ?, policy_version = ?,
            level_updated_at = ?, updated_at = ?
        WHERE wallet = ?
        """,
        (target.value, compact_reason, compact_policy, ts, ts, normalized),
    )
    conn.execute(
        """
        INSERT INTO wallet_level_events(
            wallet, from_level, to_level, reason, policy_version, facts_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            normalized,
            current.level.value,
            target.value,
            compact_reason,
            compact_policy,
            json.dumps(facts or {}, ensure_ascii=False, sort_keys=True, default=str),
            ts,
        ),
    )
    return LevelDecision(target, compact_reason)


def set_wallet_hard_risk_block(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    blocked: bool,
    now: int | None = None,
) -> WalletLevelRecord:
    """Set a conservative risk stop without rewriting the wallet's level."""

    normalized = normalize_wallet(wallet)
    ts = int(time.time()) if now is None else int(now)
    ensure_wallet_level(conn, normalized, reason="risk_state", now=ts)
    conn.execute(
        "UPDATE wallet_levels SET hard_risk_block = ?, updated_at = ? WHERE wallet = ?",
        (1 if blocked else 0, ts, normalized),
    )
    return get_wallet_level(conn, normalized)
