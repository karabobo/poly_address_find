"""Single ingress boundary for wallet discovery sources.

Ingress records provenance and recent sightings, then may advance a wallet from
L0 to L1. It never schedules history collection or performs quality scoring.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Iterable

from pm_robot.models import CandidateAddress
from pm_robot.storage.repository import upsert_candidate
from pm_robot.storage.wallet_levels import (
    advance_wallet_level,
    ensure_wallet_level,
    get_wallet_level,
    normalize_wallet,
)
from pm_robot.wallet_levels import (
    RECENT_SAMPLE_TRADE_LIMIT,
    RECENT_SAMPLE_VOLUME_GATE_USDC,
    WalletLevel,
)


@dataclass(frozen=True)
class WalletSightingResult:
    wallet: str
    level: WalletLevel
    reason: str
    candidate_updated: bool
    promoted: bool
    new_trade_count: int


def record_wallet_sighting(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    recent_trades: Iterable[dict[str, Any]] = (),
    verified_trade: bool = False,
    trusted_source: bool = False,
    allow_l1: bool = True,
    refresh_existing_candidate: bool = True,
    now: int | None = None,
) -> WalletSightingResult:
    """Record one source sighting and optionally perform only the L0 to L1 step."""

    wallet = normalize_wallet(candidate.address)
    ts = int(time.time()) if now is None else int(now)
    normalized_candidate = CandidateAddress(
        address=wallet,
        sources=candidate.sources,
        labels=candidate.labels,
        notes=candidate.notes,
        links=candidate.links,
        status=candidate.status,
    )
    ensure_wallet_level(conn, wallet, reason="source_sighting", now=ts)
    new_trade_count, observed_sample_volume = _record_observation(
        conn,
        normalized_candidate,
        recent_trades=list(recent_trades),
        now=ts,
    )

    existing_candidate = conn.execute(
        "SELECT 1 FROM candidate_wallets WHERE address = ?",
        (wallet,),
    ).fetchone() is not None
    qualification_reason = _qualification_reason(
        existing_candidate=existing_candidate,
        verified_trade=verified_trade,
        trusted_source=trusted_source,
        observed_sample_volume=observed_sample_volume,
    )
    should_write_candidate = (
        existing_candidate and refresh_existing_candidate
    ) or (
        not existing_candidate and allow_l1 and bool(qualification_reason)
    )

    candidate_updated = False
    promoted = False
    if should_write_candidate:
        upsert_candidate(
            conn,
            normalized_candidate,
        )
        candidate_updated = True
        level = get_wallet_level(conn, wallet)
        if level.level is WalletLevel.L0 and not level.hard_risk_block:
            advance_wallet_level(
                conn,
                wallet,
                to_level=WalletLevel.L1,
                reason=qualification_reason or "existing_candidate",
                facts={
                    "verified_trade": bool(verified_trade),
                    "trusted_source": bool(trusted_source),
                    "observed_sample_volume_usdc": observed_sample_volume,
                    "source": candidate.sources,
                },
                now=ts,
            )
        current = get_wallet_level(conn, wallet)
        if current.level is not WalletLevel.L0:
            _mark_promoted(conn, wallet, qualification_reason or "existing_candidate", now=ts)
            promoted = not existing_candidate

    current = get_wallet_level(conn, wallet)
    return WalletSightingResult(
        wallet=wallet,
        level=current.level,
        reason=qualification_reason,
        candidate_updated=candidate_updated,
        promoted=promoted,
        new_trade_count=new_trade_count,
    )


def _qualification_reason(
    *,
    existing_candidate: bool,
    verified_trade: bool,
    trusted_source: bool,
    observed_sample_volume: float,
) -> str:
    if existing_candidate:
        return "existing_candidate"
    if trusted_source:
        return "trusted_source"
    if (
        verified_trade
        and observed_sample_volume >= RECENT_SAMPLE_VOLUME_GATE_USDC
    ):
        return "observed_sample_volume_at_least_100_usdc"
    return ""


def _record_observation(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    recent_trades: list[dict[str, Any]],
    now: int,
) -> tuple[int, float]:
    wallet = candidate.address
    existing = conn.execute(
        "SELECT * FROM observed_wallets WHERE wallet = ?",
        (wallet,),
    ).fetchone()
    merged_trades, new_trade_count = _merge_recent_trades(
        _decode_trades(existing["recent_trades_json"] if existing else "[]"),
        recent_trades,
        now=now,
    )
    recent_total = sum(float(row.get("usdc_size") or 0.0) for row in merged_trades)
    recent_max = max(
        (float(row.get("usdc_size") or 0.0) for row in merged_trades),
        default=0.0,
    )
    previous_count = int(existing["observed_trade_count"] or 0) if existing else 0
    first_seen_at = int(existing["first_seen_at"] or now) if existing else now
    conn.execute(
        """
        INSERT INTO observed_wallets(
            wallet, sources, labels, notes, links, status,
            observed_trade_count, recent_trade_count, recent_usdc_total,
            recent_max_trade_usdc, recent_trades_json, promoted_at,
            promotion_reason, first_seen_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            sources = excluded.sources,
            labels = excluded.labels,
            notes = excluded.notes,
            links = excluded.links,
            status = excluded.status,
            observed_trade_count = excluded.observed_trade_count,
            recent_trade_count = excluded.recent_trade_count,
            recent_usdc_total = excluded.recent_usdc_total,
            recent_max_trade_usdc = excluded.recent_max_trade_usdc,
            recent_trades_json = excluded.recent_trades_json,
            updated_at = excluded.updated_at
        """,
        (
            wallet,
            _merge_text(existing["sources"] if existing else "", candidate.sources),
            _merge_text(existing["labels"] if existing else "", candidate.labels),
            _merge_text(existing["notes"] if existing else "", candidate.notes),
            _merge_text(existing["links"] if existing else "", candidate.links),
            candidate.status or (existing["status"] if existing else ""),
            previous_count + new_trade_count,
            len(merged_trades),
            recent_total,
            recent_max,
            json.dumps(merged_trades, ensure_ascii=False, sort_keys=True),
            existing["promoted_at"] if existing else None,
            existing["promotion_reason"] if existing else "",
            first_seen_at,
            now,
        ),
    )
    return new_trade_count, recent_total


def _mark_promoted(conn: sqlite3.Connection, wallet: str, reason: str, *, now: int) -> None:
    conn.execute(
        """
        UPDATE observed_wallets
        SET promoted_at = COALESCE(promoted_at, ?),
            promotion_reason = CASE
                WHEN promotion_reason = '' THEN ?
                ELSE promotion_reason
            END,
            updated_at = ?
        WHERE wallet = ?
        """,
        (now, str(reason or "l1_qualified")[:500], now, wallet),
    )


def _merge_recent_trades(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    *,
    now: int,
) -> tuple[list[dict[str, Any]], int]:
    rows_by_key: dict[str, dict[str, Any]] = {}
    for row in existing:
        normalized = _normalize_trade(row, now=now)
        if normalized["key"]:
            rows_by_key[normalized["key"]] = normalized
    existing_keys = set(rows_by_key)
    for row in incoming:
        if not isinstance(row, dict):
            continue
        normalized = _normalize_trade(row, now=now)
        if normalized["key"]:
            rows_by_key[normalized["key"]] = normalized
    merged = sorted(
        rows_by_key.values(),
        key=lambda row: (
            int(row.get("timestamp") or 0),
            int(row.get("observed_at") or 0),
            str(row.get("key") or ""),
        ),
        reverse=True,
    )[:RECENT_SAMPLE_TRADE_LIMIT]
    return merged, len(set(rows_by_key) - existing_keys)


def _normalize_trade(row: dict[str, Any], *, now: int) -> dict[str, Any]:
    timestamp = _safe_int(row.get("timestamp"))
    observed_at = _safe_int(row.get("observed_at")) or now
    usdc_size = _safe_float(row.get("usdc_size"))
    market = str(row.get("market") or "").strip()
    side = str(row.get("side") or "").strip().upper()
    tx_hash = str(row.get("transaction_hash") or "").strip()
    key = str(row.get("key") or "").strip()
    if not key:
        key = "|".join((tx_hash, str(timestamp), market, side, f"{usdc_size:.8f}"))
    return {
        "key": key,
        "timestamp": timestamp,
        "observed_at": observed_at,
        "market": market,
        "side": side,
        "usdc_size": usdc_size,
        "transaction_hash": tx_hash,
    }


def _decode_trades(raw_json: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(raw_json or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def _merge_text(existing: str, incoming: str, *, max_len: int = 4000) -> str:
    values: list[str] = []
    seen: set[str] = set()
    for raw in (existing or "", incoming or ""):
        for part in raw.split("|"):
            item = part.strip()
            if item and item not in seen:
                seen.add(item)
                values.append(item)
    return " | ".join(values)[:max_len]


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0
