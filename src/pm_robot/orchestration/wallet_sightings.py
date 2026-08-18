"""Single ingress boundary for wallet discovery sources.

Ingress records provenance and recent sightings, then may advance a wallet from
L0 to L1. It never schedules history collection or performs quality scoring.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Iterable

from pm_robot.models import CandidateAddress
from pm_robot.storage.repository import upsert_candidate
from pm_robot.storage.wallet_levels import (
    WalletLevelRecord,
    advance_wallet_level,
    ensure_wallet_level,
    get_wallet_level,
    get_wallet_levels_for_addresses,
    normalize_wallet,
)
from pm_robot.wallet_levels import (
    RECENT_OBSERVED_MIN_TRADE_COUNT,
    RECENT_SAMPLE_TRADE_LIMIT,
    RECENT_SAMPLE_VOLUME_GATE_USDC,
    WalletLevel,
)


_QUALIFIED_L0_ADMISSION_QUERY = """
    SELECT observed.*
    FROM observed_wallets AS observed
         INDEXED BY idx_observed_wallets_l0_admission
    JOIN wallet_levels AS levels ON levels.wallet = observed.wallet
    WHERE levels.level = 'l0'
      AND levels.hard_risk_block = 0
      AND observed.promoted_at IS NULL
      AND observed.recent_trade_count > 0
      AND observed.recent_trade_count >= 2
      AND observed.recent_usdc_total >= ?
    ORDER BY observed.recent_usdc_total DESC,
             observed.first_seen_at ASC,
             observed.wallet ASC
    LIMIT ?
"""

_TIMESTAMPED_DISCOVERY_STATUS_RE = re.compile(
    r"^(activity_discovered|rtds_activity_discovered|leaderboard_discovered):\d+$"
)


@dataclass(frozen=True)
class WalletSightingResult:
    wallet: str
    level: WalletLevel
    reason: str
    candidate_updated: bool
    promoted: bool
    new_trade_count: int


@dataclass(frozen=True)
class _ObservedWalletRecord:
    wallet: str
    sources: str
    labels: str
    notes: str
    links: str
    status: str
    observed_trade_count: int
    recent_trade_count: int
    recent_usdc_total: float
    recent_max_trade_usdc: float
    recent_trades_json: str
    promoted_at: int | None
    promotion_reason: str
    first_seen_at: int
    updated_at: int


def admit_qualified_observed_wallets(
    conn: sqlite3.Connection,
    *,
    limit: int,
    now: int | None = None,
) -> int:
    """Admit bounded L0 overflow after ingress has stored a qualifying sample."""

    bounded_limit = max(0, int(limit))
    if bounded_limit == 0:
        return 0
    ts = int(time.time()) if now is None else int(now)
    rows = conn.execute(
        _QUALIFIED_L0_ADMISSION_QUERY,
        (
            RECENT_SAMPLE_VOLUME_GATE_USDC,
            bounded_limit,
        ),
    ).fetchall()
    admitted = 0
    for row in rows:
        wallet = str(row["wallet"])
        upsert_candidate(
            conn,
            CandidateAddress(
                address=wallet,
                sources=str(row["sources"] or ""),
                labels=str(row["labels"] or ""),
                notes=str(row["notes"] or ""),
                links=str(row["links"] or ""),
                status=str(row["status"] or ""),
            ),
            now=ts,
        )
        decision = advance_wallet_level(
            conn,
            wallet,
            to_level=WalletLevel.L1,
            reason="deferred_l0_admission",
            policy_version="ingress_v2",
            facts={
                "recent_trade_count": int(row["recent_trade_count"] or 0),
                "recent_usdc_total": float(row["recent_usdc_total"] or 0.0),
                "source": str(row["sources"] or ""),
            },
            now=ts,
        )
        if decision.level is WalletLevel.L1:
            _mark_promoted(conn, wallet, "deferred_l0_admission", now=ts)
            admitted += 1
    return admitted


def record_wallet_sighting(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    recent_trades: Iterable[dict[str, Any]] = (),
    verified_trade: bool = False,
    trusted_source: bool = False,
    allow_l1: bool = True,
    refresh_existing_candidate: bool = True,
    candidate_snapshot: dict[str, CandidateAddress] | None = None,
    level_snapshot: dict[str, WalletLevelRecord] | None = None,
    observed_snapshot: dict[str, _ObservedWalletRecord] | None = None,
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
    level = ensure_wallet_level(
        conn,
        wallet,
        reason="source_sighting",
        level_snapshot=level_snapshot,
        now=ts,
    )
    new_trade_count, observed_trade_count, observed_sample_volume = _record_observation(
        conn,
        normalized_candidate,
        # Only source-verified economic events may contribute to the L0 gate.
        recent_trades=list(recent_trades) if verified_trade else [],
        observed_snapshot=observed_snapshot,
        now=ts,
    )

    preloaded_candidate = (
        candidate_snapshot.get(wallet) if candidate_snapshot is not None else None
    )
    if candidate_snapshot is None:
        row = conn.execute(
            """
            SELECT address, sources, labels, notes, links, status
            FROM candidate_wallets
            WHERE address = ?
            """,
            (wallet,),
        ).fetchone()
        preloaded_candidate = (
            CandidateAddress(
                address=str(row["address"]),
                sources=str(row["sources"] or ""),
                labels=str(row["labels"] or ""),
                notes=str(row["notes"] or ""),
                links=str(row["links"] or ""),
                status=str(row["status"] or ""),
            )
            if row is not None
            else None
        )
    existing_candidate = preloaded_candidate is not None
    qualification_reason = _qualification_reason(
        existing_candidate=existing_candidate,
        existing_candidate_trusted=_has_trusted_candidate_provenance(preloaded_candidate),
        verified_trade=verified_trade,
        trusted_source=trusted_source,
        observed_trade_count=observed_trade_count,
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
            preloaded_existing=preloaded_candidate,
            existing_preloaded=candidate_snapshot is not None,
            now=ts,
        )
        if candidate_snapshot is not None:
            candidate_snapshot[wallet] = _merge_candidate_snapshot(
                preloaded_candidate,
                normalized_candidate,
            )
        candidate_updated = True
        if (
            qualification_reason
            and level.level is WalletLevel.L0
            and not level.hard_risk_block
        ):
            advance_wallet_level(
                conn,
                wallet,
                to_level=WalletLevel.L1,
                reason=qualification_reason,
                facts={
                    "verified_trade": bool(verified_trade),
                    "trusted_source": bool(trusted_source),
                    "observed_trade_count": observed_trade_count,
                    "observed_sample_volume_usdc": observed_sample_volume,
                    "source": candidate.sources,
                },
                level_snapshot=level_snapshot,
                now=ts,
            )
        current = _current_level(conn, wallet, level_snapshot)
        if qualification_reason and current.level is not WalletLevel.L0:
            _mark_promoted(conn, wallet, qualification_reason, now=ts)
            if observed_snapshot is not None and wallet in observed_snapshot:
                observed_snapshot[wallet] = _promoted_observation(
                    observed_snapshot[wallet],
                    qualification_reason,
                    now=ts,
                )
            promoted = not existing_candidate

    current = _current_level(conn, wallet, level_snapshot)
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
    existing_candidate_trusted: bool,
    verified_trade: bool,
    trusted_source: bool,
    observed_trade_count: int,
    observed_sample_volume: float,
) -> str:
    if trusted_source:
        return "trusted_source"
    if existing_candidate and existing_candidate_trusted:
        return "trusted_candidate_provenance"
    if (
        verified_trade
        and observed_trade_count >= RECENT_OBSERVED_MIN_TRADE_COUNT
        and observed_sample_volume >= RECENT_SAMPLE_VOLUME_GATE_USDC
    ):
        return "observed_resource_gate"
    return ""


def _has_trusted_candidate_provenance(candidate: CandidateAddress | None) -> bool:
    if candidate is None:
        return False
    provenance = " | ".join(
        (
            candidate.sources,
            candidate.labels,
            candidate.notes,
            candidate.links,
            candidate.status,
        )
    ).lower()
    trusted_tokens = (
        "manual",
        "watchlist",
        "leaderboard",
        "bitget",
        "polydata",
    )
    return any(token in provenance for token in trusted_tokens)


def _record_observation(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    recent_trades: list[dict[str, Any]],
    observed_snapshot: dict[str, _ObservedWalletRecord] | None = None,
    now: int,
) -> tuple[int, int, float]:
    wallet = candidate.address
    existing = (
        observed_snapshot.get(wallet)
        if observed_snapshot is not None
        else conn.execute(
            "SELECT * FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()
    )
    merged_trades, new_trade_count = _merge_recent_trades(
        _decode_trades(_observed_value(existing, "recent_trades_json", "[]")),
        recent_trades,
        now=now,
    )
    recent_total = sum(float(row.get("usdc_size") or 0.0) for row in merged_trades)
    recent_max = max(
        (float(row.get("usdc_size") or 0.0) for row in merged_trades),
        default=0.0,
    )
    previous_count = int(_observed_value(existing, "observed_trade_count", 0) or 0)
    first_seen_at = int(_observed_value(existing, "first_seen_at", now) or now)
    sources = _merge_text(_observed_value(existing, "sources", ""), candidate.sources)
    labels = _merge_text(_observed_value(existing, "labels", ""), candidate.labels)
    notes = _merge_text(_observed_value(existing, "notes", ""), candidate.notes)
    links = _merge_text(_observed_value(existing, "links", ""), candidate.links)
    status = _canonical_observation_status(
        candidate.status or str(_observed_value(existing, "status", "") or "")
    )
    promoted_at = _observed_value(existing, "promoted_at", None)
    promotion_reason = str(_observed_value(existing, "promotion_reason", "") or "")
    recent_trades_json = json.dumps(merged_trades, ensure_ascii=False, sort_keys=True)
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
            sources,
            labels,
            notes,
            links,
            status,
            previous_count + new_trade_count,
            len(merged_trades),
            recent_total,
            recent_max,
            recent_trades_json,
            promoted_at,
            promotion_reason,
            first_seen_at,
            now,
        ),
    )
    if observed_snapshot is not None:
        observed_snapshot[wallet] = _ObservedWalletRecord(
            wallet=wallet,
            sources=sources,
            labels=labels,
            notes=notes,
            links=links,
            status=status,
            observed_trade_count=previous_count + new_trade_count,
            recent_trade_count=len(merged_trades),
            recent_usdc_total=recent_total,
            recent_max_trade_usdc=recent_max,
            recent_trades_json=recent_trades_json,
            promoted_at=int(promoted_at) if promoted_at is not None else None,
            promotion_reason=promotion_reason,
            first_seen_at=first_seen_at,
            updated_at=now,
        )
    return new_trade_count, len(merged_trades), recent_total


def get_observed_wallets_for_addresses(
    conn: sqlite3.Connection,
    addresses: list[str] | tuple[str, ...] | set[str],
) -> dict[str, _ObservedWalletRecord]:
    """Return observed wallet rows for a bounded wallet-address snapshot."""

    normalized = sorted(
        {normalize_wallet(address) for address in addresses if str(address or "").strip()}
    )
    if not normalized:
        return {}
    snapshot: dict[str, _ObservedWalletRecord] = {}
    chunk_size = max(1, _sqlite_variable_limit(conn))
    for offset in range(0, len(normalized), chunk_size):
        chunk = normalized[offset : offset + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT * FROM observed_wallets WHERE wallet IN ({placeholders})",
            tuple(chunk),
        ).fetchall()
        snapshot.update({str(row["wallet"]): _observed_from_row(row) for row in rows})
    return snapshot


def get_wallet_sighting_snapshots(
    conn: sqlite3.Connection,
    addresses: list[str] | tuple[str, ...] | set[str],
) -> tuple[dict[str, WalletLevelRecord], dict[str, _ObservedWalletRecord]]:
    """Prefetch level and observation rows used by batched sighting writes."""

    return (
        get_wallet_levels_for_addresses(conn, addresses),
        get_observed_wallets_for_addresses(conn, addresses),
    )


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


def _current_level(
    conn: sqlite3.Connection,
    wallet: str,
    level_snapshot: dict[str, WalletLevelRecord] | None,
) -> WalletLevelRecord:
    if level_snapshot is not None and wallet in level_snapshot:
        return level_snapshot[wallet]
    return get_wallet_level(conn, wallet)


def _merge_candidate_snapshot(
    existing: CandidateAddress | None,
    incoming: CandidateAddress,
) -> CandidateAddress:
    if existing is None:
        return incoming
    return CandidateAddress(
        address=incoming.address,
        sources=_merge_text(existing.sources, incoming.sources),
        labels=_merge_text(existing.labels, incoming.labels),
        notes=_merge_text(existing.notes, incoming.notes),
        links=_merge_text(existing.links, incoming.links),
        status=_canonical_observation_status(incoming.status),
    )


def _canonical_observation_status(value: str) -> str:
    """Keep discovery status categorical; timestamps belong in updated_at."""

    status = str(value or "").strip()
    match = _TIMESTAMPED_DISCOVERY_STATUS_RE.fullmatch(status)
    return match.group(1) if match else status


def _promoted_observation(
    existing: _ObservedWalletRecord,
    reason: str,
    *,
    now: int,
) -> _ObservedWalletRecord:
    return _ObservedWalletRecord(
        wallet=existing.wallet,
        sources=existing.sources,
        labels=existing.labels,
        notes=existing.notes,
        links=existing.links,
        status=existing.status,
        observed_trade_count=existing.observed_trade_count,
        recent_trade_count=existing.recent_trade_count,
        recent_usdc_total=existing.recent_usdc_total,
        recent_max_trade_usdc=existing.recent_max_trade_usdc,
        recent_trades_json=existing.recent_trades_json,
        promoted_at=existing.promoted_at if existing.promoted_at is not None else now,
        promotion_reason=existing.promotion_reason or str(reason or "l1_qualified")[:500],
        first_seen_at=existing.first_seen_at,
        updated_at=now,
    )


def _observed_from_row(row: sqlite3.Row) -> _ObservedWalletRecord:
    return _ObservedWalletRecord(
        wallet=str(row["wallet"]),
        sources=str(row["sources"] or ""),
        labels=str(row["labels"] or ""),
        notes=str(row["notes"] or ""),
        links=str(row["links"] or ""),
        status=str(row["status"] or ""),
        observed_trade_count=int(row["observed_trade_count"] or 0),
        recent_trade_count=int(row["recent_trade_count"] or 0),
        recent_usdc_total=float(row["recent_usdc_total"] or 0.0),
        recent_max_trade_usdc=float(row["recent_max_trade_usdc"] or 0.0),
        recent_trades_json=str(row["recent_trades_json"] or "[]"),
        promoted_at=int(row["promoted_at"]) if row["promoted_at"] is not None else None,
        promotion_reason=str(row["promotion_reason"] or ""),
        first_seen_at=int(row["first_seen_at"] or 0),
        updated_at=int(row["updated_at"] or 0),
    )


def _observed_value(
    existing: _ObservedWalletRecord | sqlite3.Row | None,
    name: str,
    default: Any,
) -> Any:
    if existing is None:
        return default
    if isinstance(existing, _ObservedWalletRecord):
        return getattr(existing, name)
    return existing[name]


def _sqlite_variable_limit(conn: sqlite3.Connection) -> int:
    try:
        rows = conn.execute("PRAGMA compile_options").fetchall()
    except sqlite3.Error:
        return 999
    for row in rows:
        option = str(row[0])
        if option.startswith("MAX_VARIABLE_NUMBER="):
            try:
                return max(1, int(option.partition("=")[2]))
            except ValueError:
                return 999
    return 999


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
    usdc_size = max(0.0, _safe_float(row.get("usdc_size")))
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
        parsed = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def _safe_int(value: Any) -> int:
    try:
        parsed = float(value or 0)
        return int(parsed) if math.isfinite(parsed) else 0
    except (TypeError, ValueError, OverflowError):
        return 0
