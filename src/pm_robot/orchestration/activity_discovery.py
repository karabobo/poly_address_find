"""Candidate discovery from recent public Polymarket trade activity."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
import json
from typing import Any
from pathlib import Path

from pm_robot.clients.http import HttpClientError
from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.storage.repository import (
    get_wallet_features,
    seed_evidence_backfill_budget,
    summary_only_wallets,
    upsert_candidate,
    upsert_wallet_feature,
)

OBSERVED_RECENT_TRADE_LIMIT = 10
OBSERVED_REPEAT_TRADE_COUNT_THRESHOLD = 2
OBSERVED_CUMULATIVE_USDC_THRESHOLD = 300.0
OBSERVED_LARGE_SINGLE_TRADE_USDC_THRESHOLD = 5_000.0


@dataclass(frozen=True)
class ActivityDiscoverySummary:
    pages_attempted: int
    pages_succeeded: int
    events_seen: int
    wallets_seen: int
    candidates_inserted_or_updated: int
    features_updated: int
    observed_wallets: int
    promoted_wallets: int
    status: str
    error: str = ""
    output_path: str = ""


def discover_activity_candidates(
    conn: sqlite3.Connection,
    *,
    pages: int = 5,
    page_limit: int = 100,
    min_trades: int = 2,
    min_usdc_volume: float = 20.0,
    min_trade_filter_usdc: float = 0.0,
    max_candidates: int = 200,
    sleep_seconds: float = 0.25,
    output_path: Path | None = None,
    write_db: bool = True,
    client: PublicPolymarketClient | None = None,
) -> ActivityDiscoverySummary:
    client = client or PublicPolymarketClient(conn=conn)
    attempted = 0
    succeeded = 0
    events_seen = 0
    wallets: dict[str, dict[str, Any]] = {}
    error = ""
    fetch_status = ""

    for page in range(max(pages, 0)):
        attempted += 1
        if page > 0 and sleep_seconds > 0:
            time.sleep(sleep_seconds)
        try:
            rows = client.recent_trades(
                limit=page_limit,
                offset=page * page_limit,
                min_cash_usdc=min_trade_filter_usdc,
            )
        except HttpClientError as exc:
            error = f"recent_trades:{exc.error_type}:{exc.status_code or ''}:{exc}"
            if succeeded:
                fetch_status = "partial"
            elif exc.status_code in {400, 403, 429, 503}:
                fetch_status = "limited"
            else:
                fetch_status = "failed"
            break
        except Exception as exc:
            error = f"recent_trades:{exc}"
            fetch_status = "partial" if succeeded else "failed"
            break
        succeeded += 1
        if not rows:
            break
        events_seen += len(rows)
        _merge_activity_rows(wallets, rows)

    now = int(time.time())
    if write_db:
        result = _persist_activity_items(
            conn,
            wallets,
            max_candidates=max_candidates,
            now=now,
            source="polymarket_trades_global",
            labels="trade_activity_seed",
            status_prefix="activity_discovered",
            target_depth=200,
        )
    else:
        result = _dry_run_activity_items(wallets, min_trades=min_trades, min_usdc_volume=min_usdc_volume, max_candidates=max_candidates)
    if output_path is not None:
        _write_candidate_export(
            output_path,
            result["promoted_items"],
            generated_at=now,
            pages_attempted=attempted,
            pages_succeeded=succeeded,
            events_seen=events_seen,
            wallets_seen=len(wallets),
        )
    return _summary(
        attempted,
        succeeded,
        events_seen,
        wallets,
        result["candidates"],
        result["features"],
        result["observed"],
        result["promoted"],
        fetch_status or ("ok" if succeeded else "failed"),
        error,
        output_path=str(output_path or ""),
    )


def process_activity_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    source: str,
    labels: str,
    status_prefix: str,
    min_trade_usdc: float = 0.0,
    max_candidates: int = 200,
    target_depth: int = 200,
    now: int | None = None,
) -> ActivityDiscoverySummary:
    wallets: dict[str, dict[str, Any]] = {}
    _merge_activity_rows(wallets, rows, min_trade_usdc=min_trade_usdc)
    ts = now or int(time.time())
    result = _persist_activity_items(
        conn,
        wallets,
        max_candidates=max_candidates,
        now=ts,
        source=source,
        labels=labels,
        status_prefix=status_prefix,
        target_depth=target_depth,
    )
    return _summary(
        0,
        0,
        len(rows),
        wallets,
        result["candidates"],
        result["features"],
        result["observed"],
        result["promoted"],
        "ok",
        "",
        output_path="",
    )


def _summary(
    attempted: int,
    succeeded: int,
    events_seen: int,
    wallets: dict[str, dict[str, Any]],
    candidates: int,
    features: int,
    observed: int,
    promoted: int,
    status: str,
    error: str,
    output_path: str,
) -> ActivityDiscoverySummary:
    return ActivityDiscoverySummary(
        pages_attempted=attempted,
        pages_succeeded=succeeded,
        events_seen=events_seen,
        wallets_seen=len(wallets),
        candidates_inserted_or_updated=candidates,
        features_updated=features,
        observed_wallets=observed,
        promoted_wallets=promoted,
        status=status,
        error=error,
        output_path=output_path,
    )


def _candidate_from_activity(item: dict[str, Any], *, now: int) -> CandidateAddress:
    return _candidate_from_activity_source(
        item,
        now=now,
        source="polymarket_trades_global",
        labels="trade_activity_seed",
        status_prefix="activity_discovered",
    )


def _candidate_from_activity_source(
    item: dict[str, Any],
    *,
    now: int,
    source: str,
    labels: str,
    status_prefix: str,
) -> CandidateAddress:
    wallet = item["wallet"]
    markets = sorted(item["markets"])
    names = sorted(item["names"])
    notes = [
        f"activity_trades={item['trade_count']}",
        f"activity_usdc={round(item['usdc_volume'], 4)}",
        f"activity_markets={len(markets)}",
    ]
    if names:
        notes.append(f"name={names[0]}")
    if markets:
        notes.append("markets=" + ",".join(markets[:5]))
    return CandidateAddress(
        address=wallet,
        sources=source,
        labels=labels,
        notes=" | ".join(notes),
        links=f"https://polymarket.com/profile/{wallet}",
        status=f"{status_prefix}:{now}",
    )


def _export_item(item: dict[str, Any], *, generated_at: int) -> dict[str, Any]:
    candidate = _candidate_from_activity(item, now=generated_at)
    return {
        "address": candidate.address,
        "sources": candidate.sources,
        "labels": candidate.labels,
        "notes": candidate.notes,
        "links": candidate.links,
        "status": candidate.status,
        "evidence": {
            "trade_count": item["trade_count"],
            "buy_count": item["buy_count"],
            "sell_count": item["sell_count"],
            "usdc_volume": round(item["usdc_volume"], 6),
            "market_count": len(item["markets"]),
            "markets": sorted(item["markets"])[:20],
            "latest_ts": item["latest_ts"],
            "names": sorted(item["names"])[:5],
        },
    }


def _write_candidate_export(
    output_path: Path,
    selected: list[dict[str, Any]],
    *,
    generated_at: int,
    pages_attempted: int,
    pages_succeeded: int,
    events_seen: int,
    wallets_seen: int,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": generated_at,
        "source": "polymarket_trades_global",
        "mode": "github_probe_export",
        "stats": {
            "pages_attempted": pages_attempted,
            "pages_succeeded": pages_succeeded,
            "events_seen": events_seen,
            "wallets_seen": wallets_seen,
            "candidate_count": len(selected),
        },
        "candidates": [_export_item(item, generated_at=generated_at) for item in selected],
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _backfill_priority(item: dict[str, Any]) -> int:
    volume = float(item.get("usdc_volume") or 0.0)
    trades = int(item.get("trade_count") or 0)
    if volume >= 1_000 or trades >= 20:
        return 20
    if volume >= 200 or trades >= 8:
        return 35
    return 50


def _feature_from_activity(item: dict[str, Any], existing: WalletFeatures | None) -> WalletFeatures:
    extra = dict(existing.extra) if existing else {}
    extra["activity_discovery"] = {
        "trade_count": item["trade_count"],
        "buy_count": item["buy_count"],
        "sell_count": item["sell_count"],
        "usdc_volume": round(item["usdc_volume"], 6),
        "market_count": len(item["markets"]),
        "latest_ts": item["latest_ts"],
    }
    return WalletFeatures(
        address=item["wallet"],
        recent_30d_volume_usdc=item["usdc_volume"],
        hygiene_status=(existing.hygiene_status if existing else "") or "clean",
        extra=extra,
    )


def _merge_activity_rows(
    wallets: dict[str, dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    min_trade_usdc: float = 0.0,
) -> None:
    for row in rows:
        wallet = _wallet_from_activity(row)
        if not wallet:
            continue
        usdc_size = _trade_usdc(row)
        if min_trade_usdc > 0 and usdc_size < min_trade_usdc:
            continue
        item = wallets.setdefault(
            wallet,
            {
                "wallet": wallet,
                "trade_count": 0,
                "buy_count": 0,
                "sell_count": 0,
                "usdc_volume": 0.0,
                "markets": set(),
                "latest_ts": 0,
                "names": set(),
                "recent_trades": [],
            },
        )
        item["trade_count"] += 1
        side = str(row.get("side") or "").upper()
        if side == "BUY":
            item["buy_count"] += 1
        elif side == "SELL":
            item["sell_count"] += 1
        item["usdc_volume"] += usdc_size
        market = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "")
        if market:
            item["markets"].add(market)
        item["latest_ts"] = max(int(item["latest_ts"]), int(_float(row.get("timestamp")) or 0))
        name = str(row.get("name") or row.get("pseudonym") or "").strip()
        if name:
            item["names"].add(name)
        item["recent_trades"].append(_observed_trade_from_activity(row))


def _sorted_activity_items(wallets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        wallets.values(),
        key=lambda item: (item["usdc_volume"], item["trade_count"], item["latest_ts"]),
        reverse=True,
    )


def _persist_activity_items(
    conn: sqlite3.Connection,
    wallets: dict[str, dict[str, Any]],
    *,
    max_candidates: int,
    now: int,
    source: str,
    labels: str,
    status_prefix: str,
    target_depth: int,
) -> dict[str, Any]:
    existing = get_wallet_features(conn)
    archived = summary_only_wallets(conn, wallets)
    candidates = 0
    features = 0
    observed = 0
    promoted = 0
    promoted_items: list[dict[str, Any]] = []
    for item in _sorted_activity_items(wallets):
        existing_candidate = conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?",
            (item["wallet"].lower(),),
        ).fetchone() is not None
        candidate = _candidate_from_activity_source(
            item,
            now=now,
            source=source,
            labels=labels,
            status_prefix=status_prefix,
        )
        observation = _record_observed_wallet(
            conn,
            candidate,
            item,
            now=now,
            existing_candidate=existing_candidate,
        )
        observed += 1
        if item["wallet"] in archived:
            continue
        if not observation["promotion_reason"]:
            continue
        is_new_promotion = not existing_candidate
        if is_new_promotion and promoted >= max_candidates:
            continue
        if is_new_promotion:
            promoted += 1
            promoted_items.append(item)
        _mark_observed_wallet_promoted(conn, item["wallet"], observation["promotion_reason"], now=now)
        upsert_candidate(conn, candidate)
        seed_evidence_backfill_budget(
            conn,
            item["wallet"],
            source=source,
            priority=_backfill_priority(item),
            target_depth=target_depth,
            evidence=_export_item(item, generated_at=now)["evidence"],
            now=now,
        )
        candidates += 1
        upsert_wallet_feature(conn, _feature_from_activity(item, existing.get(item["wallet"])))
        features += 1
    conn.commit()
    return {
        "candidates": candidates,
        "features": features,
        "observed": observed,
        "promoted": promoted,
        "promoted_items": promoted_items,
    }


def _dry_run_activity_items(
    wallets: dict[str, dict[str, Any]],
    *,
    min_trades: int,
    min_usdc_volume: float,
    max_candidates: int,
) -> dict[str, Any]:
    promoted = 0
    promoted_items: list[dict[str, Any]] = []
    for item in _sorted_activity_items(wallets):
        if item["trade_count"] < min_trades and item["usdc_volume"] < min_usdc_volume:
            continue
        observation = _observation_snapshot(item["recent_trades"], existing_row=None)
        if not _promotion_reason(observation, existing_candidate=False):
            continue
        promoted += 1
        promoted_items.append(item)
        if promoted >= max_candidates:
            break
    return {
        "candidates": 0,
        "features": 0,
        "observed": len(wallets),
        "promoted": promoted,
        "promoted_items": promoted_items,
    }


def _record_observed_wallet(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    item: dict[str, Any],
    *,
    now: int,
    existing_candidate: bool = False,
) -> dict[str, Any]:
    wallet = candidate.address.lower()
    existing = conn.execute("SELECT * FROM observed_wallets WHERE wallet = ?", (wallet,)).fetchone()
    recent_trades, new_trade_count = _merge_recent_observed_trades(
        _decode_recent_trades(existing["recent_trades_json"] if existing else "[]"),
        item.get("recent_trades") or [],
        now=now,
    )
    snapshot = _observation_snapshot(
        recent_trades,
        existing_row=existing,
        existing_candidate=existing_candidate,
    )
    observed_trade_count = int(existing["observed_trade_count"] or 0) + new_trade_count if existing else new_trade_count
    first_seen_at = int(existing["first_seen_at"] or now) if existing else now
    sources = _merge_observation_text(existing["sources"] if existing else "", candidate.sources)
    labels = _merge_observation_text(existing["labels"] if existing else "", candidate.labels)
    notes = _merge_observation_text(existing["notes"] if existing else "", candidate.notes)
    links = _merge_observation_text(existing["links"] if existing else "", candidate.links)
    status = candidate.status or (existing["status"] if existing else "")
    promotion_reason = snapshot["promotion_reason"]
    promoted_at = existing["promoted_at"] if existing else None
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
            promoted_at = COALESCE(observed_wallets.promoted_at, excluded.promoted_at),
            promotion_reason = CASE
                WHEN excluded.promotion_reason != '' THEN excluded.promotion_reason
                ELSE observed_wallets.promotion_reason
            END,
            updated_at = excluded.updated_at
        """,
        (
            wallet,
            sources,
            labels,
            notes,
            links,
            status,
            observed_trade_count,
            snapshot["recent_trade_count"],
            snapshot["recent_usdc_total"],
            snapshot["recent_max_trade_usdc"],
            json.dumps(recent_trades, ensure_ascii=False, sort_keys=True),
            promoted_at,
            promotion_reason,
            first_seen_at,
            now,
        ),
    )
    snapshot["observed_trade_count"] = observed_trade_count
    return snapshot


def _mark_observed_wallet_promoted(
    conn: sqlite3.Connection,
    wallet: str,
    reason: str,
    *,
    now: int,
) -> None:
    conn.execute(
        """
        UPDATE observed_wallets
        SET promoted_at = COALESCE(promoted_at, ?),
            promotion_reason = CASE WHEN ? != '' THEN ? ELSE promotion_reason END,
            updated_at = ?
        WHERE wallet = ?
        """,
        (now, reason, reason, now, wallet.lower()),
    )


def _observation_snapshot(
    recent_trades: list[dict[str, Any]],
    *,
    existing_row: sqlite3.Row | None,
    existing_candidate: bool = False,
) -> dict[str, Any]:
    recent_usdc_total = sum(float(trade.get("usdc_size") or 0.0) for trade in recent_trades)
    recent_max_trade_usdc = max((float(trade.get("usdc_size") or 0.0) for trade in recent_trades), default=0.0)
    observed_trade_count = int(existing_row["observed_trade_count"] or 0) if existing_row else len(recent_trades)
    snapshot = {
        "observed_trade_count": observed_trade_count,
        "recent_trade_count": len(recent_trades),
        "recent_usdc_total": recent_usdc_total,
        "recent_max_trade_usdc": recent_max_trade_usdc,
        "promotion_reason": "",
    }
    snapshot["promotion_reason"] = _promotion_reason(
        snapshot,
        existing_candidate=existing_candidate,
    )
    return snapshot


def _promotion_reason(observation: dict[str, Any], *, existing_candidate: bool) -> str:
    """Require repeat activity before noisy trade streams create a new candidate."""
    if existing_candidate:
        return "existing_candidate"
    if (
        float(observation.get("recent_max_trade_usdc") or 0.0)
        >= OBSERVED_LARGE_SINGLE_TRADE_USDC_THRESHOLD
    ):
        return f"single_trade_usdc>={int(OBSERVED_LARGE_SINGLE_TRADE_USDC_THRESHOLD)}"
    if (
        int(observation.get("recent_trade_count") or 0)
        >= OBSERVED_REPEAT_TRADE_COUNT_THRESHOLD
        and float(observation.get("recent_usdc_total") or 0.0)
        >= OBSERVED_CUMULATIVE_USDC_THRESHOLD
    ):
        return f"recent_{OBSERVED_RECENT_TRADE_LIMIT}_trade_usdc_total>={int(OBSERVED_CUMULATIVE_USDC_THRESHOLD)}"
    return ""


def _merge_recent_observed_trades(
    existing_trades: list[dict[str, Any]],
    incoming_trades: list[dict[str, Any]],
    *,
    now: int,
) -> tuple[list[dict[str, Any]], int]:
    by_key: dict[str, dict[str, Any]] = {}
    for trade in existing_trades:
        normalized = _normalize_observed_trade(trade, now=now)
        key = str(normalized.get("key") or "")
        if key:
            by_key[key] = normalized
    existing_keys = set(by_key)
    for trade in incoming_trades:
        normalized = _normalize_observed_trade(trade, now=now)
        key = str(normalized.get("key") or "")
        if not key:
            continue
        previous = by_key.get(key)
        if previous and int(previous.get("observed_at") or 0) >= int(normalized.get("observed_at") or 0):
            continue
        by_key[key] = normalized
    merged = sorted(
        by_key.values(),
        key=lambda trade: (
            int(trade.get("timestamp") or 0),
            int(trade.get("observed_at") or 0),
            str(trade.get("key") or ""),
        ),
        reverse=True,
    )[:OBSERVED_RECENT_TRADE_LIMIT]
    new_count = len(set(by_key) - existing_keys)
    return merged, new_count


def _normalize_observed_trade(trade: dict[str, Any], *, now: int) -> dict[str, Any]:
    key = str(trade.get("key") or "").strip()
    timestamp = int(_float(trade.get("timestamp")) or 0)
    observed_at = int(_float(trade.get("observed_at")) or 0) or now
    usdc_size = float(_float(trade.get("usdc_size")) or 0.0)
    market = str(trade.get("market") or "").strip()
    side = str(trade.get("side") or "").strip().upper()
    tx_hash = str(trade.get("transaction_hash") or "").strip()
    if not key:
        key = _observed_trade_key(
            timestamp=timestamp,
            market=market,
            side=side,
            usdc_size=usdc_size,
            tx_hash=tx_hash,
        )
    return {
        "key": key,
        "timestamp": timestamp,
        "observed_at": observed_at,
        "market": market,
        "side": side,
        "usdc_size": usdc_size,
        "transaction_hash": tx_hash,
    }


def _decode_recent_trades(raw_json: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(raw_json or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _observed_trade_from_activity(row: dict[str, Any]) -> dict[str, Any]:
    timestamp = int(_float(row.get("timestamp")) or 0)
    market = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "").strip()
    side = str(row.get("side") or "").strip().upper()
    usdc_size = _trade_usdc(row)
    tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or "").strip()
    return {
        "key": _observed_trade_key(
            timestamp=timestamp,
            market=market,
            side=side,
            usdc_size=usdc_size,
            tx_hash=tx_hash,
        ),
        "timestamp": timestamp,
        "observed_at": timestamp,
        "market": market,
        "side": side,
        "usdc_size": usdc_size,
        "transaction_hash": tx_hash,
    }


def _observed_trade_key(
    *,
    timestamp: int,
    market: str,
    side: str,
    usdc_size: float,
    tx_hash: str,
) -> str:
    return "|".join(
        [
            tx_hash,
            str(timestamp),
            market,
            side,
            f"{usdc_size:.8f}",
        ]
    )


def _merge_observation_text(existing: str, incoming: str, *, sep: str = " | ", max_len: int = 4000) -> str:
    values: list[str] = []
    seen: set[str] = set()
    for raw in (existing or "", incoming or ""):
        for part in raw.split("|"):
            item = part.strip()
            if not item or item in seen:
                continue
            seen.add(item)
            values.append(item)
    return sep.join(values)[:max_len]


def _wallet_from_activity(row: dict[str, Any]) -> str:
    for key in ("proxyWallet", "proxy_wallet", "wallet", "address", "user"):
        wallet = str(row.get(key) or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            return wallet
    return ""


def _trade_usdc(row: dict[str, Any]) -> float:
    explicit = _float(row.get("usdcSize") or row.get("usdc_size"))
    if explicit is not None:
        return explicit
    size = _float(row.get("size")) or 0.0
    price = _float(row.get("price")) or 0.0
    return size * price


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
