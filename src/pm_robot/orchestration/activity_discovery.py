"""Candidate discovery from recent public Polymarket trade activity."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.clients.http import HttpClientError
from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.storage.repository import (
    get_wallet_features,
    upsert_wallet_feature,
)


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


def discover_activity_candidates(
    conn: sqlite3.Connection,
    *,
    pages: int = 5,
    page_limit: int = 100,
    min_trade_filter_usdc: float = 0.0,
    max_candidates: int = 200,
    sleep_seconds: float = 0.25,
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
    result = _persist_activity_items(
        conn,
        wallets,
        max_candidates=max_candidates,
        now=now,
        source="polymarket_trades_global",
        labels="trade_activity_seed",
        status_prefix="activity_discovered",
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
        recent_30d_volume_usdc=(
            existing.recent_30d_volume_usdc if existing else None
        ),
        hygiene_status=existing.hygiene_status if existing else "",
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
) -> dict[str, Any]:
    existing = get_wallet_features(conn)
    candidates = 0
    features = 0
    observed = 0
    promoted = 0
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
        allow_l1 = existing_candidate or promoted < max_candidates
        sighting = record_wallet_sighting(
            conn,
            candidate,
            recent_trades=item.get("recent_trades") or [],
            verified_trade=True,
            allow_l1=allow_l1,
            now=now,
        )
        observed += 1
        if not sighting.candidate_updated:
            continue
        if sighting.promoted:
            promoted += 1
        candidates += 1
        upsert_wallet_feature(conn, _feature_from_activity(item, existing.get(item["wallet"])))
        features += 1
    conn.commit()
    return {
        "candidates": candidates,
        "features": features,
        "observed": observed,
        "promoted": promoted,
    }


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
