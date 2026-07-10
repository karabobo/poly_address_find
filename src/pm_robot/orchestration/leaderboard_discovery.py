"""Discover active candidate wallets from public leaderboard snapshots."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.storage.repository import get_wallet_features, upsert_candidate, upsert_wallet_feature


DEFAULT_METRICS = ("profit", "volume")
DEFAULT_WINDOWS = ("1d", "7d", "30d")
DEFAULT_CATEGORIES = ("OVERALL", "POLITICS", "SPORTS", "CRYPTO", "ECONOMICS", "TECH", "FINANCE")
DEFAULT_TIME_PERIODS = ("DAY", "WEEK", "MONTH", "ALL")
DEFAULT_ORDER_BYS = ("PNL", "VOL")
DEFAULT_V1_LIMIT = 50
DEFAULT_V1_PAGES = 2


@dataclass(frozen=True)
class LeaderboardDiscoverySummary:
    snapshots_attempted: int
    snapshots_succeeded: int
    v1_snapshots_attempted: int
    v1_snapshots_succeeded: int
    candidates_seen: int
    candidates_inserted_or_updated: int
    features_updated: int
    status: str
    error: str = ""


def discover_leaderboard_candidates(
    conn: sqlite3.Connection,
    *,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
    windows: tuple[str, ...] = DEFAULT_WINDOWS,
    categories: tuple[str, ...] = DEFAULT_CATEGORIES,
    time_periods: tuple[str, ...] = DEFAULT_TIME_PERIODS,
    order_bys: tuple[str, ...] = DEFAULT_ORDER_BYS,
    v1_limit: int = DEFAULT_V1_LIMIT,
    v1_pages: int = DEFAULT_V1_PAGES,
    client: PublicPolymarketClient | None = None,
) -> LeaderboardDiscoverySummary:
    client = client or PublicPolymarketClient(conn=conn)
    attempted = 0
    succeeded = 0
    seen: dict[str, dict[str, Any]] = {}
    error = ""
    for metric in metrics:
        for window in windows:
            attempted += 1
            try:
                rows = client.leaderboard(metric, window=window)
                succeeded += 1
            except Exception as exc:
                error = f"{metric}:{window}: {exc}"
                continue
            for rank, row in enumerate(rows, start=1):
                wallet = _wallet_from_row(row)
                if not wallet:
                    continue
                item = seen.setdefault(
                    wallet,
                    {
                        "wallet": wallet,
                        "name": _display_name(row),
                        "leaderboard": {},
                    },
                )
                item["leaderboard"][f"{metric}_{window}"] = {
                    "rank": rank,
                    "amount": _float(row.get("amount")),
                }
                if not item.get("name"):
                    item["name"] = _display_name(row)

    v1_attempted = 0
    v1_succeeded = 0
    normalized_limit = min(max(int(v1_limit or DEFAULT_V1_LIMIT), 1), DEFAULT_V1_LIMIT)
    normalized_pages = max(int(v1_pages or 0), 0)
    for category in categories:
        normalized_category = category.strip().upper()
        if not normalized_category:
            continue
        for period in time_periods:
            normalized_period = period.strip().upper()
            if not normalized_period:
                continue
            for order_by in order_bys:
                normalized_order_by = order_by.strip().upper()
                if not normalized_order_by:
                    continue
                for page in range(normalized_pages):
                    v1_attempted += 1
                    try:
                        rows = client.trader_leaderboard(
                            category=normalized_category,
                            time_period=normalized_period,
                            order_by=normalized_order_by,
                            limit=normalized_limit,
                            offset=page * normalized_limit,
                        )
                        v1_succeeded += 1
                    except Exception as exc:
                        error = f"v1:{normalized_category}:{normalized_period}:{normalized_order_by}:{page}: {exc}"
                        continue
                    if not rows:
                        break
                    for rank, row in enumerate(rows, start=page * normalized_limit + 1):
                        wallet = _wallet_from_row(row)
                        if not wallet:
                            continue
                        item = seen.setdefault(
                            wallet,
                            {
                                "wallet": wallet,
                                "name": _display_name(row),
                                "leaderboard": {},
                                "v1_leaderboard": {},
                            },
                        )
                        item.setdefault("v1_leaderboard", {})
                        key = f"{normalized_category}_{normalized_period}_{normalized_order_by}"
                        item["v1_leaderboard"][key] = {
                            "rank": rank,
                            "pnl": _float(row.get("pnl")),
                            "vol": _float(row.get("vol")),
                            "user_name": str(row.get("userName") or row.get("user_name") or "").strip(),
                            "x_username": str(row.get("xUsername") or row.get("x_username") or "").strip(),
                            "verified_badge": bool(row.get("verifiedBadge") or row.get("verified_badge")),
                        }
                        if not item.get("name"):
                            item["name"] = _display_name(row)

    existing = get_wallet_features(conn)
    candidate_count = 0
    feature_count = 0
    now = int(time.time())
    for wallet, item in seen.items():
        upsert_candidate(conn, _candidate_from_item(item, now=now))
        candidate_count += 1
        feature = _feature_from_item(item, existing.get(wallet))
        if feature is not None:
            upsert_wallet_feature(conn, feature)
            feature_count += 1
    conn.commit()
    return LeaderboardDiscoverySummary(
        snapshots_attempted=attempted,
        snapshots_succeeded=succeeded,
        v1_snapshots_attempted=v1_attempted,
        v1_snapshots_succeeded=v1_succeeded,
        candidates_seen=len(seen),
        candidates_inserted_or_updated=candidate_count,
        features_updated=feature_count,
        status="ok" if succeeded or v1_succeeded else "failed",
        error=error,
    )


def _wallet_from_row(row: dict[str, Any]) -> str:
    wallet = str(row.get("proxyWallet") or row.get("wallet") or row.get("address") or "").strip().lower()
    if wallet.startswith("0x") and len(wallet) == 42:
        return wallet
    return ""


def _display_name(row: dict[str, Any]) -> str:
    return str(row.get("pseudonym") or row.get("name") or row.get("userName") or "").strip()


def _candidate_from_item(item: dict[str, Any], *, now: int) -> CandidateAddress:
    leaderboard = item["leaderboard"]
    v1_leaderboard = item.get("v1_leaderboard") or {}
    notes = [f"{key}=rank:{value.get('rank')},amount:{round(value.get('amount') or 0, 4)}" for key, value in sorted(leaderboard.items())]
    notes.extend(
        f"{key}=rank:{value.get('rank')},pnl:{round(value.get('pnl') or 0, 4)},vol:{round(value.get('vol') or 0, 4)}"
        for key, value in sorted(v1_leaderboard.items())
    )
    if item.get("name"):
        notes.insert(0, f"name={item['name']}")
    sources = "polymarket_leaderboard"
    labels = "active_leaderboard"
    if v1_leaderboard:
        sources = "polymarket_leaderboard | polymarket_v1_leaderboard"
        labels = "active_leaderboard | category_leaderboard"
    return CandidateAddress(
        address=item["wallet"],
        sources=sources,
        labels=labels,
        notes=" | ".join(notes),
        links=f"https://polymarket.com/profile/{item['wallet']}",
        status=f"leaderboard_discovered:{now}",
    )


def _feature_from_item(item: dict[str, Any], existing: WalletFeatures | None) -> WalletFeatures | None:
    leaderboard = item["leaderboard"]
    v1_leaderboard = item.get("v1_leaderboard") or {}
    volume_30d = _amount(leaderboard, "volume_30d")
    volume_7d = _amount(leaderboard, "volume_7d")
    volume_1d = _amount(leaderboard, "volume_1d")
    v1_month_volume = _v1_amount(v1_leaderboard, "OVERALL_MONTH_VOL", "vol")
    v1_week_volume = _v1_amount(v1_leaderboard, "OVERALL_WEEK_VOL", "vol")
    v1_day_volume = _v1_amount(v1_leaderboard, "OVERALL_DAY_VOL", "vol")
    v1_all_pnl = _v1_amount(v1_leaderboard, "OVERALL_ALL_PNL", "pnl")
    v1_month_pnl = _v1_amount(v1_leaderboard, "OVERALL_MONTH_PNL", "pnl")
    recent_volume = (
        volume_30d
        if volume_30d is not None
        else v1_month_volume
        if v1_month_volume is not None
        else volume_7d
        if volume_7d is not None
        else v1_week_volume
        if v1_week_volume is not None
        else volume_1d
        if volume_1d is not None
        else v1_day_volume
    )
    net_pnl = v1_all_pnl if v1_all_pnl is not None else v1_month_pnl
    extra = dict(existing.extra) if existing else {}
    extra["leaderboard_discovery"] = leaderboard
    if v1_leaderboard:
        extra["official_v1_leaderboard_discovery"] = v1_leaderboard
    if item.get("name"):
        extra["leaderboard_name"] = item["name"]
    if recent_volume is None and net_pnl is None and existing is None:
        return WalletFeatures(address=item["wallet"], hygiene_status="clean", extra=extra)
    return WalletFeatures(
        address=item["wallet"],
        recent_30d_volume_usdc=recent_volume,
        net_pnl_usdc=net_pnl if net_pnl is not None else existing.net_pnl_usdc if existing else None,
        hygiene_status=(existing.hygiene_status if existing else "") or "clean",
        extra=extra,
    )


def _amount(leaderboard: dict[str, Any], key: str) -> float | None:
    value = leaderboard.get(key)
    if not isinstance(value, dict):
        return None
    return _float(value.get("amount"))


def _v1_amount(leaderboard: dict[str, Any], key: str, field: str) -> float | None:
    value = leaderboard.get(key)
    if not isinstance(value, dict):
        return None
    return _float(value.get(field))


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
