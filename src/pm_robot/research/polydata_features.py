"""Convert Polydata trader JSON into pm_robot wallet features."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pm_robot.models import CandidateAddress, WalletFeatures


def load_polydata_traders(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("traders"), list):
        return payload["traders"]
    if isinstance(payload, list):
        return payload
    raise ValueError("Polydata input must be a list or an object with a traders list")


def polydata_candidate(row: dict[str, Any], *, source_name: str) -> CandidateAddress | None:
    wallet = str(row.get("wallet") or "").strip().lower()
    if not wallet:
        return None
    labels = []
    if row.get("smart_level"):
        labels.append(str(row["smart_level"]))
    if row.get("bot_verdict"):
        labels.append(str(row["bot_verdict"]))
    notes = []
    if row.get("nickname"):
        notes.append(f"nickname={row['nickname']}")
    if row.get("rank"):
        notes.append(f"rank={row['rank']}")
    return CandidateAddress(
        address=wallet,
        sources=source_name,
        labels=" | ".join(labels),
        notes=" | ".join(notes),
        links=f"https://polydata.pro/traders/{wallet}",
        status="polydata_import",
    )


def polydata_features(row: dict[str, Any]) -> WalletFeatures | None:
    wallet = str(row.get("wallet") or "").strip().lower()
    if not wallet:
        return None

    overview = _dict(row.get("overview"))
    pnl = _dict(row.get("pnl"))
    dca = _dict(row.get("dca"))
    bot = _dict(row.get("bot"))
    profile = _dict(row.get("profile"))
    risk = _dict(row.get("risk"))
    categories = _dict(row.get("categories"))

    trade_win = _as_float(row.get("win_rate"))
    if pnl.get("win_rate") is not None:
        trade_win = _as_float(pnl.get("win_rate"))
    event_win = _as_float(pnl.get("pos_event_win_rate"))
    primary_category = _primary_category(categories)

    net_pnl = _first_float(row, "net_pnl", default=None)
    if net_pnl is None:
        net_pnl = _as_float(overview.get("net_pnl"))
    volume = _first_float(row, "volume", default=None)
    if volume is None:
        volume = _as_float(overview.get("total_buy"))

    extra = {
        "polydata_rank": row.get("rank"),
        "polydata_nickname": row.get("nickname"),
        "smart_score": row.get("smart_score"),
        "smart_level": row.get("smart_level"),
        "profit_factor": pnl.get("profit_factor"),
        "sharpe": risk.get("sharpe"),
        "hhi": risk.get("hhi"),
        "top5_pct": risk.get("top5_pct"),
        "bot_verdict": row.get("bot_verdict") or bot.get("verdict"),
    }
    return WalletFeatures(
        address=wallet,
        cumulative_win_rate=_normalize_rate(trade_win),
        recent_30d_volume_usdc=None,
        net_pnl_usdc=net_pnl,
        total_volume_usdc=volume,
        event_win_rate=_normalize_rate(event_win),
        trade_win_rate=_normalize_rate(trade_win),
        avg_dca_entries=_as_float(dca.get("avg_dca")),
        sell_pct=_as_float(profile.get("sell_pct")),
        bot_score=_first_float(row, "bot_score", default=_as_float(bot.get("score"))),
        trades_per_day=_as_float(bot.get("trades_per_day")),
        median_gap_sec=_as_float(bot.get("med_gap")),
        maker_fraction=None,
        leader_in_degree=None,
        copy_event_count=None,
        copy_market_count=None,
        containment_pct_median=None,
        copy_stream_roi=None,
        edge_retention_pct=None,
        walk_forward_consistency_pct=None,
        survival_score=_first_float(row, "smart_score", default=None),
        single_market_pnl_share=_single_market_share(pnl),
        net_to_gross_exposure=None,
        hygiene_status="clean",
        primary_category=primary_category,
        last_active_days_ago=None,
        extra={k: v for k, v in extra.items() if v not in (None, "")},
    )


def extract_polydata(path: Path) -> tuple[list[CandidateAddress], list[WalletFeatures]]:
    source_name = f"polydata:{path.name}"
    candidates: list[CandidateAddress] = []
    features: list[WalletFeatures] = []
    for row in load_polydata_traders(path):
        candidate = polydata_candidate(row, source_name=source_name)
        feature = polydata_features(row)
        if candidate:
            candidates.append(candidate)
        if feature:
            features.append(feature)
    return candidates, features


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(row: dict[str, Any], key: str, *, default: float | None = None) -> float | None:
    return _as_float(row.get(key)) if row.get(key) is not None else default


def _normalize_rate(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 100.0 if value > 1.0 else value


def _primary_category(categories: dict[str, Any]) -> str:
    best_name = ""
    best_volume = -1.0
    for name, payload in categories.items():
        if not isinstance(payload, dict):
            continue
        volume = _as_float(payload.get("volume")) or 0.0
        if volume > best_volume:
            best_name = str(name).lower()
            best_volume = volume
    return best_name


def _single_market_share(pnl: dict[str, Any]) -> float | None:
    total = abs(_as_float(pnl.get("pos_realized_pnl")) or 0.0)
    top3 = abs(_as_float(pnl.get("top3_pnl")) or 0.0)
    if total <= 0 or top3 <= 0:
        return None
    return min(top3 / total, 1.0)
