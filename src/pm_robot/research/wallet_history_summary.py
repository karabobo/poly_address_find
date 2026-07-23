"""Strategy-neutral wallet history summaries and relative-ranking inputs."""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median
from typing import Any

from pm_robot.wallet_levels import HistoryDepth


METHODOLOGY_VERSION = "wallet_history_summary_v2"


@dataclass(frozen=True)
class WalletHistorySummary:
    history_depth: HistoryDepth
    activity_count: int
    distinct_markets: int
    non_fast_trade_count: int
    fast_market_share: float
    total_volume_usdc: float
    buy_count: int
    sell_count: int
    median_gap_sec: float | None
    trades_per_day: float | None
    market_volume_top_share: float
    oldest_timestamp: int | None
    latest_timestamp: int | None
    strategy_tags: tuple[str, ...]
    risk_flags: tuple[str, ...]
    research_score: float
    score_components: dict[str, float]


def summarize_wallet_history(
    rows: list[dict[str, Any]],
    *,
    history_depth: HistoryDepth,
    estimated_pnl_usdc: float | None,
    cost_roi_estimate: float | None,
    now: int,
) -> WalletHistorySummary:
    """Build compact evidence without treating a strategy label as disqualifying."""

    del now
    trades = [row for row in rows if isinstance(row, dict) and _type(row) == "TRADE"]
    timestamps = sorted(_timestamp(row) for row in trades if _timestamp(row) > 0)
    gaps = [right - left for left, right in zip(timestamps, timestamps[1:]) if right >= left]
    markets = [_market(row) for row in trades]
    market_volumes: dict[str, float] = {}
    total_volume = 0.0
    for row, market in zip(trades, markets):
        volume = max(0.0, _trade_usdc(row))
        total_volume += volume
        if market:
            market_volumes[market] = market_volumes.get(market, 0.0) + volume
    active_span_days = (
        max((timestamps[-1] - timestamps[0]) / 86_400, 1.0)
        if timestamps
        else None
    )
    trades_per_day = len(trades) / active_span_days if active_span_days else None
    median_gap = median(gaps) if gaps else None
    fast_count = sum(1 for market in markets if _is_fast_market(market))
    non_fast_count = sum(1 for market in markets if market and not _is_fast_market(market))
    distinct_markets = len({market for market in markets if market})
    top_market_volume = max(market_volumes.values(), default=0.0)
    top_share = top_market_volume / total_volume if total_volume > 0 else 0.0
    buy_count = sum(1 for row in trades if _side(row) == "BUY")
    sell_count = sum(1 for row in trades if _side(row) == "SELL")
    fast_share = fast_count / len(trades) if trades else 0.0
    strategy_tags = _strategy_tags(
        activity_count=len(trades),
        distinct_markets=distinct_markets,
        fast_share=fast_share,
        median_gap=median_gap,
        buy_count=buy_count,
        sell_count=sell_count,
    )
    risk_flags = _risk_flags(
        activity_count=len(trades),
        top_share=top_share,
        estimated_pnl_usdc=estimated_pnl_usdc,
    )
    score_components = _score_components(
        activity_count=len(trades),
        distinct_markets=distinct_markets,
        total_volume=total_volume,
        top_share=top_share,
        timestamps=timestamps,
        estimated_pnl_usdc=estimated_pnl_usdc,
        cost_roi_estimate=cost_roi_estimate,
    )
    research_score = sum(
        score_components[name] * weight
        for name, weight in (
            ("pnl", 0.25),
            ("roi", 0.15),
            ("breadth", 0.20),
            ("activity", 0.20),
            ("persistence", 0.10),
            ("concentration", 0.10),
        )
    )
    return WalletHistorySummary(
        history_depth=HistoryDepth(history_depth),
        activity_count=len(trades),
        distinct_markets=distinct_markets,
        non_fast_trade_count=non_fast_count,
        fast_market_share=fast_share,
        total_volume_usdc=total_volume,
        buy_count=buy_count,
        sell_count=sell_count,
        median_gap_sec=median_gap,
        trades_per_day=trades_per_day,
        market_volume_top_share=top_share,
        oldest_timestamp=timestamps[0] if timestamps else None,
        latest_timestamp=timestamps[-1] if timestamps else None,
        strategy_tags=tuple(strategy_tags),
        risk_flags=tuple(risk_flags),
        research_score=round(_clip(research_score, 0.0, 100.0), 6),
        score_components={key: round(value, 6) for key, value in score_components.items()},
    )


def _score_components(
    *,
    activity_count: int,
    distinct_markets: int,
    total_volume: float,
    top_share: float,
    timestamps: list[int],
    estimated_pnl_usdc: float | None,
    cost_roi_estimate: float | None,
) -> dict[str, float]:
    pnl = float(estimated_pnl_usdc or 0.0)
    pnl_component = 50.0 + math.copysign(min(50.0, math.log1p(abs(pnl)) * 8.0), pnl)
    # Missing ROI is weaker evidence than a measured zero return. Keep it above
    # a hard failure while preventing unknown profitability from ranking neutral.
    roi_component = 25.0 if cost_roi_estimate is None else 50.0 + float(cost_roi_estimate) * 100.0
    breadth = math.log1p(max(0, distinct_markets)) / math.log(21.0) * 100.0
    count_score = math.log1p(max(0, activity_count)) / math.log(1_001.0) * 100.0
    volume_score = math.log1p(max(0.0, total_volume)) / math.log(100_001.0) * 100.0
    activity = (count_score + volume_score) / 2.0
    span_days = (timestamps[-1] - timestamps[0]) / 86_400 if len(timestamps) >= 2 else 0.0
    persistence = math.log1p(max(0.0, span_days)) / math.log(366.0) * 100.0
    return {
        "pnl": _clip(pnl_component, 0.0, 100.0),
        "roi": _clip(roi_component, 0.0, 100.0),
        "breadth": _clip(breadth, 0.0, 100.0),
        "activity": _clip(activity, 0.0, 100.0),
        "persistence": _clip(persistence, 0.0, 100.0),
        "concentration": _clip((1.0 - top_share) * 100.0, 0.0, 100.0),
    }


def _strategy_tags(
    *,
    activity_count: int,
    distinct_markets: int,
    fast_share: float,
    median_gap: float | None,
    buy_count: int,
    sell_count: int,
) -> list[str]:
    tags: list[str] = []
    if fast_share >= 0.7 and activity_count >= 10:
        tags.append("fast_market_specialist")
    if median_gap is not None and median_gap <= 60 and activity_count >= 50:
        tags.append("high_frequency")
    if distinct_markets >= 10:
        tags.append("multi_market")
    if buy_count and sell_count:
        tags.append("two_sided")
    return tags


def _risk_flags(
    *,
    activity_count: int,
    top_share: float,
    estimated_pnl_usdc: float | None,
) -> list[str]:
    flags: list[str] = []
    if activity_count < 25:
        flags.append("thin_history")
    if top_share >= 0.8 and activity_count >= 10:
        flags.append("single_market_concentration")
    if estimated_pnl_usdc is not None and estimated_pnl_usdc < 0:
        flags.append("negative_pnl_estimate")
    return flags


def _market(row: dict[str, Any]) -> str:
    return str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "").strip()


def _type(row: dict[str, Any]) -> str:
    return str(row.get("type") or "TRADE").strip().upper()


def _side(row: dict[str, Any]) -> str:
    return str(row.get("side") or "").strip().upper()


def _timestamp(row: dict[str, Any]) -> int:
    try:
        return int(float(row.get("timestamp") or 0))
    except (TypeError, ValueError):
        return 0


def _trade_usdc(row: dict[str, Any]) -> float:
    explicit = row.get("usdcSize")
    if explicit is None:
        explicit = row.get("usdc_size")
    if explicit is not None:
        return _float(explicit)
    return _float(row.get("size")) * _float(row.get("price"))


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _is_fast_market(market: str) -> bool:
    value = market.lower()
    return "updown-5m" in value or "up-or-down-5m" in value


def _clip(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))
