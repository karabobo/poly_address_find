"""Independent L6 validation metrics; this module never changes wallet levels."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from statistics import median
from typing import Any, Iterable

from pm_robot.research.pnl_estimates import estimate_wallet_pnl


L6_VALIDATION_POLICY_VERSION = "l6_independent_v2"


class L6ValidationDecision(str, Enum):
    PASS = "pass"
    WARNING = "warning"
    FAIL = "fail"


@dataclass(frozen=True)
class L6ValidationPolicy:
    """Versioned evidence floors and hard contradiction boundaries for L6."""

    version: str = L6_VALIDATION_POLICY_VERSION
    window_seconds: int = 90 * 86_400
    recent_window_seconds: int = 30 * 86_400
    min_closed_positions: int = 10
    min_active_weeks: int = 4
    min_timestamp_coverage: float = 0.90
    min_positive_week_ratio: float = 0.50
    min_official_profit_intensity: float = 0.002
    hard_top_market_profit_share: float = 0.75
    hard_top_day_profit_share: float = 0.60
    hard_max_drawdown_ratio: float = 1.00


@dataclass(frozen=True)
class L6ValidationResult:
    decision: L6ValidationDecision
    reason: str
    policy_version: str
    coverage_start: int
    coverage_end: int
    closed_position_count: int
    timestamped_closed_position_count: int
    activity_count: int
    active_weeks: int
    positive_week_ratio: float
    realized_pnl_usdc: float
    recent_realized_pnl_usdc: float
    open_pnl_usdc: float
    max_drawdown_usdc: float
    max_drawdown_ratio: float
    top_market_profit_share: float
    top_day_profit_share: float
    churn_ratio: float
    unrealized_profit_share: float
    official_all_pnl_usdc: float | None
    official_all_volume_usdc: float | None
    official_profit_intensity: float | None
    official_month_pnl_usdc: float | None
    official_week_pnl_usdc: float | None
    abnormal_flags: tuple[str, ...]
    evidence_metrics: dict[str, Any]


def evaluate_l6_validation(
    *,
    current_positions: Iterable[dict[str, Any]],
    closed_positions: Iterable[dict[str, Any]],
    activity: Iterable[dict[str, Any]],
    leaderboard_rows: Iterable[dict[str, Any]] = (),
    current_positions_complete: bool = True,
    closed_positions_complete: bool = True,
    activity_complete: bool = True,
    now: int,
    policy: L6ValidationPolicy | None = None,
) -> L6ValidationResult:
    """Validate independent profit, persistence, and anomaly evidence for one L5 wallet."""

    active_policy = policy or L6ValidationPolicy()
    coverage_end = int(now)
    coverage_start = coverage_end - max(1, int(active_policy.window_seconds))
    recent_start = coverage_end - max(1, int(active_policy.recent_window_seconds))
    current_rows = _dict_rows(current_positions)
    closed_rows = _dedupe_rows(_dict_rows(closed_positions))
    activity_rows = _dedupe_rows(_dict_rows(activity))
    leaderboard_data = _dict_rows(leaderboard_rows)
    (
        official_all_pnl,
        official_all_volume,
        official_month_pnl,
        official_week_pnl,
    ) = _leaderboard_profit_metrics(leaderboard_data)
    official_profit_intensity = (
        official_all_pnl / official_all_volume
        if official_all_pnl is not None
        and official_all_pnl > 0
        and official_all_volume is not None
        and official_all_volume > 0
        else None
    )

    timestamped_closed = [row for row in closed_rows if _timestamp(row) > 0]
    window_closed = [
        row for row in timestamped_closed if coverage_start <= _timestamp(row) <= coverage_end
    ]
    pnl_rows = [row for row in window_closed if _float_value(row, "realizedPnl", "realized_pnl", "pnl") is not None]
    realized_events = [
        (_timestamp(row), _float_value(row, "realizedPnl", "realized_pnl", "pnl") or 0.0, _market(row))
        for row in pnl_rows
    ]
    realized_events.sort(key=lambda item: (item[0], item[2], item[1]))
    realized_pnl = sum(item[1] for item in realized_events)
    recent_realized = sum(item[1] for item in realized_events if item[0] >= recent_start)

    weekly_pnl: dict[int, float] = {}
    market_positive: dict[str, float] = {}
    day_positive: dict[int, float] = {}
    total_positive = 0.0
    for timestamp, pnl, market in realized_events:
        week = timestamp // (7 * 86_400)
        weekly_pnl[week] = weekly_pnl.get(week, 0.0) + pnl
        if pnl > 0:
            total_positive += pnl
            market_key = market or "unknown"
            market_positive[market_key] = market_positive.get(market_key, 0.0) + pnl
            day = timestamp // 86_400
            day_positive[day] = day_positive.get(day, 0.0) + pnl
    active_weeks = len(weekly_pnl)
    positive_week_ratio = (
        sum(1 for pnl in weekly_pnl.values() if pnl > 0) / active_weeks
        if active_weeks
        else 0.0
    )
    top_market_share = max(market_positive.values(), default=0.0) / total_positive if total_positive else 0.0
    top_day_share = max(day_positive.values(), default=0.0) / total_positive if total_positive else 0.0
    max_drawdown = _max_drawdown([pnl for _timestamp_value, pnl, _market_value in realized_events])
    max_drawdown_ratio = max_drawdown / max(total_positive, 1.0)

    pnl_estimate = estimate_wallet_pnl(current_rows, ())
    open_pnl = pnl_estimate.open_estimated_pnl_usdc
    positive_open = max(0.0, open_pnl)
    unrealized_share = positive_open / max(positive_open + max(0.0, realized_pnl), 1.0)

    window_activity = [
        row for row in activity_rows if coverage_start <= _timestamp(row) <= coverage_end
    ]
    activity_metrics, anomaly_flags = _activity_anomalies(window_activity)
    timestamp_coverage = len(timestamped_closed) / len(closed_rows) if closed_rows else 0.0

    hard_failures: list[str] = []
    bounded_evidence_warnings: list[str] = []
    if not current_positions_complete:
        bounded_evidence_warnings.append("current_positions_incomplete")
    if not closed_positions_complete:
        bounded_evidence_warnings.append("closed_positions_incomplete")
    if not activity_complete:
        bounded_evidence_warnings.append("activity_incomplete")
    if len(window_closed) < active_policy.min_closed_positions:
        bounded_evidence_warnings.append("insufficient_closed_positions")
    if timestamp_coverage < active_policy.min_timestamp_coverage:
        bounded_evidence_warnings.append("insufficient_timestamp_coverage")
    if active_weeks < active_policy.min_active_weeks:
        bounded_evidence_warnings.append("insufficient_active_weeks")

    bounded_evidence_sufficient = not bounded_evidence_warnings
    if bounded_evidence_sufficient and realized_pnl <= 0:
        hard_failures.append("non_positive_realized_pnl")
    if bounded_evidence_sufficient and recent_realized < 0:
        hard_failures.append("negative_recent_realized_pnl")
    if bounded_evidence_sufficient and positive_week_ratio < active_policy.min_positive_week_ratio:
        hard_failures.append("weak_positive_week_ratio")
    if bounded_evidence_sufficient and top_market_share > active_policy.hard_top_market_profit_share:
        hard_failures.append("extreme_market_profit_concentration")
    if bounded_evidence_sufficient and top_day_share > active_policy.hard_top_day_profit_share:
        hard_failures.append("extreme_day_profit_concentration")
    if bounded_evidence_sufficient and max_drawdown_ratio > active_policy.hard_max_drawdown_ratio:
        hard_failures.append("extreme_realized_drawdown")

    evidence_warnings = list(bounded_evidence_warnings)
    if official_all_pnl is None:
        evidence_warnings.append("official_all_time_pnl_incomplete")
    elif official_all_pnl <= 0:
        # The official lifetime result is a contradiction even when bounded evidence is thin.
        hard_failures.append("non_positive_official_all_time_pnl")
    elif official_all_volume is None or official_all_volume <= 0:
        evidence_warnings.append("official_all_time_volume_incomplete")
    elif (
        official_profit_intensity is not None
        and official_profit_intensity < active_policy.min_official_profit_intensity
    ):
        evidence_warnings.append("weak_official_profit_intensity")

    if active_weeks and active_weeks < 8:
        anomaly_flags.append("limited_weekly_history")
    if positive_week_ratio and positive_week_ratio < 0.60:
        anomaly_flags.append("borderline_positive_week_ratio")
    if top_market_share > 0.40:
        anomaly_flags.append("market_profit_concentration")
    if top_day_share > 0.35:
        anomaly_flags.append("day_profit_concentration")
    if unrealized_share > 0.50:
        anomaly_flags.append("unrealized_profit_dominance")

    if hard_failures:
        decision = L6ValidationDecision.FAIL
        reason_parts = hard_failures
    elif evidence_warnings:
        decision = L6ValidationDecision.WARNING
        reason_parts = evidence_warnings
    else:
        decision = L6ValidationDecision.PASS
        reason_parts = ["independent_validation_passed"]

    evidence_metrics = {
        "current_positions_complete": bool(current_positions_complete),
        "closed_positions_complete": bool(closed_positions_complete),
        "activity_complete": bool(activity_complete),
        "timestamp_coverage": round(timestamp_coverage, 6),
        "total_positive_pnl_usdc": round(total_positive, 6),
        "leaderboard_rows": len(leaderboard_data),
        "official_all_time_pnl_available": official_all_pnl is not None,
        "official_all_time_volume_available": official_all_volume is not None,
        "min_official_profit_intensity": active_policy.min_official_profit_intensity,
        **activity_metrics,
    }
    return L6ValidationResult(
        decision=decision,
        reason=";".join(reason_parts),
        policy_version=active_policy.version,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        closed_position_count=len(window_closed),
        timestamped_closed_position_count=len(timestamped_closed),
        activity_count=len(window_activity),
        active_weeks=active_weeks,
        positive_week_ratio=round(positive_week_ratio, 6),
        realized_pnl_usdc=round(realized_pnl, 6),
        recent_realized_pnl_usdc=round(recent_realized, 6),
        open_pnl_usdc=round(open_pnl, 6),
        max_drawdown_usdc=round(max_drawdown, 6),
        max_drawdown_ratio=round(max_drawdown_ratio, 6),
        top_market_profit_share=round(top_market_share, 6),
        top_day_profit_share=round(top_day_share, 6),
        churn_ratio=round(float(activity_metrics["churn_ratio"]), 6),
        unrealized_profit_share=round(unrealized_share, 6),
        official_all_pnl_usdc=_rounded_optional(official_all_pnl),
        official_all_volume_usdc=_rounded_optional(official_all_volume),
        official_profit_intensity=_rounded_optional(official_profit_intensity),
        official_month_pnl_usdc=_rounded_optional(official_month_pnl),
        official_week_pnl_usdc=_rounded_optional(official_week_pnl),
        abnormal_flags=tuple(sorted(set(anomaly_flags))),
        evidence_metrics=evidence_metrics,
    )


def _activity_anomalies(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    trades = [row for row in rows if _activity_type(row) == "TRADE"]
    gross_volume = sum(max(0.0, _trade_usdc(row)) for row in trades)
    signed_cashflow = sum(
        _trade_usdc(row) * (1.0 if _side(row) == "SELL" else -1.0)
        for row in trades
    )
    churn_ratio = gross_volume / max(abs(signed_cashflow), 100.0)
    special_types = {"SPLIT", "MERGE", "CONVERSION", "REWARD", "MAKER_REBATE", "REFERRAL_REWARD"}
    special_count = sum(1 for row in rows if _activity_type(row) in special_types)
    special_share = special_count / len(rows) if rows else 0.0
    timestamps = sorted(_timestamp(row) for row in trades if _timestamp(row) > 0)
    gaps = [right - left for left, right in zip(timestamps, timestamps[1:]) if right >= left]
    median_gap = median(gaps) if gaps else None
    gap_cv = _coefficient_of_variation(gaps)
    flags: list[str] = []
    if len(trades) >= 100 and churn_ratio > 50.0:
        flags.append("high_turnover_low_net_flow")
    if len(rows) >= 20 and special_share > 0.50:
        flags.append("mechanical_activity_dominance")
    if len(trades) >= 50 and gap_cv is not None and gap_cv < 0.10:
        flags.append("highly_regular_trade_timing")
    if len(trades) >= 200 and median_gap is not None and median_gap <= 5:
        flags.append("extreme_burst_frequency")
    return (
        {
            "trade_count": len(trades),
            "gross_trade_volume_usdc": round(gross_volume, 6),
            "signed_trade_cashflow_usdc": round(signed_cashflow, 6),
            "churn_ratio": round(churn_ratio, 6),
            "special_activity_share": round(special_share, 6),
            "median_trade_gap_seconds": median_gap,
            "trade_gap_cv": round(gap_cv, 6) if gap_cv is not None else None,
        },
        flags,
    )


def _leaderboard_profit_metrics(
    rows: list[dict[str, Any]],
) -> tuple[float | None, float | None, float | None, float | None]:
    """Extract official PnL cross-checks; profit intensity is not account ROI."""

    by_period: dict[str, dict[str, Any]] = {}
    for row in rows:
        period = str(
            row.get("validationTimePeriod")
            or row.get("timePeriod")
            or row.get("time_period")
            or ""
        ).strip().upper()
        if period in {"ALL", "MONTH", "WEEK"} and period not in by_period:
            by_period[period] = row
    all_row = by_period.get("ALL", {})
    return (
        _float_value(all_row, "pnl"),
        _float_value(all_row, "vol", "volume"),
        _float_value(by_period.get("MONTH", {}), "pnl"),
        _float_value(by_period.get("WEEK", {}), "pnl"),
    )


def _rounded_optional(value: float | None) -> float | None:
    return round(value, 6) if value is not None else None


def _max_drawdown(pnl_events: list[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for pnl in pnl_events:
        cumulative += pnl
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return drawdown


def _coefficient_of_variation(values: list[int]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    if mean <= 0:
        return None
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance) / mean


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
        unique[key] = row
    return list(unique.values())


def _dict_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if isinstance(row, dict)]


def _timestamp(row: dict[str, Any]) -> int:
    for key in ("timestamp", "closedAt", "closed_at", "endDate", "end_date"):
        value = row.get(key)
        if value is None or value == "":
            continue
        try:
            return int(float(value))
        except (TypeError, ValueError):
            try:
                text = str(value).strip().replace("Z", "+00:00")
                parsed = datetime.fromisoformat(text)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp())
            except ValueError:
                continue
    return 0


def _market(row: dict[str, Any]) -> str:
    for key in ("conditionId", "condition_id", "eventSlug", "event_slug", "slug", "title"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _activity_type(row: dict[str, Any]) -> str:
    return str(row.get("type") or "TRADE").strip().upper()


def _side(row: dict[str, Any]) -> str:
    return str(row.get("side") or "").strip().upper()


def _trade_usdc(row: dict[str, Any]) -> float:
    direct = _float_value(row, "usdcSize", "usdc_size")
    if direct is not None:
        return max(0.0, direct)
    return max(0.0, (_float_value(row, "size") or 0.0) * (_float_value(row, "price") or 0.0))


def _float_value(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        if value is None or value == "" or isinstance(value, bool):
            continue
        try:
            parsed = float(str(value).strip().replace(",", "").removeprefix("$"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed):
            return parsed
    return None
