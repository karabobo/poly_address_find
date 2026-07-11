"""Paper-driven wallet scoring."""

from __future__ import annotations

import math
import re
from dataclasses import asdict
from typing import Any

from pm_robot.config import penalty, threshold, weight
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.risk.gates import hygiene_block_reason, hedge_block_reason

ADDRESS_RE = re.compile(r"0x[a-f0-9]{40}")
INCOMPLETE_HYGIENE_REASONS = {
    "hygiene_evidence_incomplete",
}


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def pct(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 100.0 if value > 1.0 else value


def score_candidate(
    candidate: CandidateAddress,
    features: WalletFeatures | None,
    policy: dict[str, Any],
) -> ScoreBreakdown:
    address = candidate.address.lower()
    if not ADDRESS_RE.fullmatch(address):
        return ScoreBreakdown(address, 0.0, CandidateStage.REJECTED, "invalid_address", {}, {})

    if features is None:
        return ScoreBreakdown(
            address=address,
            leader_score=0.0,
            stage=CandidateStage.NEEDS_DATA,
            reason="no_wallet_metrics_attached",
            components={},
            penalties={},
        )

    h_reason = hygiene_block_reason(features, policy)
    if h_reason and h_reason not in INCOMPLETE_HYGIENE_REASONS:
        return ScoreBreakdown(address, 0.0, CandidateStage.BLOCKED_HYGIENE, h_reason, {}, {})

    missing = _missing_required_components(features, policy)
    if missing:
        return ScoreBreakdown(
            address=address,
            leader_score=0.0,
            stage=CandidateStage.NEEDS_DATA,
            reason="missing_required_score_components:" + ",".join(missing),
            components={},
            penalties={},
        )

    if h_reason in INCOMPLETE_HYGIENE_REASONS:
        return ScoreBreakdown(address, 0.0, CandidateStage.NEEDS_DATA, h_reason, {}, {})

    materiality_reason = economic_materiality_reason(features, policy)
    if materiality_reason:
        return ScoreBreakdown(
            address=address,
            leader_score=0.0,
            stage=CandidateStage.NEEDS_DATA,
            reason=materiality_reason,
            components={},
            penalties={},
        )

    c_reason = hedge_block_reason(features, policy)
    blocked_copyability = bool(c_reason)

    components = _components(features, policy)
    penalties = _penalties(features, policy)
    if blocked_copyability:
        penalties["hedge_or_arbitrage_unreplicable"] = penalty(
            policy, "hedge_or_arbitrage_unreplicable", 25
        )

    raw = sum(components.values()) - sum(penalties.values())
    score = round(clamp(raw / max(_max_score(policy), 1.0), 0.0, 1.0) * 100.0, 2)
    stage, reason = _stage(score, features, policy, blocked_copyability, c_reason)
    return ScoreBreakdown(address, score, stage, reason, components, penalties)


def _components(features: WalletFeatures, policy: dict[str, Any]) -> dict[str, float]:
    win = pct(features.cumulative_win_rate) or pct(features.trade_win_rate) or 0.0
    event_win = pct(features.event_win_rate) or 0.0
    recent_vol = features.recent_30d_volume_usdc or 0.0
    net_pnl = max(features.net_pnl_usdc or 0.0, 0.0)
    total_vol = features.total_volume_usdc or 0.0
    dca = features.avg_dca_entries or 0.0
    sell_pct = features.sell_pct or 0.0
    copyability_validated = _has_validated_copyability(features, policy)
    copy_events = (features.copy_event_count or 0.0) if copyability_validated else 0.0
    copy_markets = (features.copy_market_count or 0.0) if copyability_validated else 0.0
    copy_roi = (features.copy_stream_roi or 0.0) if copyability_validated else 0.0
    edge_retention = features.edge_retention_pct or 0.0
    survival = features.survival_score or 0.0

    volume_return = net_pnl / total_vol if total_vol > 0 else 0.0
    copy_graph = min(copy_events / 25.0, 1.0) * 0.65 + min(copy_markets / 10.0, 1.0) * 0.35
    # Raw copy-candidate links are discovery hints, not proof that we can copy
    # the wallet. Execution credit starts only after copyability is validated.
    execution = 0.0
    if copyability_validated:
        execution = 0.5
        if edge_retention:
            execution = clamp(edge_retention / 100.0)
        elif features.median_gap_sec and features.median_gap_sec >= 5:
            execution = 0.7

    copy_stream_component = 0.0
    if copyability_validated:
        copy_stream_component = weight(policy, "copy_stream_roi", 10) * clamp((copy_roi + 0.01) / 0.05)

    return {
        "cumulative_win_rate": weight(policy, "cumulative_win_rate", 18) * clamp((win - 0.45) / 0.35),
        "recent_30d_activity": weight(policy, "recent_30d_activity", 14) * clamp(math.log1p(recent_vol) / math.log(1_000_000)),
        "net_pnl_quality": weight(policy, "net_pnl_quality", 10) * clamp(math.log10(net_pnl + 1) / 7.0 + volume_return),
        "event_win_rate": weight(policy, "event_win_rate", 14) * clamp((event_win - 0.55) / 0.35),
        "dca_intensity": weight(policy, "dca_intensity", 10) * clamp(math.log1p(dca) / math.log(51)),
        "low_sell_rate": weight(policy, "low_sell_rate", 8) * clamp(1.0 - sell_pct / 35.0),
        "copy_leader_graph": weight(policy, "copy_leader_graph", 16) * clamp(copy_graph),
        "copy_stream_roi": copy_stream_component,
        "slow_resolution_specialization": weight(policy, "slow_resolution_specialization", 6) * _category_score(features.primary_category),
        "execution_copyability": weight(policy, "execution_copyability", 10) * execution,
        "risk_adjusted_performance": weight(policy, "risk_adjusted_performance", 8) * clamp(survival / 100.0),
    }


def _has_validated_copyability(features: WalletFeatures, policy: dict[str, Any]) -> bool:
    min_backtest_trades = int(threshold(policy, "min_copy_backtest_trades", 5))
    try:
        backtest_trades = int(features.extra.get("copy_backtest_trade_count") or 0)
    except (TypeError, ValueError):
        backtest_trades = 0
    try:
        validated_pairs = int(features.extra.get("copy_validated_pair_count") or 0)
    except (TypeError, ValueError):
        validated_pairs = 0
    source = str(features.extra.get("copy_stream_roi_source") or "")
    if backtest_trades >= min_backtest_trades:
        return True
    if validated_pairs > 0:
        return True
    if source == "copy_leader_performance":
        return True
    if features.edge_retention_pct is not None and features.walk_forward_consistency_pct is not None:
        return True
    if str(features.extra.get("copy_graph_qualified_follower_count") or "") not in {"", "0", "0.0"}:
        return True
    return False


def _missing_required_components(features: WalletFeatures, policy: dict[str, Any]) -> list[str]:
    required = policy.get("required_score_components", [])
    if not isinstance(required, list):
        return []
    missing: list[str] = []
    for name in required:
        key = str(name)
        if not key:
            continue
        if hasattr(features, key):
            value = getattr(features, key)
        else:
            value = features.extra.get(key)
        if value is None or value == "":
            missing.append(key)
    return missing


def _penalties(features: WalletFeatures, policy: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    bot = features.bot_score or 0.0
    if bot >= threshold(policy, "pure_bot_score", 80):
        out["pure_bot_or_hft"] = penalty(policy, "pure_bot_or_hft", 20)
    if features.trades_per_day and features.trades_per_day >= threshold(policy, "max_hft_trades_per_day", 500):
        out["hft_trades_per_day"] = penalty(policy, "pure_bot_or_hft", 20) * 0.5
    if features.median_gap_sec and features.median_gap_sec <= threshold(policy, "max_hft_median_gap_sec", 5):
        out["hft_median_gap"] = penalty(policy, "pure_bot_or_hft", 20) * 0.5
    if features.single_market_pnl_share and features.single_market_pnl_share > threshold(policy, "max_single_market_pnl_share", 0.5):
        out["single_market_pnl_concentration"] = penalty(policy, "single_market_pnl_concentration", 15)
    if features.net_pnl_usdc is not None and features.net_pnl_usdc < 0:
        out["negative_lifetime_pnl"] = penalty(policy, "negative_lifetime_pnl", 20)
    if features.last_active_days_ago and features.last_active_days_ago > 30:
        out["stale_wallet"] = penalty(policy, "stale_wallet", 15)
    return out


def economic_materiality_reason(features: WalletFeatures, policy: dict[str, Any]) -> str | None:
    """Return the shared policy reason that blocks economically immaterial wallets."""
    total_volume = features.total_volume_usdc
    if total_volume is None:
        return "missing_economic_materiality:total_volume_usdc"
    min_total_volume = threshold(policy, "min_directional_volume_usdc", 1000)
    if total_volume < min_total_volume:
        return f"insufficient_total_volume_usdc:{total_volume:.2f}<{min_total_volume:.2f}"

    recent_volume = features.recent_30d_volume_usdc
    if recent_volume is None:
        return "missing_economic_materiality:recent_30d_volume_usdc"
    min_recent_volume = threshold(policy, "min_recent_30d_volume_usdc", 500)
    if recent_volume < min_recent_volume:
        return f"insufficient_recent_30d_volume_usdc:{recent_volume:.2f}<{min_recent_volume:.2f}"

    net_pnl = features.net_pnl_usdc
    if net_pnl is None:
        return "missing_economic_materiality:net_pnl_usdc"
    min_net_pnl = threshold(policy, "min_net_pnl_usdc_for_candidate", 50)
    if net_pnl < min_net_pnl:
        return f"insufficient_net_pnl_usdc:{net_pnl:.2f}<{min_net_pnl:.2f}"

    copy_backtest_pnl = features.extra.get("copy_backtest_net_pnl_usdc")
    if copy_backtest_pnl is not None:
        min_copy_backtest_pnl = threshold(policy, "min_copy_backtest_net_pnl_usdc", 5)
        try:
            copy_backtest_value = float(copy_backtest_pnl)
        except (TypeError, ValueError):
            copy_backtest_value = 0.0
        if copy_backtest_value < min_copy_backtest_pnl:
            return (
                "insufficient_copy_backtest_net_pnl_usdc:"
                f"{copy_backtest_value:.2f}<{min_copy_backtest_pnl:.2f}"
            )
    return None


def _category_score(category: str) -> float:
    c = (category or "").lower()
    if c in {"politics", "geopolitics", "economics"}:
        return 1.0
    if c in {"sports", "weather"}:
        return 0.55
    if c == "crypto":
        return 0.25
    return 0.2 if c else 0.0


def _max_score(policy: dict[str, Any]) -> float:
    return sum(float(v) for v in policy.get("weights", {}).values())


def _stage(
    score: float,
    features: WalletFeatures,
    policy: dict[str, Any],
    blocked_copyability: bool,
    copyability_reason: str | None,
) -> tuple[CandidateStage, str]:
    if blocked_copyability:
        return CandidateStage.BLOCKED_COPYABILITY, copyability_reason or "blocked_copyability"
    bands = policy.get("review_bands", {})
    paper_min = float(bands.get("paper_candidate", 70))
    watch_min = float(bands.get("watchlist", 50))
    reject_below = float(bands.get("reject_below", 35))
    if score >= paper_min:
        if not _has_validated_copyability(features, policy):
            return CandidateStage.NEEDS_REVIEW, "copyability_evidence_unvalidated"
        if features.edge_retention_pct is not None and features.walk_forward_consistency_pct is not None:
            return CandidateStage.PAPER_APPROVED, "score_and_validation_present"
        return CandidateStage.PAPER_CANDIDATE, "score_above_paper_threshold"
    validation_min = float(bands.get("formal_validation_candidate", 40))
    min_backtest_trades = int(threshold(policy, "min_copy_backtest_trades", 5))
    if (
        score >= validation_min
        and (features.edge_retention_pct or 0) >= 60
        and (features.walk_forward_consistency_pct or 0) >= 55
        and (features.copy_event_count or 0) >= threshold(policy, "min_copy_events", 5)
        and int(features.extra.get("copy_backtest_trade_count") or 0) >= min_backtest_trades
    ):
        return CandidateStage.NEEDS_REVIEW, "validated_copy_stream_below_paper_score"
    if score >= watch_min:
        return CandidateStage.NEEDS_REVIEW, "watchlist_score"
    if score < reject_below:
        return CandidateStage.REJECTED, "score_below_reject_band"
    return CandidateStage.NEEDS_REVIEW, "borderline_score"


def review_row(candidate: CandidateAddress, score: ScoreBreakdown) -> dict[str, Any]:
    return {
        "address": candidate.address,
        "review_stage": score.stage.value,
        "review_reason": score.reason,
        "leader_score": score.leader_score,
        "sources": candidate.sources,
        "labels": candidate.labels,
        "notes": candidate.notes,
        "links": candidate.links,
        "status": candidate.status,
        "components_json": asdict(score)["components"],
        "penalties_json": asdict(score)["penalties"],
    }
