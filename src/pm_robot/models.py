"""Shared domain models for the Polymarket copy-trading robot."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class CandidateStage(str, Enum):
    """Research/scoring stage; not an L1/L2/L3 evidence tier."""

    IMPORTED = "imported"
    NEEDS_DATA = "needs_data"
    NEEDS_REVIEW = "needs_manual_review"
    PAPER_CANDIDATE = "paper_candidate"
    PAPER_APPROVED = "paper_approved"
    LIVE_ELIGIBLE = "live_eligible"
    REJECTED = "rejected"
    BLOCKED_HYGIENE = "blocked_hygiene"
    BLOCKED_COPYABILITY = "blocked_copyability"


class ExecutionMode(str, Enum):
    SHADOW = "shadow"
    PAPER = "paper"
    LIVE = "live"


@dataclass(frozen=True)
class CandidateAddress:
    address: str
    sources: str = ""
    labels: str = ""
    notes: str = ""
    links: str = ""
    status: str = ""


@dataclass(frozen=True)
class WalletFeatures:
    address: str
    cumulative_win_rate: float | None = None
    recent_30d_volume_usdc: float | None = None
    net_pnl_usdc: float | None = None
    total_volume_usdc: float | None = None
    event_win_rate: float | None = None
    trade_win_rate: float | None = None
    avg_dca_entries: float | None = None
    sell_pct: float | None = None
    bot_score: float | None = None
    trades_per_day: float | None = None
    median_gap_sec: float | None = None
    maker_fraction: float | None = None
    leader_in_degree: float | None = None
    copy_event_count: float | None = None
    copy_market_count: float | None = None
    containment_pct_median: float | None = None
    copy_stream_roi: float | None = None
    edge_retention_pct: float | None = None
    walk_forward_consistency_pct: float | None = None
    survival_score: float | None = None
    single_market_pnl_share: float | None = None
    net_to_gross_exposure: float | None = None
    hygiene_status: str = ""
    primary_category: str = ""
    last_active_days_ago: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScoreBreakdown:
    address: str
    leader_score: float
    stage: CandidateStage
    reason: str
    components: dict[str, float]
    penalties: dict[str, float]


@dataclass(frozen=True)
class TradeSignal:
    signal_id: str
    wallet: str
    market_slug: str
    asset_id: str
    outcome: str
    side: str
    price: float
    detected_at: int
    source: str = "wallet_copy"
    confidence: float = 1.0
    validation_cohort: str = "validation"
    best_bid: float | None = None
    best_ask: float | None = None
    executable_price: float | None = None
    fillable_stake_usd: float = 0.0
    quote_snapshot_at: int = 0
    quote_latency_ms: int = 0
    quote_source: str = ""
    quote_json: str = "{}"


@dataclass(frozen=True)
class ExecutionDecision:
    mode: ExecutionMode
    accepted: bool
    reason: str
    stake_usd: float = 0.0
    route: str = "none"
    executable_price: float | None = None
    fee_usd: float = 0.0
    slippage_bps: float = 0.0
