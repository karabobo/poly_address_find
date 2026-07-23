"""Shared research data models for wallet discovery."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


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
    survival_score: float | None = None
    single_market_pnl_share: float | None = None
    hygiene_status: str = ""
    primary_category: str = ""
    last_active_days_ago: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)
