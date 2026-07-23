"""Canonical discovery-only wallet level contract.

Wallet levels describe the product funnel and resource allocation. History depth
and executable queue state are separate concepts and deliberately avoid L names.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


RECENT_SAMPLE_TRADE_LIMIT = 10
RECENT_SAMPLE_VOLUME_GATE_USDC = 100.0


class WalletLevel(str, Enum):
    """Ordered wallet discovery levels from raw sighting to verified elite."""

    L0 = "l0"
    L1 = "l1"
    L2 = "l2"
    L3 = "l3"
    L4 = "l4"
    L5 = "l5"
    L6 = "l6"


class HistoryDepth(str, Enum):
    """Internal evidence coverage; never a wallet quality level."""

    NONE = "none"
    SAMPLE = "sample"
    LIGHT = "light"
    DEEP = "deep"


class CollectionAction(str, Enum):
    """Network/storage work requested by the level reconciler."""

    NONE = "none"
    SCREEN_RECENT = "screen_recent"
    COLLECT_LIGHT_HISTORY = "collect_light_history"
    COLLECT_DEEP_HISTORY = "collect_deep_history"


WALLET_LEVELS = tuple(level.value for level in WalletLevel)
HISTORY_DEPTHS = tuple(depth.value for depth in HistoryDepth)
COLLECTION_ACTIONS = tuple(action.value for action in CollectionAction)


@dataclass(frozen=True)
class LevelFacts:
    """Current facts used to advance at most one discovery level."""

    current_level: WalletLevel
    verified_trade: bool = False
    trusted_source: bool = False
    screen_complete: bool = False
    sample_trade_count: int = 0
    sample_volume_usdc: float = 0.0
    light_history_complete: bool = False
    deep_history_complete: bool = False
    selected_for_l3: bool = False
    selected_for_l4: bool = False
    selected_for_l5: bool = False
    independent_validation_passed: bool = False
    hard_risk_block: bool = False


@dataclass(frozen=True)
class LevelDecision:
    """One monotonic level decision and its auditable reason."""

    level: WalletLevel
    reason: str


def decide_wallet_level(facts: LevelFacts) -> LevelDecision:
    """Advance no more than one level; never auto-demote an existing wallet."""

    current = WalletLevel(facts.current_level)
    if facts.hard_risk_block:
        return LevelDecision(current, "hard_risk_block")

    if current is WalletLevel.L0:
        if facts.trusted_source:
            return LevelDecision(WalletLevel.L1, "trusted_source")
        if (
            facts.verified_trade
            and facts.sample_volume_usdc >= RECENT_SAMPLE_VOLUME_GATE_USDC
        ):
            return LevelDecision(WalletLevel.L1, "observed_sample_volume")
        return LevelDecision(current, "awaiting_resource_admission")

    if current is WalletLevel.L1:
        if not facts.screen_complete:
            return LevelDecision(current, "awaiting_recent_screen")
        if (
            facts.sample_trade_count > 0
            and facts.sample_volume_usdc >= RECENT_SAMPLE_VOLUME_GATE_USDC
        ):
            return LevelDecision(WalletLevel.L2, "recent_sample_volume")
        return LevelDecision(current, "recent_sample_below_resource_gate")

    if current is WalletLevel.L2:
        if facts.light_history_complete and facts.selected_for_l3:
            return LevelDecision(WalletLevel.L3, "selected_after_light_evidence")
        return LevelDecision(current, "light_evidence_or_selection_pending")

    if current is WalletLevel.L3:
        if facts.deep_history_complete and facts.selected_for_l4:
            return LevelDecision(WalletLevel.L4, "selected_after_deep_evidence")
        return LevelDecision(current, "deep_evidence_or_selection_pending")

    if current is WalletLevel.L4:
        if facts.selected_for_l5:
            return LevelDecision(WalletLevel.L5, "elite_rank_selection")
        return LevelDecision(current, "elite_rank_pending")

    if current is WalletLevel.L5:
        if facts.independent_validation_passed:
            return LevelDecision(WalletLevel.L6, "independent_validation_passed")
        return LevelDecision(current, "independent_validation_pending")

    return LevelDecision(WalletLevel.L6, "highest_level")


def next_collection_action(facts: LevelFacts) -> CollectionAction:
    """Return the only evidence action allowed at the current wallet level."""

    if facts.hard_risk_block:
        return CollectionAction.NONE
    current = WalletLevel(facts.current_level)
    if current is WalletLevel.L1 and not facts.screen_complete:
        return CollectionAction.SCREEN_RECENT
    if current is WalletLevel.L2 and not facts.light_history_complete:
        return CollectionAction.COLLECT_LIGHT_HISTORY
    if current is WalletLevel.L3 and not facts.deep_history_complete:
        return CollectionAction.COLLECT_DEEP_HISTORY
    return CollectionAction.NONE
