import pytest

from pm_robot.research.wallet_history_summary import summarize_wallet_history
from pm_robot.wallet_levels import HistoryDepth


def _row(index: int, *, market: str, usdc: float = 10.0, timestamp_step: int = 30) -> dict:
    return {
        "timestamp": 1_000 + index * timestamp_step,
        "slug": market,
        "type": "TRADE",
        "side": "BUY" if index % 2 == 0 else "SELL",
        "usdcSize": usdc,
    }


def test_fast_high_frequency_strategy_is_tagged_but_not_hygiene_blocked():
    rows = [
        _row(index, market=f"btc-up-or-down-5m-{index % 4}")
        for index in range(60)
    ]

    summary = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.LIGHT,
        estimated_pnl_usdc=120,
        cost_roi_estimate=0.12,
        now=10_000,
    )

    assert "fast_market_specialist" in summary.strategy_tags
    assert "high_frequency" in summary.strategy_tags
    assert "high_frequency" not in summary.risk_flags
    assert "fast_market_specialist" not in summary.risk_flags
    assert summary.fast_market_share == pytest.approx(1.0)
    assert summary.research_score > 50


def test_history_summary_flags_concentration_without_rejecting_strategy():
    rows = [_row(index, market="only-market", usdc=20) for index in range(40)]

    summary = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.LIGHT,
        estimated_pnl_usdc=25,
        cost_roi_estimate=None,
        now=10_000,
    )

    assert summary.market_volume_top_share == pytest.approx(1.0)
    assert "single_market_concentration" in summary.risk_flags
    assert summary.score_components["roi"] == pytest.approx(25.0)
    assert 0 <= summary.research_score <= 100


def test_history_summary_uses_observed_volume_and_distinct_markets():
    rows = [_row(index, market=f"market-{index % 5}", usdc=10 + index) for index in range(20)]

    summary = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.DEEP,
        estimated_pnl_usdc=-30,
        cost_roi_estimate=-0.1,
        now=10_000,
    )

    assert summary.activity_count == 20
    assert summary.distinct_markets == 5
    assert summary.total_volume_usdc == pytest.approx(sum(10 + index for index in range(20)))
    assert "negative_pnl_estimate" in summary.risk_flags
    assert summary.score_components["pnl"] < 50
    assert summary.score_components["roi"] < 50
