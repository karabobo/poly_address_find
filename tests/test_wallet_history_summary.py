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
        pnl_coverage="light_recent_bounded",
        official_all_pnl_usdc=120,
        official_profit_intensity=0.012,
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
        pnl_coverage="light_recent_bounded",
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
        pnl_coverage="complete",
        now=10_000,
    )

    assert summary.activity_count == 20
    assert summary.distinct_markets == 5
    assert summary.total_volume_usdc == pytest.approx(sum(10 + index for index in range(20)))
    assert "negative_pnl_estimate" in summary.risk_flags
    assert summary.score_components["pnl"] < 50
    assert summary.score_components["roi"] < 50


def test_history_summary_dedupes_same_transaction_with_extra_metadata():
    trade = {
        **_row(1, market="market-a", usdc=25),
        "tradeId": "trade-a",
        "transactionHash": "0x" + "a" * 64,
        "asset": "asset-a",
        "outcome": "YES",
    }
    duplicate = {
        key: value
        for key, value in trade.items()
        if key not in {"asset", "transactionHash"}
    }
    duplicate.update(
        name="enriched copy",
        profileImage="https://example.test/a",
    )

    summary = summarize_wallet_history(
        [trade, duplicate],
        history_depth=HistoryDepth.LIGHT,
        estimated_pnl_usdc=None,
        cost_roi_estimate=None,
        now=10_000,
    )

    assert summary.activity_count == 1
    assert summary.total_volume_usdc == pytest.approx(25)
    assert summary.distinct_markets == 1


def test_bounded_pnl_is_not_ranked_as_lifetime_profit():
    rows = [_row(index, market=f"market-{index % 8}", usdc=50) for index in range(100)]

    bounded = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.DEEP,
        estimated_pnl_usdc=500_000,
        cost_roi_estimate=5.0,
        pnl_coverage="deep_recent_bounded",
        now=10_000,
    )
    complete = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.DEEP,
        estimated_pnl_usdc=500_000,
        cost_roi_estimate=5.0,
        pnl_coverage="complete",
        now=10_000,
    )

    assert bounded.score_components["pnl"] == pytest.approx(25.0)
    assert bounded.score_components["roi"] == pytest.approx(25.0)
    assert "pnl_evidence_incomplete" in bounded.risk_flags
    assert complete.score_components["pnl"] > bounded.score_components["pnl"]


def test_declared_profit_anchors_avoid_small_pnl_saturation():
    rows = [_row(index, market=f"market-{index % 8}", usdc=50) for index in range(100)]

    small = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.DEEP,
        estimated_pnl_usdc=517,
        cost_roi_estimate=0.01,
        pnl_coverage="complete",
        now=10_000,
    )
    large = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.DEEP,
        estimated_pnl_usdc=100_000,
        cost_roi_estimate=0.01,
        pnl_coverage="complete",
        now=10_000,
    )

    assert 50 < small.score_components["pnl"] < 80
    assert large.score_components["pnl"] > small.score_components["pnl"]


def test_official_pnl_changes_diagnostic_but_not_forward_selection_score():
    rows = [_row(index, market=f"market-{index % 6}", usdc=75) for index in range(80)]

    negative = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.DEEP,
        estimated_pnl_usdc=None,
        cost_roi_estimate=None,
        official_all_pnl_usdc=-100_000,
        official_profit_intensity=-0.1,
        now=10_000,
    )
    positive = summarize_wallet_history(
        rows,
        history_depth=HistoryDepth.DEEP,
        estimated_pnl_usdc=None,
        cost_roi_estimate=None,
        official_all_pnl_usdc=1_000_000,
        official_profit_intensity=0.2,
        now=10_000,
    )

    assert positive.research_score > negative.research_score
    assert positive.diagnostic_score == positive.research_score
    assert negative.forward_selection_score == pytest.approx(
        positive.forward_selection_score
    )
