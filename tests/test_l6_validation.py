from __future__ import annotations

import pytest

from pm_robot.research.l6_validation import (
    L6ValidationDecision,
    evaluate_l6_validation,
)


NOW = 20_000_000


def _closed_rows(*, market: str | None = None, pnl: float = 10.0):
    rows = []
    for index in range(12):
        rows.append(
            {
                "timestamp": NOW - (index * 3 + 2) * 86_400,
                "conditionId": market or f"market-{index % 4}",
                "realizedPnl": pnl,
                "totalBought": 100,
                "asset": f"asset-{index}",
            }
        )
    return rows


def _activity_rows():
    return [
        {
            "timestamp": NOW - (index + 1) * 40_000,
            "type": "TRADE",
            "side": "BUY" if index % 2 == 0 else "SELL",
            "usdcSize": 100 + index,
            "transactionHash": f"0x{index:064x}",
        }
        for index in range(30)
    ]


def _leaderboard_rows(*, all_pnl: float = 100.0, all_volume: float = 10_000.0):
    return [
        {"validationTimePeriod": "WEEK", "pnl": 10.0, "vol": 1_000.0},
        {"validationTimePeriod": "MONTH", "pnl": 40.0, "vol": 4_000.0},
        {"validationTimePeriod": "ALL", "pnl": all_pnl, "vol": all_volume},
    ]


def test_l6_validation_passes_sustained_realized_profit_without_hard_anomaly():
    result = evaluate_l6_validation(
        current_positions=[{"cashPnl": 5, "initialValue": 100}],
        closed_positions=_closed_rows(),
        activity=_activity_rows(),
        leaderboard_rows=_leaderboard_rows(),
        now=NOW,
    )

    assert result.decision is L6ValidationDecision.PASS
    assert result.realized_pnl_usdc == 120
    assert result.recent_realized_pnl_usdc > 0
    assert result.active_weeks >= 4
    assert result.top_market_profit_share <= 0.40
    assert result.official_all_pnl_usdc == 100
    assert result.official_profit_intensity == pytest.approx(0.01)


def test_l6_validation_warns_when_independent_evidence_is_too_thin():
    result = evaluate_l6_validation(
        current_positions=[],
        closed_positions=_closed_rows()[:2],
        activity=_activity_rows(),
        leaderboard_rows=_leaderboard_rows(),
        now=NOW,
    )

    assert result.decision is L6ValidationDecision.WARNING
    assert "insufficient_closed_positions" in result.reason
    assert "insufficient_active_weeks" in result.reason


def test_l6_validation_fails_extreme_single_market_profit_concentration():
    result = evaluate_l6_validation(
        current_positions=[],
        closed_positions=_closed_rows(market="one-market"),
        activity=_activity_rows(),
        leaderboard_rows=_leaderboard_rows(),
        now=NOW,
    )

    assert result.decision is L6ValidationDecision.FAIL
    assert "extreme_market_profit_concentration" in result.reason
    assert "market_profit_concentration" in result.abnormal_flags


def test_l6_validation_does_not_pass_truncated_sources():
    result = evaluate_l6_validation(
        current_positions=[],
        closed_positions=_closed_rows(),
        activity=_activity_rows(),
        leaderboard_rows=_leaderboard_rows(),
        closed_positions_complete=False,
        now=NOW,
    )

    assert result.decision is L6ValidationDecision.WARNING
    assert result.evidence_metrics["closed_positions_complete"] is False


def test_l6_validation_does_not_hide_truncated_current_positions_as_activity_gap():
    result = evaluate_l6_validation(
        current_positions=[],
        closed_positions=_closed_rows(),
        activity=_activity_rows(),
        leaderboard_rows=_leaderboard_rows(),
        current_positions_complete=False,
        now=NOW,
    )

    assert result.decision is L6ValidationDecision.WARNING
    assert "current_positions_incomplete" in result.reason
    assert result.evidence_metrics["current_positions_complete"] is False
    assert result.evidence_metrics["activity_complete"] is True


def test_l6_validation_fails_non_positive_official_all_time_pnl_even_when_history_is_thin():
    result = evaluate_l6_validation(
        current_positions=[],
        closed_positions=_closed_rows()[:2],
        activity=_activity_rows(),
        leaderboard_rows=_leaderboard_rows(all_pnl=-1.0),
        now=NOW,
    )

    assert result.decision is L6ValidationDecision.FAIL
    assert result.reason == "non_positive_official_all_time_pnl"
    assert result.official_all_pnl_usdc == -1.0


def test_l6_validation_warns_for_weak_positive_official_profit_intensity():
    result = evaluate_l6_validation(
        current_positions=[],
        closed_positions=_closed_rows(),
        activity=_activity_rows(),
        leaderboard_rows=_leaderboard_rows(all_pnl=1.0, all_volume=1_000.0),
        now=NOW,
    )

    assert result.decision is L6ValidationDecision.WARNING
    assert result.reason == "weak_official_profit_intensity"
    assert result.official_profit_intensity == pytest.approx(0.001)


def test_l6_validation_warns_when_official_all_time_crosscheck_is_missing():
    result = evaluate_l6_validation(
        current_positions=[],
        closed_positions=_closed_rows(),
        activity=_activity_rows(),
        leaderboard_rows=[],
        now=NOW,
    )

    assert result.decision is L6ValidationDecision.WARNING
    assert result.reason == "official_all_time_pnl_incomplete"
    assert result.official_all_pnl_usdc is None


@pytest.mark.parametrize(
    ("wallet", "official_pnl", "official_volume", "expected", "reason"),
    [
        ("f7bd", 208_281.77, 8_869_206.71, L6ValidationDecision.PASS, "independent_validation_passed"),
        ("0cbb", 43_222.69, 1_029_311.51, L6ValidationDecision.PASS, "independent_validation_passed"),
        ("8cbb", 38_348.63, 3_492_206.71, L6ValidationDecision.PASS, "independent_validation_passed"),
        ("c443", 1_395.82, 981_928.05, L6ValidationDecision.WARNING, "weak_official_profit_intensity"),
        ("0da5", 1_044.91, 2_317_863.45, L6ValidationDecision.WARNING, "weak_official_profit_intensity"),
        ("014c", -2_030.95, 1_083_861.09, L6ValidationDecision.FAIL, "non_positive_official_all_time_pnl"),
        ("0a86", -2_831.48, 899_443.99, L6ValidationDecision.FAIL, "non_positive_official_all_time_pnl"),
        ("6884", -7_187.30, 2_154_768.49, L6ValidationDecision.FAIL, "non_positive_official_all_time_pnl"),
    ],
)
def test_l6_v2_classifies_external_eight_wallet_regression_sample(
    wallet,
    official_pnl,
    official_volume,
    expected,
    reason,
):
    del wallet
    result = evaluate_l6_validation(
        current_positions=[],
        closed_positions=_closed_rows(),
        activity=_activity_rows(),
        leaderboard_rows=_leaderboard_rows(
            all_pnl=official_pnl,
            all_volume=official_volume,
        ),
        now=NOW,
    )

    assert result.decision is expected
    assert result.reason == reason
