from pathlib import Path

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress, CandidateStage, WalletFeatures
from pm_robot.research.scoring import score_candidate


POLICY = load_policy(Path("config/leader_scoring_policy.json"))


def test_candidate_without_features_needs_data():
    candidate = CandidateAddress(address="0x" + "1" * 40)
    score = score_candidate(candidate, None, POLICY)
    assert score.stage == CandidateStage.NEEDS_DATA
    assert score.reason == "no_wallet_metrics_attached"


def test_wash_wallet_is_hygiene_blocked():
    candidate = CandidateAddress(address="0x" + "2" * 40)
    features = WalletFeatures(address=candidate.address, hygiene_status="wash")
    score = score_candidate(candidate, features, POLICY)
    assert score.stage == CandidateStage.BLOCKED_HYGIENE


def test_strong_wallet_can_enter_paper_candidate():
    candidate = CandidateAddress(address="0x" + "3" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.72,
        recent_30d_volume_usdc=750_000,
        net_pnl_usdc=250_000,
        total_volume_usdc=5_000_000,
        event_win_rate=0.88,
        trade_win_rate=0.58,
        avg_dca_entries=25,
        sell_pct=2,
        bot_score=45,
        maker_fraction=0.1,
        leader_in_degree=8,
        copy_event_count=40,
        copy_market_count=12,
        containment_pct_median=0.95,
        copy_stream_roi=0.025,
        edge_retention_pct=70,
        walk_forward_consistency_pct=60,
        survival_score=70,
        single_market_pnl_share=0.2,
        net_to_gross_exposure=0.7,
        hygiene_status="clean",
        primary_category="politics",
        extra={"paper_roi_after_slippage": 0.08},
    )
    score = score_candidate(candidate, features, POLICY)
    assert score.stage in {CandidateStage.PAPER_CANDIDATE, CandidateStage.PAPER_APPROVED}
    assert score.leader_score >= 70


def test_missing_required_score_components_needs_data():
    candidate = CandidateAddress(address="0x" + "4" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.72,
        recent_30d_volume_usdc=5_000,
        net_pnl_usdc=500,
        total_volume_usdc=10_000,
        hygiene_status="clean",
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.stage == CandidateStage.NEEDS_DATA
    assert score.reason.startswith("missing_required_score_components:")
    assert "maker_fraction" not in score.reason


def test_economic_materiality_precedes_missing_expensive_score_components():
    candidate = CandidateAddress(address="0x" + "6" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.72,
        recent_30d_volume_usdc=100,
        net_pnl_usdc=20,
        total_volume_usdc=100,
        hygiene_status="clean",
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.stage == CandidateStage.NEEDS_DATA
    assert score.reason == "insufficient_total_volume_usdc:100.00<1000.00"


def test_missing_maker_taker_evidence_does_not_block_paper_candidate():
    candidate = CandidateAddress(address="0x" + "7" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.72,
        recent_30d_volume_usdc=750_000,
        net_pnl_usdc=250_000,
        total_volume_usdc=5_000_000,
        event_win_rate=0.88,
        trade_win_rate=0.58,
        avg_dca_entries=25,
        sell_pct=2,
        bot_score=45,
        maker_fraction=0.1,
        leader_in_degree=8,
        copy_event_count=40,
        copy_market_count=12,
        containment_pct_median=0.95,
        copy_stream_roi=0.025,
        edge_retention_pct=70,
        walk_forward_consistency_pct=60,
        survival_score=70,
        single_market_pnl_share=0.2,
        net_to_gross_exposure=0.7,
        hygiene_status="clean",
        primary_category="politics",
        extra={
            "maker_fraction_source": "public_activity_no_maker_flags_observed",
            "paper_roi_after_slippage": 0.08,
        },
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.stage in {CandidateStage.PAPER_CANDIDATE, CandidateStage.PAPER_APPROVED}
    assert score.reason != "maker_taker_evidence_incomplete"


def test_high_maker_fraction_remains_hygiene_blocked_when_evidence_exists():
    candidate = CandidateAddress(address="0x" + "8" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.72,
        recent_30d_volume_usdc=750_000,
        net_pnl_usdc=250_000,
        total_volume_usdc=5_000_000,
        event_win_rate=0.88,
        trade_win_rate=0.58,
        avg_dca_entries=25,
        sell_pct=2,
        bot_score=45,
        maker_fraction=0.85,
        leader_in_degree=8,
        copy_event_count=40,
        copy_market_count=12,
        containment_pct_median=0.95,
        copy_stream_roi=0.025,
        edge_retention_pct=70,
        walk_forward_consistency_pct=60,
        survival_score=70,
        single_market_pnl_share=0.2,
        net_to_gross_exposure=0.7,
        hygiene_status="screened",
        primary_category="politics",
        extra={
            "maker_fraction_source": "polymarket_data_api_trades_takerOnly_comparison",
            "paper_roi_after_slippage": 0.08,
        },
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.stage == CandidateStage.BLOCKED_HYGIENE
    assert score.reason == "maker_fraction_above_directional_threshold"


def test_validated_copy_stream_can_enter_formal_paper_below_high_score_band():
    candidate = CandidateAddress(address="0x" + "5" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.55,
        recent_30d_volume_usdc=10_000,
        net_pnl_usdc=750,
        total_volume_usdc=20_000,
        trade_win_rate=0.5,
        avg_dca_entries=2,
        sell_pct=20,
        bot_score=70,
        maker_fraction=0.1,
        leader_in_degree=1,
        copy_event_count=7,
        copy_market_count=7,
        copy_stream_roi=0.2,
        edge_retention_pct=90,
        walk_forward_consistency_pct=100,
        survival_score=90,
        single_market_pnl_share=0.2,
        net_to_gross_exposure=0.9,
        hygiene_status="screened",
        extra={
            "maker_fraction_source": "polymarket_data_api_trades_takerOnly_comparison",
            "copy_backtest_trade_count": 8,
            "copy_backtest_net_pnl_usdc": 25,
        },
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.leader_score >= 40
    assert score.leader_score < POLICY["review_bands"]["paper_candidate"]
    assert score.stage == CandidateStage.NEEDS_REVIEW
    assert score.reason == "validated_copy_stream_below_paper_score"


def test_unvalidated_copy_candidate_signal_does_not_receive_copyability_score_credit():
    candidate = CandidateAddress(address="0x" + "9" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.72,
        recent_30d_volume_usdc=750_000,
        net_pnl_usdc=250_000,
        total_volume_usdc=5_000_000,
        event_win_rate=0.88,
        trade_win_rate=0.58,
        avg_dca_entries=25,
        sell_pct=2,
        bot_score=45,
        maker_fraction=0.1,
        leader_in_degree=1,
        copy_event_count=40,
        copy_market_count=12,
        containment_pct_median=0.77,
        copy_stream_roi=0.0,
        survival_score=70,
        single_market_pnl_share=0.2,
        net_to_gross_exposure=0.7,
        hygiene_status="clean",
        primary_category="politics",
        extra={
            "copy_stream_roi_source": "copy_candidate_pair_stats_unvalidated_default_zero",
            "copy_candidate_event_count": 40,
            "copy_candidate_market_count": 12,
            "copy_validated_pair_count": 0,
        },
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.components["copy_leader_graph"] == 0
    assert score.components["copy_stream_roi"] == 0
    assert score.components["execution_copyability"] == 0
    assert score.stage == CandidateStage.NEEDS_REVIEW


def test_high_score_without_validated_copyability_stays_in_review():
    candidate = CandidateAddress(address="0x" + "a" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.95,
        recent_30d_volume_usdc=100_000_000,
        net_pnl_usdc=20_000_000,
        total_volume_usdc=50_000_000,
        event_win_rate=0.95,
        trade_win_rate=0.9,
        avg_dca_entries=45,
        sell_pct=1,
        bot_score=20,
        median_gap_sec=30,
        maker_fraction=0.1,
        leader_in_degree=4,
        copy_event_count=80,
        copy_market_count=30,
        containment_pct_median=0.8,
        copy_stream_roi=0.0,
        survival_score=100,
        single_market_pnl_share=0.2,
        net_to_gross_exposure=0.8,
        hygiene_status="clean",
        primary_category="politics",
        extra={
            "copy_stream_roi_source": "copy_candidate_pair_stats_without_backtest_default_zero",
            "copy_candidate_event_count": 80,
            "copy_candidate_market_count": 30,
            "copy_validated_pair_count": 0,
            "copy_backtest_trade_count": 0,
            "copy_graph_qualified_follower_count": 0,
        },
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.components["copy_leader_graph"] == 0
    assert score.components["copy_stream_roi"] == 0
    assert score.components["execution_copyability"] == 0
    assert score.leader_score >= 70
    assert score.stage == CandidateStage.NEEDS_REVIEW
    assert score.reason == "copyability_evidence_unvalidated"


def test_stale_copyability_diagnostics_do_not_validate_an_empty_current_sample():
    candidate = CandidateAddress(address="0x" + "b" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=0.95,
        recent_30d_volume_usdc=100_000_000,
        net_pnl_usdc=20_000_000,
        total_volume_usdc=50_000_000,
        event_win_rate=0.95,
        trade_win_rate=0.9,
        avg_dca_entries=45,
        sell_pct=1,
        bot_score=20,
        median_gap_sec=30,
        maker_fraction=0.1,
        leader_in_degree=0,
        copy_event_count=0,
        copy_market_count=0,
        containment_pct_median=0,
        copy_stream_roi=0,
        survival_score=100,
        single_market_pnl_share=0.2,
        net_to_gross_exposure=0.8,
        hygiene_status="clean",
        primary_category="politics",
        extra={
            "copy_backtest_trade_count": 58,
            "copy_graph_qualified_follower_count": 1,
            "copy_stream_roi_source": "no_copy_backtest_default_zero",
        },
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.components["copy_leader_graph"] == 0
    assert score.components["copy_stream_roi"] == 0
    assert score.components["execution_copyability"] == 0
    assert score.stage == CandidateStage.NEEDS_REVIEW
    assert score.reason == "copyability_evidence_unvalidated"


def test_low_absolute_profit_wallet_cannot_be_promoted_by_copy_sample():
    candidate = CandidateAddress(address="0x" + "6" * 40)
    features = WalletFeatures(
        address=candidate.address,
        cumulative_win_rate=1.0,
        recent_30d_volume_usdc=34.6,
        net_pnl_usdc=2.34,
        total_volume_usdc=362.58,
        event_win_rate=1.0,
        trade_win_rate=0.0,
        avg_dca_entries=1,
        sell_pct=0,
        bot_score=8,
        maker_fraction=0.07,
        leader_in_degree=1,
        copy_event_count=6,
        copy_market_count=6,
        copy_stream_roi=0.0368,
        edge_retention_pct=71,
        walk_forward_consistency_pct=100,
        survival_score=99,
        single_market_pnl_share=0.31,
        net_to_gross_exposure=1.0,
        hygiene_status="screened",
        primary_category="crypto",
        extra={
            "maker_fraction_source": "polymarket_data_api_trades_takerOnly_comparison",
            "copy_backtest_trade_count": 6,
            "copy_backtest_net_pnl_usdc": 2.21,
        },
    )

    score = score_candidate(candidate, features, POLICY)

    assert score.leader_score == 0
    assert score.stage == CandidateStage.NEEDS_DATA
    assert score.reason.startswith("insufficient_total_volume_usdc:")
