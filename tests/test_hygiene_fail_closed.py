from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress, CandidateStage, WalletFeatures
from pm_robot.research.scoring import score_candidate


def _candidate() -> CandidateAddress:
    return CandidateAddress(address="0x" + "1" * 40, sources="test")


def test_missing_hygiene_evidence_blocks_scoring():
    features = WalletFeatures(
        address=_candidate().address,
        total_volume_usdc=10_000,
        recent_30d_volume_usdc=5_000,
        net_pnl_usdc=500,
        hygiene_status="incomplete",
        maker_fraction=None,
    )

    result = score_candidate(_candidate(), features, load_policy())

    assert result.stage == CandidateStage.NEEDS_DATA
    assert "missing_required_score_components" in result.reason


def test_missing_maker_provenance_no_longer_blocks_complete_wallet():
    features = WalletFeatures(
        address=_candidate().address,
        cumulative_win_rate=0.72,
        recent_30d_volume_usdc=750_000,
        net_pnl_usdc=250_000,
        total_volume_usdc=5_000_000,
        event_win_rate=0.88,
        trade_win_rate=0.58,
        avg_dca_entries=25,
        sell_pct=2,
        bot_score=45,
        hygiene_status="clean",
        maker_fraction=0.0,
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
        primary_category="politics",
        extra={"maker_fraction_source": "public_activity_no_maker_flags_observed"},
    )

    result = score_candidate(_candidate(), features, load_policy())

    assert result.stage in {CandidateStage.PAPER_CANDIDATE, CandidateStage.PAPER_APPROVED}
