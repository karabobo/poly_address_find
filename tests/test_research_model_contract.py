from __future__ import annotations

import dataclasses

from pm_robot.config import RobotSettings
from pm_robot.models import CandidateAddress, WalletFeatures
from pm_robot.pipeline_terms import ACTIVE_PIPELINE_JOB_TYPES, PIPELINE_JOB_TYPES, PipelineJobType


def test_robot_settings_has_no_execution_or_paper_runtime_fields() -> None:
    field_names = {field.name for field in dataclasses.fields(RobotSettings)}

    assert "execution_mode" not in field_names
    assert "paper_ledger_path" not in field_names
    assert "paper_bankroll_usd" not in field_names
    assert "policy_path" not in field_names
    assert "candidate_review_path" not in field_names
    assert not hasattr(RobotSettings(), "assert_safe")


def test_shared_models_export_only_discovery_data_models() -> None:
    import pm_robot.models as models

    assert hasattr(models, "CandidateAddress")
    assert hasattr(models, "WalletFeatures")
    assert not hasattr(models, "CandidateStage")
    assert not hasattr(models, "LegacyCandidateStage")
    assert not hasattr(models, "ExecutionMode")
    assert not hasattr(models, "TradeSignal")
    assert not hasattr(models, "ExecutionDecision")
    assert not hasattr(models, "ScoreBreakdown")

    candidate = CandidateAddress(address="0xabc", sources="manual")
    features = WalletFeatures(address=candidate.address, net_pnl_usdc=1.0)

    assert candidate.sources == "manual"
    assert features.extra == {}


def test_wallet_features_has_no_copy_runtime_fields() -> None:
    field_names = {field.name for field in dataclasses.fields(WalletFeatures)}

    assert "copy_event_count" not in field_names
    assert "copy_market_count" not in field_names
    assert "copy_stream_roi" not in field_names
    assert "maker_fraction" not in field_names
    assert "leader_in_degree" not in field_names
    assert "containment_pct_median" not in field_names
    assert "edge_retention_pct" not in field_names
    assert "walk_forward_consistency_pct" not in field_names
    assert "net_to_gross_exposure" not in field_names


def test_pipeline_terms_expose_only_active_l0_l6_jobs() -> None:
    assert tuple(job.value for job in PipelineJobType) == (
        "wallet_recent_screen",
        "wallet_history_collect",
        "wallet_l6_validate",
    )
    assert ACTIVE_PIPELINE_JOB_TYPES == (
        "wallet_recent_screen",
        "wallet_history_collect",
        "wallet_l6_validate",
    )
    assert PIPELINE_JOB_TYPES == ACTIVE_PIPELINE_JOB_TYPES
