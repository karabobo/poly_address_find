from pm_robot.orchestration.review_disposition import review_disposition
from pm_robot.pipeline_terms import COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON


def _base_row(**overrides):
    row = {
        "candidate_stage": "needs_manual_review",
        "leader_score": 68,
        "activity_count": 1000,
        "evidence_tier": "l3_deep",
        "evidence_status": "summary_ready",
        "copyability_status": "done",
        "copyability_scan_mode": "deep",
        "feature_copy_event_count": 12,
        "feature_copy_market_count": 3,
        "qualified_follower_count": 0,
        "backtest_trade_count": 0,
        "edge_retention_pct": 0,
        "walk_forward_consistency_pct": 0,
    }
    row.update(overrides)
    return row


def test_deep_copyability_near_miss_is_an_automatic_watch_state():
    disposition = review_disposition(_base_row())

    assert disposition.key == "copyability_near_miss"
    assert disposition.handling == "watch"
    assert disposition.operator_required is False


def test_needs_data_deep_near_miss_remains_an_automatic_watch_state():
    disposition = review_disposition(
        _base_row(
            candidate_stage="needs_data",
            review_reason=COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON,
            leader_score=44,
        )
    )

    assert disposition.key == "copyability_near_miss"
    assert disposition.handling == "watch"
    assert disposition.operator_required is False


def test_only_evidence_ready_above_threshold_review_requires_operator():
    disposition = review_disposition(
        _base_row(
            leader_score=72,
            qualified_follower_count=2,
            backtest_trade_count=8,
        )
    )

    assert disposition.key == "manual_review"
    assert disposition.handling == "manual"
    assert disposition.operator_required is True


def test_thin_history_stays_owned_by_automatic_pipeline():
    disposition = review_disposition(_base_row(activity_count=80))

    assert disposition.key == "thin_evidence"
    assert disposition.handling == "automatic"
    assert disposition.operator_required is False


def test_explicit_paper_readiness_prevents_false_manual_review():
    disposition = review_disposition(
        _base_row(
            leader_score=72,
            qualified_follower_count=2,
            backtest_trade_count=8,
            paper_evidence_ready=False,
        )
    )

    assert disposition.key == "paper_evidence_incomplete"
    assert disposition.handling == "automatic"
    assert disposition.operator_required is False


def test_current_bounded_evidence_overrides_stale_incomplete_reason():
    disposition = review_disposition(
        _base_row(
            leader_score=72,
            qualified_follower_count=2,
            backtest_trade_count=8,
            review_reason="paper_evidence_tier_incomplete",
            evidence_tier="l2_medium",
            evidence_status="summary_ready",
            evidence_current_stage="deep_done",
            evidence_activity_count=500,
            distinct_markets=20,
            non_fast_trade_count=100,
        )
    )

    assert disposition.key == "manual_review"
    assert disposition.operator_required is True
