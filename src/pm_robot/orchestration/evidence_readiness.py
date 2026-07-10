"""Shared evidence readiness gates for review and dashboard code."""

from __future__ import annotations

from typing import Any

from pm_robot.pipeline_terms import EvidenceJobStage, EvidenceStatus, EvidenceTier


BOUNDED_DEEP_MIN_ACTIVITY_COUNT = 500
BOUNDED_DEEP_MIN_DISTINCT_MARKETS = 20
BOUNDED_DEEP_MIN_NON_FAST_TRADE_COUNT = 100


def paper_evidence_ready(row: Any) -> bool:
    """Return whether research evidence is sufficient for paper-stage review."""

    if not row:
        return False
    evidence_tier = _row_value(row, "discovery_tier", "evidence_tier")
    evidence_status = _row_value(row, "evidence_status")
    if (
        evidence_tier == EvidenceTier.L3_DEEP.value
        and evidence_status == EvidenceStatus.SUMMARY_READY.value
    ):
        return True
    current_stage = _row_value(row, "current_stage", "evidence_current_stage")
    return (
        current_stage == EvidenceJobStage.DEEP_DONE.value
        and evidence_status == EvidenceStatus.SUMMARY_READY.value
        and _row_int(row, "activity_count", "evidence_activity_count") >= BOUNDED_DEEP_MIN_ACTIVITY_COUNT
        and _row_int(row, "distinct_markets") >= BOUNDED_DEEP_MIN_DISTINCT_MARKETS
        and _row_int(row, "non_fast_trade_count") >= BOUNDED_DEEP_MIN_NON_FAST_TRADE_COUNT
    )


def paper_evidence_ready_sql(alias: str = "wps") -> str:
    """SQL equivalent of paper_evidence_ready for wallet_processing_state rows."""

    prefix = f"{alias}." if alias else ""
    return f"""(
        (
            COALESCE({prefix}discovery_tier, '') = '{EvidenceTier.L3_DEEP.value}'
            AND COALESCE({prefix}evidence_status, '') = '{EvidenceStatus.SUMMARY_READY.value}'
        )
        OR (
            COALESCE({prefix}current_stage, '') = '{EvidenceJobStage.DEEP_DONE.value}'
            AND COALESCE({prefix}evidence_status, '') = '{EvidenceStatus.SUMMARY_READY.value}'
            AND COALESCE({prefix}activity_count, 0) >= {BOUNDED_DEEP_MIN_ACTIVITY_COUNT}
            AND COALESCE({prefix}distinct_markets, 0) >= {BOUNDED_DEEP_MIN_DISTINCT_MARKETS}
            AND COALESCE({prefix}non_fast_trade_count, 0) >= {BOUNDED_DEEP_MIN_NON_FAST_TRADE_COUNT}
        )
    )"""


def _row_value(row: Any, *keys: str) -> str:
    for key in keys:
        try:
            value = row[key]
        except (KeyError, IndexError, TypeError):
            continue
        if value is not None:
            return str(value)
    return ""


def _row_int(row: Any, *keys: str) -> int:
    for key in keys:
        try:
            value = row[key]
        except (KeyError, IndexError, TypeError):
            continue
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0
    return 0
