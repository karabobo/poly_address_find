"""Canonical pipeline terminology.

The database keeps several historical column names for compatibility.  Code
should use these terms when deciding whether a value describes wallet review
state, evidence depth, or an executable queue job.
"""

from __future__ import annotations

from enum import Enum

from pm_robot.models import CandidateStage


class EvidenceTier(str, Enum):
    """Wallet evidence depth, not candidate quality or execution readiness."""

    L0_DISCOVERED = "l0_discovered"
    L1_LIGHT = "l1_light"
    L2_MEDIUM = "l2_medium"
    L3_DEEP = "l3_deep"


class EvidenceJobStage(str, Enum):
    """Historical evidence job stage stored in legacy budget/job fields."""

    LIGHT_PENDING = "light_pending"
    LIGHT_DONE = "light_done"
    MEDIUM_PENDING = "medium_pending"
    MEDIUM_DONE = "medium_done"
    DEEP_PENDING = "deep_pending"
    DEEP_DONE = "deep_done"


class PipelineJobType(str, Enum):
    """Executable queue categories in pipeline_jobs.job_type."""

    WALLET_EVIDENCE_BACKFILL = "wallet_evidence_backfill"
    COPYABILITY_EVIDENCE = "copyability_evidence"


class EvidenceStatus(str, Enum):
    """State summary in wallet_processing_state.evidence_status."""

    PENDING = "pending"
    NEEDS_LIGHT = "needs_light"
    NEEDS_MEDIUM = "needs_medium"
    NEEDS_DEEP = "needs_deep"
    QUEUED = "queued"
    SUMMARY_READY = "summary_ready"
    PAUSED = "paused"


EVIDENCE_TIERS = tuple(tier.value for tier in EvidenceTier)
EVIDENCE_JOB_STAGES = tuple(stage.value for stage in EvidenceJobStage)
PENDING_EVIDENCE_JOB_STAGES = (
    EvidenceJobStage.LIGHT_PENDING.value,
    EvidenceJobStage.MEDIUM_PENDING.value,
    EvidenceJobStage.DEEP_PENDING.value,
)
TERMINAL_EVIDENCE_JOB_STAGES = (
    EvidenceJobStage.LIGHT_DONE.value,
    EvidenceJobStage.MEDIUM_DONE.value,
    EvidenceJobStage.DEEP_DONE.value,
)
DEFAULT_EVIDENCE_JOB_STAGE = EvidenceJobStage.LIGHT_PENDING.value
PIPELINE_JOB_TYPES = tuple(job_type.value for job_type in PipelineJobType)
EVIDENCE_STATUSES = tuple(status.value for status in EvidenceStatus)
CANDIDATE_STAGES = tuple(stage.value for stage in CandidateStage)

# Candidate stages that still belong to the review/paper compatibility funnel.
# The string values are persisted in SQLite, so keep these constants as aliases
# over the historical storage terms rather than renaming database values.
REVIEW_FUNNEL_CANDIDATE_STAGES = (
    CandidateStage.NEEDS_REVIEW.value,
    CandidateStage.PAPER_CANDIDATE.value,
    CandidateStage.PAPER_APPROVED.value,
    CandidateStage.LIVE_ELIGIBLE.value,
)
PAPER_ELIGIBLE_CANDIDATE_STAGES = (
    CandidateStage.PAPER_CANDIDATE.value,
    CandidateStage.PAPER_APPROVED.value,
    CandidateStage.LIVE_ELIGIBLE.value,
)
PROVISIONAL_CANDIDATE_STAGES = (CandidateStage.NEEDS_REVIEW.value,)
PUBLISHABLE_CANDIDATE_STAGE = CandidateStage.LIVE_ELIGIBLE.value
PAPER_READY_CANDIDATE_STAGES = PAPER_ELIGIBLE_CANDIDATE_STAGES
COMPATIBLE_PIPELINE_STAGE_ORDER = (
    CandidateStage.LIVE_ELIGIBLE.value,
    CandidateStage.PAPER_APPROVED.value,
    CandidateStage.PAPER_CANDIDATE.value,
    CandidateStage.NEEDS_REVIEW.value,
)
