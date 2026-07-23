"""Canonical queue terminology for the L0-L6 wallet discovery pipeline."""

from __future__ import annotations

from enum import Enum


class PipelineJobType(str, Enum):
    """Active queue categories in pipeline_jobs.job_type."""

    WALLET_RECENT_SCREEN = "wallet_recent_screen"
    WALLET_HISTORY_COLLECT = "wallet_history_collect"
    WALLET_L6_VALIDATE = "wallet_l6_validate"


ACTIVE_PIPELINE_JOB_TYPES = (
    PipelineJobType.WALLET_RECENT_SCREEN.value,
    PipelineJobType.WALLET_HISTORY_COLLECT.value,
    PipelineJobType.WALLET_L6_VALIDATE.value,
)
PIPELINE_JOB_TYPES = ACTIVE_PIPELINE_JOB_TYPES
