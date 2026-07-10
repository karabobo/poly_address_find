"""Local, safe pipeline-cycle orchestration.

The cycle is intentionally conservative: by default it is a dry-run report.
When `execute_plan=True`, it only performs local DB/state work and queue
planning.  It does not run network workers, paper trading, NAS deployment, or
systemd/compose actions.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pm_robot.orchestration.copyability_evidence import plan_copyability_evidence_jobs
from pm_robot.orchestration.eligibility_repair import plan_eligibility_repair_jobs
from pm_robot.orchestration.feature_materializer import materialize_wallet_features
from pm_robot.orchestration.pipeline_smoothness import pipeline_smoothness_report
from pm_robot.orchestration.review_pipeline import score_database
from pm_robot.orchestration.wallet_pipeline import plan_wallet_pipeline_jobs
from pm_robot.storage.repository import materialize_wallet_processing_state


@dataclass(frozen=True)
class PipelineCycleOptions:
    execute_plan: bool = False
    top: int = 20
    min_score: float = 40.0
    state_limit: int = 0
    repair_limit: int = 100
    shard_count: int = 3
    wallet_light_limit: int = 30
    wallet_medium_limit: int = 20
    wallet_deep_limit: int = 5
    copyability_limit: int = 50
    copyability_min_activity_events: int = 25
    feature_limit: int = 10
    feature_min_activity_events: int = 25
    score_limit: int = 0
    run_scoring: bool = True
    policy_path: Path | None = None


def run_pipeline_cycle(conn: sqlite3.Connection, options: PipelineCycleOptions) -> dict[str, Any]:
    """Run or preview one local smoothness cycle."""

    dry_run = not options.execute_plan
    steps: list[dict[str, Any]] = []
    before = pipeline_smoothness_report(
        conn,
        top=options.top,
        min_score=options.min_score,
        min_copyability_activity_events=options.copyability_min_activity_events,
    )

    eligibility_preview = plan_eligibility_repair_jobs(
        conn,
        limit=options.repair_limit,
        min_score=options.min_score,
        shard_count=options.shard_count,
        min_copyability_activity_events=options.copyability_min_activity_events,
        dry_run=True,
    )
    steps.append(
        _step(
            "eligibility_repair_preview",
            "preview",
            eligibility_preview.__dict__,
        )
    )

    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "executed": False,
            "safety": _safety_contract(),
            "before": before,
            "steps": steps + _dry_run_steps(options),
            "after": before,
            "recommended_command": "pipeline-cycle --execute-plan",
        }

    state = materialize_wallet_processing_state(conn, limit=options.state_limit)
    steps.append(_step("wallet_pipeline_state_materialize", "executed", state))

    after_state = pipeline_smoothness_report(
        conn,
        top=options.top,
        min_score=options.min_score,
        min_copyability_activity_events=options.copyability_min_activity_events,
    )
    steps.append(_step("pipeline_smoothness_after_state", "observed", _compact_smoothness(after_state)))

    repair = plan_eligibility_repair_jobs(
        conn,
        limit=options.repair_limit,
        min_score=options.min_score,
        shard_count=options.shard_count,
        min_copyability_activity_events=options.copyability_min_activity_events,
        dry_run=False,
    )
    steps.append(_step("eligibility_repair_plan", "executed", repair.__dict__))

    wallet_plan = plan_wallet_pipeline_jobs(
        conn,
        light_limit=options.wallet_light_limit,
        medium_limit=options.wallet_medium_limit,
        deep_limit=options.wallet_deep_limit,
        shard_count=options.shard_count,
    )
    steps.append(_step("wallet_pipeline_plan", "executed", wallet_plan.__dict__))

    copyability_plan = plan_copyability_evidence_jobs(
        conn,
        limit=options.copyability_limit,
        min_score=options.min_score,
        min_activity_events=options.copyability_min_activity_events,
        shard_count=options.shard_count,
    )
    steps.append(_step("copyability_plan", "executed", copyability_plan.__dict__))

    features = materialize_wallet_features(
        conn,
        limit=options.feature_limit,
        min_activity_events=options.feature_min_activity_events,
    )
    steps.append(_step("materialize_features", "executed", features.__dict__))

    if options.run_scoring:
        if options.policy_path is None:
            raise ValueError("policy_path is required when run_scoring is enabled")
        scores = score_database(
            conn,
            policy_path=options.policy_path,
            export_path=None,
            incremental=True,
            limit=options.score_limit,
        )
        steps.append(_step("incremental_score", "executed", scores))
    else:
        steps.append(_step("incremental_score", "skipped", {"reason": "run_scoring_disabled"}))

    after = pipeline_smoothness_report(
        conn,
        top=options.top,
        min_score=options.min_score,
        min_copyability_activity_events=options.copyability_min_activity_events,
    )
    return {
        "ok": True,
        "dry_run": False,
        "executed": True,
        "safety": _safety_contract(),
        "before": before,
        "steps": steps,
        "after": after,
    }


def _dry_run_steps(options: PipelineCycleOptions) -> list[dict[str, Any]]:
    return [
        _step(
            "wallet_pipeline_state_materialize",
            "would_execute",
            {"limit": options.state_limit},
        ),
        _step(
            "wallet_pipeline_plan",
            "would_execute",
            {
                "light_limit": options.wallet_light_limit,
                "medium_limit": options.wallet_medium_limit,
                "deep_limit": options.wallet_deep_limit,
                "shard_count": options.shard_count,
            },
        ),
        _step(
            "copyability_plan",
            "would_execute",
            {
                "limit": options.copyability_limit,
                "min_score": options.min_score,
                "min_activity_events": options.copyability_min_activity_events,
                "shard_count": options.shard_count,
            },
        ),
        _step(
            "materialize_features",
            "would_execute",
            {
                "limit": options.feature_limit,
                "min_activity_events": options.feature_min_activity_events,
            },
        ),
        _step(
            "incremental_score",
            "would_execute" if options.run_scoring else "skipped",
            {"limit": options.score_limit, "incremental": True},
        ),
        _step("pipeline_smoothness_after", "would_observe", {"top": options.top}),
    ]


def _compact_smoothness(report: dict[str, Any]) -> dict[str, Any]:
    eligibility = report.get("eligibility", {})
    return {
        "stage_counts": report.get("stage_counts", {}),
        "eligibility": {
            "wallets_scanned": eligibility.get("wallets_scanned", 0),
            "paper_eligible": eligibility.get("paper_eligible", 0),
            "paper_ineligible": eligibility.get("paper_ineligible", 0),
            "reason_counts": eligibility.get("reason_counts", {}),
            "action_counts": eligibility.get("action_counts", {}),
        },
        "next_steps": report.get("next_steps", []),
    }


def _step(name: str, status: str, data: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, "status": status, "data": data}


def _safety_contract() -> dict[str, Any]:
    return {
        "local_only": True,
        "runs_network_workers": False,
        "runs_paper_trading": False,
        "deploys_nas": False,
        "mutates_database_when_execute_plan": True,
    }
