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
from typing import Any, Callable

from pm_robot.orchestration.copyability_evidence import plan_copyability_evidence_jobs
from pm_robot.orchestration.eligibility_repair import prepare_eligibility_repairs
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
    state_stale_only: bool = False
    state_commit_every: int = 100
    repair_limit: int = 100
    wallet_shard_count: int = 3
    wallet_light_limit: int = 30
    wallet_medium_limit: int = 20
    wallet_deep_limit: int = 5
    wallet_max_active_jobs: int = 240
    copyability_limit: int = 50
    copyability_max_active_jobs: int = 50
    copyability_min_activity_events: int = 25
    copyability_shard_count: int = 1
    copyability_rescan_seconds: int = 21_600
    feature_limit: int = 10
    feature_min_activity_events: int = 25
    score_limit: int = 0
    run_scoring: bool = True
    policy_path: Path | None = None
    continue_on_error: bool = False
    include_diagnostics: bool = True


StepReporter = Callable[[dict[str, Any]], None]


def run_pipeline_cycle(
    conn: sqlite3.Connection,
    options: PipelineCycleOptions,
    *,
    step_reporter: StepReporter | None = None,
) -> dict[str, Any]:
    """Run or preview one local smoothness cycle."""

    dry_run = not options.execute_plan
    steps: list[dict[str, Any]] = []
    policy_path = options.policy_path
    if options.execute_plan and options.run_scoring and policy_path is None:
        raise ValueError("policy_path is required when run_scoring is enabled")
    before = (
        pipeline_smoothness_report(
            conn,
            top=options.top,
            min_score=options.min_score,
            min_copyability_activity_events=options.copyability_min_activity_events,
        )
        if dry_run or options.include_diagnostics
        else {}
    )

    if dry_run:
        eligibility_preview = prepare_eligibility_repairs(
            conn,
            limit=options.repair_limit,
            min_score=options.min_score,
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

    _run_isolated_step(
        conn,
        steps,
        "eligibility_repair_prepare",
        lambda: prepare_eligibility_repairs(
            conn,
            limit=options.repair_limit,
            min_score=options.min_score,
            min_copyability_activity_events=options.copyability_min_activity_events,
            dry_run=False,
        ),
        continue_on_error=options.continue_on_error,
        step_reporter=step_reporter,
    )

    # Repair budgets must become processing state before the canonical planner runs.
    _run_isolated_step(
        conn,
        steps,
        "wallet_pipeline_state_materialize",
        lambda: materialize_wallet_processing_state(
            conn,
            limit=options.state_limit,
            commit_every=options.state_commit_every,
            stale_only=options.state_stale_only,
        ),
        continue_on_error=options.continue_on_error,
        step_reporter=step_reporter,
    )

    if options.include_diagnostics:
        _run_isolated_step(
            conn,
            steps,
            "pipeline_smoothness_after_state",
            lambda: _compact_smoothness(
                pipeline_smoothness_report(
                    conn,
                    top=options.top,
                    min_score=options.min_score,
                    min_copyability_activity_events=options.copyability_min_activity_events,
                )
            ),
            status="observed",
            continue_on_error=options.continue_on_error,
            step_reporter=step_reporter,
        )

    _run_isolated_step(
        conn,
        steps,
        "wallet_pipeline_plan",
        lambda: plan_wallet_pipeline_jobs(
            conn,
            light_limit=options.wallet_light_limit,
            medium_limit=options.wallet_medium_limit,
            deep_limit=options.wallet_deep_limit,
            shard_count=options.wallet_shard_count,
            max_active_jobs=options.wallet_max_active_jobs,
        ),
        continue_on_error=options.continue_on_error,
        step_reporter=step_reporter,
    )

    _run_isolated_step(
        conn,
        steps,
        "copyability_plan",
        lambda: plan_copyability_evidence_jobs(
            conn,
            limit=options.copyability_limit,
            max_active_jobs=options.copyability_max_active_jobs,
            min_score=options.min_score,
            min_activity_events=options.copyability_min_activity_events,
            shard_count=options.copyability_shard_count,
            rescan_seconds=options.copyability_rescan_seconds,
        ),
        continue_on_error=options.continue_on_error,
        step_reporter=step_reporter,
    )

    _run_isolated_step(
        conn,
        steps,
        "materialize_features",
        lambda: materialize_wallet_features(
            conn,
            limit=options.feature_limit,
            min_activity_events=options.feature_min_activity_events,
        ),
        continue_on_error=options.continue_on_error,
        step_reporter=step_reporter,
    )

    if options.run_scoring:
        assert policy_path is not None
        _run_isolated_step(
            conn,
            steps,
            "incremental_score",
            lambda: score_database(
                conn,
                policy_path=policy_path,
                export_path=None,
                incremental=True,
                limit=options.score_limit,
            ),
            continue_on_error=options.continue_on_error,
            step_reporter=step_reporter,
        )
    else:
        _append_step(
            steps,
            _step("incremental_score", "skipped", {"reason": "run_scoring_disabled"}),
            step_reporter=step_reporter,
        )

    after = {}
    if options.include_diagnostics:
        try:
            after = pipeline_smoothness_report(
                conn,
                top=options.top,
                min_score=options.min_score,
                min_copyability_activity_events=options.copyability_min_activity_events,
            )
        except Exception as exc:
            conn.rollback()
            if not options.continue_on_error:
                raise
            after = before
            _append_step(
                steps,
                _failed_step("pipeline_smoothness_after", exc),
                step_reporter=step_reporter,
            )
    failed_steps = [step["name"] for step in steps if step["status"] == "failed"]
    return {
        "ok": not failed_steps,
        "partial": bool(failed_steps),
        "failed_steps": failed_steps,
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
            {
                "limit": options.state_limit,
                "stale_only": options.state_stale_only,
                "commit_every": options.state_commit_every,
            },
        ),
        _step(
            "wallet_pipeline_plan",
            "would_execute",
            {
                "light_limit": options.wallet_light_limit,
                "medium_limit": options.wallet_medium_limit,
                "deep_limit": options.wallet_deep_limit,
                "shard_count": options.wallet_shard_count,
                "max_active_jobs": options.wallet_max_active_jobs,
            },
        ),
        _step(
            "copyability_plan",
            "would_execute",
            {
                "limit": options.copyability_limit,
                "max_active_jobs": options.copyability_max_active_jobs,
                "min_score": options.min_score,
                "min_activity_events": options.copyability_min_activity_events,
                "shard_count": options.copyability_shard_count,
                "rescan_seconds": options.copyability_rescan_seconds,
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


def _run_isolated_step(
    conn: sqlite3.Connection,
    steps: list[dict[str, Any]],
    name: str,
    operation: Callable[[], Any],
    *,
    status: str = "executed",
    continue_on_error: bool,
    step_reporter: StepReporter | None,
) -> Any | None:
    """Run one committed phase; a failed phase cannot poison later phases."""

    try:
        result = operation()
    except Exception as exc:
        conn.rollback()
        failed = _failed_step(name, exc)
        _append_step(steps, failed, step_reporter=step_reporter)
        if not continue_on_error:
            raise
        return None
    data = result if isinstance(result, dict) else result.__dict__
    _append_step(steps, _step(name, status, data), step_reporter=step_reporter)
    return result


def _failed_step(name: str, exc: Exception) -> dict[str, Any]:
    return _step(
        name,
        "failed",
        {"error_type": type(exc).__name__, "error": str(exc)[:1000]},
    )


def _append_step(
    steps: list[dict[str, Any]],
    step: dict[str, Any],
    *,
    step_reporter: StepReporter | None,
) -> None:
    steps.append(step)
    if step_reporter is None:
        return
    try:
        step_reporter(step)
    except Exception:
        # Observability must not change scheduling behavior.
        pass


def _safety_contract() -> dict[str, Any]:
    return {
        "local_only": True,
        "runs_network_workers": False,
        "runs_paper_trading": False,
        "deploys_nas": False,
        "mutates_database_when_execute_plan": True,
    }
