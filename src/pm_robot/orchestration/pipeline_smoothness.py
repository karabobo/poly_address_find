"""Read-only pipeline smoothness report.

This report answers the operational question: "where are promising wallets
stuck right now?"  It deliberately does not run network workers or mutate
state; repair planning remains explicit through `eligibility-repair-plan`.
"""

from __future__ import annotations

import sqlite3
import time
from collections import Counter
from typing import Any

from pm_robot.orchestration.copyability_evidence import JOB_TYPE as COPYABILITY_JOB_TYPE
from pm_robot.orchestration.eligibility_repair import (
    ACTION_COPYABILITY,
    ACTION_FEATURE_MATERIALIZE,
    ACTION_SOURCE_REVIEW,
    ACTION_WALLET_EVIDENCE,
    _actions_for_reasons,
)
from pm_robot.orchestration.wallet_pipeline import JOB_TYPE as WALLET_PIPELINE_JOB_TYPE
from pm_robot.pipeline_terms import REVIEW_FUNNEL_CANDIDATE_STAGES
from pm_robot.risk.eligibility import (
    paper_eligibility_status,
    publish_eligibility_status,
    winner_library_eligibility_status,
)
from pm_robot.storage.repository import (
    evidence_backfill_summary,
    pipeline_job_summary,
    wallet_processing_state_summary,
)


SMOOTHNESS_STAGES = REVIEW_FUNNEL_CANDIDATE_STAGES


def pipeline_smoothness_report(
    conn: sqlite3.Connection,
    *,
    top: int = 20,
    min_score: float = 0.0,
    min_copyability_activity_events: int = 25,
    now: int | None = None,
) -> dict[str, Any]:
    generated_at = now or int(time.time())
    stage_counts = _stage_counts(conn)
    rows = _eligibility_rows(conn, min_score=min_score)
    reason_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    paper_eligible = 0
    publish_eligible = 0
    winner_eligible = 0
    stuck: list[dict[str, Any]] = []

    for row in rows:
        wallet = str(row["address"]).lower()
        paper = paper_eligibility_status(conn, wallet)
        publish = publish_eligibility_status(conn, wallet)
        winner = winner_library_eligibility_status(conn, wallet)
        paper_eligible += int(paper.eligible)
        publish_eligible += int(publish.eligible)
        winner_eligible += int(winner.eligible)
        if paper.eligible:
            continue
        reasons = tuple(paper.reasons)
        actions = _actions_for_reasons(
            reasons,
            trade_events=int(row["trade_events"] or 0),
            min_copyability_activity_events=min_copyability_activity_events,
        )
        reason_counts.update(reasons)
        action_counts.update(actions)
        stuck.append(
            {
                "wallet": wallet,
                "candidate_stage": row["candidate_stage"],
                "leader_score": float(row["leader_score"] or 0.0),
                "review_reason": row["review_reason"] or "",
                "trade_events": int(row["trade_events"] or 0),
                "paper_reasons": list(reasons),
                "recommended_actions": list(actions),
                "wallet_pipeline_status": row["wallet_pipeline_status"] or "",
                "copyability_status": row["copyability_status"] or "",
                "next_action": row["next_action"] or "",
                "evidence_status": row["evidence_status"] or "",
                "updated_at": int(row["updated_at"] or 0),
            }
        )

    stuck.sort(
        key=lambda item: (
            -len(item["recommended_actions"]),
            -float(item["leader_score"]),
            int(item["trade_events"]),
            str(item["wallet"]),
        )
    )
    queues = _queue_summary(conn)
    eligibility = {
        "wallets_scanned": len(rows),
        "paper_eligible": paper_eligible,
        "paper_ineligible": len(rows) - paper_eligible,
        "publish_eligible": publish_eligible,
        "winner_library_eligible": winner_eligible,
        "reason_counts": dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "action_counts": dict(sorted(action_counts.items(), key=lambda item: (-item[1], item[0]))),
    }
    return {
        "ok": True,
        "generated_at": generated_at,
        "min_score": min_score,
        "top": top,
        "stage_counts": stage_counts,
        "eligibility": eligibility,
        "queues": queues,
        "top_stuck_wallets": stuck[: max(0, top)],
        "next_steps": _next_steps(eligibility, queues),
    }


def _stage_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT candidate_stage, COUNT(*) AS count
        FROM candidate_wallets
        GROUP BY candidate_stage
        ORDER BY count DESC, candidate_stage ASC
        """
    ).fetchall()
    return {str(row["candidate_stage"] or ""): int(row["count"] or 0) for row in rows}


def _eligibility_rows(conn: sqlite3.Connection, *, min_score: float) -> list[sqlite3.Row]:
    return conn.execute(
        f"""
        WITH latest AS (
            SELECT ls.*
            FROM leader_scores ls
            JOIN (
                SELECT address, MAX(score_id) AS max_id
                FROM leader_scores
                GROUP BY address
            ) latest_id
              ON latest_id.address = ls.address
             AND latest_id.max_id = ls.score_id
        ),
        trade_counts AS (
            SELECT address, COUNT(*) AS trade_events
            FROM wallet_activity
            WHERE type = 'TRADE'
            GROUP BY address
        ),
        active_jobs AS (
            SELECT wallet, job_type, GROUP_CONCAT(DISTINCT status) AS statuses
            FROM pipeline_jobs
            WHERE status IN ('queued', 'running', 'failed')
            GROUP BY wallet, job_type
        )
        SELECT
            cw.address,
            cw.candidate_stage,
            cw.updated_at,
            COALESCE(latest.leader_score, 0) AS leader_score,
            COALESCE(latest.review_reason, '') AS review_reason,
            COALESCE(tc.trade_events, 0) AS trade_events,
            COALESCE(wps.evidence_status, '') AS evidence_status,
            COALESCE(wps.next_action, '') AS next_action,
            COALESCE(wallet_job.statuses, '') AS wallet_pipeline_status,
            COALESCE(copy_job.statuses, '') AS copyability_status
        FROM candidate_wallets cw
        LEFT JOIN latest
          ON latest.address = cw.address
        LEFT JOIN trade_counts tc
          ON tc.address = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN active_jobs wallet_job
          ON wallet_job.job_type = ?
         AND wallet_job.wallet = cw.address
        LEFT JOIN active_jobs copy_job
          ON copy_job.job_type = ?
         AND copy_job.wallet = cw.address
        WHERE cw.candidate_stage IN ({",".join("?" for _ in SMOOTHNESS_STAGES)})
          AND COALESCE(latest.leader_score, 0) >= ?
        ORDER BY
            CASE cw.candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                WHEN 'needs_manual_review' THEN 3
                ELSE 4
            END ASC,
            COALESCE(latest.leader_score, 0) DESC,
            COALESCE(tc.trade_events, 0) ASC,
            cw.updated_at DESC,
            cw.address ASC
        """,
        (WALLET_PIPELINE_JOB_TYPE, COPYABILITY_JOB_TYPE, *SMOOTHNESS_STAGES, min_score),
    ).fetchall()


def _queue_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "wallet_pipeline": pipeline_job_summary(conn, job_type=WALLET_PIPELINE_JOB_TYPE),
        "copyability": pipeline_job_summary(conn, job_type=COPYABILITY_JOB_TYPE),
        "all_pipeline_jobs": pipeline_job_summary(conn),
        "evidence_backfill_budget": evidence_backfill_summary(conn),
        "wallet_processing_state": wallet_processing_state_summary(conn),
    }


def _next_steps(eligibility: dict[str, Any], queues: dict[str, Any]) -> list[str]:
    steps: list[str] = []
    action_counts = eligibility.get("action_counts", {})
    if int(action_counts.get(ACTION_WALLET_EVIDENCE, 0)) > 0:
        steps.append("run eligibility-repair-plan, then wallet-pipeline-plan/worker for thin activity evidence")
    if int(action_counts.get(ACTION_COPYABILITY, 0)) > 0:
        steps.append("run eligibility-repair-plan, then copyability-plan/worker for copyability blockers")
    if int(action_counts.get(ACTION_FEATURE_MATERIALIZE, 0)) > 0:
        steps.append("run materialize-features and ingest-trade-roles for hygiene/maker blockers")
    if int(action_counts.get(ACTION_SOURCE_REVIEW, 0)) > 0:
        steps.append("review source provenance for wallets with insufficient source_count")
    if int(eligibility.get("paper_eligible", 0)) > 0:
        steps.append("paper-run has eligible wallets available")
    if not steps:
        queued = _queued_count(queues.get("all_pipeline_jobs", {}))
        if queued > 0:
            steps.append("drain existing queued pipeline jobs before rescoring")
        else:
            steps.append("no immediate paper blockers found in scanned stages")
    return steps


def _queued_count(summary: dict[str, Any]) -> int:
    count = 0
    for row in summary.get("statuses", []):
        if str(row.get("status") or "") == "queued":
            count += int(row.get("count") or 0)
    return count
