"""Prepare eligibility blockers for the canonical evidence planners.

The paper/publish gates intentionally fail closed.  This planner keeps the
pipeline smooth by turning actionable paper-eligibility blockers into evidence
budgets and planner-ready repair signals. It never writes pipeline jobs.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.orchestration.evidence_backfill import LIGHT_DEPTH
from pm_robot.pipeline_terms import PipelineJobType, REVIEW_FUNNEL_CANDIDATE_STAGES
from pm_robot.risk.eligibility import paper_eligibility_status
from pm_robot.storage.repository import seed_evidence_backfill_budget


REPAIR_SOURCE = "eligibility_repair"
REPAIR_STAGES = REVIEW_FUNNEL_CANDIDATE_STAGES
ACTION_WALLET_EVIDENCE = PipelineJobType.WALLET_EVIDENCE_BACKFILL.value
ACTION_COPYABILITY = PipelineJobType.COPYABILITY_EVIDENCE.value
ACTION_FEATURE_MATERIALIZE = "feature_materialize_recommended"
ACTION_SOURCE_REVIEW = "source_provenance_review"


@dataclass(frozen=True)
class EligibilityRepairPreparationSummary:
    wallets_seen: int
    wallets_ineligible: int
    evidence_budgets_seeded: int
    wallet_repairs_prepared: int
    copyability_repairs_ready: int
    reason_counts: dict[str, int]
    action_counts: dict[str, int]
    dry_run: bool
    status: str


def prepare_eligibility_repairs(
    conn: sqlite3.Connection,
    *,
    limit: int = 100,
    min_score: float = 40.0,
    min_copyability_activity_events: int = 25,
    now: int | None = None,
    dry_run: bool = False,
) -> EligibilityRepairPreparationSummary:
    """Prepare evidence work for wallets blocked by paper eligibility.

    Watchlist/review wallets still do not bypass paper gates; instead, high
    scoring provisional wallets receive an evidence budget or become eligible
    for the dedicated copyability planner. Queue admission remains owned by the
    canonical wallet and copyability planners.
    """

    ts = now or int(time.time())
    rows = _candidate_rows(conn, limit=limit, min_score=min_score)
    ineligible = 0
    evidence_budgets_seeded = 0
    wallet_repairs = 0
    copyability_repairs = 0
    reason_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}

    for row in rows:
        wallet = str(row["address"]).lower()
        result = paper_eligibility_status(conn, wallet)
        if result.eligible:
            continue
        ineligible += 1
        reasons = tuple(result.reasons)
        _increment_counts(reason_counts, reasons)
        actions = _actions_for_reasons(
            reasons,
            trade_events=int(row["trade_events"] or 0),
            min_copyability_activity_events=min_copyability_activity_events,
        )
        _increment_counts(action_counts, actions)
        if dry_run:
            continue
        if ACTION_WALLET_EVIDENCE in actions:
            wallet_repairs += 1
            evidence_budgets_seeded += int(
                _seed_wallet_evidence_budget(
                    conn,
                    wallet=wallet,
                    leader_score=float(row["leader_score"] or 0.0),
                    reasons=reasons,
                    now=ts,
                )
            )
        if ACTION_COPYABILITY in actions:
            copyability_repairs += 1
    if not dry_run:
        conn.commit()
    return EligibilityRepairPreparationSummary(
        wallets_seen=len(rows),
        wallets_ineligible=ineligible,
        evidence_budgets_seeded=evidence_budgets_seeded,
        wallet_repairs_prepared=wallet_repairs,
        copyability_repairs_ready=copyability_repairs,
        reason_counts=dict(sorted(reason_counts.items())),
        action_counts=dict(sorted(action_counts.items())),
        dry_run=dry_run,
        status="ok",
    )


def _candidate_rows(
    conn: sqlite3.Connection,
    *,
    limit: int,
    min_score: float,
) -> list[sqlite3.Row]:
    limit_sql = ""
    params: list[Any] = [min_score]
    if limit > 0:
        limit_sql = "LIMIT ?"
        params.append(limit)
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
        )
        SELECT
            cw.address,
            cw.candidate_stage,
            COALESCE(latest.leader_score, 0) AS leader_score,
            COALESCE(latest.review_reason, '') AS review_reason,
            (
                SELECT COUNT(*)
                FROM wallet_activity wa
                WHERE wa.address = cw.address
                  AND wa.type = 'TRADE'
            ) AS trade_events
        FROM candidate_wallets cw
        LEFT JOIN latest
          ON latest.address = cw.address
        WHERE cw.candidate_stage IN ({",".join("?" for _ in REPAIR_STAGES)})
          AND COALESCE(latest.leader_score, 0) >= ?
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
        ORDER BY
            CASE cw.candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                WHEN 'needs_manual_review' THEN 3
                ELSE 4
            END ASC,
            COALESCE(latest.leader_score, 0) DESC,
            trade_events ASC,
            cw.updated_at DESC,
            cw.address ASC
        {limit_sql}
        """,
        (*REPAIR_STAGES, *params),
    ).fetchall()


def _actions_for_reasons(
    reasons: tuple[str, ...],
    *,
    trade_events: int,
    min_copyability_activity_events: int,
) -> tuple[str, ...]:
    actions: list[str] = []
    if "insufficient_trade_events" in reasons:
        actions.append(ACTION_WALLET_EVIDENCE)
    if "missing_copyability_evidence" in reasons:
        if trade_events >= min_copyability_activity_events:
            actions.append(ACTION_COPYABILITY)
        else:
            actions.append(ACTION_WALLET_EVIDENCE)
    if any(reason.startswith("hygiene_status:") for reason in reasons):
        actions.append(ACTION_FEATURE_MATERIALIZE)
        if trade_events < LIGHT_DEPTH:
            actions.append(ACTION_WALLET_EVIDENCE)
    if "maker_fraction_missing" in reasons:
        actions.append(ACTION_FEATURE_MATERIALIZE)
    if "insufficient_source_count" in reasons:
        actions.append(ACTION_SOURCE_REVIEW)
    return tuple(dict.fromkeys(actions))


def _seed_wallet_evidence_budget(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    leader_score: float,
    reasons: tuple[str, ...],
    now: int,
) -> bool:
    before = conn.total_changes
    seed_evidence_backfill_budget(
        conn,
        wallet,
        source=REPAIR_SOURCE,
        priority=_priority(leader_score),
        target_depth=LIGHT_DEPTH,
        evidence={
            "source": REPAIR_SOURCE,
            "eligibility_reasons": list(reasons),
            "leader_score": leader_score,
            "planned_at": now,
        },
        now=now,
    )
    return conn.total_changes > before


def _priority(score: float) -> int:
    if score >= 70:
        return 4
    if score >= 60:
        return 6
    if score >= 50:
        return 8
    return 12


def _increment_counts(counts: dict[str, int], keys: tuple[str, ...]) -> None:
    for key in keys:
        counts[key] = counts.get(key, 0) + 1
