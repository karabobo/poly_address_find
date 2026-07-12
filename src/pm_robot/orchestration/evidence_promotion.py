"""Policy-aware admission from completed evidence into the next network depth."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pm_robot.config import load_policy
from pm_robot.models import WalletFeatures
from pm_robot.orchestration.evidence_backfill import (
    DEEP_DEPTH,
    LIGHT_DEPTH,
    MEDIUM_DEPTH,
    summarize_wallet_evidence,
)
from pm_robot.orchestration.feature_materializer import MATERIALIZER_VERSION
from pm_robot.pipeline_terms import (
    EvidenceJobStage,
    EvidenceStatus,
    PipelineJobType,
    evidence_promotion_approval_reason,
    evidence_promotion_deferred_reason,
)
from pm_robot.risk.gates import hedge_block_reason, hygiene_block_reason
from pm_robot.research.scoring import economic_materiality_reason
from pm_robot.storage.db import retry_sqlite_locked
from pm_robot.storage.repository import (
    _feature_from_row,
    evidence_promotion_approval_is_current,
    sync_wallet_processing_state,
    upsert_wallet_evidence_summary,
)


@dataclass(frozen=True)
class EvidencePromotionSummary:
    targets_seen: int
    medium_approved: int
    deep_approved: int
    deferred: int
    queued_jobs_superseded: int
    stale_approvals_invalidated: int
    pending_states_normalized: int
    processing_states_reconciled: int
    waiting_for_fresh_features: int
    status: str
    error: str = ""


@dataclass(frozen=True)
class _PreparedPromotion:
    wallet: str
    source_stage: str
    job_action: str
    next_stage: str
    next_depth: int
    stop_reason: str
    evidence: dict[str, Any]
    approved: bool


def promote_wallet_evidence(
    conn: sqlite3.Connection,
    *,
    policy_path: Path,
    limit: int = 100,
    now: int | None = None,
) -> EvidencePromotionSummary:
    """Apply local policy gates; this function never fetches network evidence."""

    ts = now or int(time.time())
    policy = load_policy(policy_path)
    policy_version = str(policy.get("version") or "unknown")
    invalidated, superseded, normalized, reconciled = _prepare_promotion_state(
        conn,
        policy_version=policy_version,
        now=ts,
    )
    rows, waiting = _promotion_targets(
        conn,
        policy_version=policy_version,
        limit=max(0, int(limit)),
    )
    medium_approved = 0
    deep_approved = 0
    deferred = 0
    errors: list[str] = []
    prepared: list[_PreparedPromotion] = []

    for row in rows:
        wallet = str(row["wallet"]).lower()
        source_stage = str(row["stage"] or "")
        job_action, target_depth, terminal_stage, terminal_depth = _promotion_transition(
            source_stage
        )
        try:
            evidence = summarize_wallet_evidence(conn, wallet)
            reason = _promotion_evidence_block_reason(
                evidence=evidence,
                job_action=job_action,
            )
            features_fresh = bool(row["features_fresh"])
            if reason is None:
                if not features_fresh:
                    waiting += 1
                    continue
                feature_row = conn.execute(
                    "SELECT * FROM wallet_features WHERE address = ?",
                    (wallet,),
                ).fetchone()
                if feature_row is None:
                    raise ValueError("wallet_features_missing")
                reason = _promotion_block_reason(
                    features=_feature_from_row(feature_row),
                    evidence=evidence,
                    job_action=job_action,
                    leader_score=row["leader_score"],
                    score_fresh=bool(row["score_fresh"]),
                    policy=policy,
                )
            approved = reason is None
            if reason is None:
                next_stage = job_action
                next_depth = target_depth
                stop_reason = evidence_promotion_approval_reason(
                    job_action,
                    policy_version,
                    feature_updated_at=int(row["feature_updated_at"] or 0),
                    activity_count=int(evidence["activity_count"]),
                )
            else:
                next_stage = terminal_stage
                next_depth = terminal_depth
                stop_reason = evidence_promotion_deferred_reason(
                    job_action,
                    policy_version,
                    reason,
                )
            evidence["promotion"] = {
                "job_action": job_action,
                "approved": approved,
                "reason": reason or "approved",
                "policy_version": policy_version,
                "feature_updated_at": int(row["feature_updated_at"] or 0),
                "activity_count": int(evidence["activity_count"]),
                "features_fresh": features_fresh,
                "materializer_version": (
                    MATERIALIZER_VERSION if features_fresh else ""
                ),
                "evaluated_at": ts,
            }
            prepared.append(
                _PreparedPromotion(
                    wallet=wallet,
                    source_stage=source_stage,
                    job_action=job_action,
                    next_stage=next_stage,
                    next_depth=next_depth,
                    stop_reason=stop_reason,
                    evidence=evidence,
                    approved=approved,
                )
            )
        except Exception as exc:
            errors.append(f"{wallet}: {exc}")

    for item in prepared:
        try:
            _persist_promotion_result(conn, item, now=ts)
        except Exception as exc:
            errors.append(f"{item.wallet}: {exc}")
            continue
        if item.approved and item.job_action == EvidenceJobStage.MEDIUM_PENDING.value:
            medium_approved += 1
        elif item.approved:
            deep_approved += 1
        else:
            deferred += 1
    return EvidencePromotionSummary(
        targets_seen=len(rows),
        medium_approved=medium_approved,
        deep_approved=deep_approved,
        deferred=deferred,
        queued_jobs_superseded=superseded,
        stale_approvals_invalidated=invalidated,
        pending_states_normalized=normalized,
        processing_states_reconciled=reconciled,
        waiting_for_fresh_features=waiting,
        status="partial" if errors else "ok",
        error="; ".join(errors[:3]),
    )


def _prepare_promotion_state(
    conn: sqlite3.Connection,
    *,
    policy_version: str,
    now: int,
) -> tuple[int, int, int, int]:
    """Commit queue normalization before any expensive evidence reads."""

    def operation() -> tuple[int, int, int, int]:
        invalidated = _invalidate_stale_approvals(
            conn,
            policy_version=policy_version,
            now=now,
        )
        superseded = _supersede_unapproved_queued_jobs(conn, now=now)
        normalized = _normalize_unapproved_pending_states(
            conn,
            policy_version=policy_version,
            now=now,
        )
        reconciled = _reconcile_terminal_processing_states(conn, now=now)
        conn.commit()
        return invalidated, superseded, normalized, reconciled

    return retry_sqlite_locked(
        operation,
        rollback=conn.rollback,
        attempts=4,
        sleep_seconds=2.0,
    )


def _persist_promotion_result(
    conn: sqlite3.Connection,
    item: _PreparedPromotion,
    *,
    now: int,
) -> None:
    """Persist one promotion in a short transaction after evidence is computed."""

    def operation() -> None:
        try:
            _write_promotion_budget(
                conn,
                wallet=item.wallet,
                source_stage=item.source_stage,
                stage=item.next_stage,
                target_depth=item.next_depth,
                current_depth=int(item.evidence["activity_count"]),
                stop_reason=item.stop_reason,
                evidence=item.evidence,
                now=now,
            )
            upsert_wallet_evidence_summary(
                conn,
                item.wallet,
                item.evidence,
                source_artifacts=[f"sqlite://wallet_activity/{item.wallet}"],
                computed_at=now,
            )
            sync_wallet_processing_state(
                conn,
                item.wallet,
                item.evidence,
                source="evidence_promotion",
                now=now,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    retry_sqlite_locked(
        operation,
        rollback=conn.rollback,
        attempts=4,
        sleep_seconds=2.0,
    )


def _promotion_targets(
    conn: sqlite3.Connection,
    *,
    policy_version: str,
    limit: int,
) -> tuple[list[sqlite3.Row], int]:
    eligible_sql = """
        ebb.stage IN ('light_done', 'medium_done')
        AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
        AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
        AND NOT EXISTS (
            SELECT 1
            FROM pipeline_jobs running_job
            WHERE running_job.job_type = ?
              AND running_job.wallet = ebb.wallet
              AND running_job.status = 'running'
        )
    """
    fresh_sql = """
        COALESCE(json_extract(wf.extra_json, '$.feature_materializer_version'), '') = ?
        AND COALESCE(
            CAST(json_extract(wf.extra_json, '$.feature_materializer_activity_count') AS INTEGER),
            -1
        ) = COALESCE(waw.trade_count, 0)
    """
    deterministic_low_activity_sql = """
        (
            ebb.stage = 'light_done'
            AND COALESCE(waw.trade_count, 0) < 25
        )
        OR (
            ebb.stage = 'medium_done'
            AND COALESCE(waw.trade_count, 0) < 300
        )
    """
    needs_gate_sql = """
        (
            (
                ebb.stage = 'light_done'
                AND (
                    COALESCE(ebb.stop_reason, '') NOT LIKE
                        'promotion_deferred:medium_pending:' || ? || ':%'
                    OR COALESCE(wf.updated_at, 0) > ebb.updated_at
                    OR COALESCE(waw.trade_count, 0) != ebb.current_depth
                )
            )
            OR (
                ebb.stage = 'medium_done'
                AND (
                    COALESCE(ebb.stop_reason, '') NOT LIKE
                        'promotion_deferred:deep_pending:' || ? || ':%'
                    OR COALESCE(wf.updated_at, 0) > ebb.updated_at
                    OR COALESCE(waw.trade_count, 0) != ebb.current_depth
                )
            )
        )
    """
    params: tuple[Any, ...] = (
        policy_version,
        MATERIALIZER_VERSION,
        PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
        MATERIALIZER_VERSION,
        policy_version,
        policy_version,
        limit,
    )
    rows = conn.execute(
        f"""
        SELECT
            ebb.wallet,
            ebb.stage,
            ebb.stop_reason,
            ebb.priority,
            wf.updated_at AS feature_updated_at,
            COALESCE(waw.trade_count, 0) AS activity_count,
            ls.leader_score,
            CASE
                WHEN ls.scored_at IS NOT NULL
                     AND ls.scored_at >= wf.updated_at
                     AND COALESCE(ls.policy_version, '') = ?
                THEN 1
                ELSE 0
            END AS score_fresh,
            CASE WHEN {fresh_sql} THEN 1 ELSE 0 END AS features_fresh
        FROM evidence_backfill_budget ebb
        JOIN candidate_wallets cw
          ON cw.address = ebb.wallet
        LEFT JOIN wallet_activity_watermarks waw
          ON waw.address = ebb.wallet
        LEFT JOIN wallet_features wf
          ON wf.address = ebb.wallet
        LEFT JOIN leader_latest_scores ls
          ON ls.address = ebb.wallet
        LEFT JOIN wallet_registry wr
          ON wr.address = ebb.wallet
        WHERE {eligible_sql}
          AND (({fresh_sql}) OR ({deterministic_low_activity_sql}))
          AND {needs_gate_sql}
        ORDER BY
            CASE ebb.stage
                WHEN 'medium_done' THEN 0
                ELSE 1
            END,
            ebb.priority ASC,
            ebb.updated_at ASC,
            ebb.wallet ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    waiting = int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM evidence_backfill_budget ebb
            JOIN candidate_wallets cw
              ON cw.address = ebb.wallet
            LEFT JOIN wallet_activity_watermarks waw
              ON waw.address = ebb.wallet
            LEFT JOIN wallet_features wf
              ON wf.address = ebb.wallet
            LEFT JOIN wallet_registry wr
              ON wr.address = ebb.wallet
            WHERE {eligible_sql}
              AND {needs_gate_sql}
              AND NOT ({fresh_sql})
              AND NOT ({deterministic_low_activity_sql})
            """,
            (
                PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
                policy_version,
                policy_version,
                MATERIALIZER_VERSION,
            ),
        ).fetchone()[0]
    )
    return rows, waiting


def _promotion_transition(stage: str) -> tuple[str, int, str, int]:
    if stage == EvidenceJobStage.LIGHT_DONE.value:
        return (
            EvidenceJobStage.MEDIUM_PENDING.value,
            MEDIUM_DEPTH,
            EvidenceJobStage.LIGHT_DONE.value,
            LIGHT_DEPTH,
        )
    if stage == EvidenceJobStage.MEDIUM_DONE.value:
        return (
            EvidenceJobStage.DEEP_PENDING.value,
            DEEP_DEPTH,
            EvidenceJobStage.MEDIUM_DONE.value,
            MEDIUM_DEPTH,
        )
    raise ValueError(f"unsupported promotion source stage: {stage}")


def _promotion_block_reason(
    *,
    features: WalletFeatures,
    evidence: dict[str, Any],
    job_action: str,
    leader_score: Any,
    score_fresh: bool,
    policy: dict[str, Any],
) -> str | None:
    evidence_reason = _promotion_evidence_block_reason(
        evidence=evidence,
        job_action=job_action,
    )
    if evidence_reason:
        return evidence_reason
    risk_reason = hygiene_block_reason(features, policy) or hedge_block_reason(
        features,
        policy,
    )
    if risk_reason:
        return risk_reason
    materiality_reason = economic_materiality_reason(features, policy)
    if materiality_reason:
        return materiality_reason
    if job_action == EvidenceJobStage.DEEP_PENDING.value:
        if not score_fresh or leader_score is None:
            return "medium_score_not_fresh"
        minimum_score = float(
            policy.get("review_bands", {}).get("formal_validation_candidate", 40.0)
        )
        if float(leader_score) < minimum_score:
            return f"medium_score_below_{minimum_score:g}"
    return None


def _promotion_evidence_block_reason(
    *,
    evidence: dict[str, Any],
    job_action: str,
) -> str | None:
    """Return deterministic evidence-depth blocks without requiring materialized features."""

    activity_count = int(evidence.get("activity_count") or 0)
    distinct_markets = int(evidence.get("distinct_markets") or 0)
    non_fast_trades = int(evidence.get("non_fast_trade_count") or 0)
    non_fast_markets = int(evidence.get("non_fast_distinct_markets") or 0)
    fast_share = float(evidence.get("fast_market_share") or 0.0)
    if activity_count >= 50 and fast_share >= 0.85:
        return "fast_market_dominant"
    if job_action == EvidenceJobStage.MEDIUM_PENDING.value:
        if activity_count < 25:
            return "light_activity_below_25"
        if distinct_markets < 3 and non_fast_trades < 10 and non_fast_markets < 2:
            return "light_strategy_diversity"
    else:
        if activity_count < 300:
            return "medium_activity_below_300"
        if distinct_markets < 10:
            return "medium_markets_below_10"
        if non_fast_trades < 50:
            return "medium_non_fast_below_50"
    return None


def _write_promotion_budget(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    source_stage: str,
    stage: str,
    target_depth: int,
    current_depth: int,
    stop_reason: str,
    evidence: dict[str, Any],
    now: int,
) -> None:
    cursor = conn.execute(
        """
        UPDATE evidence_backfill_budget
        SET stage = ?,
            target_depth = ?,
            current_depth = ?,
            next_attempt_at = 0,
            stop_reason = ?,
            evidence_json = ?,
            updated_at = ?
        WHERE wallet = ?
          AND stage = ?
        """,
        (
            stage,
            target_depth,
            current_depth,
            stop_reason[:240],
            json.dumps(evidence, ensure_ascii=False, sort_keys=True),
            now,
            wallet,
            source_stage,
        ),
    )
    if cursor.rowcount != 1:
        raise RuntimeError("promotion_source_stage_changed")


def _supersede_unapproved_queued_jobs(
    conn: sqlite3.Connection,
    *,
    now: int,
) -> int:
    updated = 0
    pipeline_rows = conn.execute(
        """
        SELECT job_id, wallet, subject_key AS job_action
        FROM pipeline_jobs
        WHERE job_type = ?
          AND status = 'queued'
          AND subject_key IN ('medium_pending', 'deep_pending')
        """,
        (PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,),
    ).fetchall()
    for row in pipeline_rows:
        if evidence_promotion_approval_is_current(
            conn,
            wallet=str(row["wallet"]),
            job_action=str(row["job_action"]),
        ):
            continue
        updated += int(
            conn.execute(
                """
                UPDATE pipeline_jobs
                SET status = 'superseded',
                    lease_owner = NULL,
                    lease_until = 0,
                    last_error = 'awaiting_policy_evidence_promotion',
                    completed_at = ?,
                    updated_at = ?
                WHERE job_id = ? AND status = 'queued'
                """,
                (now, now, int(row["job_id"])),
            ).rowcount
            or 0
        )
    legacy_rows = conn.execute(
        """
        SELECT job_id, wallet, stage AS job_action
        FROM evidence_backfill_jobs
        WHERE status = 'queued'
          AND stage IN ('medium_pending', 'deep_pending')
        """
    ).fetchall()
    for row in legacy_rows:
        if evidence_promotion_approval_is_current(
            conn,
            wallet=str(row["wallet"]),
            job_action=str(row["job_action"]),
        ):
            continue
        updated += int(
            conn.execute(
                """
                UPDATE evidence_backfill_jobs
                SET status = 'superseded',
                    lease_owner = NULL,
                    lease_until = 0,
                    last_error = 'awaiting_policy_evidence_promotion',
                    completed_at = ?,
                    updated_at = ?
                WHERE job_id = ? AND status = 'queued'
                """,
                (now, now, int(row["job_id"])),
            ).rowcount
            or 0
        )
    return updated


def _invalidate_stale_approvals(
    conn: sqlite3.Connection,
    *,
    policy_version: str,
    now: int,
) -> int:
    """Expire approvals when policy, features, or raw activity changed."""

    rows = conn.execute(
        """
        SELECT wallet, stage
        FROM evidence_backfill_budget
        WHERE stage IN ('medium_pending', 'deep_pending')
          AND stop_reason LIKE 'promotion_approved:' || stage || ':%'
        """
    ).fetchall()
    invalidated = 0
    for row in rows:
        wallet = str(row["wallet"])
        job_action = str(row["stage"])
        if evidence_promotion_approval_is_current(
            conn,
            wallet=wallet,
            job_action=job_action,
            expected_policy_version=policy_version,
            expected_materializer_version=MATERIALIZER_VERSION,
        ):
            continue
        invalidated += int(
            conn.execute(
                """
                UPDATE evidence_backfill_budget
                SET stop_reason = ?, updated_at = ?
                WHERE wallet = ? AND stage = ?
                """,
                (
                    f"promotion_recheck_required:{job_action}:{policy_version}"[:240],
                    now,
                    wallet,
                    job_action,
                ),
            ).rowcount
            or 0
        )
    return invalidated


def _normalize_unapproved_pending_states(
    conn: sqlite3.Connection,
    *,
    policy_version: str,
    now: int,
) -> int:
    """Move legacy pending rows back to the prior completed evidence boundary."""

    transitions = {
        EvidenceJobStage.MEDIUM_PENDING.value: (
            EvidenceJobStage.LIGHT_DONE.value,
            LIGHT_DEPTH,
        ),
        EvidenceJobStage.DEEP_PENDING.value: (
            EvidenceJobStage.MEDIUM_DONE.value,
            MEDIUM_DEPTH,
        ),
    }
    rows = conn.execute(
        """
        SELECT wallet, stage
        FROM evidence_backfill_budget
        WHERE stage IN ('medium_pending', 'deep_pending')
        """
    ).fetchall()
    normalized = 0
    for row in rows:
        wallet = str(row["wallet"])
        job_action = str(row["stage"])
        if evidence_promotion_approval_is_current(
            conn,
            wallet=wallet,
            job_action=job_action,
            expected_policy_version=policy_version,
        ):
            continue
        running = conn.execute(
            """
            SELECT 1
            FROM pipeline_jobs
            WHERE job_type = ? AND wallet = ? AND subject_key = ? AND status = 'running'
            UNION ALL
            SELECT 1
            FROM evidence_backfill_jobs
            WHERE wallet = ? AND stage = ? AND status = 'running'
            LIMIT 1
            """,
            (
                PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
                wallet,
                job_action,
                wallet,
                job_action,
            ),
        ).fetchone()
        if running is not None:
            continue
        terminal_stage, terminal_depth = transitions[job_action]
        updated = int(
            conn.execute(
                """
                UPDATE evidence_backfill_budget
                SET stage = ?,
                    target_depth = ?,
                    stop_reason = ?,
                    next_attempt_at = 0,
                    updated_at = ?
                WHERE wallet = ? AND stage = ?
                """,
                (
                    terminal_stage,
                    terminal_depth,
                    f"promotion_recheck_required:{job_action}:{policy_version}"[:240],
                    now,
                    wallet,
                    job_action,
                ),
            ).rowcount
            or 0
        )
        if not updated:
            continue
        normalized += updated
    return normalized


def _reconcile_terminal_processing_states(
    conn: sqlite3.Connection,
    *,
    now: int,
) -> int:
    """Repair stale queue-facing state after a budget returns to a completed tier."""

    cursor = conn.execute(
        """
        UPDATE wallet_processing_state AS wps
        SET current_stage = CASE wps.current_stage
                WHEN 'medium_pending' THEN 'light_done'
                WHEN 'deep_pending' THEN 'medium_done'
                ELSE wps.current_stage
            END,
            evidence_status = CASE
                WHEN wps.evidence_status = ? THEN wps.evidence_status
                ELSE ?
            END,
            next_action = CASE
                WHEN wps.evidence_status = ? THEN wps.next_action
                ELSE 'score_wallet'
            END,
            next_action_at = 0,
            updated_at = ?
        WHERE (
                (
                    wps.current_stage = 'medium_pending'
                    AND EXISTS (
                        SELECT 1
                        FROM evidence_backfill_budget ebb
                        WHERE ebb.wallet = wps.wallet
                          AND ebb.stage = 'light_done'
                    )
                )
                OR (
                    wps.current_stage = 'deep_pending'
                    AND EXISTS (
                        SELECT 1
                        FROM evidence_backfill_budget ebb
                        WHERE ebb.wallet = wps.wallet
                          AND ebb.stage = 'medium_done'
                    )
                )
            )
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs running_job
              WHERE running_job.job_type = ?
                AND running_job.wallet = wps.wallet
                AND running_job.subject_key = wps.current_stage
                AND running_job.status = 'running'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM evidence_backfill_jobs running_legacy_job
              WHERE running_legacy_job.wallet = wps.wallet
                AND running_legacy_job.stage = wps.current_stage
                AND running_legacy_job.status = 'running'
          )
        """,
        (
            EvidenceStatus.PAUSED.value,
            EvidenceStatus.SUMMARY_READY.value,
            EvidenceStatus.PAUSED.value,
            now,
            PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
        ),
    )
    return int(cursor.rowcount or 0)
