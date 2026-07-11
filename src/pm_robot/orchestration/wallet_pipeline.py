"""V2 wallet evidence pipeline planner and worker."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.orchestration.evidence_backfill import (
    DEEP_DEPTH,
    LIGHT_DEPTH,
    MEDIUM_DEPTH,
    _classify_next_step,
    _fetch_activity_history,
    summarize_wallet_evidence,
)
from pm_robot.orchestration.feature_materializer import MATERIALIZER_VERSION
from pm_robot.orchestration.retry_policy import (
    is_upstream_scheduling_error,
    upstream_aware_retry_at,
)
from pm_robot.pipeline_terms import (
    DEFAULT_EVIDENCE_JOB_STAGE,
    EvidenceJobStage,
    PENDING_EVIDENCE_JOB_STAGES,
    PipelineJobType,
)
from pm_robot.storage.api_rate_limit import api_rate_limit_cooldown_wait
from pm_robot.storage.db import is_sqlite_locked_error, retry_sqlite_locked
from pm_robot.storage.repository import (
    PipelineJobLeaseLost,
    claim_pipeline_job,
    complete_pipeline_job,
    evidence_promotion_approval_is_current,
    finish_ingest_run,
    enqueue_pipeline_job,
    persist_wallet_activity,
    persist_wallet_positions,
    pipeline_job_summary,
    rebuild_wallet_episodes,
    renew_pipeline_job_lease,
    retry_pipeline_job,
    start_ingest_run,
    sync_wallet_processing_state,
    supersede_pipeline_job,
    update_evidence_backfill_budget,
    upsert_wallet_evidence_summary,
)


JOB_TYPE = PipelineJobType.WALLET_EVIDENCE_BACKFILL.value
LOCK_RETRY_ATTEMPTS = 8
LOCK_RETRY_SLEEP_SECONDS = 3.0
UPSTREAM_COOLDOWN_BLOCK_SECONDS = 30.0
WALLET_EVIDENCE_API_SCOPES = ("data:*", "data:/activity", "data:/positions")
DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS = 1_800
DEFAULT_PIPELINE_STAGE_WEIGHTS = {
    EvidenceJobStage.LIGHT_PENDING.value: 30,
    EvidenceJobStage.MEDIUM_PENDING.value: 20,
    EvidenceJobStage.DEEP_PENDING.value: 5,
}


@dataclass(frozen=True)
class WalletPipelinePlanSummary:
    targets_seen: int
    jobs_enqueued: int
    shard_count: int
    status: str
    active_jobs: int = 0
    max_active_jobs: int = 0
    throttled: bool = False
    reason: str = ""


@dataclass(frozen=True)
class WalletPipelineWorkerSummary:
    run_id: int
    shard_index: int
    shard_count: int
    jobs_attempted: int
    jobs_succeeded: int
    jobs_failed: int
    activity_events_written: int
    positions_written: int
    episodes_rebuilt: int
    stage_updates: dict[str, int]
    status: str
    error: str = ""


def plan_wallet_pipeline_jobs(
    conn: sqlite3.Connection,
    *,
    policy_version: str = "",
    light_limit: int = DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.LIGHT_PENDING.value],
    medium_limit: int = DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.MEDIUM_PENDING.value],
    deep_limit: int = DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.DEEP_PENDING.value],
    shard_count: int = 3,
    max_active_jobs: int = 240,
    lock_retry_attempts: int = LOCK_RETRY_ATTEMPTS,
    lock_retry_sleep_seconds: float = LOCK_RETRY_SLEEP_SECONDS,
    now: int | None = None,
) -> WalletPipelinePlanSummary:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")

    def _plan_once() -> WalletPipelinePlanSummary:
        conn.commit()
        ts = now or int(time.time())
        prefetched_targets = _prefetch_pipeline_targets(
            conn,
            policy_version=policy_version,
            light_limit=light_limit,
            medium_limit=medium_limit,
            deep_limit=deep_limit,
            now=ts,
        )
        conn.execute("BEGIN IMMEDIATE")
        try:
            summary = _plan_wallet_pipeline_jobs_once(
                conn,
                policy_version=policy_version,
                light_limit=light_limit,
                medium_limit=medium_limit,
                deep_limit=deep_limit,
                shard_count=shard_count,
                max_active_jobs=max_active_jobs,
                now=ts,
                prefetched_targets=prefetched_targets,
            )
            conn.commit()
            return summary
        except BaseException:
            conn.rollback()
            raise

    return retry_sqlite_locked(
        _plan_once,
        rollback=conn.rollback,
        attempts=lock_retry_attempts,
        sleep_seconds=lock_retry_sleep_seconds,
    )


def _plan_wallet_pipeline_jobs_once(
    conn: sqlite3.Connection,
    *,
    policy_version: str,
    light_limit: int,
    medium_limit: int,
    deep_limit: int,
    shard_count: int,
    max_active_jobs: int,
    now: int,
    prefetched_targets: dict[str, list[dict[str, Any]]],
) -> WalletPipelinePlanSummary:
    ts = now
    active_jobs = _active_wallet_pipeline_jobs(conn)
    if max_active_jobs > 0 and active_jobs >= max_active_jobs:
        return WalletPipelinePlanSummary(
            targets_seen=0,
            jobs_enqueued=0,
            shard_count=shard_count,
            status="ok",
            active_jobs=active_jobs,
            max_active_jobs=max_active_jobs,
            throttled=True,
            reason="active_queue_waterline",
        )
    available_slots = (
        max(0, max_active_jobs - active_jobs)
        if max_active_jobs > 0
        else None
    )
    targets = _select_pipeline_targets(
        conn,
        policy_version=policy_version,
        light_limit=light_limit,
        medium_limit=medium_limit,
        deep_limit=deep_limit,
        max_targets=available_slots,
        now=ts,
        prefetched_targets=prefetched_targets,
    )
    enqueued = 0
    for target in targets:
        wallet = str(target["wallet"]).lower()
        job_action = str(target["next_action"] or "")
        evidence_job_stage, target_depth = _stage_depth(job_action)
        # Legacy pipeline_jobs.tier is a dedupe scope, not the evidence source of truth.
        job_scope = str(target["discovery_tier"] or "")
        enqueued += 1 if enqueue_pipeline_job(
            conn,
            job_type=JOB_TYPE,
            wallet=wallet,
            subject_key=evidence_job_stage,
            tier=job_scope,
            priority=int(target["priority"] or 100),
            shard=_wallet_shard(wallet, shard_count),
            input_data={
                "stage": evidence_job_stage,
                "target_depth": target_depth,
                "source": "wallet_processing_state",
                "planned_at": ts,
            },
            max_attempts=3,
            next_attempt_at=max(0, int(target["next_action_at"] or 0)),
            now=ts,
        ) else 0
    return WalletPipelinePlanSummary(
        targets_seen=len(targets),
        jobs_enqueued=enqueued,
        shard_count=shard_count,
        status="ok",
        active_jobs=active_jobs,
        max_active_jobs=max_active_jobs,
        throttled=False,
    )


def run_wallet_pipeline_worker(
    conn: sqlite3.Connection,
    *,
    policy_version: str = "",
    shard_index: int,
    shard_count: int,
    limit: int = 8,
    page_limit: int = 200,
    sleep_seconds: float = 0.02,
    lease_seconds: int = 900,
    priority_aging_seconds: int = DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS,
    worker_id: str = "",
    client: PublicPolymarketClient | None = None,
) -> WalletPipelineWorkerSummary:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be in [0, shard_count)")
    client = client or PublicPolymarketClient(conn=conn)
    worker_id = worker_id or f"wallet-pipeline-{shard_index}-{int(time.time())}"
    run_id = retry_sqlite_locked(
        lambda: start_ingest_run(conn, f"wallet_pipeline_worker_{shard_index}"),
        rollback=conn.rollback,
        attempts=LOCK_RETRY_ATTEMPTS,
        sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
    )
    attempted = 0
    succeeded = 0
    failed = 0
    activity_events_written = 0
    positions_written = 0
    episodes_rebuilt = 0
    stage_updates: dict[str, int] = {}
    status = "ok"
    error = ""
    try:
        for idx in range(max(0, limit)):
            cooldown_wait = api_rate_limit_cooldown_wait(
                conn,
                WALLET_EVIDENCE_API_SCOPES,
            )
            if cooldown_wait > UPSTREAM_COOLDOWN_BLOCK_SECONDS:
                status = "partial"
                error = f"shared upstream cooldown active for {int(cooldown_wait + 0.999)}s"
                break
            if idx > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            job = retry_sqlite_locked(
                lambda: claim_pipeline_job(
                    conn,
                    job_type=JOB_TYPE,
                    shard=shard_index,
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    priority_aging_seconds=priority_aging_seconds,
                ),
                rollback=conn.rollback,
                attempts=LOCK_RETRY_ATTEMPTS,
                sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
            )
            if job is None:
                break
            attempted += 1
            wallet = str(job["wallet"]).lower()
            input_data = _json_object(job.get("input_json"))
            stage, target_depth = _job_stage_depth(job, input_data)
            if stage != EvidenceJobStage.LIGHT_PENDING.value and not _promotion_approved(
                conn,
                wallet=wallet,
                job_action=stage,
                policy_version=policy_version,
            ):
                supersede_pipeline_job(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    reason="evidence_depth_not_approved",
                )
                conn.commit()
                stage_updates["promotion_guarded"] = stage_updates.get("promotion_guarded", 0) + 1
                continue
            try:
                for write_attempt in range(LOCK_RETRY_ATTEMPTS):
                    try:
                        now = int(time.time())
                        job_activity_events_written = 0
                        job_positions_written = 0
                        job_episodes_rebuilt = 0
                        _ensure_evidence_budget(
                            conn,
                            wallet=wallet,
                            stage=stage,
                            target_depth=target_depth,
                            priority=int(job["priority"] or 100),
                            now=now,
                        )
                        _require_pipeline_job_lease(
                            conn,
                            job_id=int(job["job_id"]),
                            worker_id=worker_id,
                            lease_seconds=lease_seconds,
                        )
                        events = _fetch_activity_history(
                            client,
                            wallet,
                            page_limit=page_limit,
                            max_events=target_depth,
                            sleep_seconds=sleep_seconds,
                        )
                        _require_pipeline_job_lease(
                            conn,
                            job_id=int(job["job_id"]),
                            worker_id=worker_id,
                            lease_seconds=lease_seconds,
                        )
                        positions = client.positions(wallet, size_threshold=0.0)
                        _require_pipeline_job_lease(
                            conn,
                            job_id=int(job["job_id"]),
                            worker_id=worker_id,
                            lease_seconds=lease_seconds,
                        )
                        job_activity_events_written = persist_wallet_activity(
                            conn,
                            wallet,
                            events,
                            ingested_at=now,
                            source="wallet_pipeline",
                            commit=False,
                        )
                        job_episodes_rebuilt = rebuild_wallet_episodes(
                            conn,
                            wallet,
                            commit=False,
                        )
                        job_positions_written = persist_wallet_positions(
                            conn,
                            wallet,
                            positions,
                            captured_at=now,
                            commit=False,
                        )
                        evidence = summarize_wallet_evidence(conn, wallet)
                        next_stage, next_depth, stop_reason, next_attempt_at = _classify_next_step(
                            stage,
                            target_depth=target_depth,
                            evidence=evidence,
                            now=now,
                        )
                        update_evidence_backfill_budget(
                            conn,
                            wallet,
                            stage=next_stage,
                            target_depth=next_depth,
                            current_depth=int(evidence["activity_count"]),
                            stop_reason=stop_reason,
                            evidence=evidence,
                            next_attempt_at=next_attempt_at,
                            now=now,
                        )
                        upsert_wallet_evidence_summary(
                            conn,
                            wallet,
                            evidence,
                            source_artifacts=[f"sqlite://wallet_activity/{wallet}"],
                            computed_at=now,
                        )
                        state = sync_wallet_processing_state(
                            conn,
                            wallet,
                            evidence,
                            source="wallet_pipeline_worker",
                            now=now,
                        )
                        completed = complete_pipeline_job(
                            conn,
                            job_id=int(job["job_id"]),
                            worker_id=worker_id,
                            output_data={
                                "wallet": wallet,
                                "stage": stage,
                                "next_stage": next_stage,
                                "target_depth": target_depth,
                                "next_depth": next_depth,
                                "activity_count": int(evidence["activity_count"]),
                                "state": state,
                            },
                            now=now,
                        )
                        if not completed:
                            raise PipelineJobLeaseLost(
                                "wallet pipeline job lease was lost before completion"
                            )
                        conn.commit()
                        activity_events_written += job_activity_events_written
                        positions_written += job_positions_written
                        episodes_rebuilt += job_episodes_rebuilt
                        succeeded += 1
                        stage_updates[next_stage] = stage_updates.get(next_stage, 0) + 1
                        break
                    except sqlite3.OperationalError as exc:
                        if not is_sqlite_locked_error(exc) or write_attempt >= LOCK_RETRY_ATTEMPTS - 1:
                            raise
                        conn.rollback()
                        time.sleep(LOCK_RETRY_SLEEP_SECONDS * (write_attempt + 1))
            except PipelineJobLeaseLost as exc:
                failed += 1
                status = "partial"
                conn.rollback()
                error = f"{wallet}: {exc}"
            except Exception as exc:
                status = "partial"
                conn.rollback()
                scheduler_deferred = is_upstream_scheduling_error(exc)
                if not scheduler_deferred:
                    failed += 1
                now = int(time.time())
                retry_at = upstream_aware_retry_at(
                    exc,
                    now=now,
                    attempts=int(job["attempts"] or 1),
                )
                error = f"{wallet}: {exc}"
                retry_sqlite_locked(
                    lambda: _retry_claimed_pipeline_job(
                        conn,
                        job_id=int(job["job_id"]),
                        worker_id=worker_id,
                        error=str(exc),
                        next_attempt_at=retry_at,
                        count_attempt=not scheduler_deferred,
                        now=now,
                    ),
                    rollback=conn.rollback,
                    attempts=LOCK_RETRY_ATTEMPTS,
                    sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
                )
                if scheduler_deferred:
                    break
        return WalletPipelineWorkerSummary(
            run_id=run_id,
            shard_index=shard_index,
            shard_count=shard_count,
            jobs_attempted=attempted,
            jobs_succeeded=succeeded,
            jobs_failed=failed,
            activity_events_written=activity_events_written,
            positions_written=positions_written,
            episodes_rebuilt=episodes_rebuilt,
            stage_updates=stage_updates,
            status=status,
            error=error,
        )
    except Exception as exc:
        status = "failed"
        error = str(exc)
        return WalletPipelineWorkerSummary(
            run_id=run_id,
            shard_index=shard_index,
            shard_count=shard_count,
            jobs_attempted=attempted,
            jobs_succeeded=succeeded,
            jobs_failed=failed,
            activity_events_written=activity_events_written,
            positions_written=positions_written,
            episodes_rebuilt=episodes_rebuilt,
            stage_updates=stage_updates,
            status=status,
            error=error,
        )
    finally:
        retry_sqlite_locked(
            lambda: finish_ingest_run(
                conn,
                run_id,
                status=status,
                wallets_attempted=attempted,
                wallets_succeeded=succeeded,
                rows_written=activity_events_written + positions_written,
                error=error,
            ),
            rollback=conn.rollback,
            attempts=LOCK_RETRY_ATTEMPTS,
            sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
        )


def _retry_claimed_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    error: str,
    next_attempt_at: int,
    count_attempt: bool,
    now: int,
) -> bool:
    retried = retry_pipeline_job(
        conn,
        job_id=job_id,
        worker_id=worker_id,
        error=error,
        next_attempt_at=next_attempt_at,
        count_attempt=count_attempt,
        now=now,
    )
    if retried:
        conn.commit()
    else:
        conn.rollback()
    return retried


def _require_pipeline_job_lease(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    lease_seconds: int,
) -> None:
    """Renew a claimed job around network I/O and release SQLite write locks."""
    renewed = renew_pipeline_job_lease(
        conn,
        job_id=job_id,
        worker_id=worker_id,
        lease_seconds=lease_seconds,
    )
    if not renewed:
        conn.rollback()
        raise PipelineJobLeaseLost("wallet pipeline job lease was lost")
    conn.commit()


def wallet_pipeline_job_status(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    priority_aging_seconds: int = DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS,
    stage_weights: dict[str, int] | None = None,
) -> dict[str, Any]:
    summary = pipeline_job_summary(conn, job_type=JOB_TYPE)
    summary.update(
        wallet_pipeline_schedule_status(
            conn,
            now=now,
            priority_aging_seconds=priority_aging_seconds,
            stage_weights=stage_weights,
        )
    )
    return summary


def wallet_pipeline_schedule_status(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    priority_aging_seconds: int = DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS,
    stage_weights: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Return read-only stage backlog and fairness state using worker aging semantics."""
    ts = int(time.time()) if now is None else int(now)
    aging_seconds = max(0, int(priority_aging_seconds))
    aging_cutoff = ts - aging_seconds
    configured_weights = dict(DEFAULT_PIPELINE_STAGE_WEIGHTS)
    if stage_weights:
        configured_weights.update(
            {
                evidence_job_stage: max(0, int(weight))
                for evidence_job_stage, weight in stage_weights.items()
                if evidence_job_stage in PENDING_EVIDENCE_JOB_STAGES
            }
        )
    scheduler_rows: dict[str, dict[str, Any]] = {}
    if _pipeline_scheduler_state_exists(conn):
        scheduler_rows = {
            str(row["subject_key"]): dict(row)
            for row in conn.execute(
                """
                SELECT subject_key, current_weight, last_selected_at, updated_at
                FROM pipeline_scheduler_state
                WHERE job_type = ?
                """,
                (JOB_TYPE,),
            ).fetchall()
        }

    placeholders = ", ".join("?" for _stage in PENDING_EVIDENCE_JOB_STAGES)
    stage_rows = {
        str(row["job_action"]): dict(row)
        for row in conn.execute(
            f"""
            SELECT
                subject_key AS job_action,
                SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued_count,
                SUM(
                    CASE
                        WHEN status = 'queued'
                             AND attempts < max_attempts
                             AND next_attempt_at <= ?
                        THEN 1 ELSE 0
                    END
                ) AS due_queued_count,
                SUM(
                    CASE
                        WHEN status = 'queued'
                             AND attempts < max_attempts
                             AND next_attempt_at > ?
                        THEN 1 ELSE 0
                    END
                ) AS deferred_queued_count,
                SUM(
                    CASE WHEN status = 'queued' AND attempts >= max_attempts THEN 1 ELSE 0 END
                ) AS exhausted_queued_count,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
                MIN(
                    CASE
                        WHEN status = 'queued'
                             AND attempts < max_attempts
                             AND next_attempt_at <= ?
                        THEN updated_at
                    END
                ) AS oldest_claimable_queued_at,
                SUM(
                    CASE
                        WHEN status = 'queued'
                             AND attempts < max_attempts
                             AND next_attempt_at <= ?
                             AND ? > 0
                             AND updated_at <= ?
                        THEN 1 ELSE 0
                    END
                ) AS aged_queued_count
            FROM pipeline_jobs
            WHERE job_type = ?
              AND subject_key IN ({placeholders})
              AND status IN ('queued', 'running')
            GROUP BY subject_key
            """,
            (
                ts,
                ts,
                ts,
                ts,
                aging_seconds,
                aging_cutoff,
                JOB_TYPE,
                *PENDING_EVIDENCE_JOB_STAGES,
            ),
        ).fetchall()
    }

    stages: list[dict[str, Any]] = []
    for evidence_job_stage in PENDING_EVIDENCE_JOB_STAGES:
        row = stage_rows.get(evidence_job_stage, {})
        queued_count = int(row.get("queued_count") or 0)
        due_queued_count = int(row.get("due_queued_count") or 0)
        deferred_queued_count = int(row.get("deferred_queued_count") or 0)
        exhausted_queued_count = int(row.get("exhausted_queued_count") or 0)
        running_count = int(row.get("running_count") or 0)
        active_count = queued_count + running_count
        oldest_claimable_queued_at = int(row.get("oldest_claimable_queued_at") or 0)
        configured_weight = int(configured_weights.get(evidence_job_stage) or 0)
        scheduler = scheduler_rows.get(evidence_job_stage, {})
        stages.append(
            {
                "job_action": evidence_job_stage,
                "configured_weight": configured_weight,
                "queued_count": queued_count,
                "due_queued_count": due_queued_count,
                "deferred_queued_count": deferred_queued_count,
                "exhausted_queued_count": exhausted_queued_count,
                "running_count": running_count,
                "active_count": active_count,
                "active_per_weight": (
                    round(active_count / configured_weight, 4)
                    if configured_weight > 0
                    else None
                ),
                "aged_queued_count": int(row.get("aged_queued_count") or 0),
                "oldest_claimable_queued_at": oldest_claimable_queued_at,
                "oldest_claimable_wait_seconds": (
                    max(0, ts - oldest_claimable_queued_at)
                    if oldest_claimable_queued_at
                    else 0
                ),
                "current_weight": int(scheduler.get("current_weight") or 0),
                "last_selected_at": int(scheduler.get("last_selected_at") or 0),
                "scheduler_updated_at": int(scheduler.get("updated_at") or 0),
            }
        )
    return {
        "priority_aging_seconds": aging_seconds,
        "due_queued_count": sum(int(row["due_queued_count"]) for row in stages),
        "deferred_queued_count": sum(int(row["deferred_queued_count"]) for row in stages),
        "exhausted_queued_count": sum(int(row["exhausted_queued_count"]) for row in stages),
        "aged_queued_count": sum(int(row["aged_queued_count"]) for row in stages),
        "oldest_claimable_wait_seconds": max(
            (int(row["oldest_claimable_wait_seconds"]) for row in stages),
            default=0,
        ),
        "stage_schedule": stages,
    }


def _pipeline_scheduler_state_exists(conn: sqlite3.Connection) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pipeline_scheduler_state'"
        ).fetchone()
    )


def _active_wallet_pipeline_jobs(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM pipeline_jobs
            WHERE job_type = ?
              AND status IN ('queued', 'running')
            """,
            (JOB_TYPE,),
        ).fetchone()[0]
    )


def _select_pipeline_targets(
    conn: sqlite3.Connection,
    *,
    policy_version: str,
    light_limit: int,
    medium_limit: int,
    deep_limit: int,
    max_targets: int | None,
    now: int,
    prefetched_targets: dict[str, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    stage_limits = (
        (DEFAULT_EVIDENCE_JOB_STAGE, light_limit),
        (EvidenceJobStage.MEDIUM_PENDING.value, medium_limit),
        (EvidenceJobStage.DEEP_PENDING.value, deep_limit),
    )
    targets_by_stage = (
        _prefetch_pipeline_targets(
            conn,
            policy_version=policy_version,
            light_limit=light_limit,
            medium_limit=medium_limit,
            deep_limit=deep_limit,
            now=now,
        )
        if prefetched_targets is None
        else {
            evidence_job_stage: list(prefetched_targets.get(evidence_job_stage, ()))
            for evidence_job_stage, limit in stage_limits
            if limit > 0
        }
    )
    targets_by_stage = _revalidate_pipeline_targets(
        conn,
        targets_by_stage,
        policy_version=policy_version,
        now=now,
    )
    target_count = sum(len(stage_targets) for stage_targets in targets_by_stage.values())
    selection_limit = target_count if max_targets is None else min(max_targets, target_count)
    if selection_limit <= 0:
        return []

    stage_weights = {
        evidence_job_stage: limit
        for evidence_job_stage, limit in stage_limits
        if limit > 0
    }
    active_by_stage = _active_wallet_pipeline_jobs_by_stage(conn)
    scheduler_state = _wallet_pipeline_scheduler_state(
        conn,
        tuple(stage_weights),
    )
    stage_rank = {
        evidence_job_stage: rank
        for rank, (evidence_job_stage, _limit) in enumerate(stage_limits)
    }
    selected: list[dict[str, Any]] = []
    while len(selected) < selection_limit:
        available_stages = [
            evidence_job_stage
            for evidence_job_stage, stage_targets in targets_by_stage.items()
            if stage_targets
        ]
        if not available_stages:
            break
        for stage in available_stages:
            scheduler_state[stage]["current_weight"] += stage_weights[stage]
        minimum_active_share = min(
            Fraction(active_by_stage.get(stage, 0), stage_weights[stage])
            for stage in available_stages
        )
        least_represented_stages = [
            stage
            for stage in available_stages
            if Fraction(active_by_stage.get(stage, 0), stage_weights[stage])
            == minimum_active_share
        ]
        # Persisted smooth weighted round-robin survives fully drained planner cycles.
        evidence_job_stage = max(
            least_represented_stages,
            key=lambda stage: (
                scheduler_state[stage]["current_weight"],
                -scheduler_state[stage]["last_selected_at"],
                -stage_rank[stage],
            ),
        )
        selected.append(targets_by_stage[evidence_job_stage].pop(0))
        scheduler_state[evidence_job_stage]["current_weight"] -= sum(
            stage_weights[stage] for stage in available_stages
        )
        scheduler_state[evidence_job_stage]["last_selected_at"] = now
        active_by_stage[evidence_job_stage] = active_by_stage.get(evidence_job_stage, 0) + 1
    _persist_wallet_pipeline_scheduler_state(conn, scheduler_state, now=now)
    return selected


def _prefetch_pipeline_targets(
    conn: sqlite3.Connection,
    *,
    policy_version: str,
    light_limit: int,
    medium_limit: int,
    deep_limit: int,
    now: int,
) -> dict[str, list[dict[str, Any]]]:
    """Read planner candidates before the short queue-admission write transaction."""

    stage_limits = (
        (DEFAULT_EVIDENCE_JOB_STAGE, light_limit),
        (EvidenceJobStage.MEDIUM_PENDING.value, medium_limit),
        (EvidenceJobStage.DEEP_PENDING.value, deep_limit),
    )
    return {
        evidence_job_stage: _targets_for_action(
            conn,
            evidence_job_stage,
            limit,
            now,
            policy_version=policy_version,
        )
        for evidence_job_stage, limit in stage_limits
        if limit > 0
    }


def _revalidate_pipeline_targets(
    conn: sqlite3.Connection,
    targets_by_stage: dict[str, list[dict[str, Any]]],
    *,
    policy_version: str,
    now: int,
) -> dict[str, list[dict[str, Any]]]:
    """Recheck mutable eligibility and dedupe state while queue capacity is reserved."""

    targets = [target for stage_targets in targets_by_stage.values() for target in stage_targets]
    wallets = sorted({str(target["wallet"]).lower() for target in targets})
    if not wallets:
        return targets_by_stage
    placeholders = ", ".join("?" for _wallet in wallets)
    state_rows = conn.execute(
        f"""
        SELECT
            wps.wallet,
            wps.discovery_tier,
            wps.evidence_status,
            wps.next_action,
            wps.next_action_at,
            COALESCE(ebb.stop_reason, '') AS stop_reason,
            cw.candidate_stage
        FROM wallet_processing_state wps
        JOIN candidate_wallets cw
          ON cw.address = wps.wallet
        LEFT JOIN evidence_backfill_budget ebb
          ON ebb.wallet = wps.wallet
        LEFT JOIN wallet_registry wr
          ON wr.address = wps.wallet
        WHERE wps.wallet IN ({placeholders})
          AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
        """,
        tuple(wallets),
    ).fetchall()
    current_state = {str(row["wallet"]).lower(): row for row in state_rows}
    job_rows = conn.execute(
        f"""
        SELECT wallet, subject_key, tier, status, next_attempt_at
        FROM pipeline_jobs
        WHERE job_type = ?
          AND wallet IN ({placeholders})
        """,
        (JOB_TYPE, *wallets),
    ).fetchall()
    jobs_by_wallet: dict[str, list[sqlite3.Row]] = {}
    for row in job_rows:
        jobs_by_wallet.setdefault(str(row["wallet"]).lower(), []).append(row)

    validated: dict[str, list[dict[str, Any]]] = {}
    blocked_stages = {"rejected", "blocked_hygiene", "blocked_copyability"}
    for evidence_job_stage, stage_targets in targets_by_stage.items():
        valid_targets: list[dict[str, Any]] = []
        for target in stage_targets:
            wallet = str(target["wallet"]).lower()
            state = current_state.get(wallet)
            job_scope = str(target.get("discovery_tier") or "")
            if (
                state is None
                or str(state["next_action"] or "") != evidence_job_stage
                or int(state["next_action_at"] or 0) > now
                or str(state["evidence_status"] or "") in {"paused", "summary_ready"}
                or str(state["candidate_stage"] or "") in blocked_stages
                or str(state["discovery_tier"] or "") != job_scope
                or (
                    evidence_job_stage != EvidenceJobStage.LIGHT_PENDING.value
                    and not evidence_promotion_approval_is_current(
                        conn,
                        wallet=wallet,
                        job_action=evidence_job_stage,
                        expected_policy_version=policy_version,
                        expected_materializer_version=MATERIALIZER_VERSION,
                    )
                )
            ):
                continue
            conflict = False
            for job in jobs_by_wallet.get(wallet, ()):
                if str(job["subject_key"] or "") != evidence_job_stage:
                    continue
                status = str(job["status"] or "")
                exact_scope = str(job["tier"] or "") == job_scope
                if status in {"queued", "running"}:
                    conflict = True
                elif status == "failed" and exact_scope and int(job["next_attempt_at"] or 0) > now:
                    conflict = True
                elif status == "done" and exact_scope:
                    conflict = True
                if conflict:
                    break
            if not conflict:
                valid_targets.append(target)
        validated[evidence_job_stage] = valid_targets
    return validated


def _active_wallet_pipeline_jobs_by_stage(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT subject_key, COUNT(*) AS job_count
        FROM pipeline_jobs
        WHERE job_type = ?
          AND status IN ('queued', 'running')
        GROUP BY subject_key
        """,
        (JOB_TYPE,),
    ).fetchall()
    return {str(row["subject_key"]): int(row["job_count"]) for row in rows}


def _wallet_pipeline_scheduler_state(
    conn: sqlite3.Connection,
    evidence_job_stages: tuple[str, ...],
) -> dict[str, dict[str, int]]:
    state = {
        evidence_job_stage: {"current_weight": 0, "last_selected_at": 0}
        for evidence_job_stage in evidence_job_stages
    }
    if not evidence_job_stages:
        return state
    placeholders = ", ".join("?" for _stage in evidence_job_stages)
    rows = conn.execute(
        f"""
        SELECT subject_key, current_weight, last_selected_at
        FROM pipeline_scheduler_state
        WHERE job_type = ?
          AND subject_key IN ({placeholders})
        """,
        (JOB_TYPE, *evidence_job_stages),
    ).fetchall()
    for row in rows:
        state[str(row["subject_key"])] = {
            "current_weight": int(row["current_weight"] or 0),
            "last_selected_at": int(row["last_selected_at"] or 0),
        }
    return state


def _persist_wallet_pipeline_scheduler_state(
    conn: sqlite3.Connection,
    scheduler_state: dict[str, dict[str, int]],
    *,
    now: int,
) -> None:
    conn.executemany(
        """
        INSERT INTO pipeline_scheduler_state(
            job_type, subject_key, current_weight, last_selected_at, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(job_type, subject_key) DO UPDATE SET
            current_weight = excluded.current_weight,
            last_selected_at = excluded.last_selected_at,
            updated_at = excluded.updated_at
        """,
        (
            (
                JOB_TYPE,
                evidence_job_stage,
                int(stage_state["current_weight"]),
                int(stage_state["last_selected_at"]),
                now,
            )
            for evidence_job_stage, stage_state in scheduler_state.items()
        ),
    )


def _targets_for_action(
    conn: sqlite3.Connection,
    evidence_job_stage: str,
    limit: int,
    now: int,
    *,
    policy_version: str,
) -> list[dict[str, Any]]:
    approval_sql = _planner_promotion_approval_sql()
    rows = conn.execute(
        f"""
        SELECT
            wps.wallet,
            wps.discovery_tier,
            wps.evidence_status,
            wps.priority,
            wps.next_action,
            wps.next_action_at,
            wps.evidence_confidence,
            wps.activity_count,
            wps.updated_at
        FROM wallet_processing_state wps
        JOIN candidate_wallets cw
          ON cw.address = wps.wallet
        LEFT JOIN evidence_backfill_budget ebb
          ON ebb.wallet = wps.wallet
        LEFT JOIN wallet_features wf
          ON wf.address = wps.wallet
        LEFT JOIN leader_latest_scores ls
          ON ls.address = wps.wallet
        LEFT JOIN wallet_registry wr
          ON wr.address = wps.wallet
        WHERE wps.next_action = ?
          AND wps.next_action_at <= ?
          AND wps.evidence_status NOT IN ('paused', 'summary_ready')
          AND (
                wps.next_action = 'light_pending'
                OR {approval_sql}
          )
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs active_job
              WHERE active_job.job_type = ?
                AND active_job.wallet = wps.wallet
                AND active_job.subject_key = ?
                AND active_job.status IN ('queued', 'running')
          )
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs cooling_failed_job
              WHERE cooling_failed_job.job_type = ?
                AND cooling_failed_job.wallet = wps.wallet
                AND cooling_failed_job.subject_key = ?
                AND cooling_failed_job.tier = wps.discovery_tier
                AND cooling_failed_job.status = 'failed'
                AND cooling_failed_job.next_attempt_at > ?
          )
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs completed_job
              WHERE completed_job.job_type = ?
                AND completed_job.wallet = wps.wallet
                AND completed_job.subject_key = ?
                AND completed_job.tier = wps.discovery_tier
                AND completed_job.status = 'done'
          )
        ORDER BY
            wps.priority ASC,
            wps.evidence_confidence DESC,
            wps.activity_count DESC,
            wps.updated_at ASC,
            wps.wallet ASC
        LIMIT ?
        """,
        (
            evidence_job_stage,
            now,
            policy_version,
            JOB_TYPE,
            evidence_job_stage,
            JOB_TYPE,
            evidence_job_stage,
            now,
            JOB_TYPE,
            evidence_job_stage,
            limit,
        ),
    ).fetchall()
    return [dict(row) for row in rows]


def _planner_promotion_approval_sql() -> str:
    """Mirror the worker's snapshot guard before reserving queue capacity."""

    materializer_version = MATERIALIZER_VERSION.replace("'", "''")
    return f"""
        (
            COALESCE(CAST(json_extract(
                ebb.evidence_json,
                '$.promotion.approved'
            ) AS INTEGER), 0) = 1
            AND COALESCE(json_extract(
                ebb.evidence_json,
                '$.promotion.job_action'
            ), '') = wps.next_action
            AND COALESCE(json_extract(
                ebb.evidence_json,
                '$.promotion.policy_version'
            ), '') = ?
            AND COALESCE(CAST(json_extract(
                ebb.evidence_json,
                '$.promotion.feature_updated_at'
            ) AS INTEGER), 0) = COALESCE(wf.updated_at, 0)
            AND COALESCE(CAST(json_extract(
                ebb.evidence_json,
                '$.promotion.activity_count'
            ) AS INTEGER), -1) = (
                SELECT COUNT(*)
                FROM wallet_activity wa
                WHERE wa.address = wps.wallet AND wa.type = 'TRADE'
            )
            AND COALESCE(json_extract(
                ebb.evidence_json,
                '$.promotion.materializer_version'
            ), '') = '{materializer_version}'
            AND COALESCE(json_extract(
                wf.extra_json,
                '$.feature_materializer_version'
            ), '') = '{materializer_version}'
            AND (
                wps.next_action = 'medium_pending'
                OR COALESCE(ls.policy_version, '') = COALESCE(json_extract(
                    ebb.evidence_json,
                    '$.promotion.policy_version'
                ), '')
            )
            AND COALESCE(ebb.stop_reason, '') =
                'promotion_approved:' || wps.next_action || ':' ||
                COALESCE(json_extract(
                    ebb.evidence_json,
                    '$.promotion.policy_version'
                ), '') || ':' || CAST(COALESCE(wf.updated_at, 0) AS TEXT) || ':' ||
                CAST((
                    SELECT COUNT(*)
                    FROM wallet_activity wa
                    WHERE wa.address = wps.wallet AND wa.type = 'TRADE'
                ) AS TEXT)
        )
    """


def _stage_depth(job_action: str) -> tuple[str, int]:
    """Normalize the next queue action into job stage and history depth."""
    if job_action == EvidenceJobStage.MEDIUM_PENDING.value:
        return EvidenceJobStage.MEDIUM_PENDING.value, MEDIUM_DEPTH
    if job_action == EvidenceJobStage.DEEP_PENDING.value:
        return EvidenceJobStage.DEEP_PENDING.value, DEEP_DEPTH
    return DEFAULT_EVIDENCE_JOB_STAGE, LIGHT_DEPTH


def _job_stage_depth(job: dict[str, Any], input_data: dict[str, Any]) -> tuple[str, int]:
    evidence_job_stage = str(input_data.get("stage") or job.get("subject_key") or DEFAULT_EVIDENCE_JOB_STAGE)
    if evidence_job_stage not in PENDING_EVIDENCE_JOB_STAGES:
        evidence_job_stage = DEFAULT_EVIDENCE_JOB_STAGE
    default_depth = _stage_depth(evidence_job_stage)[1]
    try:
        target_depth = int(input_data.get("target_depth") or default_depth)
    except (TypeError, ValueError):
        target_depth = default_depth
    return evidence_job_stage, target_depth


def _promotion_approved(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    job_action: str,
    policy_version: str,
) -> bool:
    """Protect network work from legacy pending states that bypassed policy admission."""

    return evidence_promotion_approval_is_current(
        conn,
        wallet=wallet,
        job_action=job_action,
        expected_policy_version=policy_version,
        expected_materializer_version=MATERIALIZER_VERSION,
    )


def _ensure_evidence_budget(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    stage: str,
    target_depth: int,
    priority: int,
    now: int,
) -> None:
    current = int(
        conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ? AND type = 'TRADE'",
            (wallet.lower(),),
        ).fetchone()[0]
    )
    evidence = {
        "source": "wallet_pipeline_worker",
        "stage": stage,
        "target_depth": target_depth,
        "ensured_at": now,
    }
    conn.execute(
        """
        INSERT INTO evidence_backfill_budget(
            wallet, source, priority, stage, target_depth, current_depth,
            next_attempt_at, evidence_json, created_at, updated_at
        ) VALUES (?, 'pipeline_v2', ?, ?, ?, ?, 0, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            source = CASE
                WHEN evidence_backfill_budget.source = '' THEN excluded.source
                WHEN instr(evidence_backfill_budget.source, excluded.source) > 0 THEN evidence_backfill_budget.source
                ELSE evidence_backfill_budget.source || ' | ' || excluded.source
            END,
            priority = MIN(evidence_backfill_budget.priority, excluded.priority),
            stage = CASE
                WHEN evidence_backfill_budget.stage IN ('deep_done', 'paused_fast_market_specialist')
                    THEN evidence_backfill_budget.stage
                ELSE excluded.stage
            END,
            target_depth = MAX(evidence_backfill_budget.target_depth, excluded.target_depth),
            current_depth = MAX(evidence_backfill_budget.current_depth, excluded.current_depth),
            next_attempt_at = 0,
            evidence_json = excluded.evidence_json,
            updated_at = excluded.updated_at
        """,
        (
            wallet.lower(),
            int(priority),
            stage,
            int(target_depth),
            current,
            json.dumps(evidence, ensure_ascii=False, sort_keys=True),
            now,
            now,
        ),
    )


def _wallet_shard(wallet: str, shard_count: int) -> int:
    digest = hashlib.sha1(wallet.lower().encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % shard_count


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
