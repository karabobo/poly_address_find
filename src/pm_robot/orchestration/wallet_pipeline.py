"""V2 wallet evidence pipeline planner and worker."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
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
from pm_robot.pipeline_terms import (
    DEFAULT_EVIDENCE_JOB_STAGE,
    EvidenceJobStage,
    PENDING_EVIDENCE_JOB_STAGES,
    PipelineJobType,
)
from pm_robot.storage.db import is_sqlite_locked_error, retry_sqlite_locked
from pm_robot.storage.repository import (
    PipelineJobLeaseLost,
    claim_pipeline_job,
    complete_pipeline_job,
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
    update_evidence_backfill_budget,
    upsert_wallet_evidence_summary,
)


JOB_TYPE = PipelineJobType.WALLET_EVIDENCE_BACKFILL.value
LOCK_RETRY_ATTEMPTS = 8
LOCK_RETRY_SLEEP_SECONDS = 3.0


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
    light_limit: int = 30,
    medium_limit: int = 20,
    deep_limit: int = 5,
    shard_count: int = 3,
    max_active_jobs: int = 240,
    now: int | None = None,
) -> WalletPipelinePlanSummary:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")

    def _plan_once() -> WalletPipelinePlanSummary:
        return _plan_wallet_pipeline_jobs_once(
            conn,
            light_limit=light_limit,
            medium_limit=medium_limit,
            deep_limit=deep_limit,
            shard_count=shard_count,
            max_active_jobs=max_active_jobs,
            now=now,
        )

    return retry_sqlite_locked(
        _plan_once,
        rollback=conn.rollback,
        attempts=LOCK_RETRY_ATTEMPTS,
        sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
    )


def _plan_wallet_pipeline_jobs_once(
    conn: sqlite3.Connection,
    *,
    light_limit: int,
    medium_limit: int,
    deep_limit: int,
    shard_count: int,
    max_active_jobs: int,
    now: int | None,
) -> WalletPipelinePlanSummary:
    ts = now or int(time.time())
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
    targets = _select_pipeline_targets(
        conn,
        light_limit=light_limit,
        medium_limit=medium_limit,
        deep_limit=deep_limit,
        now=ts,
    )
    if max_active_jobs > 0:
        targets = targets[: max(0, max_active_jobs - active_jobs)]
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
    conn.commit()
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
    shard_index: int,
    shard_count: int,
    limit: int = 8,
    page_limit: int = 200,
    sleep_seconds: float = 0.02,
    lease_seconds: int = 900,
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
            if idx > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            job = retry_sqlite_locked(
                lambda: claim_pipeline_job(
                    conn,
                    job_type=JOB_TYPE,
                    shard=shard_index,
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
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
                failed += 1
                status = "partial"
                conn.rollback()
                now = int(time.time())
                retry_at = now + min(21_600, 900 * int(job["attempts"] or 1))
                error = f"{wallet}: {exc}"
                retry_sqlite_locked(
                    lambda: _retry_claimed_pipeline_job(
                        conn,
                        job_id=int(job["job_id"]),
                        worker_id=worker_id,
                        error=str(exc),
                        next_attempt_at=retry_at,
                        now=now,
                    ),
                    rollback=conn.rollback,
                    attempts=LOCK_RETRY_ATTEMPTS,
                    sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
                )
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
    now: int,
) -> bool:
    retried = retry_pipeline_job(
        conn,
        job_id=job_id,
        worker_id=worker_id,
        error=error,
        next_attempt_at=next_attempt_at,
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


def wallet_pipeline_job_status(conn: sqlite3.Connection) -> dict[str, Any]:
    return pipeline_job_summary(conn, job_type=JOB_TYPE)


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
    light_limit: int,
    medium_limit: int,
    deep_limit: int,
    now: int,
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    if light_limit > 0:
        targets.extend(_targets_for_action(conn, DEFAULT_EVIDENCE_JOB_STAGE, light_limit, now))
    if medium_limit > 0:
        targets.extend(
            _targets_for_action(conn, EvidenceJobStage.MEDIUM_PENDING.value, medium_limit, now)
        )
    if deep_limit > 0:
        targets.extend(
            _targets_for_action(conn, EvidenceJobStage.DEEP_PENDING.value, deep_limit, now)
        )
    return targets


def _targets_for_action(
    conn: sqlite3.Connection,
    evidence_job_stage: str,
    limit: int,
    now: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            wps.wallet,
            wps.discovery_tier,
            wps.evidence_status,
            wps.priority,
            wps.next_action,
            wps.next_action_at,
            wps.evidence_confidence,
            wps.activity_count
        FROM wallet_processing_state wps
        JOIN candidate_wallets cw
          ON cw.address = wps.wallet
        WHERE wps.next_action = ?
          AND wps.next_action_at <= ?
          AND wps.evidence_status NOT IN ('paused', 'summary_ready')
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
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
        (evidence_job_stage, now, JOB_TYPE, evidence_job_stage, JOB_TYPE, evidence_job_stage, limit),
    ).fetchall()
    return [dict(row) for row in rows]


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
