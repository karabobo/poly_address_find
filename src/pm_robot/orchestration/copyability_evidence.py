"""Dedicated copyability queue, separate from L1/L2/L3 history tiers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress
from pm_robot.orchestration.feature_materializer import materialize_wallet_feature
from pm_robot.orchestration.review_pipeline import apply_paper_evidence_guard
from pm_robot.pipeline_terms import PipelineJobType
from pm_robot.research.copy_backtest import backtest_copy_stream_for_leaders
from pm_robot.research.copy_graph import mine_copy_graph_for_leaders
from pm_robot.research.scoring import score_candidate
from pm_robot.storage.db import retry_sqlite_locked
from pm_robot.storage.repository import (
    PipelineJobLeaseLost,
    _feature_from_row,
    apply_copyability_no_signal_blocks,
    complete_pipeline_job,
    enqueue_pipeline_job,
    finish_ingest_run,
    persist_score,
    pipeline_job_summary,
    renew_pipeline_job_lease,
    retry_pipeline_job,
    start_ingest_run,
)


JOB_TYPE = PipelineJobType.COPYABILITY_EVIDENCE.value
JOB_ACTION = "copyability"
JOB_SCOPE = "copyability"
SUBJECT_KEY = JOB_ACTION
TIER = JOB_SCOPE  # Backward-compatible alias for pipeline_jobs.tier.
MISSING_COPYABILITY_PLANNER_REASON = "missing_copyability_components"
MANUAL_MISSING_COPYABILITY_PLANNER_REASON = "manual_missing_copyability"
SCORE_REVIEW_PLANNER_REASON = "score_review"
LIGHT_NO_SIGNAL_DEEP_RESCAN_PLANNER_REASON = "light_no_signal_deep_rescan"
LIGHT_NO_SIGNAL_DEEP_RESCAN_MIN_SCORE = 55.0
LIGHT_SCAN_MAX_LEADER_EVENTS = 600
LIGHT_SCAN_MAX_FOLLOWERS_PER_EVENT = 80
LOCK_RETRY_ATTEMPTS = 8
LOCK_RETRY_SLEEP_SECONDS = 3.0


@dataclass(frozen=True)
class CopyabilityPlanSummary:
    targets_seen: int
    jobs_enqueued: int
    jobs_reprioritized: int
    shard_count: int
    status: str
    active_jobs: int = 0
    max_active_jobs: int = 0
    available_slots: int = 0
    throttled: bool = False
    reason: str = ""


@dataclass(frozen=True)
class CopyabilityWorkerSummary:
    run_id: int
    shard_index: int
    shard_count: int
    jobs_attempted: int
    jobs_succeeded: int
    jobs_failed: int
    links_written: int
    pair_stats_written: int
    qualified_pairs: int
    backtest_trades_written: int
    leader_performance_written: int
    features_materialized: int
    scores_written: int
    no_signal_blocks: int
    status: str
    error: str = ""


def plan_copyability_evidence_jobs(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    max_active_jobs: int = 50,
    min_score: float = 40.0,
    min_activity_events: int = 25,
    shard_count: int = 3,
    rescan_seconds: int = 21_600,
    lock_retry_attempts: int = LOCK_RETRY_ATTEMPTS,
    lock_retry_sleep_seconds: float = LOCK_RETRY_SLEEP_SECONDS,
    now: int | None = None,
) -> CopyabilityPlanSummary:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")

    def _plan_once() -> CopyabilityPlanSummary:
        conn.commit()
        ts = now or int(time.time())
        batch_limit = max(0, int(limit))
        prefetched_targets = _select_copyability_targets(
            conn,
            limit=batch_limit,
            min_score=min_score,
            min_activity_events=min_activity_events,
            rescan_seconds=rescan_seconds,
            now=ts,
        )
        priority_updates = _copyability_priority_updates(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            summary = _plan_copyability_evidence_jobs_once(
                conn,
                limit=limit,
                max_active_jobs=max_active_jobs,
                min_score=min_score,
                min_activity_events=min_activity_events,
                shard_count=shard_count,
                rescan_seconds=rescan_seconds,
                now=ts,
                prefetched_targets=prefetched_targets,
                priority_updates=priority_updates,
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


def _plan_copyability_evidence_jobs_once(
    conn: sqlite3.Connection,
    *,
    limit: int,
    max_active_jobs: int,
    min_score: float,
    min_activity_events: int,
    shard_count: int,
    rescan_seconds: int,
    now: int,
    prefetched_targets: list[sqlite3.Row],
    priority_updates: list[tuple[int, int]],
) -> CopyabilityPlanSummary:
    """Reserve copyability queue capacity and enqueue work under one write lock."""

    ts = now
    active_jobs = _active_copyability_job_count(conn)
    batch_limit = max(0, int(limit))
    waterline_slots = (
        max(0, int(max_active_jobs) - active_jobs)
        if max_active_jobs > 0
        else batch_limit
    )
    available_slots = min(batch_limit, waterline_slots)
    if max_active_jobs > 0 and waterline_slots == 0:
        return CopyabilityPlanSummary(
            targets_seen=0,
            jobs_enqueued=0,
            jobs_reprioritized=0,
            shard_count=shard_count,
            status="backlog_active",
            active_jobs=active_jobs,
            max_active_jobs=int(max_active_jobs),
            available_slots=0,
            throttled=True,
            reason="active_queue_waterline",
        )
    targets = _revalidate_copyability_targets(
        conn,
        prefetched_targets,
        min_score=min_score,
        min_activity_events=min_activity_events,
        rescan_seconds=rescan_seconds,
        now=ts,
    )[:available_slots]
    enqueued = 0
    for target in targets:
        wallet = str(target["address"]).lower()
        _reset_retriable_existing_job(conn, wallet=wallet, now=ts)
        scan_input = _target_scan_input(target)
        enqueued += 1 if enqueue_pipeline_job(
            conn,
            job_type=JOB_TYPE,
            wallet=wallet,
            subject_key=SUBJECT_KEY,
            tier=TIER,
            priority=_target_priority(target),
            shard=_wallet_shard(wallet, shard_count),
            input_data={
                "source": "copyability_planner",
                "planned_at": ts,
                "leader_score": _float_or_none(target["leader_score"]),
                "review_reason": target["review_reason"],
                "activity_count": int(target["activity_count"] or 0),
                "pair_count": int(target["pair_count"] or 0),
                "max_pair_events": int(target["max_pair_events"] or 0),
                "max_pair_markets": int(target["max_pair_markets"] or 0),
                "planner_reason": target["planner_reason"],
                "candidate_stage": target["candidate_stage"],
                "distinct_markets": int(target["distinct_markets"] or 0),
                "non_fast_trade_count": int(target["non_fast_trade_count"] or 0),
                **scan_input,
            },
            max_attempts=3,
            next_attempt_at=0,
            now=ts,
        ) else 0
    reprioritized = _apply_copyability_priority_updates(conn, priority_updates, now=ts)
    return CopyabilityPlanSummary(
        targets_seen=len(targets),
        jobs_enqueued=enqueued,
        jobs_reprioritized=reprioritized,
        shard_count=shard_count,
        status="ok",
        active_jobs=active_jobs,
        max_active_jobs=max(0, int(max_active_jobs)),
        available_slots=available_slots,
        throttled=False,
    )


def run_copyability_evidence_worker(
    conn: sqlite3.Connection,
    *,
    shard_index: int,
    shard_count: int = 3,
    limit: int = 4,
    lease_seconds: int = 7_200,
    worker_id: str = "",
    policy_path: str = "",
    max_leader_events: int = 3_000,
    max_followers_per_event: int = 200,
    prefer_scan_mode: str = "",
) -> CopyabilityWorkerSummary:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be in [0, shard_count)")
    worker_id = worker_id or f"copyability-{shard_index}-{int(time.time())}"
    policy = load_policy(Path(policy_path)) if policy_path else load_policy()
    if shard_count == 1:
        _normalize_single_shard_jobs(conn, now=int(time.time()))
    run_id = retry_sqlite_locked(
        lambda: start_ingest_run(conn, _copyability_worker_ingest_type(shard_index, worker_id)),
        rollback=conn.rollback,
        attempts=LOCK_RETRY_ATTEMPTS,
        sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
    )
    attempted = 0
    succeeded = 0
    failed = 0
    links_written = 0
    pair_stats_written = 0
    qualified_pairs = 0
    backtest_trades_written = 0
    leader_performance_written = 0
    features_materialized = 0
    scores_written = 0
    no_signal_blocks = 0
    status = "ok"
    error = ""
    try:
        for _ in range(max(0, limit)):
            job = retry_sqlite_locked(
                lambda: _claim_copyability_job(
                    conn,
                    shard=shard_index,
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    prefer_scan_mode=prefer_scan_mode,
                ),
                rollback=conn.rollback,
                attempts=LOCK_RETRY_ATTEMPTS,
                sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
            )
            if job is None:
                break
            attempted += 1
            wallet = str(job["wallet"]).lower()
            job_input = _json_object(job.get("input_json"))
            graph_scan_mode = str(job_input.get("graph_scan_mode") or "default")
            effective_max_leader_events = _bounded_positive_int(
                job_input.get("graph_max_leader_events"),
                fallback=max_leader_events,
            )
            effective_max_followers_per_event = _bounded_positive_int(
                job_input.get("graph_max_followers_per_event"),
                fallback=max_followers_per_event,
            )
            try:
                now = int(time.time())
                _require_copyability_job_lease(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                )
                graph = mine_copy_graph_for_leaders(
                    conn,
                    policy,
                    [wallet],
                    max_leader_events=effective_max_leader_events,
                    max_followers_per_event=effective_max_followers_per_event,
                    now=now,
                    commit=False,
                )
                _require_copyability_job_lease(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    commit=False,
                )
                backtest = backtest_copy_stream_for_leaders(
                    conn,
                    policy,
                    [wallet],
                    now=now,
                    commit=False,
                )
                _require_copyability_job_lease(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    commit=False,
                )
                materialized = materialize_wallet_feature(
                    conn,
                    wallet,
                    now=now,
                    refresh_copyability=True,
                    commit=False,
                )
                scored = (
                    _score_wallet_after_copyability(
                        conn,
                        wallet=wallet,
                        policy=policy,
                        policy_version=str(policy.get("version", "")),
                    )
                    if materialized
                    else False
                )
                no_signal_blocked = (
                    apply_copyability_no_signal_blocks(
                        conn,
                        wallet=wallet,
                        allow_running=True,
                        now=int(time.time()),
                    )
                    if scored
                    else 0
                )
                now = int(time.time())
                _require_copyability_job_lease(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                    commit=False,
                )
                completed = complete_pipeline_job(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    output_data={
                        "wallet": wallet,
                        "graph": graph.__dict__,
                        "backtest": backtest.__dict__,
                        "features_materialized": materialized,
                        "score_written": scored,
                        "no_signal_blocked": no_signal_blocked,
                        "graph_scan_mode": graph_scan_mode,
                        "graph_max_leader_events": effective_max_leader_events,
                        "graph_max_followers_per_event": effective_max_followers_per_event,
                    },
                    now=now,
                )
                if not completed:
                    raise PipelineJobLeaseLost(
                        "copyability job lease was lost before completion"
                    )
                conn.commit()
                succeeded += 1
                links_written += graph.links_written
                pair_stats_written += graph.pair_stats_written
                qualified_pairs += graph.qualified_pairs
                backtest_trades_written += backtest.trades_written
                leader_performance_written += backtest.leader_performance_written
                features_materialized += 1 if materialized else 0
                scores_written += 1 if scored else 0
                no_signal_blocks += no_signal_blocked
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
                retried = retry_pipeline_job(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    error=str(exc),
                    next_attempt_at=retry_at,
                    now=now,
                )
                if retried:
                    conn.commit()
                else:
                    conn.rollback()
        return CopyabilityWorkerSummary(
            run_id=run_id,
            shard_index=shard_index,
            shard_count=shard_count,
            jobs_attempted=attempted,
            jobs_succeeded=succeeded,
            jobs_failed=failed,
            links_written=links_written,
            pair_stats_written=pair_stats_written,
            qualified_pairs=qualified_pairs,
            backtest_trades_written=backtest_trades_written,
            leader_performance_written=leader_performance_written,
            features_materialized=features_materialized,
            scores_written=scores_written,
            no_signal_blocks=no_signal_blocks,
            status=status,
            error=error,
        )
    except Exception as exc:
        status = "failed"
        error = str(exc)
        return CopyabilityWorkerSummary(
            run_id=run_id,
            shard_index=shard_index,
            shard_count=shard_count,
            jobs_attempted=attempted,
            jobs_succeeded=succeeded,
            jobs_failed=failed,
            links_written=links_written,
            pair_stats_written=pair_stats_written,
            qualified_pairs=qualified_pairs,
            backtest_trades_written=backtest_trades_written,
            leader_performance_written=leader_performance_written,
            features_materialized=features_materialized,
            scores_written=scores_written,
            no_signal_blocks=no_signal_blocks,
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
                rows_written=links_written + pair_stats_written + backtest_trades_written,
                error=error,
            ),
            rollback=conn.rollback,
            attempts=LOCK_RETRY_ATTEMPTS,
            sleep_seconds=LOCK_RETRY_SLEEP_SECONDS,
        )


def copyability_evidence_job_status(conn: sqlite3.Connection) -> dict[str, Any]:
    return pipeline_job_summary(conn, job_type=JOB_TYPE)


def _require_copyability_job_lease(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    lease_seconds: int,
    commit: bool = True,
) -> None:
    """Renew a claimed copyability job and fail closed when ownership changed."""
    renewed = renew_pipeline_job_lease(
        conn,
        job_id=job_id,
        worker_id=worker_id,
        lease_seconds=lease_seconds,
        now=int(time.time()),
    )
    if not renewed:
        conn.rollback()
        raise PipelineJobLeaseLost("copyability job lease was lost")
    if commit:
        conn.commit()


def _score_wallet_after_copyability(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    policy: dict[str, Any],
    policy_version: str,
) -> bool:
    """Write a fresh score for the wallet whose copyability evidence just changed."""

    wallet = wallet.lower()
    candidate_row = conn.execute(
        """
        SELECT address, sources, labels, notes, links, status
        FROM candidate_wallets
        WHERE address = ?
        """,
        (wallet,),
    ).fetchone()
    feature_row = conn.execute(
        "SELECT * FROM wallet_features WHERE address = ?",
        (wallet,),
    ).fetchone()
    if not candidate_row or not feature_row:
        return False
    candidate = CandidateAddress(
        address=candidate_row["address"],
        sources=candidate_row["sources"],
        labels=candidate_row["labels"],
        notes=candidate_row["notes"],
        links=candidate_row["links"],
        status=candidate_row["status"],
    )
    score = apply_paper_evidence_guard(
        conn,
        score_candidate(candidate, _feature_from_row(feature_row), policy),
    )
    persist_score(conn, score, policy_version=policy_version)
    return True


def _claim_copyability_job(
    conn: sqlite3.Connection,
    *,
    shard: int,
    worker_id: str,
    lease_seconds: int,
    prefer_scan_mode: str = "",
    now: int | None = None,
) -> dict[str, Any] | None:
    ts = now or int(time.time())
    conn.execute("BEGIN IMMEDIATE")
    _requeue_owned_running_copyability_jobs(conn, worker_id=worker_id, now=ts)
    preferred_row = _select_claimable_copyability_job(
        conn,
        shard=shard,
        now=ts,
        prefer_scan_mode=prefer_scan_mode,
    )
    best_row = _select_claimable_copyability_job(
        conn,
        shard=shard,
        now=ts,
        prefer_scan_mode="",
    )
    row = _choose_preferred_scan_job(preferred_row, best_row)
    if row is None:
        conn.commit()
        return None
    conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = 'running',
            lease_owner = ?,
            lease_until = ?,
            attempts = attempts + 1,
            updated_at = ?
        WHERE job_id = ?
        """,
        (worker_id, ts + lease_seconds, ts, row["job_id"]),
    )
    conn.commit()
    out = dict(row)
    out["attempts"] = int(out.get("attempts") or 0) + 1
    out["lease_owner"] = worker_id
    out["lease_until"] = ts + lease_seconds
    return out


def _requeue_owned_running_copyability_jobs(
    conn: sqlite3.Connection,
    *,
    worker_id: str,
    now: int,
) -> None:
    """A copyability worker loop is single-threaded; old owned running jobs are leftovers."""

    if not worker_id:
        return
    conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = 'queued',
            lease_owner = NULL,
            lease_until = 0,
            next_attempt_at = 0,
            last_error = CASE
                WHEN last_error = '' THEN 'superseded_running_owner_requeued_by_worker'
                ELSE last_error
            END,
            updated_at = ?
        WHERE job_type = ?
          AND status = 'running'
          AND lease_owner = ?
        """,
        (now, JOB_TYPE, worker_id),
    )


def _choose_preferred_scan_job(
    preferred_row: sqlite3.Row | None,
    best_row: sqlite3.Row | None,
) -> sqlite3.Row | None:
    """Prefer a scan lane only when it does not jump ahead of higher-priority work."""

    if preferred_row is None:
        return best_row
    if best_row is None:
        return preferred_row
    preferred_priority = int(preferred_row["priority"] or 100)
    best_priority = int(best_row["priority"] or 100)
    return preferred_row if preferred_priority <= best_priority else best_row


def _select_claimable_copyability_job(
    conn: sqlite3.Connection,
    *,
    shard: int,
    now: int,
    prefer_scan_mode: str,
) -> sqlite3.Row | None:
    scan_filter = ""
    params: list[Any] = [JOB_TYPE, int(shard), int(now), int(now)]
    if prefer_scan_mode:
        scan_filter = """
          AND (
                input_json LIKE ?
             OR input_json LIKE ?
          )
        """
        params.extend(_scan_mode_like_patterns(prefer_scan_mode))
    return conn.execute(
        f"""
        SELECT *
        FROM pipeline_jobs
        WHERE job_type = ?
          AND shard = ?
          AND next_attempt_at <= ?
          AND attempts < max_attempts
          AND (
                status = 'queued'
                OR (status = 'running' AND lease_until <= ?)
          )
          {scan_filter}
        ORDER BY priority ASC, updated_at ASC, job_id ASC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()


def _scan_mode_like_patterns(scan_mode: str) -> tuple[str, str]:
    value = str(scan_mode or "").strip()
    return (
        f'%"graph_scan_mode": "{value}"%',
        f'%"graph_scan_mode":"{value}"%',
    )


def _copyability_worker_ingest_type(shard_index: int, worker_id: str) -> str:
    safe_worker = "".join(ch if ch.isalnum() else "_" for ch in worker_id.lower()).strip("_")
    safe_worker = safe_worker[:64] or "worker"
    return f"copyability_evidence_worker_{shard_index}_{safe_worker}"


def _select_copyability_targets(
    conn: sqlite3.Connection,
    *,
    limit: int,
    min_score: float,
    min_activity_events: int,
    rescan_seconds: int,
    now: int,
    addresses: tuple[str, ...] = (),
) -> list[sqlite3.Row]:
    stale_before = now - max(0, int(rescan_seconds))
    normalized_addresses = tuple(dict.fromkeys(address.lower() for address in addresses if address))
    latest_address_filter = ""
    candidate_address_filter = ""
    if normalized_addresses:
        placeholders = ", ".join("?" for _address in normalized_addresses)
        latest_address_filter = f"WHERE address IN ({placeholders})"
        candidate_address_filter = f"AND cw.address IN ({placeholders})"
    return conn.execute(
        f"""
        WITH latest AS (
            SELECT ls.*
            FROM leader_scores ls
            JOIN (
                SELECT address, MAX(score_id) AS max_id
                FROM leader_scores
                {latest_address_filter}
                GROUP BY address
            ) latest_id
              ON latest_id.address = ls.address
             AND latest_id.max_id = ls.score_id
        ),
        candidate_base AS (
            SELECT
                cw.address,
                cw.candidate_stage,
                latest.leader_score,
                latest.review_stage,
                latest.review_reason,
                COALESCE(
                    wps.activity_count,
                    (
                        SELECT COUNT(*)
                        FROM wallet_activity wa
                        WHERE wa.address = cw.address
                          AND wa.type = 'TRADE'
                    ),
                    0
                ) AS activity_count,
                COALESCE(wps.distinct_markets, 0) AS distinct_markets,
                COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
                COALESCE(wps.discovery_tier, '') AS discovery_tier,
                COALESCE(wps.evidence_status, '') AS evidence_status,
                COALESCE(wps.current_stage, '') AS evidence_job_stage,
                pj.job_id AS existing_job_id,
                COALESCE(pj.status, '') AS existing_job_status,
                COALESCE(pj.next_attempt_at, 0) AS existing_next_attempt_at,
                COALESCE(pj.completed_at, 0) AS existing_completed_at,
                COALESCE(
                    json_extract(pj.output_json, '$.graph_scan_mode'),
                    json_extract(pj.input_json, '$.graph_scan_mode'),
                    ''
                ) AS existing_scan_mode,
                COALESCE(wf.leader_in_degree, 0) AS feature_leader_in_degree,
                COALESCE(wf.copy_event_count, 0) AS feature_copy_event_count,
                COALESCE(wf.copy_market_count, 0) AS feature_copy_market_count,
                COALESCE(json_extract(wf.extra_json, '$.copy_candidate_pair_count'), 0) AS feature_copy_candidate_pair_count,
                COALESCE(json_extract(wf.extra_json, '$.copy_candidate_event_count'), 0) AS feature_copy_candidate_event_count,
                COALESCE(json_extract(wf.extra_json, '$.copy_candidate_market_count'), 0) AS feature_copy_candidate_market_count,
                COALESCE(json_extract(wf.extra_json, '$.copy_validated_pair_count'), 0) AS feature_copy_validated_pair_count
            FROM candidate_wallets cw
            JOIN latest
              ON latest.address = cw.address
            LEFT JOIN wallet_processing_state wps
              ON wps.wallet = cw.address
            LEFT JOIN wallet_features wf
              ON wf.address = cw.address
            LEFT JOIN wallet_registry wr
              ON wr.address = cw.address
            LEFT JOIN pipeline_jobs pj
              ON pj.job_type = ?
             AND pj.wallet = cw.address
             AND pj.tier = ?
             AND pj.subject_key = ?
            WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
              AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
              {candidate_address_filter}
              AND (
                    cw.candidate_stage IN (
                        'needs_manual_review',
                        'paper_candidate',
                        'paper_approved',
                        'live_eligible'
                    )
                 OR (
                        latest.review_stage = 'needs_data'
                        AND latest.review_reason LIKE 'missing_required_score_components:%'
                        AND (
                               latest.review_reason LIKE '%leader_in_degree%'
                            OR latest.review_reason LIKE '%copy_event_count%'
                            OR latest.review_reason LIKE '%copy_market_count%'
                            OR latest.review_reason LIKE '%copy_stream_roi%'
                        )
                    )
              )
        ),
        eligible_candidates AS (
            SELECT *
            FROM candidate_base
            WHERE activity_count >= ?
              AND (
                    (
                        candidate_stage IN (
                            'needs_manual_review',
                            'paper_candidate',
                            'paper_approved',
                            'live_eligible'
                        )
                        AND leader_score >= ?
                    )
                 OR (
                        candidate_stage = 'needs_manual_review'
                        AND review_stage = 'needs_manual_review'
                        AND existing_job_id IS NULL
                        AND (
                               evidence_status = 'summary_ready'
                            OR evidence_job_stage IN ('medium_done', 'deep_done')
                            OR discovery_tier IN ('l2_medium', 'l3_deep')
                        )
                    )
                 OR (
                        candidate_stage = 'needs_manual_review'
                        AND review_stage = 'needs_manual_review'
                        AND leader_score >= ?
                        AND existing_job_status = 'done'
                        AND existing_scan_mode NOT IN ('', 'default', 'deep')
                        AND COALESCE(feature_leader_in_degree, 0) = 0
                        AND COALESCE(feature_copy_event_count, 0) = 0
                        AND COALESCE(feature_copy_market_count, 0) = 0
                        AND COALESCE(feature_copy_candidate_pair_count, 0) = 0
                        AND COALESCE(feature_copy_candidate_event_count, 0) = 0
                        AND COALESCE(feature_copy_candidate_market_count, 0) = 0
                        AND COALESCE(feature_copy_validated_pair_count, 0) = 0
                    )
                 OR (
                        review_stage = 'needs_data'
                        AND review_reason LIKE 'missing_required_score_components:%'
                        AND (
                               review_reason LIKE '%leader_in_degree%'
                            OR review_reason LIKE '%copy_event_count%'
                            OR review_reason LIKE '%copy_market_count%'
                            OR review_reason LIKE '%copy_stream_roi%'
                        )
                        AND (
                               evidence_status = 'summary_ready'
                            OR evidence_job_stage IN ('medium_done', 'deep_done')
                            OR discovery_tier IN ('l2_medium', 'l3_deep')
                        )
                    )
              )
              AND (
                    existing_job_id IS NULL
                 OR (
                        existing_job_status = 'failed'
                    AND existing_next_attempt_at <= ?
                    )
                 OR (
                        existing_job_status = 'done'
                    AND existing_completed_at <= ?
                 )
                 OR (
                        candidate_stage = 'needs_manual_review'
                    AND review_stage = 'needs_manual_review'
                    AND leader_score >= ?
                    AND existing_job_status = 'done'
                    AND existing_scan_mode NOT IN ('', 'default', 'deep')
                    AND COALESCE(feature_leader_in_degree, 0) = 0
                    AND COALESCE(feature_copy_event_count, 0) = 0
                    AND COALESCE(feature_copy_market_count, 0) = 0
                    AND COALESCE(feature_copy_candidate_pair_count, 0) = 0
                    AND COALESCE(feature_copy_candidate_event_count, 0) = 0
                    AND COALESCE(feature_copy_candidate_market_count, 0) = 0
                    AND COALESCE(feature_copy_validated_pair_count, 0) = 0
                 )
              )
        ),
        pair_agg AS (
            SELECT
                cps.leader_wallet AS address,
                COUNT(*) AS pair_count,
                MAX(cps.copy_event_count) AS max_pair_events,
                MAX(cps.copy_market_count) AS max_pair_markets,
                MAX(cps.containment_pct) AS max_containment_pct,
                MAX(cps.leader_precedes_pct) AS max_leader_precedes_pct
            FROM copy_pair_stats cps
            JOIN eligible_candidates eligible
              ON eligible.address = cps.leader_wallet
            GROUP BY cps.leader_wallet
        ),
        base AS (
            SELECT
                eligible.*,
                COALESCE(pair_agg.pair_count, 0) AS pair_count,
                COALESCE(pair_agg.max_pair_events, 0) AS max_pair_events,
                COALESCE(pair_agg.max_pair_markets, 0) AS max_pair_markets,
                COALESCE(pair_agg.max_containment_pct, 0) AS max_containment_pct,
                COALESCE(pair_agg.max_leader_precedes_pct, 0) AS max_leader_precedes_pct
            FROM eligible_candidates eligible
            LEFT JOIN pair_agg
              ON pair_agg.address = eligible.address
        )
        SELECT
            *,
            CASE
                WHEN candidate_stage = 'needs_manual_review'
                 AND review_stage = 'needs_manual_review'
                 AND existing_job_status = 'done'
                 AND existing_scan_mode NOT IN ('', 'default', 'deep')
                THEN 'light_no_signal_deep_rescan'
                WHEN candidate_stage = 'needs_manual_review'
                 AND review_stage = 'needs_manual_review'
                 AND existing_job_id IS NULL
                THEN 'manual_missing_copyability'
                WHEN review_stage = 'needs_data'
                 AND review_reason LIKE 'missing_required_score_components:%'
                 AND (
                        review_reason LIKE '%leader_in_degree%'
                     OR review_reason LIKE '%copy_event_count%'
                     OR review_reason LIKE '%copy_market_count%'
                     OR review_reason LIKE '%copy_stream_roi%'
                 )
                THEN 'missing_copyability_components'
                ELSE 'score_review'
            END AS planner_reason
        FROM base
        ORDER BY
            CASE
                WHEN max_pair_events >= 5
                 AND max_pair_markets >= 3 THEN 0
                WHEN planner_reason = 'score_review' AND leader_score >= 60 THEN 1
                WHEN planner_reason = 'score_review' AND leader_score >= 50 THEN 2
                WHEN planner_reason = 'manual_missing_copyability'
                 AND discovery_tier = 'l3_deep'
                 AND activity_count >= 1000 THEN 3
                WHEN planner_reason = 'missing_copyability_components'
                 AND discovery_tier = 'l3_deep'
                 AND activity_count >= 1000 THEN 3
                WHEN planner_reason = 'manual_missing_copyability' THEN 4
                WHEN planner_reason = 'missing_copyability_components' THEN 4
                ELSE 3
            END ASC,
            leader_score DESC,
            max_pair_events DESC,
            activity_count DESC,
            address ASC
        LIMIT ?
        """,
        (
            *normalized_addresses,
            JOB_TYPE,
            TIER,
            SUBJECT_KEY,
            *normalized_addresses,
            int(min_activity_events),
            float(min_score),
            LIGHT_NO_SIGNAL_DEEP_RESCAN_MIN_SCORE,
            now,
            stale_before,
            LIGHT_NO_SIGNAL_DEEP_RESCAN_MIN_SCORE,
            int(limit),
        ),
    ).fetchall()


def _revalidate_copyability_targets(
    conn: sqlite3.Connection,
    targets: list[sqlite3.Row],
    *,
    min_score: float,
    min_activity_events: int,
    rescan_seconds: int,
    now: int,
) -> list[sqlite3.Row]:
    """Re-run eligibility for the small prefetched set while queue slots are reserved."""

    wallets = sorted({str(target["address"]).lower() for target in targets})
    if not wallets:
        return []
    current_targets = _select_copyability_targets(
        conn,
        limit=len(wallets),
        min_score=min_score,
        min_activity_events=min_activity_events,
        rescan_seconds=rescan_seconds,
        now=now,
        addresses=tuple(wallets),
    )
    current_by_wallet = {str(row["address"]).lower(): row for row in current_targets}
    return [
        current_by_wallet[wallet]
        for target in targets
        if (wallet := str(target["address"]).lower()) in current_by_wallet
    ]


def _active_copyability_job_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM pipeline_jobs
        WHERE job_type = ?
          AND status IN ('queued', 'running')
        """,
        (JOB_TYPE,),
    ).fetchone()
    return int(row["count"] or 0) if row else 0


def _reset_retriable_existing_job(conn: sqlite3.Connection, *, wallet: str, now: int) -> None:
    conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = 'queued',
            lease_owner = NULL,
            lease_until = 0,
            attempts = 0,
            next_attempt_at = 0,
            last_error = CASE WHEN status = 'failed' THEN last_error ELSE '' END,
            updated_at = ?
        WHERE job_type = ?
          AND wallet = ?
          AND tier = ?
          AND subject_key = ?
          AND status IN ('done', 'failed')
        """,
        (now, JOB_TYPE, wallet.lower(), TIER, SUBJECT_KEY),
    )


def _normalize_single_shard_jobs(conn: sqlite3.Connection, *, now: int) -> None:
    conn.execute(
        """
        UPDATE pipeline_jobs
        SET shard = 0,
            status = CASE
                WHEN status = 'running' THEN 'queued'
                ELSE status
            END,
            lease_owner = CASE
                WHEN status = 'running' THEN NULL
                ELSE lease_owner
            END,
            lease_until = CASE
                WHEN status = 'running' THEN 0
                ELSE lease_until
            END,
            updated_at = ?
        WHERE job_type = ?
          AND shard != 0
          AND (
                status IN ('queued', 'failed')
             OR (status = 'running' AND lease_until <= ?)
          )
        """,
        (now, JOB_TYPE, now),
    )
    conn.commit()


def _copyability_priority_updates(conn: sqlite3.Connection) -> list[tuple[int, int]]:
    """Calculate queued-job priority changes before opening the write transaction."""

    rows = conn.execute(
        """
        WITH queued_jobs AS (
            SELECT *
            FROM pipeline_jobs
            WHERE job_type = ?
              AND status = 'queued'
        ),
        latest AS (
            SELECT ls.*
            FROM leader_scores ls
            JOIN (
                SELECT q.wallet AS address, MAX(ls.score_id) AS max_id
                FROM queued_jobs q
                JOIN leader_scores ls
                  ON ls.address = q.wallet
                GROUP BY q.wallet
            ) latest_id
              ON latest_id.address = ls.address
             AND latest_id.max_id = ls.score_id
        ),
        pair_agg AS (
            SELECT
                leader_wallet AS address,
                MAX(copy_event_count) AS max_pair_events,
                MAX(copy_market_count) AS max_pair_markets
            FROM copy_pair_stats
            JOIN queued_jobs q
              ON q.wallet = copy_pair_stats.leader_wallet
            GROUP BY leader_wallet
        )
        SELECT
            pj.job_id,
            pj.priority,
            COALESCE(cw.candidate_stage, '') AS candidate_stage,
            COALESCE(latest.leader_score, 0) AS leader_score,
            COALESCE(latest.review_stage, '') AS review_stage,
            COALESCE(wps.activity_count, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(pair_agg.max_pair_events, 0) AS max_pair_events,
            COALESCE(pair_agg.max_pair_markets, 0) AS max_pair_markets,
            CASE
                WHEN COALESCE(json_extract(pj.input_json, '$.planner_reason'), '') = 'light_no_signal_deep_rescan'
                THEN 'light_no_signal_deep_rescan'
                WHEN cw.candidate_stage = 'needs_manual_review'
                 AND latest.review_stage = 'needs_manual_review'
                 AND COALESCE(pair_agg.max_pair_events, 0) = 0
                 AND COALESCE(pair_agg.max_pair_markets, 0) = 0
                THEN 'manual_missing_copyability'
                WHEN latest.review_stage = 'needs_data'
                 AND latest.review_reason LIKE 'missing_required_score_components:%'
                 AND (
                        latest.review_reason LIKE '%leader_in_degree%'
                     OR latest.review_reason LIKE '%copy_event_count%'
                     OR latest.review_reason LIKE '%copy_market_count%'
                     OR latest.review_reason LIKE '%copy_stream_roi%'
                 )
                THEN 'missing_copyability_components'
                ELSE 'score_review'
            END AS planner_reason
        FROM queued_jobs pj
        LEFT JOIN candidate_wallets cw
          ON cw.address = pj.wallet
        LEFT JOIN latest
          ON latest.address = pj.wallet
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = pj.wallet
        LEFT JOIN pair_agg
          ON pair_agg.address = pj.wallet
        """,
        (JOB_TYPE,),
    ).fetchall()
    updates: list[tuple[int, int]] = []
    for row in rows:
        priority = _target_priority(row)
        if int(row["priority"] or 0) == priority:
            continue
        updates.append((int(row["job_id"]), priority))
    return updates


def _apply_copyability_priority_updates(
    conn: sqlite3.Connection,
    updates: list[tuple[int, int]],
    *,
    now: int,
) -> int:
    changed = 0
    for job_id, priority in updates:
        cur = conn.execute(
            """
            UPDATE pipeline_jobs
            SET priority = ?,
                updated_at = ?
            WHERE job_id = ?
              AND status = 'queued'
              AND priority != ?
            """,
            (priority, now, job_id, priority),
        )
        changed += max(0, int(cur.rowcount))
    return changed


def _target_priority(target: sqlite3.Row) -> int:
    planner_reason = str(_row_value(target, "planner_reason", "") or "")
    score = float(_row_value(target, "leader_score", 0.0) or 0.0)
    max_events = int(_row_value(target, "max_pair_events", 0) or 0)
    max_markets = int(_row_value(target, "max_pair_markets", 0) or 0)
    activity_count = int(_row_value(target, "activity_count", 0) or 0)
    distinct_markets = int(_row_value(target, "distinct_markets", 0) or 0)
    if planner_reason in {MISSING_COPYABILITY_PLANNER_REASON, MANUAL_MISSING_COPYABILITY_PLANNER_REASON}:
        if activity_count >= 1000 and distinct_markets >= 10:
            priority = 14
        elif activity_count >= 200 and distinct_markets >= 5:
            priority = 16
        else:
            priority = 20
    elif score >= 65:
        priority = 3
    elif score >= 60:
        priority = 5
    elif score >= 55:
        priority = 8
    elif score >= 50:
        priority = 12
    elif score >= 45:
        priority = 16
    else:
        priority = 20
    if max_events >= 5 and max_markets >= 3:
        priority = max(3, priority - 2)
    return priority


def _target_scan_input(target: sqlite3.Row) -> dict[str, Any]:
    planner_reason = str(_row_value(target, "planner_reason", "") or "")
    if planner_reason != MISSING_COPYABILITY_PLANNER_REASON:
        return {"graph_scan_mode": "deep"}
    return {
        "graph_scan_mode": "light_missing_copyability",
        "graph_max_leader_events": LIGHT_SCAN_MAX_LEADER_EVENTS,
        "graph_max_followers_per_event": LIGHT_SCAN_MAX_FOLLOWERS_PER_EVENT,
    }


def _row_value(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def _wallet_shard(wallet: str, shard_count: int) -> int:
    digest = hashlib.sha1(wallet.lower().encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % shard_count


def _bounded_positive_int(value: Any, *, fallback: int) -> int:
    base = max(1, int(fallback))
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return base
    if parsed <= 0:
        return base
    return max(1, min(parsed, base))


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
