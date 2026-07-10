"""Budgeted historical evidence backfill for newly discovered wallets."""

from __future__ import annotations

import json
import hashlib
import sqlite3
import time
from dataclasses import dataclass
from statistics import median
from typing import Any

from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.orchestration.retry_policy import (
    is_upstream_scheduling_error,
    upstream_aware_retry_at,
)
from pm_robot.pipeline_terms import (
    DEFAULT_EVIDENCE_JOB_STAGE,
    EvidenceJobStage,
    REVIEW_FUNNEL_CANDIDATE_STAGES,
)
from pm_robot.storage.repository import (
    claim_evidence_backfill_job,
    complete_evidence_backfill_job,
    enqueue_evidence_backfill_job,
    finish_ingest_run,
    evidence_backfill_job_summary,
    list_evidence_backfill_targets,
    persist_wallet_activity,
    persist_wallet_positions,
    rebuild_wallet_episodes,
    retry_evidence_backfill_job,
    seed_missing_evidence_backfill_budgets,
    start_ingest_run,
    sync_wallet_processing_state,
    update_evidence_backfill_budget,
    upsert_wallet_evidence_summary,
)


LIGHT_DEPTH = 200
MEDIUM_DEPTH = 1_000
DEEP_DEPTH = 3_000


@dataclass(frozen=True)
class EvidenceBackfillSummary:
    run_id: int
    seeded: int
    wallets_attempted: int
    wallets_succeeded: int
    activity_events_written: int
    positions_written: int
    episodes_rebuilt: int
    stage_updates: dict[str, int]
    status: str
    error: str = ""


@dataclass(frozen=True)
class BackfillPrioritizationSummary:
    wallets_matched: int
    budgets_updated: int
    target_stage: str
    target_depth: int
    min_score: float


@dataclass(frozen=True)
class QueuedBackfillPlanSummary:
    seeded: int
    targets_seen: int
    jobs_enqueued: int
    shard_count: int
    status: str


@dataclass(frozen=True)
class QueuedBackfillWorkerSummary:
    run_id: int
    shard_index: int
    shard_count: int
    jobs_attempted: int
    jobs_succeeded: int
    jobs_failed: int
    activity_events_written: int
    positions_written: int
    episodes_rebuilt: int
    status: str
    error: str = ""


def prioritize_backfill_from_scores(
    conn: sqlite3.Connection,
    *,
    min_score: float = 40.0,
    limit: int = 50,
    target_stage: str = EvidenceJobStage.MEDIUM_PENDING.value,
    target_depth: int = MEDIUM_DEPTH,
    priority: int = 10,
    source: str = "score_priority",
    now: int | None = None,
) -> BackfillPrioritizationSummary:
    ts = now or int(time.time())
    rows = conn.execute(
        f"""
        WITH latest AS (
            SELECT
                address,
                leader_score,
                review_stage,
                ROW_NUMBER() OVER (
                    PARTITION BY address
                    ORDER BY scored_at DESC, score_id DESC
                ) AS rn
            FROM leader_scores
        ),
        activity_counts AS (
            SELECT address, COUNT(activity_id) AS activity_count
            FROM wallet_activity
            GROUP BY address
        )
        SELECT
            cw.address,
            latest.leader_score,
            latest.review_stage,
            COALESCE(ac.activity_count, 0) AS activity_count,
            COALESCE(ebb.stage, '') AS existing_stage
        FROM latest
        JOIN candidate_wallets cw
          ON cw.address = latest.address
        LEFT JOIN activity_counts ac
          ON ac.address = latest.address
        LEFT JOIN evidence_backfill_budget ebb
          ON ebb.wallet = latest.address
        WHERE latest.rn = 1
          AND latest.leader_score >= ?
          AND latest.review_stage IN ({",".join("?" for _ in REVIEW_FUNNEL_CANDIDATE_STAGES)})
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND COALESCE(ac.activity_count, 0) < ?
          AND COALESCE(ebb.stage, '') NOT IN ('medium_done', 'deep_done', 'paused_fast_market_specialist')
        ORDER BY latest.leader_score DESC, COALESCE(ac.activity_count, 0) ASC, cw.updated_at DESC
        LIMIT ?
        """,
        (min_score, *REVIEW_FUNNEL_CANDIDATE_STAGES, target_depth, limit),
    ).fetchall()
    updated = 0
    for row in rows:
        evidence = {
            "leader_score": float(row["leader_score"] or 0.0),
            "review_stage": str(row["review_stage"] or ""),
            "previous_stage": str(row["existing_stage"] or ""),
            "prioritized_at": ts,
        }
        conn.execute(
            """
            INSERT INTO evidence_backfill_budget(
                wallet, source, priority, stage, target_depth, current_depth,
                next_attempt_at, evidence_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                source = CASE
                    WHEN evidence_backfill_budget.source = '' THEN excluded.source
                    WHEN instr(evidence_backfill_budget.source, excluded.source) > 0 THEN evidence_backfill_budget.source
                    ELSE evidence_backfill_budget.source || ' | ' || excluded.source
                END,
                priority = MIN(evidence_backfill_budget.priority, excluded.priority),
                stage = CASE
                    WHEN evidence_backfill_budget.stage IN ('medium_done', 'deep_done', 'paused_fast_market_specialist')
                        THEN evidence_backfill_budget.stage
                    WHEN evidence_backfill_budget.current_depth >= excluded.target_depth
                        THEN evidence_backfill_budget.stage
                    ELSE excluded.stage
                END,
                target_depth = MAX(evidence_backfill_budget.target_depth, excluded.target_depth),
                current_depth = excluded.current_depth,
                next_attempt_at = 0,
                evidence_json = excluded.evidence_json,
                updated_at = excluded.updated_at
            """,
            (
                str(row["address"]).lower(),
                source,
                priority,
                target_stage,
                target_depth,
                int(row["activity_count"] or 0),
                json.dumps(evidence, ensure_ascii=False, sort_keys=True),
                ts,
                ts,
            ),
        )
        updated += 1
    conn.commit()
    return BackfillPrioritizationSummary(
        wallets_matched=len(rows),
        budgets_updated=updated,
        target_stage=target_stage,
        target_depth=target_depth,
        min_score=min_score,
    )


def plan_queued_evidence_backfill(
    conn: sqlite3.Connection,
    *,
    light_limit: int = 30,
    medium_limit: int = 20,
    deep_limit: int = 3,
    shard_count: int = 3,
    now: int | None = None,
) -> QueuedBackfillPlanSummary:
    ts = now or int(time.time())
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    seeded = seed_missing_evidence_backfill_budgets(conn)
    targets = _select_targets(
        conn,
        light_limit=light_limit,
        medium_limit=medium_limit,
        deep_limit=deep_limit,
    )
    enqueued = 0
    for target in targets:
        wallet = str(target["wallet"]).lower()
        enqueued += 1 if enqueue_evidence_backfill_job(
            conn,
            wallet=wallet,
            stage=str(target["stage"]),
            target_depth=int(target["target_depth"] or LIGHT_DEPTH),
            priority=int(target["priority"] or 100),
            shard=_wallet_shard(wallet, shard_count),
            now=ts,
        ) else 0
    conn.commit()
    return QueuedBackfillPlanSummary(
        seeded=seeded,
        targets_seen=len(targets),
        jobs_enqueued=enqueued,
        shard_count=shard_count,
        status="ok",
    )


def run_queued_evidence_backfill_worker(
    conn: sqlite3.Connection,
    *,
    shard_index: int,
    shard_count: int,
    limit: int = 8,
    page_limit: int = 200,
    sleep_seconds: float = 0.02,
    lease_seconds: int = 900,
    max_attempts: int = 3,
    worker_id: str = "",
    client: PublicPolymarketClient | None = None,
) -> QueuedBackfillWorkerSummary:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be in [0, shard_count)")
    client = client or PublicPolymarketClient(conn=conn)
    worker_id = worker_id or f"evidence-worker-{shard_index}-{int(time.time())}"
    run_id = start_ingest_run(conn, f"evidence_backfill_worker_{shard_index}")
    attempted = 0
    succeeded = 0
    failed = 0
    activity_events_written = 0
    positions_written = 0
    episodes_rebuilt = 0
    status = "ok"
    error = ""
    try:
        for idx in range(max(0, limit)):
            if idx > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            job = claim_evidence_backfill_job(
                conn,
                shard_index=shard_index,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
            )
            if job is None:
                break
            attempted += 1
            wallet = str(job["wallet"]).lower()
            target_depth = int(job["target_depth"] or LIGHT_DEPTH)
            try:
                events = _fetch_activity_history(
                    client,
                    wallet,
                    page_limit=page_limit,
                    max_events=target_depth,
                    sleep_seconds=sleep_seconds,
                )
                positions = client.positions(wallet, size_threshold=0.0)
                now = int(time.time())
                activity_events_written += persist_wallet_activity(
                    conn,
                    wallet,
                    events,
                    ingested_at=now,
                    source="evidence_backfill",
                )
                episodes_rebuilt += rebuild_wallet_episodes(conn, wallet)
                positions_written += persist_wallet_positions(conn, wallet, positions, captured_at=now)
                evidence = summarize_wallet_evidence(conn, wallet)
                next_stage, next_depth, stop_reason, next_attempt_at = _classify_next_step(
                    str(job["stage"]),
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
                sync_wallet_processing_state(
                    conn,
                    wallet,
                    evidence,
                    source="evidence_backfill_worker",
                    now=now,
                )
                complete_evidence_backfill_job(conn, job_id=int(job["job_id"]), now=now)
                conn.commit()
                succeeded += 1
            except Exception as exc:
                status = "partial"
                attempts = int(job["attempts"] or 1)
                scheduler_deferred = is_upstream_scheduling_error(exc)
                if not scheduler_deferred:
                    failed += 1
                now = int(time.time())
                retry_at = upstream_aware_retry_at(
                    exc,
                    now=now,
                    attempts=attempts,
                )
                error = f"{wallet}: {exc}"
                retry_evidence_backfill_job(
                    conn,
                    job_id=int(job["job_id"]),
                    error=str(exc),
                    next_attempt_at=retry_at,
                    failed=not scheduler_deferred and attempts >= max_attempts,
                    count_attempt=not scheduler_deferred,
                    now=now,
                )
                conn.commit()
                if scheduler_deferred:
                    break
        return QueuedBackfillWorkerSummary(
            run_id=run_id,
            shard_index=shard_index,
            shard_count=shard_count,
            jobs_attempted=attempted,
            jobs_succeeded=succeeded,
            jobs_failed=failed,
            activity_events_written=activity_events_written,
            positions_written=positions_written,
            episodes_rebuilt=episodes_rebuilt,
            status=status,
            error=error,
        )
    except Exception as exc:
        status = "failed"
        error = str(exc)
        return QueuedBackfillWorkerSummary(
            run_id=run_id,
            shard_index=shard_index,
            shard_count=shard_count,
            jobs_attempted=attempted,
            jobs_succeeded=succeeded,
            jobs_failed=failed,
            activity_events_written=activity_events_written,
            positions_written=positions_written,
            episodes_rebuilt=episodes_rebuilt,
            status=status,
            error=error,
        )
    finally:
        finish_ingest_run(
            conn,
            run_id,
            status=status,
            wallets_attempted=attempted,
            wallets_succeeded=succeeded,
            rows_written=activity_events_written + positions_written,
            error=error,
        )


def queued_evidence_backfill_status(conn: sqlite3.Connection) -> dict[str, Any]:
    return evidence_backfill_job_summary(conn)


def run_evidence_backfill(
    conn: sqlite3.Connection,
    *,
    light_limit: int = 10,
    medium_limit: int = 3,
    deep_limit: int = 1,
    page_limit: int = 100,
    sleep_seconds: float = 0.25,
    client: PublicPolymarketClient | None = None,
) -> EvidenceBackfillSummary:
    client = client or PublicPolymarketClient(conn=conn)
    run_id = start_ingest_run(conn, "evidence_backfill")
    seeded = seed_missing_evidence_backfill_budgets(conn)
    targets = _select_targets(
        conn,
        light_limit=light_limit,
        medium_limit=medium_limit,
        deep_limit=deep_limit,
    )
    attempted = 0
    succeeded = 0
    activity_events_written = 0
    positions_written = 0
    episodes_rebuilt = 0
    stage_updates: dict[str, int] = {}
    status = "ok"
    error = ""
    try:
        for idx, target in enumerate(targets):
            attempted += 1
            if idx > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            wallet = str(target["wallet"])
            target_depth = int(target["target_depth"] or LIGHT_DEPTH)
            try:
                events = _fetch_activity_history(
                    client,
                    wallet,
                    page_limit=page_limit,
                    max_events=target_depth,
                    sleep_seconds=sleep_seconds,
                )
                now = int(time.time())
                activity_events_written += persist_wallet_activity(
                    conn,
                    wallet,
                    events,
                    ingested_at=now,
                    source="evidence_backfill",
                )
                episodes_rebuilt += rebuild_wallet_episodes(conn, wallet)
                positions = client.positions(wallet, size_threshold=0.0)
                positions_written += persist_wallet_positions(conn, wallet, positions, captured_at=now)
                evidence = summarize_wallet_evidence(conn, wallet)
                next_stage, next_depth, stop_reason, next_attempt_at = _classify_next_step(
                    str(target["stage"]),
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
                sync_wallet_processing_state(
                    conn,
                    wallet,
                    evidence,
                    source="evidence_backfill",
                    now=now,
                )
                conn.commit()
                succeeded += 1
                stage_updates[next_stage] = stage_updates.get(next_stage, 0) + 1
            except Exception as exc:
                status = "partial"
                scheduler_deferred = is_upstream_scheduling_error(exc)
                now = int(time.time())
                retry_at = upstream_aware_retry_at(
                    exc,
                    now=now,
                    attempts=int(target["error_count"] or 0) + 1,
                    base_delay_seconds=1_800,
                )
                error = f"{wallet}: {exc}"
                update_evidence_backfill_budget(
                    conn,
                    wallet,
                    stage=str(target["stage"]),
                    target_depth=target_depth,
                    current_depth=int(target["activity_count"] or 0),
                    stop_reason="upstream_scheduler_deferred" if scheduler_deferred else "",
                    error="" if scheduler_deferred else str(exc),
                    next_attempt_at=retry_at,
                    now=now,
                )
                conn.commit()
                if scheduler_deferred:
                    break
        return EvidenceBackfillSummary(
            run_id=run_id,
            seeded=seeded,
            wallets_attempted=attempted,
            wallets_succeeded=succeeded,
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
        return EvidenceBackfillSummary(
            run_id=run_id,
            seeded=seeded,
            wallets_attempted=attempted,
            wallets_succeeded=succeeded,
            activity_events_written=activity_events_written,
            positions_written=positions_written,
            episodes_rebuilt=episodes_rebuilt,
            stage_updates=stage_updates,
            status=status,
            error=error,
        )
    finally:
        finish_ingest_run(
            conn,
            run_id,
            status=status,
            wallets_attempted=attempted,
            wallets_succeeded=succeeded,
            rows_written=activity_events_written + positions_written,
            error=error,
        )


def summarize_wallet_evidence(conn: sqlite3.Connection, wallet: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT timestamp, market_slug, side, usdc_size
        FROM wallet_activity
        WHERE address = ? AND type = 'TRADE'
        ORDER BY timestamp ASC, activity_id ASC
        """,
        (wallet.lower(),),
    ).fetchall()
    markets = [str(row["market_slug"] or "") for row in rows]
    non_empty_markets = [market for market in markets if market]
    fast_flags = [_is_fast_market(market) for market in markets]
    non_fast_markets = [market for market, is_fast in zip(markets, fast_flags) if market and not is_fast]
    timestamps = [int(row["timestamp"] or 0) for row in rows if int(row["timestamp"] or 0) > 0]
    gaps = [b - a for a, b in zip(timestamps, timestamps[1:]) if b >= a]
    buy_count = sum(1 for row in rows if str(row["side"] or "").upper() == "BUY")
    sell_count = sum(1 for row in rows if str(row["side"] or "").upper() == "SELL")
    volume = sum(float(row["usdc_size"] or 0.0) for row in rows)
    activity_count = len(rows)
    fast_count = sum(1 for flag in fast_flags if flag)
    return {
        "activity_count": activity_count,
        "distinct_markets": len(set(non_empty_markets)),
        "non_fast_trade_count": len(non_fast_markets),
        "non_fast_distinct_markets": len(set(non_fast_markets)),
        "fast_market_share": (fast_count / activity_count) if activity_count else 0.0,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "total_usdc_volume": round(volume, 6),
        "median_gap_sec": median(gaps) if gaps else None,
        "oldest_ts": min(timestamps) if timestamps else None,
        "latest_ts": max(timestamps) if timestamps else None,
        "sample_markets": sorted(set(non_empty_markets))[:20],
    }


def _select_targets(
    conn: sqlite3.Connection,
    *,
    light_limit: int,
    medium_limit: int,
    deep_limit: int,
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    if light_limit > 0:
        targets.extend(list_evidence_backfill_targets(conn, stage=DEFAULT_EVIDENCE_JOB_STAGE, limit=light_limit))
    if medium_limit > 0:
        targets.extend(
            list_evidence_backfill_targets(conn, stage=EvidenceJobStage.MEDIUM_PENDING.value, limit=medium_limit)
        )
    if deep_limit > 0:
        targets.extend(
            list_evidence_backfill_targets(conn, stage=EvidenceJobStage.DEEP_PENDING.value, limit=deep_limit)
        )
    return targets


def _fetch_activity_history(
    client: PublicPolymarketClient,
    wallet: str,
    *,
    page_limit: int,
    max_events: int,
    sleep_seconds: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    effective_page_limit = max(1, min(page_limit, max_events))
    while len(out) < max_events:
        batch = client.activity(wallet, limit=effective_page_limit, offset=offset)
        if not batch:
            break
        remaining = max_events - len(out)
        out.extend(batch[:remaining])
        if len(batch) < effective_page_limit:
            break
        offset += effective_page_limit
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return out


def _classify_next_step(
    current_stage: str,
    *,
    target_depth: int,
    evidence: dict[str, Any],
    now: int,
) -> tuple[str, int, str, int]:
    activity_count = int(evidence["activity_count"])
    fast_share = float(evidence["fast_market_share"])
    distinct_markets = int(evidence["distinct_markets"])
    non_fast_trades = int(evidence["non_fast_trade_count"])
    non_fast_markets = int(evidence["non_fast_distinct_markets"])
    if activity_count >= 50 and fast_share >= 0.85:
        return "paused_fast_market_specialist", target_depth, "fast_market_specialist", 0
    if current_stage == EvidenceJobStage.LIGHT_PENDING.value:
        if activity_count < 25:
            return EvidenceJobStage.LIGHT_DONE.value, LIGHT_DEPTH, "insufficient_public_activity_depth", 0
        if distinct_markets >= 3 or non_fast_trades >= 10 or non_fast_markets >= 2:
            return EvidenceJobStage.MEDIUM_PENDING.value, MEDIUM_DEPTH, "", now + 60
        return EvidenceJobStage.LIGHT_DONE.value, LIGHT_DEPTH, "thin_or_low_diversity_activity", 0
    if current_stage == EvidenceJobStage.MEDIUM_PENDING.value:
        if activity_count >= min(target_depth, 300) and distinct_markets >= 10 and non_fast_trades >= 50:
            return EvidenceJobStage.DEEP_PENDING.value, DEEP_DEPTH, "", now + 120
        return EvidenceJobStage.MEDIUM_DONE.value, MEDIUM_DEPTH, "medium_evidence_collected", 0
    if current_stage == EvidenceJobStage.DEEP_PENDING.value:
        return EvidenceJobStage.DEEP_DONE.value, DEEP_DEPTH, "deep_evidence_collected", 0
    return current_stage, target_depth, "", 0


def _is_fast_market(market_slug: str) -> bool:
    value = market_slug.lower()
    return "updown-5m" in value or "btc-up-or-down-5m" in value or value.startswith("btc-updown-5m")


def _wallet_shard(wallet: str, shard_count: int) -> int:
    digest = hashlib.sha1(wallet.lower().encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % shard_count
