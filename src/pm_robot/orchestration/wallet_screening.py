"""Bounded L1 wallet screen using at most ten recent trades.

The screen stores compact summaries only. It may advance L1 to L2, but it never
writes raw activity history or schedules deeper evidence collection.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.pipeline_terms import PipelineJobType
from pm_robot.orchestration.retry_policy import (
    is_upstream_scheduling_error,
    upstream_aware_retry_at,
)
from pm_robot.storage.repository import (
    claim_pipeline_job,
    complete_pipeline_job,
    enqueue_pipeline_job,
    retry_pipeline_job,
)
from pm_robot.storage.wallet_levels import advance_wallet_level, get_wallet_level
from pm_robot.wallet_levels import (
    HistoryDepth,
    RECENT_SAMPLE_TRADE_LIMIT,
    RECENT_SAMPLE_VOLUME_GATE_USDC,
    WalletLevel,
)


JOB_TYPE = PipelineJobType.WALLET_RECENT_SCREEN.value
SCREEN_POLICY_VERSION = "v1"
JOB_ACTION = f"screen_recent:{SCREEN_POLICY_VERSION}"
JOB_SCOPE = HistoryDepth.SAMPLE.value
SAMPLE_TRADE_LIMIT = RECENT_SAMPLE_TRADE_LIMIT
SAMPLE_VOLUME_GATE_USDC = RECENT_SAMPLE_VOLUME_GATE_USDC
DEFAULT_RESCREEN_AFTER_SECONDS = 7 * 86_400


@dataclass(frozen=True)
class WalletScreenPlanSummary:
    targets_seen: int
    jobs_enqueued: int
    active_jobs: int
    max_active_jobs: int
    throttled: bool
    status: str


@dataclass(frozen=True)
class WalletScreenWorkerSummary:
    jobs_attempted: int
    jobs_succeeded: int
    jobs_failed: int
    jobs_deferred: int
    promoted_l2: int
    status: str
    error: str = ""


def plan_wallet_screen_jobs(
    conn: sqlite3.Connection,
    *,
    limit: int = 100,
    max_active_jobs: int = 500,
    shard_count: int = 3,
    rescreen_after_seconds: int = DEFAULT_RESCREEN_AFTER_SECONDS,
    now: int | None = None,
) -> WalletScreenPlanSummary:
    """Queue first screens, plus stale failures that have a newer sighting."""

    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    ts = int(time.time()) if now is None else int(now)
    active_jobs = int(
        conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs "
            "WHERE job_type = ? "
            "AND (status = 'running' OR (status = 'queued' AND attempts < max_attempts))",
            (JOB_TYPE,),
        ).fetchone()[0]
    )
    slots = max(0, int(limit))
    if max_active_jobs > 0:
        slots = min(slots, max(0, int(max_active_jobs) - active_jobs))
    if slots == 0:
        return WalletScreenPlanSummary(
            targets_seen=0,
            jobs_enqueued=0,
            active_jobs=active_jobs,
            max_active_jobs=max(0, int(max_active_jobs)),
            throttled=max_active_jobs > 0 and active_jobs >= max_active_jobs,
            status="ok",
        )

    rows = conn.execute(
        """
        SELECT
            levels.wallet,
            levels.last_seen_at,
            COALESCE(observed.sources, '') AS sources,
            COALESCE(observed.recent_usdc_total, 0) AS observed_usdc,
            COALESCE(observed.recent_trade_count, 0) AS observed_trades,
            COALESCE(screen.updated_at, 0) AS screen_updated_at
        FROM wallet_levels AS levels
        LEFT JOIN observed_wallets AS observed ON observed.wallet = levels.wallet
        LEFT JOIN wallet_screen_summaries AS screen ON screen.wallet = levels.wallet
        WHERE levels.level = 'l1'
          AND levels.hard_risk_block = 0
          AND NOT EXISTS (
                SELECT 1
                FROM pipeline_jobs AS active_job
                WHERE active_job.job_type = ?
                  AND active_job.wallet = levels.wallet
                  AND (
                        active_job.status = 'running'
                     OR (active_job.status = 'queued' AND active_job.attempts < active_job.max_attempts)
                  )
          )
          AND (
                screen.wallet IS NULL
             OR screen.screen_complete = 0
             OR (
                    screen.screen_qualified = 0
                AND screen.updated_at <= ?
                AND levels.last_seen_at > screen.updated_at
             )
          )
        ORDER BY levels.last_seen_at DESC, levels.wallet ASC
        LIMIT ?
        """,
        (
            JOB_TYPE,
            ts - max(0, int(rescreen_after_seconds)),
            max(slots * 4, slots),
        ),
    ).fetchall()
    targets = _fair_targets([dict(row) for row in rows], limit=slots)
    enqueued = 0
    for target in targets:
        wallet = str(target["wallet"])
        job_action = _screen_job_action(target)
        enqueued += int(
            enqueue_pipeline_job(
                conn,
                job_type=JOB_TYPE,
                wallet=wallet,
                job_action=job_action,
                job_scope=JOB_SCOPE,
                priority=_screen_priority(target),
                shard=_wallet_shard(wallet, shard_count),
                input_data={
                    "action": JOB_ACTION,
                    "job_action": job_action,
                    "sample_limit": SAMPLE_TRADE_LIMIT,
                    "planned_at": ts,
                },
                max_attempts=3,
                now=ts,
            )
        )
    return WalletScreenPlanSummary(
        targets_seen=len(targets),
        jobs_enqueued=enqueued,
        active_jobs=active_jobs,
        max_active_jobs=max(0, int(max_active_jobs)),
        throttled=False,
        status="ok",
    )


def run_wallet_screen_worker(
    conn: sqlite3.Connection,
    *,
    shard_index: int,
    shard_count: int = 3,
    limit: int = 20,
    lease_seconds: int = 300,
    worker_id: str = "",
    client: PublicPolymarketClient | None = None,
) -> WalletScreenWorkerSummary:
    """Execute L1 screens and persist only compact screen and PnL summaries."""

    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be in [0, shard_count)")
    client = client or PublicPolymarketClient(conn=conn)
    worker_id = worker_id or f"wallet-screen-{shard_index}-{int(time.time())}"
    attempted = 0
    succeeded = 0
    failed = 0
    deferred = 0
    promoted_l2 = 0
    error = ""

    for _ in range(max(0, int(limit))):
        job = claim_pipeline_job(
            conn,
            job_type=JOB_TYPE,
            shard=shard_index,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )
        if job is None:
            break
        attempted += 1
        wallet = str(job["wallet"]).lower()
        try:
            level = get_wallet_level(conn, wallet)
            if level.level is not WalletLevel.L1 or level.hard_risk_block:
                complete_pipeline_job(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    output_data={"status": "skipped", "level": level.level.value},
                )
                conn.commit()
                succeeded += 1
                continue

            trades = client.wallet_trades(
                wallet,
                limit=SAMPLE_TRADE_LIMIT,
                offset=0,
                taker_only=False,
            )
            now = int(time.time())
            sample = _summarize_trades(trades)
            qualified = (
                sample["trade_count"] > 0
                and sample["volume_usdc"] >= SAMPLE_VOLUME_GATE_USDC
            )
            screen_reason = (
                "sample_volume_at_least_100_usdc"
                if qualified
                else "sample_volume_below_100_usdc"
            )
            _persist_screen(
                conn,
                wallet=wallet,
                sample=sample,
                qualified=qualified,
                reason=screen_reason,
                now=now,
            )
            if qualified:
                decision = advance_wallet_level(
                    conn,
                    wallet,
                    to_level=WalletLevel.L2,
                    reason="recent_sample_volume",
                    policy_version=SCREEN_POLICY_VERSION,
                    facts={
                        "sample_trade_count": sample["trade_count"],
                        "sample_volume_usdc": sample["volume_usdc"],
                        "sample_market_count": sample["market_count"],
                    },
                    now=now,
                )
                promoted_l2 += int(decision.level is WalletLevel.L2)
            completed = complete_pipeline_job(
                conn,
                job_id=int(job["job_id"]),
                worker_id=worker_id,
                output_data={
                    "qualified": qualified,
                    "sample_trade_count": sample["trade_count"],
                    "sample_volume_usdc": sample["volume_usdc"],
                    "level": get_wallet_level(conn, wallet).level.value,
                },
                now=now,
            )
            if not completed:
                raise RuntimeError("wallet screen job lease lost")
            conn.commit()
            succeeded += 1
        except Exception as exc:
            conn.rollback()
            scheduler_deferred = is_upstream_scheduling_error(exc)
            if scheduler_deferred:
                deferred += 1
            else:
                failed += 1
            error = str(exc)
            now = int(time.time())
            retry_pipeline_job(
                conn,
                job_id=int(job["job_id"]),
                worker_id=worker_id,
                error=error,
                next_attempt_at=upstream_aware_retry_at(
                    exc,
                    now=now,
                    attempts=int(job["attempts"] or 1),
                ),
                count_attempt=not scheduler_deferred,
                now=now,
            )
            conn.commit()
            if scheduler_deferred:
                break

    return WalletScreenWorkerSummary(
        jobs_attempted=attempted,
        jobs_succeeded=succeeded,
        jobs_failed=failed,
        jobs_deferred=deferred,
        promoted_l2=promoted_l2,
        status="partial" if failed or deferred else "ok",
        error=error,
    )


def _persist_screen(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    sample: dict[str, Any],
    qualified: bool,
    reason: str,
    now: int,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_screen_summaries(
            wallet, sample_limit, sample_trade_count, sample_volume_usdc,
            sample_market_count, latest_trade_at, screen_complete,
            screen_qualified, screen_reason, source_snapshot_json,
            computed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            sample_limit = excluded.sample_limit,
            sample_trade_count = excluded.sample_trade_count,
            sample_volume_usdc = excluded.sample_volume_usdc,
            sample_market_count = excluded.sample_market_count,
            latest_trade_at = excluded.latest_trade_at,
            screen_complete = 1,
            screen_qualified = excluded.screen_qualified,
            screen_reason = excluded.screen_reason,
            source_snapshot_json = excluded.source_snapshot_json,
            computed_at = excluded.computed_at,
            updated_at = excluded.updated_at
        """,
        (
            wallet,
            SAMPLE_TRADE_LIMIT,
            sample["trade_count"],
            sample["volume_usdc"],
            sample["market_count"],
            sample["latest_trade_at"],
            int(qualified),
            reason,
            json.dumps({"method": "recent_trades", "limit": SAMPLE_TRADE_LIMIT}),
            now,
            now,
        ),
    )


def _summarize_trades(rows: list[dict[str, Any]]) -> dict[str, Any]:
    trades = [row for row in rows[:SAMPLE_TRADE_LIMIT] if isinstance(row, dict)]
    volumes = [_trade_usdc(row) for row in trades]
    markets = {
        str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "")
        for row in trades
    }
    markets.discard("")
    return {
        "trade_count": len(trades),
        "volume_usdc": sum(volumes),
        "market_count": len(markets),
        "latest_trade_at": max((_int(row.get("timestamp")) for row in trades), default=0),
    }


def _trade_usdc(row: dict[str, Any]) -> float:
    explicit = row.get("usdcSize")
    if explicit is None:
        explicit = row.get("usdc_size")
    if explicit is not None:
        return max(0.0, _float(explicit))
    return max(0.0, _float(row.get("size")) * _float(row.get("price")))


def _fair_targets(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(_source_bucket(str(row.get("sources") or "")), []).append(row)
    selected: list[dict[str, Any]] = []
    names = sorted(buckets)
    while names and len(selected) < limit:
        next_names: list[str] = []
        for name in names:
            bucket = buckets[name]
            if bucket and len(selected) < limit:
                selected.append(bucket.pop(0))
            if bucket:
                next_names.append(name)
        names = next_names
    return selected


def _source_bucket(sources: str) -> str:
    lowered = sources.lower()
    if "leaderboard" in lowered:
        return "leaderboard"
    if "manual" in lowered or "bitget" in lowered:
        return "curated"
    if "polydata" in lowered:
        return "polydata"
    return "stream"


def _screen_priority(target: dict[str, Any]) -> int:
    source = _source_bucket(str(target.get("sources") or ""))
    base = {"leaderboard": 10, "curated": 20, "polydata": 25, "stream": 40}[source]
    observed_usdc = _float(target.get("observed_usdc"))
    return max(1, base - min(10, int(observed_usdc // 100)))


def _screen_job_action(target: dict[str, Any]) -> str:
    """Use the previous result timestamp as the next immutable retry generation."""

    screen_updated_at = _int(target.get("screen_updated_at"))
    if screen_updated_at <= 0:
        return JOB_ACTION
    return f"{JOB_ACTION}:refresh:{screen_updated_at}"


def _wallet_shard(wallet: str, shard_count: int) -> int:
    digest = hashlib.sha256(wallet.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0
