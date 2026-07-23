"""Queue-backed light/deep history collection with direct Parquet storage."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pm_robot.clients.polymarket_public import (
    MAX_CLOSED_POSITIONS_LIMIT,
    PublicPolymarketClient,
)
from pm_robot.models import WalletFeatures
from pm_robot.pipeline_terms import PipelineJobType
from pm_robot.orchestration.retry_policy import (
    is_upstream_scheduling_error,
    upstream_aware_retry_at,
)
from pm_robot.research.wallet_history_summary import (
    METHODOLOGY_VERSION,
    WalletHistorySummary,
    summarize_wallet_history,
)
from pm_robot.research.pnl_estimates import PnlEstimate, estimate_wallet_pnl
from pm_robot.storage.repository import (
    claim_pipeline_job,
    complete_pipeline_job,
    enqueue_pipeline_job,
    retry_pipeline_job,
    upsert_wallet_feature,
)
from pm_robot.storage.wallet_history_store import (
    WalletHistoryArtifact,
    discard_uncommitted_wallet_history_artifact,
    persist_wallet_history_artifact,
)
from pm_robot.storage.wallet_levels import get_wallet_level
from pm_robot.wallet_levels import HistoryDepth, WalletLevel


JOB_TYPE = PipelineJobType.WALLET_HISTORY_COLLECT.value
HISTORY_POLICY_VERSION = "v1"
LIGHT_ACTION = f"collect_light_history:{HISTORY_POLICY_VERSION}"
DEEP_ACTION = f"collect_deep_history:{HISTORY_POLICY_VERSION}"
LIGHT_HISTORY_LIMIT = 200
DEEP_HISTORY_LIMIT = 1_000
PAGE_LIMIT = 100
PNL_REFRESH_SECONDS = 86_400
LIGHT_CLOSED_POSITION_LIMIT = MAX_CLOSED_POSITIONS_LIMIT
DEEP_CLOSED_POSITION_LIMIT = 500
DEFAULT_LIGHT_REFRESH_SECONDS = 30 * 86_400
DEFAULT_DEEP_REFRESH_SECONDS = 7 * 86_400
DEFAULT_PRIORITY_AGING_SECONDS = 3_600


@dataclass(frozen=True)
class WalletHistoryPlanSummary:
    targets_seen: int
    jobs_enqueued: int
    active_jobs: int
    max_active_jobs: int
    throttled: bool
    status: str


@dataclass(frozen=True)
class WalletHistoryWorkerSummary:
    jobs_attempted: int
    jobs_succeeded: int
    jobs_failed: int
    jobs_deferred: int
    light_completed: int
    deep_completed: int
    rows_archived: int
    status: str
    error: str = ""


def plan_wallet_history_jobs(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    max_active_jobs: int = 200,
    shard_count: int = 3,
    light_refresh_seconds: int = DEFAULT_LIGHT_REFRESH_SECONDS,
    deep_refresh_seconds: int = DEFAULT_DEEP_REFRESH_SECONDS,
    now: int | None = None,
) -> WalletHistoryPlanSummary:
    """Queue initial evidence and activity-driven refreshes at the allowed depth."""

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
        return WalletHistoryPlanSummary(
            targets_seen=0,
            jobs_enqueued=0,
            active_jobs=active_jobs,
            max_active_jobs=max(0, int(max_active_jobs)),
            throttled=max_active_jobs > 0 and active_jobs >= max_active_jobs,
            status="ok",
        )
    candidate_pool_per_depth = max(slots * 4, slots)
    rows = conn.execute(
        """
        WITH evidence AS (
            SELECT
                levels.wallet,
                levels.level,
                levels.last_seen_at,
                COALESCE(observed.sources, '') AS sources,
                COALESCE(summary.history_depth, '') AS current_depth,
                COALESCE(summary.methodology_version, '') AS current_methodology_version,
                COALESCE(summary.research_score, 0) AS research_score,
                COALESCE(summary.updated_at, 0) AS summary_updated_at,
                CASE
                    WHEN levels.level IN ('l3', 'l4', 'l5', 'l6') THEN 'deep'
                    ELSE 'light'
                END AS target_depth,
                CASE
                    WHEN summary.wallet IS NOT NULL
                         AND COALESCE(summary.methodology_version, '') != ? THEN 1
                    ELSE 0
                END AS methodology_stale
            FROM wallet_levels AS levels
            LEFT JOIN observed_wallets AS observed ON observed.wallet = levels.wallet
            LEFT JOIN wallet_history_summaries AS summary ON summary.wallet = levels.wallet
            WHERE levels.hard_risk_block = 0
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
                    (levels.level = 'l2' AND summary.wallet IS NULL)
                 OR (
                        levels.level = 'l2'
                    AND summary.history_depth = 'light'
                    AND (
                            COALESCE(summary.methodology_version, '') != ?
                         OR (
                                summary.updated_at <= ?
                            AND levels.last_seen_at > summary.updated_at
                         )
                    )
                 )
                 OR (
                        levels.level IN ('l3', 'l4', 'l5', 'l6')
                    AND (
                            summary.wallet IS NULL
                         OR COALESCE(summary.history_depth, '') != 'deep'
                         OR COALESCE(summary.methodology_version, '') != ?
                         OR (
                                summary.updated_at <= ?
                            AND levels.last_seen_at > summary.updated_at
                         )
                    )
                 )
              )
        ), eligible AS (
            SELECT
                evidence.*,
                CASE
                    WHEN methodology_stale = 1 AND level = 'l6' THEN 0
                    WHEN methodology_stale = 1 AND level = 'l5' THEN 1
                    WHEN methodology_stale = 1 AND level = 'l4' THEN 2
                    WHEN methodology_stale = 1 AND level = 'l3' THEN 3
                    WHEN level IN ('l3', 'l4', 'l5', 'l6') AND current_depth != 'deep' THEN 4
                    WHEN methodology_stale = 1 THEN 5
                    WHEN level IN ('l3', 'l4', 'l5', 'l6') THEN 6
                    WHEN current_depth = '' THEN 7
                    ELSE 8
                END AS urgency
            FROM evidence
        ), ranked AS (
            SELECT
                eligible.*,
                ROW_NUMBER() OVER (
                    PARTITION BY target_depth
                    ORDER BY urgency, research_score DESC, last_seen_at DESC, wallet ASC
                ) AS depth_rank
            FROM eligible
        )
        SELECT
            wallet, level, last_seen_at, sources, current_depth,
            current_methodology_version, methodology_stale,
            research_score, summary_updated_at, target_depth, urgency
        FROM ranked
        WHERE depth_rank <= ?
        ORDER BY depth_rank, target_depth, urgency, research_score DESC, wallet ASC
        """,
        (
            METHODOLOGY_VERSION,
            JOB_TYPE,
            METHODOLOGY_VERSION,
            ts - max(0, int(light_refresh_seconds)),
            METHODOLOGY_VERSION,
            ts - max(0, int(deep_refresh_seconds)),
            candidate_pool_per_depth,
        ),
    ).fetchall()
    targets = _fair_targets([dict(row) for row in rows], limit=slots)
    enqueued = 0
    for target in targets:
        wallet = str(target["wallet"])
        depth = (
            HistoryDepth.DEEP
            if target["level"] in {
                WalletLevel.L3.value,
                WalletLevel.L4.value,
                WalletLevel.L5.value,
                WalletLevel.L6.value,
            }
            else HistoryDepth.LIGHT
        )
        action = DEEP_ACTION if depth is HistoryDepth.DEEP else LIGHT_ACTION
        job_action = _history_job_action(target, depth=depth, base_action=action)
        enqueued += int(
            enqueue_pipeline_job(
                conn,
                job_type=JOB_TYPE,
                wallet=wallet,
                job_action=job_action,
                job_scope=depth.value,
                priority=_history_priority(target, depth=depth),
                shard=_wallet_shard(wallet, shard_count),
                input_data={
                    "action": action,
                    "job_action": job_action,
                    "history_depth": depth.value,
                    "methodology_version": METHODOLOGY_VERSION,
                    "refresh_reason": _history_refresh_reason(target, depth=depth),
                    "target_rows": _target_rows(depth),
                    "planned_at": ts,
                },
                max_attempts=3,
                now=ts,
            )
        )
    return WalletHistoryPlanSummary(
        targets_seen=len(targets),
        jobs_enqueued=enqueued,
        active_jobs=active_jobs,
        max_active_jobs=max(0, int(max_active_jobs)),
        throttled=False,
        status="ok",
    )


def run_wallet_history_worker(
    conn: sqlite3.Connection,
    *,
    archive_dir: Path,
    shard_index: int,
    shard_count: int = 3,
    limit: int = 5,
    lease_seconds: int = 900,
    priority_aging_seconds: int = DEFAULT_PRIORITY_AGING_SECONDS,
    sleep_seconds: float = 0.0,
    worker_id: str = "",
    client: PublicPolymarketClient | None = None,
) -> WalletHistoryWorkerSummary:
    """Fetch approved history depth and persist raw rows directly to Parquet."""

    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be in [0, shard_count)")
    client = client or PublicPolymarketClient(conn=conn)
    worker_id = worker_id or f"wallet-history-{shard_index}-{int(time.time())}"
    attempted = 0
    succeeded = 0
    failed = 0
    deferred = 0
    light_completed = 0
    deep_completed = 0
    rows_archived = 0
    error = ""

    for index in range(max(0, int(limit))):
        if index and sleep_seconds > 0:
            time.sleep(sleep_seconds)
        job = claim_pipeline_job(
            conn,
            job_type=JOB_TYPE,
            shard=shard_index,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            priority_aging_seconds=priority_aging_seconds,
        )
        if job is None:
            break
        attempted += 1
        wallet = str(job["wallet"]).lower()
        artifact: WalletHistoryArtifact | None = None
        try:
            depth = HistoryDepth(str(job["job_scope"]))
            level = get_wallet_level(conn, wallet)
            allowed_levels = (
                {WalletLevel.L2}
                if depth is HistoryDepth.LIGHT
                else {WalletLevel.L3, WalletLevel.L4, WalletLevel.L5, WalletLevel.L6}
            )
            if level.level not in allowed_levels or level.hard_risk_block:
                complete_pipeline_job(
                    conn,
                    job_id=int(job["job_id"]),
                    worker_id=worker_id,
                    output_data={"status": "skipped", "level": level.level.value},
                )
                conn.commit()
                succeeded += 1
                continue
            history_rows = _fetch_history(
                client,
                wallet,
                max_rows=_target_rows(depth),
                page_limit=PAGE_LIMIT,
                sleep_seconds=sleep_seconds,
            )
            now = int(time.time())
            pnl, roi = _load_or_refresh_pnl(
                conn,
                client=client,
                wallet=wallet,
                history_depth=depth,
                sleep_seconds=sleep_seconds,
                now=now,
            )
            summary = summarize_wallet_history(
                history_rows,
                history_depth=depth,
                estimated_pnl_usdc=pnl,
                cost_roi_estimate=roi,
                now=now,
            )
            artifact = persist_wallet_history_artifact(
                conn,
                archive_dir=archive_dir,
                wallet=wallet,
                history_depth=depth,
                rows=history_rows,
                now=now,
            )
            _persist_history_summary(conn, wallet=wallet, artifact=artifact, summary=summary, now=now)
            _update_wallet_feature(conn, wallet=wallet, summary=summary, pnl=pnl, now=now)
            completed = complete_pipeline_job(
                conn,
                job_id=int(job["job_id"]),
                worker_id=worker_id,
                output_data={
                    "history_depth": depth.value,
                    "artifact_id": artifact.artifact_id,
                    "row_count": artifact.row_count,
                    "research_score": summary.research_score,
                },
                now=now,
            )
            if not completed:
                raise RuntimeError("wallet history job lease lost")
            conn.commit()
            succeeded += 1
            rows_archived += artifact.row_count
            light_completed += int(depth is HistoryDepth.LIGHT)
            deep_completed += int(depth is HistoryDepth.DEEP)
        except Exception as exc:
            conn.rollback()
            if artifact is not None:
                discard_uncommitted_wallet_history_artifact(
                    conn,
                    archive_dir=archive_dir,
                    artifact=artifact,
                )
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

    return WalletHistoryWorkerSummary(
        jobs_attempted=attempted,
        jobs_succeeded=succeeded,
        jobs_failed=failed,
        jobs_deferred=deferred,
        light_completed=light_completed,
        deep_completed=deep_completed,
        rows_archived=rows_archived,
        status="partial" if failed or deferred else "ok",
        error=error,
    )


def _fetch_history(
    client: PublicPolymarketClient,
    wallet: str,
    *,
    max_rows: int,
    page_limit: int,
    sleep_seconds: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    bounded_page = max(1, min(page_limit, max_rows))
    while len(rows) < max_rows:
        batch = client.activity(wallet, limit=bounded_page, offset=offset)
        if not batch:
            break
        remaining = max_rows - len(rows)
        rows.extend(batch[:remaining])
        if len(batch) < bounded_page:
            break
        offset += bounded_page
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return rows


def _load_or_refresh_pnl(
    conn: sqlite3.Connection,
    *,
    client: PublicPolymarketClient,
    wallet: str,
    history_depth: HistoryDepth,
    sleep_seconds: float,
    now: int,
) -> tuple[float | None, float | None]:
    row = conn.execute(
        "SELECT total_estimated_pnl_usdc, cost_roi_estimate, coverage, captured_at "
        "FROM wallet_pnl_summaries WHERE wallet = ?",
        (wallet,),
    ).fetchone()
    if (
        row
        and int(row["captured_at"] or 0) >= now - PNL_REFRESH_SECONDS
        and _pnl_cache_satisfies(history_depth, str(row["coverage"] or ""))
    ):
        return (
            float(row["total_estimated_pnl_usdc"] or 0.0),
            float(row["cost_roi_estimate"])
            if row["cost_roi_estimate"] is not None
            else None,
        )

    positions = client.positions(wallet, size_threshold=0.0)
    closed, coverage = _fetch_closed_positions(
        client,
        wallet,
        history_depth=history_depth,
        sleep_seconds=sleep_seconds,
    )
    values = client.position_values(wallet)
    estimate = estimate_wallet_pnl(positions, closed)
    current_position_value = sum(
        _float(value.get("value")) for value in values if isinstance(value, dict)
    )
    _persist_pnl_summary(
        conn,
        wallet=wallet,
        estimate=estimate,
        current_position_value=current_position_value,
        coverage=coverage,
        now=now,
    )
    return estimate.total_estimated_pnl_usdc, estimate.cost_roi_estimate


def _persist_pnl_summary(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    estimate: PnlEstimate,
    current_position_value: float,
    coverage: str,
    now: int,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_pnl_summaries(
            wallet, current_position_value_usdc, open_estimated_pnl_usdc,
            closed_realized_pnl_usdc, total_estimated_pnl_usdc,
            capital_basis_usdc, cost_roi_estimate, open_position_count,
            closed_position_count, coverage, methodology_version,
            captured_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            current_position_value_usdc = excluded.current_position_value_usdc,
            open_estimated_pnl_usdc = excluded.open_estimated_pnl_usdc,
            closed_realized_pnl_usdc = excluded.closed_realized_pnl_usdc,
            total_estimated_pnl_usdc = excluded.total_estimated_pnl_usdc,
            capital_basis_usdc = excluded.capital_basis_usdc,
            cost_roi_estimate = excluded.cost_roi_estimate,
            open_position_count = excluded.open_position_count,
            closed_position_count = excluded.closed_position_count,
            coverage = excluded.coverage,
            methodology_version = excluded.methodology_version,
            captured_at = excluded.captured_at,
            updated_at = excluded.updated_at
        """,
        (
            wallet,
            current_position_value,
            estimate.open_estimated_pnl_usdc,
            estimate.closed_realized_pnl_usdc,
            estimate.total_estimated_pnl_usdc,
            estimate.capital_basis_usdc or 0.0,
            estimate.cost_roi_estimate,
            estimate.open_positions_count,
            estimate.closed_positions_count,
            coverage,
            "cost_basis_v2",
            now,
            now,
        ),
    )


def _fetch_closed_positions(
    client: PublicPolymarketClient,
    wallet: str,
    *,
    history_depth: HistoryDepth,
    sleep_seconds: float,
) -> tuple[list[dict[str, Any]], str]:
    """Fetch depth-appropriate closed positions and report bounded coverage."""

    max_rows = (
        DEEP_CLOSED_POSITION_LIMIT
        if history_depth is HistoryDepth.DEEP
        else LIGHT_CLOSED_POSITION_LIMIT
    )
    rows: list[dict[str, Any]] = []
    exhausted = False
    offset = 0
    while len(rows) < max_rows:
        batch = client.closed_positions(
            wallet,
            limit=MAX_CLOSED_POSITIONS_LIMIT,
            offset=offset,
            size_threshold=0.0,
        )
        remaining = max_rows - len(rows)
        rows.extend(batch[:remaining])
        if len(batch) < MAX_CLOSED_POSITIONS_LIMIT:
            exhausted = True
            break
        if len(rows) >= max_rows:
            break
        offset += MAX_CLOSED_POSITIONS_LIMIT
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    if exhausted:
        return rows, "complete"
    if history_depth is HistoryDepth.DEEP:
        return rows, "deep_bounded"
    return rows, "light_bounded"


def _pnl_cache_satisfies(history_depth: HistoryDepth, coverage: str) -> bool:
    if coverage == "complete":
        return True
    if history_depth is HistoryDepth.DEEP:
        return coverage == "deep_bounded"
    return coverage in {"light_bounded", "deep_bounded", "bounded_public_api"}


def _persist_history_summary(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    artifact: WalletHistoryArtifact,
    summary: WalletHistorySummary,
    now: int,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_history_summaries(
            wallet, artifact_id, history_depth, activity_count,
            distinct_markets, non_fast_trade_count, fast_market_share,
            total_volume_usdc, buy_count, sell_count, median_gap_sec,
            trades_per_day, market_volume_top_share, oldest_timestamp,
            latest_timestamp, strategy_tags_json, risk_flags_json,
            research_score, score_components_json, methodology_version,
            computed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            artifact_id = excluded.artifact_id,
            history_depth = excluded.history_depth,
            activity_count = excluded.activity_count,
            distinct_markets = excluded.distinct_markets,
            non_fast_trade_count = excluded.non_fast_trade_count,
            fast_market_share = excluded.fast_market_share,
            total_volume_usdc = excluded.total_volume_usdc,
            buy_count = excluded.buy_count,
            sell_count = excluded.sell_count,
            median_gap_sec = excluded.median_gap_sec,
            trades_per_day = excluded.trades_per_day,
            market_volume_top_share = excluded.market_volume_top_share,
            oldest_timestamp = excluded.oldest_timestamp,
            latest_timestamp = excluded.latest_timestamp,
            strategy_tags_json = excluded.strategy_tags_json,
            risk_flags_json = excluded.risk_flags_json,
            research_score = excluded.research_score,
            score_components_json = excluded.score_components_json,
            methodology_version = excluded.methodology_version,
            computed_at = excluded.computed_at,
            updated_at = excluded.updated_at
        """,
        (
            wallet,
            artifact.artifact_id,
            summary.history_depth.value,
            summary.activity_count,
            summary.distinct_markets,
            summary.non_fast_trade_count,
            summary.fast_market_share,
            summary.total_volume_usdc,
            summary.buy_count,
            summary.sell_count,
            summary.median_gap_sec,
            summary.trades_per_day,
            summary.market_volume_top_share,
            summary.oldest_timestamp,
            summary.latest_timestamp,
            json.dumps(summary.strategy_tags),
            json.dumps(summary.risk_flags),
            summary.research_score,
            json.dumps(summary.score_components, sort_keys=True),
            METHODOLOGY_VERSION,
            now,
            now,
        ),
    )


def _update_wallet_feature(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    summary: WalletHistorySummary,
    pnl: float | None,
    now: int,
) -> None:
    existing = conn.execute(
        "SELECT extra_json FROM wallet_features WHERE address = ?",
        (wallet,),
    ).fetchone()
    try:
        extra = json.loads(existing["extra_json"] or "{}") if existing else {}
    except json.JSONDecodeError:
        extra = {}
    if not isinstance(extra, dict):
        extra = {}
    extra["wallet_history"] = {
        "methodology_version": METHODOLOGY_VERSION,
        "history_depth": summary.history_depth.value,
        "activity_count": summary.activity_count,
        "distinct_markets": summary.distinct_markets,
        "fast_market_share": summary.fast_market_share,
        "market_volume_top_share": summary.market_volume_top_share,
        "strategy_tags": list(summary.strategy_tags),
        "risk_flags": list(summary.risk_flags),
        "research_score": summary.research_score,
        "score_components": summary.score_components,
        "updated_at": now,
    }
    sell_pct = (
        summary.sell_count / max(summary.buy_count + summary.sell_count, 1) * 100.0
    )
    last_active_days = (
        max(0.0, (now - summary.latest_timestamp) / 86_400)
        if summary.latest_timestamp
        else None
    )
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            net_pnl_usdc=pnl,
            total_volume_usdc=summary.total_volume_usdc,
            sell_pct=sell_pct,
            trades_per_day=summary.trades_per_day,
            median_gap_sec=summary.median_gap_sec,
            last_active_days_ago=last_active_days,
            extra=extra,
        ),
    )


def _fair_targets(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    # A methodology rollout is finite and correctness-critical. Refresh the
    # highest research levels first, then resume normal source/depth fairness.
    methodology_refreshes = sorted(
        (row for row in rows if int(row.get("methodology_stale") or 0)),
        key=_methodology_refresh_sort_key,
    )
    selected = methodology_refreshes[:limit]
    selected_wallets = {str(row["wallet"]) for row in selected}
    if len(selected) >= limit:
        return selected

    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row["wallet"]) in selected_wallets:
            continue
        key = f"{_target_depth(row)}:{_source_bucket(str(row.get('sources') or ''))}"
        buckets.setdefault(key, []).append(row)
    names = sorted(buckets)
    while names and len(selected) < limit:
        remaining: list[str] = []
        for name in names:
            bucket = buckets[name]
            if bucket and len(selected) < limit:
                selected.append(bucket.pop(0))
            if bucket:
                remaining.append(name)
        names = remaining
    return selected


def _target_depth(target: dict[str, Any]) -> str:
    if target.get("level") in {
        WalletLevel.L3.value,
        WalletLevel.L4.value,
        WalletLevel.L5.value,
        WalletLevel.L6.value,
    }:
        return HistoryDepth.DEEP.value
    return HistoryDepth.LIGHT.value


def _source_bucket(sources: str) -> str:
    lowered = sources.lower()
    if "leaderboard" in lowered:
        return "leaderboard"
    if "manual" in lowered or "bitget" in lowered:
        return "curated"
    if "polydata" in lowered:
        return "polydata"
    return "stream"


def _history_priority(target: dict[str, Any], *, depth: HistoryDepth) -> int:
    if int(target.get("methodology_stale") or 0):
        return {
            WalletLevel.L6.value: 0,
            WalletLevel.L5.value: 1,
            WalletLevel.L4.value: 2,
            WalletLevel.L3.value: 3,
            WalletLevel.L2.value: 20,
        }.get(str(target.get("level") or ""), 20)
    if depth is HistoryDepth.DEEP and str(target.get("current_depth") or "") != "deep":
        return 5
    base = 10 if depth is HistoryDepth.DEEP else 30
    score = float(target.get("research_score") or 0.0)
    return max(1, base - min(10, int(score // 10)))


def _methodology_refresh_sort_key(target: dict[str, Any]) -> tuple[int, float, int, str]:
    level_rank = {
        WalletLevel.L6.value: 0,
        WalletLevel.L5.value: 1,
        WalletLevel.L4.value: 2,
        WalletLevel.L3.value: 3,
        WalletLevel.L2.value: 4,
    }
    return (
        level_rank.get(str(target.get("level") or ""), 99),
        -float(target.get("research_score") or 0.0),
        -int(target.get("last_seen_at") or 0),
        str(target.get("wallet") or ""),
    )


def _history_refresh_reason(target: dict[str, Any], *, depth: HistoryDepth) -> str:
    if int(target.get("methodology_stale") or 0):
        return "methodology_upgrade"
    if str(target.get("current_depth") or "") != depth.value:
        return "required_depth"
    return "new_activity_after_refresh_window"


def _history_job_action(
    target: dict[str, Any],
    *,
    depth: HistoryDepth,
    base_action: str,
) -> str:
    """Give each completed snapshot one immutable, deduplicated refresh job."""

    if str(target.get("current_depth") or "") != depth.value:
        return base_action
    summary_updated_at = int(target.get("summary_updated_at") or 0)
    if summary_updated_at <= 0:
        return base_action
    return f"{base_action}:refresh:{summary_updated_at}"


def _target_rows(depth: HistoryDepth) -> int:
    return DEEP_HISTORY_LIMIT if depth is HistoryDepth.DEEP else LIGHT_HISTORY_LIMIT


def _wallet_shard(wallet: str, shard_count: int) -> int:
    digest = hashlib.sha256(wallet.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
