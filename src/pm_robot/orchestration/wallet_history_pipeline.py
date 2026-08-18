"""Queue-backed light/deep history collection with direct Parquet storage."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pm_robot.clients.http import HttpClientError
from pm_robot.clients.polymarket_public import (
    MAX_CLOSED_POSITIONS_LIMIT,
    PublicPolymarketClient,
    leaderboard_row_matches_wallet,
)
from pm_robot.models import WalletFeatures
from pm_robot.pipeline_terms import PipelineJobType
from pm_robot.orchestration.retry_policy import (
    is_upstream_scheduling_error,
    upstream_aware_retry_at,
)
from pm_robot.orchestration.wallet_screening import current_screen_is_history_eligible
from pm_robot.research.wallet_history_summary import (
    METHODOLOGY_VERSION,
    WalletHistorySummary,
    summarize_wallet_history,
)
from pm_robot.research.pnl_estimates import PnlEstimate, estimate_wallet_pnl
from pm_robot.storage.db import is_sqlite_locked_error
from pm_robot.storage.repository import (
    PIPELINE_TERMINAL_FAILED_STATUS,
    claim_pipeline_job,
    complete_pipeline_job,
    enqueue_pipeline_job,
    retry_pipeline_job,
    upsert_wallet_feature,
)
from pm_robot.storage.wallet_history_store import (
    WalletHistoryArtifact,
    discard_uncommitted_wallet_history_artifact,
    load_active_wallet_history_artifact,
    persist_wallet_history_artifact,
)
from pm_robot.storage.wallet_history_planner_state import (
    apply_wallet_history_planner_sightings,
    load_wallet_history_planner_level_rows,
    replace_wallet_history_planner_state_rows,
    select_wallet_history_planner_refresh_wallets,
    select_wallet_history_planner_sightings,
    wallet_history_planner_bootstrap_complete,
    wallet_history_planner_dirty_backlog_empty,
)
from pm_robot.storage.wallet_levels import get_wallet_level
from pm_robot.wallet_levels import HistoryDepth, WalletLevel


JOB_TYPE = PipelineJobType.WALLET_HISTORY_COLLECT.value
HISTORY_POLICY_VERSION = METHODOLOGY_VERSION
LIGHT_ACTION = f"collect_light_history:{HISTORY_POLICY_VERSION}"
DEEP_ACTION = f"collect_deep_history:{HISTORY_POLICY_VERSION}"
LIGHT_HISTORY_LIMIT = 200
DEEP_HISTORY_LIMIT = 1_000
PAGE_LIMIT = 100
PNL_REFRESH_SECONDS = 86_400
PNL_INCOMPLETE_REFRESH_SECONDS = 6 * 3_600
PNL_METHODOLOGY_VERSION = "pnl_evidence_v3"
LIGHT_CLOSED_POSITION_LIMIT = MAX_CLOSED_POSITIONS_LIMIT
DEEP_CLOSED_POSITION_LIMIT = 200
DEFAULT_LIGHT_REFRESH_SECONDS = 30 * 86_400
DEFAULT_DEEP_REFRESH_SECONDS = 7 * 86_400
DEFAULT_PRIORITY_AGING_SECONDS = 3_600
L2_DEEP_MIN_ACTIVITY_COUNT = 25
L2_DEEP_MIN_MARKET_COUNT = 2
L2_DEEP_MIN_VOLUME_USDC = 500.0
L2_TRUSTED_SLOW_ACTIVITY_COUNT = 10
L2_TRUSTED_SLOW_MARKET_COUNT = 1
L2_TRUSTED_SLOW_VOLUME_USDC = 100.0
L2_TRUSTED_SLOW_MAX_PER_ROUND = 2
REUSE_ONLY_REFRESH_REASONS = frozenset(
    {"methodology_upgrade", "pnl_evidence_refresh"}
)
_PLAN_REFRESH_LANES = (
    "required_depth",
    "methodology_upgrade",
    "new_activity_after_refresh_window",
    "pnl_evidence_refresh",
)
_PLAN_TARGET_DEPTHS = ("deep", "light")
_PLAN_LEVEL_BATCH_SIZE = 500
_PLAN_SIGHTING_BATCH_SIZE = 5_000


class WalletHistoryTerminalDataQualityError(RuntimeError):
    """Non-transient local history data corruption that retry budget cannot repair."""


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


@dataclass(frozen=True)
class WalletPnlEvidence:
    estimated_pnl_usdc: float | None
    cost_roi_estimate: float | None
    coverage: str
    official_all_pnl_usdc: float | None
    official_all_volume_usdc: float | None
    official_profit_intensity: float | None

    @property
    def rankable_pnl_usdc(self) -> float | None:
        if self.official_all_pnl_usdc is not None:
            return self.official_all_pnl_usdc
        if self.coverage == "complete":
            return self.estimated_pnl_usdc
        return None


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
    """Queue initial evidence, safe rescoring, and activity-driven refreshes."""

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
        # Queue pressure blocks new network work, not state-only sighting coalescing.
        _refresh_wallet_history_planner_sightings(
            conn,
            limit=max(_PLAN_SIGHTING_BATCH_SIZE, max(1, int(limit)) * 8),
            light_refresh_seconds=light_refresh_seconds,
            deep_refresh_seconds=deep_refresh_seconds,
            now=ts,
        )
        commit = getattr(conn, "commit", None)
        if commit is not None:
            commit()
        return WalletHistoryPlanSummary(
            targets_seen=0,
            jobs_enqueued=0,
            active_jobs=active_jobs,
            max_active_jobs=max(0, int(max_active_jobs)),
            throttled=max_active_jobs > 0 and active_jobs >= max_active_jobs,
            status="ok",
        )
    candidate_pool_per_lane = max(slots * 4, slots)
    rows = _select_wallet_history_plan_candidates(
        conn,
        lane_limit=candidate_pool_per_lane,
        light_refresh_seconds=light_refresh_seconds,
        deep_refresh_seconds=deep_refresh_seconds,
        now=ts,
    )
    warming_up = not rows and not _wallet_history_planner_ready_for_selection(conn)
    targets = _limit_l2_trusted_slow_targets(
        _fair_targets([dict(row) for row in rows], limit=len(rows))
    )
    enqueued = 0
    targets_seen = 0
    for target in targets:
        if enqueued >= slots:
            break
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
        refresh_reason = _history_refresh_reason(target, depth=depth)
        job_action = _history_job_action(
            target,
            depth=depth,
            base_action=action,
            refresh_reason=refresh_reason,
        )
        targets_seen += 1
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
                    "refresh_reason": refresh_reason,
                    "target_rows": _target_rows(depth),
                    "planned_at": ts,
                },
                max_attempts=3,
                now=ts,
            )
        )
    if enqueued:
        conn.commit()
    return WalletHistoryPlanSummary(
        targets_seen=targets_seen,
        jobs_enqueued=enqueued,
        active_jobs=active_jobs,
        max_active_jobs=max(0, int(max_active_jobs)),
        throttled=False,
        status="warming_up" if warming_up else "ok",
    )


def _wallet_history_plan_candidate_params(
    *,
    light_refresh_seconds: int = DEFAULT_LIGHT_REFRESH_SECONDS,
    deep_refresh_seconds: int,
    now: int,
) -> tuple[Any, ...]:
    del light_refresh_seconds, deep_refresh_seconds, now
    return ()


def _wallet_history_plan_candidate_scan_sql() -> str:
    return (
        "SELECT wallet, level, last_seen_at, current_depth, "
        "current_methodology_version, methodology_stale, "
        "current_pnl_methodology_version, pnl_captured_at, "
        "pnl_refresh_needed, activity_refresh_needed, research_score, "
        "summary_updated_at, target_depth, refresh_lane, urgency, "
        "sample_trade_count, sample_volume_usdc, sample_market_count "
        "FROM wallet_history_planner_state "
        "WHERE is_eligible = 1"
    )


def _select_wallet_history_plan_candidates(
    conn: sqlite3.Connection,
    *,
    lane_limit: int,
    light_refresh_seconds: int = DEFAULT_LIGHT_REFRESH_SECONDS,
    deep_refresh_seconds: int,
    now: int,
) -> list[dict[str, Any]]:
    """Fetch bounded planner candidates per lane without SQLite temp sorting."""

    bounded_limit = max(0, int(lane_limit))
    if bounded_limit == 0:
        return []
    sighting_limit = max(_PLAN_SIGHTING_BATCH_SIZE, bounded_limit * 8)
    _refresh_wallet_history_planner_sightings(
        conn,
        limit=sighting_limit,
        light_refresh_seconds=light_refresh_seconds,
        deep_refresh_seconds=deep_refresh_seconds,
        now=now,
    )
    _refresh_wallet_history_planner_state(
        conn,
        limit=max(
            _PLAN_LEVEL_BATCH_SIZE,
            bounded_limit * len(_PLAN_TARGET_DEPTHS) * len(_PLAN_REFRESH_LANES),
        ),
        light_refresh_seconds=light_refresh_seconds,
        deep_refresh_seconds=deep_refresh_seconds,
        now=now,
    )
    _refresh_wallet_history_planner_sightings(
        conn,
        limit=sighting_limit,
        light_refresh_seconds=light_refresh_seconds,
        deep_refresh_seconds=deep_refresh_seconds,
        now=now,
    )
    commit = getattr(conn, "commit", None)
    if commit is not None:
        commit()
    if not _wallet_history_planner_ready_for_selection(conn):
        return []
    rows = _select_wallet_history_plan_state_lanes(conn, lane_limit=bounded_limit)
    _attach_wallet_history_plan_sources(conn, rows)

    return _rank_wallet_history_plan_candidates(rows)


def _wallet_history_planner_ready_for_selection(conn: sqlite3.Connection) -> bool:
    return (
        wallet_history_planner_bootstrap_complete(conn)
        and wallet_history_planner_dirty_backlog_empty(conn)
    )


def _refresh_wallet_history_planner_sightings(
    conn: sqlite3.Connection,
    *,
    limit: int,
    light_refresh_seconds: int,
    deep_refresh_seconds: int,
    now: int,
) -> int:
    """Coalesce activity timestamps without touching evidence side tables."""

    claims = select_wallet_history_planner_sightings(conn, limit=limit)
    if not claims:
        return 0
    updates: list[tuple[int, int, str]] = []
    for claim in claims:
        target_depth = (
            HistoryDepth.DEEP.value
            if claim.level in {"l3", "l4", "l5", "l6"}
            else HistoryDepth.LIGHT.value
        )
        next_refresh_at = int(claim.next_refresh_at)
        if (
            claim.current_depth == target_depth
            and claim.last_seen_at > claim.summary_updated_at
        ):
            refresh_seconds = (
                deep_refresh_seconds
                if target_depth == HistoryDepth.DEEP.value
                else light_refresh_seconds
            )
            activity_due_at = claim.summary_updated_at + max(
                0, int(refresh_seconds)
            )
            if activity_due_at > 0:
                next_refresh_at = _earliest_positive_timestamp(
                    next_refresh_at,
                    activity_due_at,
                )
        updates.append(
            (claim.last_seen_at, next_refresh_at, claim.wallet)
        )
    apply_wallet_history_planner_sightings(
        conn,
        updates=updates,
        claims=claims,
    )
    return len(claims)


def _earliest_positive_timestamp(current: int, candidate: int) -> int:
    positive = [value for value in (int(current), int(candidate)) if value > 0]
    return min(positive) if positive else 0


def _refresh_wallet_history_planner_state(
    conn: sqlite3.Connection,
    *,
    limit: int,
    light_refresh_seconds: int,
    deep_refresh_seconds: int,
    now: int,
) -> int:
    claims = select_wallet_history_planner_refresh_wallets(
        conn,
        limit=limit,
        now=now,
    )
    if not claims:
        return 0
    wallets = [claim.wallet for claim in claims]
    light_refresh_before = int(now) - max(0, int(light_refresh_seconds))
    deep_refresh_before = int(now) - max(0, int(deep_refresh_seconds))
    pnl_refresh_before = int(now) - PNL_INCOMPLETE_REFRESH_SECONDS
    state_rows: list[dict[str, Any]] = []
    relevant_levels = {"l2", "l3", "l4", "l5", "l6"}
    for chunk in _wallet_history_plan_wallet_chunks(
        wallets,
        limit=min(_PLAN_LEVEL_BATCH_SIZE, _sqlite_variable_limit(conn)),
    ):
        level_rows = load_wallet_history_planner_level_rows(conn, wallets=chunk)
        level_rows = [
            row
            for row in level_rows
            if str(row["level"] or "") in relevant_levels
        ]
        level_wallets = [str(row["wallet"]) for row in level_rows]
        snapshots = _load_wallet_history_plan_snapshots(
            conn,
            wallets=level_wallets,
            now=now,
        )
        state_rows.extend(
            _wallet_history_planner_state_row(
                row,
                snapshots=snapshots,
                light_refresh_before=light_refresh_before,
                deep_refresh_before=deep_refresh_before,
                pnl_refresh_before=pnl_refresh_before,
                light_refresh_seconds=light_refresh_seconds,
                deep_refresh_seconds=deep_refresh_seconds,
                now=now,
                refreshed_at=now,
            )
            for row in level_rows
        )
    replace_wallet_history_planner_state_rows(
        conn,
        claims=claims,
        rows=state_rows,
    )
    return len(wallets)


def _wallet_history_planner_state_row(
    level_row: Any,
    *,
    snapshots: dict[str, Any],
    light_refresh_before: int,
    deep_refresh_before: int,
    pnl_refresh_before: int,
    light_refresh_seconds: int,
    deep_refresh_seconds: int,
    now: int,
    refreshed_at: int,
) -> dict[str, Any]:
    wallet = str(level_row["wallet"])
    level = str(level_row["level"] or "")
    hard_risk_block = int(level_row["hard_risk_block"] or 0)
    last_seen_at = int(level_row["last_seen_at"] or 0)
    target_depth = (
        HistoryDepth.DEEP.value
        if level in {"l3", "l4", "l5", "l6"}
        else HistoryDepth.LIGHT.value
    )
    summary = snapshots["summary"].get(wallet)
    screen = snapshots["screen"].get(wallet, {})
    pnl = snapshots["pnl"].get(wallet)
    current_depth = str(summary["current_depth"]) if summary is not None else ""
    current_methodology_version = (
        str(summary["current_methodology_version"]) if summary is not None else ""
    )
    research_score = float(summary["research_score"]) if summary is not None else 0.0
    summary_updated_at = (
        int(summary["summary_updated_at"]) if summary is not None else 0
    )
    current_pnl_methodology_version = (
        str(pnl["current_pnl_methodology_version"]) if pnl is not None else ""
    )
    pnl_captured_at = int(pnl["pnl_captured_at"]) if pnl is not None else 0
    methodology_stale = int(
        summary is not None and current_methodology_version != METHODOLOGY_VERSION
    )
    pnl_refresh_needed = int(
        level in {"l4", "l5", "l6"}
        and current_methodology_version == METHODOLOGY_VERSION
        and (
            pnl is None
            or current_pnl_methodology_version != PNL_METHODOLOGY_VERSION
            or (
                pnl["official_all_pnl_usdc"] is None
                and pnl_captured_at <= pnl_refresh_before
            )
        )
    )
    activity_refresh_needed = int(
        (
            level == "l2"
            and current_depth == "light"
            and summary_updated_at <= light_refresh_before
            and last_seen_at > summary_updated_at
        )
        or (
            level in {"l3", "l4", "l5", "l6"}
            and current_depth == "deep"
            and summary_updated_at <= deep_refresh_before
            and last_seen_at > summary_updated_at
        )
    )
    refresh_lane = _wallet_history_plan_refresh_lane(
        current_depth=current_depth,
        target_depth=target_depth,
        methodology_stale=methodology_stale,
        activity_refresh_needed=activity_refresh_needed,
        pnl_refresh_needed=pnl_refresh_needed,
    )
    urgency = _wallet_history_plan_urgency(
        level=level,
        current_depth=current_depth,
        methodology_stale=methodology_stale,
        pnl_refresh_needed=pnl_refresh_needed,
    )
    eligible = int(
        hard_risk_block == 0
        and level in {"l2", "l3", "l4", "l5", "l6"}
        and _wallet_history_plan_candidate_from_snapshots(
            level_row,
            snapshots=snapshots,
            light_refresh_before=light_refresh_before,
            deep_refresh_before=deep_refresh_before,
            pnl_refresh_before=pnl_refresh_before,
        )
        is not None
    )
    return {
        "wallet": wallet,
        "level": level,
        "hard_risk_block": hard_risk_block,
        "last_seen_at": last_seen_at,
        "current_depth": current_depth,
        "current_methodology_version": current_methodology_version,
        "methodology_stale": methodology_stale,
        "current_pnl_methodology_version": current_pnl_methodology_version,
        "pnl_captured_at": pnl_captured_at,
        "pnl_refresh_needed": pnl_refresh_needed,
        "activity_refresh_needed": activity_refresh_needed,
        "research_score": research_score,
        "summary_updated_at": summary_updated_at,
        "sample_trade_count": int(screen.get("sample_trade_count", 0) or 0),
        "sample_volume_usdc": float(screen.get("sample_volume_usdc", 0.0) or 0.0),
        "sample_market_count": int(screen.get("sample_market_count", 0) or 0),
        "target_depth": target_depth,
        "refresh_lane": refresh_lane if eligible else "",
        "urgency": urgency,
        "is_eligible": eligible,
        "next_refresh_at": _wallet_history_plan_next_refresh_at(
            snapshots["jobs"]["deferred_actions"],
            wallet=wallet,
            level=level,
            target_depth=target_depth,
            current_depth=current_depth,
            current_methodology_version=current_methodology_version,
            current_pnl_methodology_version=current_pnl_methodology_version,
            pnl_official_all_pnl_usdc=(
                pnl["official_all_pnl_usdc"] if pnl is not None else None
            ),
            summary_updated_at=summary_updated_at,
            pnl_captured_at=pnl_captured_at,
            last_seen_at=last_seen_at,
            light_refresh_seconds=light_refresh_seconds,
            deep_refresh_seconds=deep_refresh_seconds,
            now=now,
        ),
        "refreshed_at": int(refreshed_at),
    }


def _select_wallet_history_plan_state_lanes(
    conn: sqlite3.Connection,
    *,
    lane_limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for target_depth in _PLAN_TARGET_DEPTHS:
        for refresh_lane in _PLAN_REFRESH_LANES:
            rows.extend(
                dict(row)
                for row in conn.execute(
                    _wallet_history_plan_lane_sql(),
                    (target_depth, refresh_lane, max(0, int(lane_limit))),
                )
            )
    return rows


def _wallet_history_plan_lane_sql() -> str:
    return (
        "SELECT wallet, level, last_seen_at, current_depth, "
        "current_methodology_version, methodology_stale, "
        "current_pnl_methodology_version, pnl_captured_at, "
        "pnl_refresh_needed, activity_refresh_needed, research_score, "
        "summary_updated_at, target_depth, refresh_lane, urgency, "
        "sample_trade_count, sample_volume_usdc, sample_market_count "
        "FROM wallet_history_planner_state "
        "WHERE target_depth = ? AND refresh_lane = ? AND is_eligible = 1 "
        "ORDER BY urgency ASC, research_score DESC, sample_market_count DESC, "
        "sample_volume_usdc DESC, sample_trade_count DESC, last_seen_at DESC, "
        "wallet ASC "
        "LIMIT ?"
    )


def _iter_wallet_history_plan_candidate_batches(
    conn: sqlite3.Connection,
    level_rows: Any,
    *,
    light_refresh_seconds: int,
    deep_refresh_seconds: int,
    now: int,
) -> Any:
    batch_limit = min(_PLAN_LEVEL_BATCH_SIZE, _sqlite_variable_limit(conn))
    batch: list[Any] = []
    for row in level_rows:
        batch.append(row)
        if len(batch) >= batch_limit:
            yield from _iter_wallet_history_plan_candidates(
                batch,
                snapshots=_load_wallet_history_plan_snapshots(
                    conn,
                    wallets=[str(item["wallet"]) for item in batch],
                    now=now,
                ),
                light_refresh_seconds=light_refresh_seconds,
                deep_refresh_seconds=deep_refresh_seconds,
                now=now,
            )
            batch = []
    if batch:
        yield from _iter_wallet_history_plan_candidates(
            batch,
            snapshots=_load_wallet_history_plan_snapshots(
                conn,
                wallets=[str(item["wallet"]) for item in batch],
                now=now,
            ),
            light_refresh_seconds=light_refresh_seconds,
            deep_refresh_seconds=deep_refresh_seconds,
            now=now,
        )


def _load_wallet_history_plan_snapshots(
    conn: sqlite3.Connection,
    *,
    wallets: list[str],
    now: int,
) -> dict[str, Any]:
    return {
        "screen": _wallet_history_plan_screen_snapshot(conn, wallets=wallets),
        "summary": _wallet_history_plan_summary_snapshot(conn, wallets=wallets),
        "pnl": _wallet_history_plan_pnl_snapshot(conn, wallets=wallets),
        "jobs": _wallet_history_plan_job_snapshot(conn, wallets=wallets, now=now),
    }


def _wallet_history_plan_wallet_filter(wallets: list[str]) -> tuple[str, tuple[str, ...]]:
    placeholders = ", ".join("?" for _ in wallets)
    return placeholders, tuple(wallets)


def _wallet_history_plan_screen_snapshot(
    conn: sqlite3.Connection,
    *,
    wallets: list[str],
) -> dict[str, dict[str, Any]]:
    if not wallets:
        return {}
    placeholders, params = _wallet_history_plan_wallet_filter(wallets)
    return {
        str(row["wallet"]): {
            "sample_trade_count": int(row["sample_trade_count"] or 0),
            "sample_volume_usdc": float(row["sample_volume_usdc"] or 0.0),
            "sample_market_count": int(row["sample_market_count"] or 0),
            "screen_complete": int(row["screen_complete"] or 0),
            "screen_qualified": int(row["screen_qualified"] or 0),
            "source_snapshot_json": str(row["source_snapshot_json"] or "{}"),
        }
        for row in conn.execute(
            """
            SELECT wallet,
                   COALESCE(sample_trade_count, 0) AS sample_trade_count,
                   COALESCE(sample_volume_usdc, 0) AS sample_volume_usdc,
                   COALESCE(sample_market_count, 0) AS sample_market_count,
                   COALESCE(screen_complete, 0) AS screen_complete,
                   COALESCE(screen_qualified, 0) AS screen_qualified,
                   COALESCE(source_snapshot_json, '{{}}') AS source_snapshot_json
            FROM wallet_screen_summaries
            WHERE wallet IN ({placeholders})
            """
            .format(placeholders=placeholders),
            params,
        )
    }


def _wallet_history_plan_summary_snapshot(
    conn: sqlite3.Connection,
    *,
    wallets: list[str],
) -> dict[str, dict[str, Any]]:
    if not wallets:
        return {}
    placeholders, params = _wallet_history_plan_wallet_filter(wallets)
    return {
        str(row["wallet"]): {
            "current_depth": str(row["history_depth"] or ""),
            "current_methodology_version": str(row["methodology_version"] or ""),
            "research_score": float(row["research_score"] or 0.0),
            "summary_updated_at": int(row["updated_at"] or 0),
        }
        for row in conn.execute(
            """
            SELECT wallet, history_depth, methodology_version,
                   COALESCE(research_score, 0) AS research_score,
                   COALESCE(updated_at, 0) AS updated_at
            FROM wallet_history_summaries
            WHERE wallet IN ({placeholders})
            """
            .format(placeholders=placeholders),
            params,
        )
    }


def _wallet_history_plan_pnl_snapshot(
    conn: sqlite3.Connection,
    *,
    wallets: list[str],
) -> dict[str, dict[str, Any]]:
    if not wallets:
        return {}
    placeholders, params = _wallet_history_plan_wallet_filter(wallets)
    return {
        str(row["wallet"]): {
            "current_pnl_methodology_version": str(row["methodology_version"] or ""),
            "pnl_captured_at": int(row["captured_at"] or 0),
            "official_all_pnl_usdc": row["official_all_pnl_usdc"],
        }
        for row in conn.execute(
            """
            SELECT wallet, methodology_version, COALESCE(captured_at, 0) AS captured_at,
                   official_all_pnl_usdc
            FROM wallet_pnl_summaries
            WHERE wallet IN ({placeholders})
            """
            .format(placeholders=placeholders),
            params,
        )
    }


def _wallet_history_plan_job_snapshot(
    conn: sqlite3.Connection,
    *,
    wallets: list[str],
    now: int,
) -> dict[str, Any]:
    active_wallets: set[str] = set()
    terminal_scopes: set[tuple[str, str]] = set()
    deferred_actions: dict[tuple[str, str, str], int] = {}
    if not wallets:
        return {
            "active_wallets": active_wallets,
            "terminal_scopes": terminal_scopes,
            "deferred_actions": deferred_actions,
        }
    for chunk in _wallet_history_plan_wallet_chunks(
        wallets,
        limit=max(1, _sqlite_variable_limit(conn) - 3),
    ):
        placeholders, wallet_params = _wallet_history_plan_wallet_filter(chunk)
        for row in conn.execute(
            f"""
            SELECT wallet, job_scope, job_action, status, attempts, max_attempts,
                   next_attempt_at
            FROM pipeline_jobs
            WHERE job_type = ?
              AND wallet IN ({placeholders})
              AND (
                    status IN ('running', 'queued', ?)
                 OR (status = 'failed' AND next_attempt_at > ?)
              )
            """,
            (JOB_TYPE, *wallet_params, PIPELINE_TERMINAL_FAILED_STATUS, int(now)),
        ):
            status = str(row["status"] or "")
            wallet = str(row["wallet"])
            if status == "running" or (
                status == "queued"
                and int(row["attempts"] or 0) < int(row["max_attempts"] or 0)
            ):
                active_wallets.add(wallet)
            elif status == PIPELINE_TERMINAL_FAILED_STATUS:
                terminal_scopes.add((wallet, str(row["job_scope"] or "")))
            elif status == "failed":
                deferred_actions[
                    (
                        wallet,
                        str(row["job_scope"] or ""),
                        str(row["job_action"] or ""),
                    )
                ] = int(row["next_attempt_at"] or 0)

    return {
        "active_wallets": active_wallets,
        "terminal_scopes": terminal_scopes,
        "deferred_actions": deferred_actions,
    }


def _iter_wallet_history_plan_candidates(
    level_rows: Any,
    *,
    snapshots: dict[str, Any],
    light_refresh_seconds: int,
    deep_refresh_seconds: int,
    now: int,
) -> Any:
    light_refresh_before = int(now) - max(0, int(light_refresh_seconds))
    deep_refresh_before = int(now) - max(0, int(deep_refresh_seconds))
    pnl_refresh_before = int(now) - PNL_INCOMPLETE_REFRESH_SECONDS
    for row in level_rows:
        candidate = _wallet_history_plan_candidate_from_snapshots(
            row,
            snapshots=snapshots,
            light_refresh_before=light_refresh_before,
            deep_refresh_before=deep_refresh_before,
            pnl_refresh_before=pnl_refresh_before,
        )
        if candidate is not None:
            yield candidate


def _wallet_history_plan_candidate_from_snapshots(
    level_row: Any,
    *,
    snapshots: dict[str, Any],
    light_refresh_before: int,
    deep_refresh_before: int,
    pnl_refresh_before: int,
) -> dict[str, Any] | None:
    wallet = str(level_row["wallet"])
    level = str(level_row["level"] or "")
    last_seen_at = int(level_row["last_seen_at"] or 0)
    target_depth = (
        HistoryDepth.DEEP.value
        if level in {"l3", "l4", "l5", "l6"}
        else HistoryDepth.LIGHT.value
    )
    jobs = snapshots["jobs"]
    if wallet in jobs["active_wallets"]:
        return None
    if (wallet, target_depth) in jobs["terminal_scopes"]:
        return None

    summary = snapshots["summary"].get(wallet)
    screen = snapshots["screen"].get(wallet, {})
    if level == "l2" and not current_screen_is_history_eligible(screen):
        return None
    pnl = snapshots["pnl"].get(wallet)
    current_depth = str(summary["current_depth"]) if summary is not None else ""
    current_methodology_version = (
        str(summary["current_methodology_version"]) if summary is not None else ""
    )
    research_score = (
        float(summary["research_score"]) if summary is not None else 0.0
    )
    summary_updated_at = (
        int(summary["summary_updated_at"]) if summary is not None else 0
    )
    current_pnl_methodology_version = (
        str(pnl["current_pnl_methodology_version"]) if pnl is not None else ""
    )
    pnl_captured_at = int(pnl["pnl_captured_at"]) if pnl is not None else 0
    methodology_stale = int(
        summary is not None and current_methodology_version != METHODOLOGY_VERSION
    )
    pnl_refresh_needed = int(
        level in {"l4", "l5", "l6"}
        and current_methodology_version == METHODOLOGY_VERSION
        and (
            pnl is None
            or current_pnl_methodology_version != PNL_METHODOLOGY_VERSION
            or (
                pnl["official_all_pnl_usdc"] is None
                and pnl_captured_at <= pnl_refresh_before
            )
        )
    )
    activity_refresh_needed = int(
        (
            level == "l2"
            and current_depth == "light"
            and summary_updated_at <= light_refresh_before
            and last_seen_at > summary_updated_at
        )
        or (
            level in {"l3", "l4", "l5", "l6"}
            and current_depth == "deep"
            and summary_updated_at <= deep_refresh_before
            and last_seen_at > summary_updated_at
        )
    )

    if _wallet_history_plan_deferred_failure_blocks(
        jobs["deferred_actions"],
        wallet=wallet,
        target_depth=target_depth,
        summary_updated_at=summary_updated_at,
        pnl_captured_at=pnl_captured_at,
    ):
        return None
    if not _wallet_history_plan_candidate_is_eligible(
        level=level,
        summary_exists=summary is not None,
        current_depth=current_depth,
        current_methodology_version=current_methodology_version,
        methodology_stale=methodology_stale,
        activity_refresh_needed=activity_refresh_needed,
        pnl_refresh_needed=pnl_refresh_needed,
    ):
        return None

    refresh_lane = _wallet_history_plan_refresh_lane(
        current_depth=current_depth,
        target_depth=target_depth,
        methodology_stale=methodology_stale,
        activity_refresh_needed=activity_refresh_needed,
        pnl_refresh_needed=pnl_refresh_needed,
    )
    return {
        "wallet": wallet,
        "level": level,
        "last_seen_at": last_seen_at,
        "current_depth": current_depth,
        "current_methodology_version": current_methodology_version,
        "methodology_stale": methodology_stale,
        "current_pnl_methodology_version": current_pnl_methodology_version,
        "pnl_captured_at": pnl_captured_at,
        "pnl_refresh_needed": pnl_refresh_needed,
        "activity_refresh_needed": activity_refresh_needed,
        "research_score": research_score,
        "summary_updated_at": summary_updated_at,
        "target_depth": target_depth,
        "refresh_lane": refresh_lane,
        "urgency": _wallet_history_plan_urgency(
            level=level,
            current_depth=current_depth,
            methodology_stale=methodology_stale,
            pnl_refresh_needed=pnl_refresh_needed,
        ),
        "sample_trade_count": int(screen.get("sample_trade_count", 0) or 0),
        "sample_volume_usdc": float(screen.get("sample_volume_usdc", 0.0) or 0.0),
        "sample_market_count": int(screen.get("sample_market_count", 0) or 0),
    }


def _wallet_history_plan_deferred_failure_blocks(
    deferred_actions: dict[tuple[str, str, str], int],
    *,
    wallet: str,
    target_depth: str,
    summary_updated_at: int,
    pnl_captured_at: int,
) -> bool:
    base_action = (
        DEEP_ACTION if target_depth == HistoryDepth.DEEP.value else LIGHT_ACTION
    )
    pnl_marker = pnl_captured_at if pnl_captured_at else summary_updated_at
    blocked_actions = {
        f"{base_action}:refresh:{summary_updated_at}",
        f"{base_action}:pnl:{pnl_marker}",
    }
    return any(
        (wallet, target_depth, action) in deferred_actions for action in blocked_actions
    )


def _wallet_history_plan_next_refresh_at(
    deferred_actions: dict[tuple[str, str, str], int],
    *,
    wallet: str,
    level: str,
    target_depth: str,
    current_depth: str,
    current_methodology_version: str,
    current_pnl_methodology_version: str,
    pnl_official_all_pnl_usdc: Any,
    summary_updated_at: int,
    pnl_captured_at: int,
    last_seen_at: int,
    light_refresh_seconds: int,
    deep_refresh_seconds: int,
    now: int,
) -> int:
    future_times: list[int] = []
    base_action = (
        DEEP_ACTION if target_depth == HistoryDepth.DEEP.value else LIGHT_ACTION
    )
    pnl_marker = pnl_captured_at if pnl_captured_at else summary_updated_at
    for action in (
        f"{base_action}:refresh:{summary_updated_at}",
        f"{base_action}:pnl:{pnl_marker}",
    ):
        unblock_at = int(deferred_actions.get((wallet, target_depth, action), 0))
        if unblock_at > int(now):
            future_times.append(unblock_at)

    if last_seen_at > summary_updated_at and current_depth == target_depth:
        if level == "l2" and target_depth == HistoryDepth.LIGHT.value:
            due_at = summary_updated_at + max(0, int(light_refresh_seconds))
            if due_at > int(now):
                future_times.append(due_at)
        elif level in {"l3", "l4", "l5", "l6"} and target_depth == HistoryDepth.DEEP.value:
            due_at = summary_updated_at + max(0, int(deep_refresh_seconds))
            if due_at > int(now):
                future_times.append(due_at)

    if (
        level in {"l4", "l5", "l6"}
        and current_methodology_version == METHODOLOGY_VERSION
        and current_pnl_methodology_version == PNL_METHODOLOGY_VERSION
        and pnl_official_all_pnl_usdc is None
        and pnl_captured_at > 0
    ):
        due_at = pnl_captured_at + PNL_INCOMPLETE_REFRESH_SECONDS
        if due_at > int(now):
            future_times.append(due_at)
    return min(future_times) if future_times else 0


def _wallet_history_plan_candidate_is_eligible(
    *,
    level: str,
    summary_exists: bool,
    current_depth: str,
    current_methodology_version: str,
    methodology_stale: int,
    activity_refresh_needed: int,
    pnl_refresh_needed: int,
) -> bool:
    if level == "l2" and not summary_exists:
        return True
    if level == "l2" and current_depth == "light":
        return bool(methodology_stale or activity_refresh_needed)
    if level in {"l3", "l4", "l5", "l6"} and (
        not summary_exists
        or current_depth != "deep"
        or current_methodology_version != METHODOLOGY_VERSION
        or activity_refresh_needed
    ):
        return True
    return bool(
        level in {"l4", "l5", "l6"}
        and current_methodology_version == METHODOLOGY_VERSION
        and pnl_refresh_needed
    )


def _wallet_history_plan_refresh_lane(
    *,
    current_depth: str,
    target_depth: str,
    methodology_stale: int,
    activity_refresh_needed: int,
    pnl_refresh_needed: int,
) -> str:
    if current_depth != target_depth:
        return "required_depth"
    if methodology_stale:
        return "methodology_upgrade"
    if activity_refresh_needed:
        return "new_activity_after_refresh_window"
    if pnl_refresh_needed:
        return "pnl_evidence_refresh"
    return "new_activity_after_refresh_window"


def _wallet_history_plan_urgency(
    *,
    level: str,
    current_depth: str,
    methodology_stale: int,
    pnl_refresh_needed: int,
) -> int:
    if methodology_stale and level == "l6":
        return 0
    if methodology_stale and level == "l5":
        return 1
    if methodology_stale and level == "l4":
        return 2
    if methodology_stale and level == "l3":
        return 3
    if pnl_refresh_needed and level == "l6":
        return 4
    if pnl_refresh_needed and level == "l5":
        return 5
    if pnl_refresh_needed and level == "l4":
        return 6
    if level in {"l3", "l4", "l5", "l6"} and current_depth != "deep":
        return 7
    if methodology_stale:
        return 8
    if level in {"l3", "l4", "l5", "l6"}:
        return 9
    if current_depth == "":
        return 10
    return 11


def _bounded_wallet_history_plan_lanes(
    rows: Any,
    *,
    lane_limit: int,
) -> list[dict[str, Any]]:
    lanes: dict[tuple[str, str], list[dict[str, Any]]] = {}
    trim_threshold = max(1, int(lane_limit)) * 2
    for row in rows:
        item = dict(row)
        lane_key = (str(item["target_depth"]), str(item["refresh_lane"]))
        lane = lanes.setdefault(lane_key, [])
        lane.append(item)
        if len(lane) > trim_threshold:
            lane.sort(key=_wallet_history_plan_lane_sort_key)
            del lane[lane_limit:]

    bounded: list[dict[str, Any]] = []
    for lane in lanes.values():
        lane.sort(key=_wallet_history_plan_lane_sort_key)
        bounded.extend(lane[:lane_limit])
    return bounded


def _attach_wallet_history_plan_sources(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
) -> None:
    """Attach observed wallet sources after bounded planner candidate selection."""

    if not rows:
        return
    wallets = list(dict.fromkeys(str(row["wallet"]) for row in rows))
    sources_by_wallet: dict[str, str] = {}
    for chunk in _wallet_history_plan_wallet_chunks(
        wallets,
        limit=_sqlite_variable_limit(conn),
    ):
        placeholders = ", ".join("?" for _ in chunk)
        for row in conn.execute(
            "SELECT wallet, COALESCE(sources, '') AS sources "
            f"FROM observed_wallets WHERE wallet IN ({placeholders})",
            tuple(chunk),
        ):
            sources_by_wallet[str(row["wallet"])] = str(row["sources"] or "")
    for row in rows:
        row["sources"] = sources_by_wallet.get(str(row["wallet"]), "")


def _sqlite_variable_limit(conn: sqlite3.Connection) -> int:
    getlimit = getattr(conn, "getlimit", None)
    if getlimit is None:
        return 999
    try:
        limit = int(getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
    except (AttributeError, TypeError, ValueError, sqlite3.Error):
        return 999
    return max(1, limit)


def _wallet_history_plan_wallet_chunks(
    wallets: list[str],
    *,
    limit: int,
) -> list[list[str]]:
    chunk_size = max(1, int(limit))
    return [
        wallets[index : index + chunk_size]
        for index in range(0, len(wallets), chunk_size)
    ]


def _rank_wallet_history_plan_candidates(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Assign the same per-lane ranks the old window query produced."""

    lanes: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        lane_key = (str(row["target_depth"]), str(row["refresh_lane"]))
        lanes.setdefault(lane_key, []).append(row)

    ranked: list[dict[str, Any]] = []
    for lane_key in sorted(lanes):
        for lane_rank, row in enumerate(
            sorted(lanes[lane_key], key=_wallet_history_plan_lane_sort_key),
            start=1,
        ):
            item = dict(row)
            item["lane_rank"] = lane_rank
            ranked.append(item)
    ranked.sort(
        key=lambda row: (
            int(row["lane_rank"]),
            str(row["target_depth"]),
            str(row["refresh_lane"]),
            int(row["urgency"]),
            -float(row["research_score"] or 0),
            str(row["wallet"]),
        )
    )
    return ranked


def _wallet_history_plan_lane_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(row["urgency"]),
        -float(row["research_score"] or 0.0),
        -int(row["sample_market_count"] or 0),
        -float(row["sample_volume_usdc"] or 0.0),
        -int(row["sample_trade_count"] or 0),
        -int(row["last_seen_at"] or 0),
        str(row["wallet"]),
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
        try:
            job = claim_pipeline_job(
                conn,
                job_type=JOB_TYPE,
                shard=shard_index,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                priority_aging_seconds=priority_aging_seconds,
            )
        except sqlite3.OperationalError as exc:
            if not is_sqlite_locked_error(exc):
                raise
            conn.rollback()
            deferred += 1
            error = "wallet history claim deferred by SQLite writer contention"
            break
        if job is None:
            break
        attempted += 1
        wallet = str(job["wallet"]).lower()
        artifact: WalletHistoryArtifact | None = None
        artifact_written = False
        artifact_reused = False
        artifact_repaired = False
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
            if depth is HistoryDepth.LIGHT:
                # A queued L2 job may predate a stricter screen policy. Recheck
                # before any upstream request so stale jobs cannot spend budget.
                screen = _wallet_history_plan_screen_snapshot(conn, wallets=[wallet]).get(wallet, {})
                if not current_screen_is_history_eligible(screen):
                    complete_pipeline_job(
                        conn,
                        job_id=int(job["job_id"]),
                        worker_id=worker_id,
                        output_data={
                            "status": "skipped_stale_l2_screen",
                            "level": level.level.value,
                            "history_depth": depth.value,
                        },
                    )
                    conn.commit()
                    succeeded += 1
                    continue
            job_input = _json_dict(job.get("input_json"))
            refresh_reason = str(job_input.get("refresh_reason") or "")
            history_rows: list[dict[str, Any]] = []
            if refresh_reason in REUSE_ONLY_REFRESH_REASONS:
                try:
                    cached = load_active_wallet_history_artifact(
                        conn,
                        archive_dir=archive_dir,
                        wallet=wallet,
                        history_depth=depth,
                    )
                except (OSError, RuntimeError, ValueError):
                    cached = None
                if cached is not None:
                    artifact, history_rows = cached
                    artifact_reused = True
                else:
                    # Missing or corrupt snapshots are rare. Re-fetching this one
                    # authorized depth repairs the artifact without stalling the
                    # same wallet forever.
                    artifact_repaired = True
            if artifact is None:
                history_rows = _fetch_history(
                    client,
                    wallet,
                    max_rows=_target_rows(depth),
                    page_limit=PAGE_LIMIT,
                    sleep_seconds=sleep_seconds,
                )
            now = int(time.time())
            pnl_evidence = _load_or_refresh_pnl(
                conn,
                client=client,
                wallet=wallet,
                history_depth=depth,
                sleep_seconds=sleep_seconds,
                now=now,
            )
            # PnL is an independent cache. Commit it before Parquet I/O so a
            # slow NAS fsync never extends the SQLite writer transaction.
            conn.commit()
            summary = summarize_wallet_history(
                history_rows,
                history_depth=depth,
                estimated_pnl_usdc=pnl_evidence.estimated_pnl_usdc,
                cost_roi_estimate=pnl_evidence.cost_roi_estimate,
                pnl_coverage=pnl_evidence.coverage,
                official_all_pnl_usdc=pnl_evidence.official_all_pnl_usdc,
                official_profit_intensity=pnl_evidence.official_profit_intensity,
                now=now,
            )
            if artifact is None:
                artifact = persist_wallet_history_artifact(
                    conn,
                    archive_dir=archive_dir,
                    wallet=wallet,
                    history_depth=depth,
                    rows=history_rows,
                    now=now,
                )
                artifact_written = True
            _persist_history_summary(conn, wallet=wallet, artifact=artifact, summary=summary, now=now)
            _update_wallet_feature(
                conn,
                wallet=wallet,
                summary=summary,
                pnl=pnl_evidence.rankable_pnl_usdc,
                now=now,
            )
            completed = complete_pipeline_job(
                conn,
                job_id=int(job["job_id"]),
                worker_id=worker_id,
                output_data={
                    "history_depth": depth.value,
                    "artifact_id": artifact.artifact_id,
                    "row_count": artifact.row_count,
                    "research_score": summary.research_score,
                    "forward_selection_score": summary.forward_selection_score,
                    "artifact_reused": artifact_reused,
                    "artifact_repaired": artifact_repaired,
                },
                now=now,
            )
            if not completed:
                raise RuntimeError("wallet history job lease lost")
            conn.commit()
            succeeded += 1
            rows_archived += artifact.row_count if artifact_written else 0
            light_completed += int(depth is HistoryDepth.LIGHT)
            deep_completed += int(depth is HistoryDepth.DEEP)
        except Exception as exc:
            conn.rollback()
            if artifact is not None and artifact_written:
                discard_uncommitted_wallet_history_artifact(
                    conn,
                    archive_dir=archive_dir,
                    artifact=artifact,
                )
            scheduler_deferred = is_upstream_scheduling_error(exc)
            terminal_error = _is_terminal_history_error(exc)
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
                fail_permanently=terminal_error,
                terminal_reason="wallet_history_data_quality" if terminal_error else "",
                terminal_policy_version=HISTORY_POLICY_VERSION if terminal_error else "",
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


def _is_terminal_history_error(exc: BaseException) -> bool:
    """Classify local data-quality failures that retrying cannot repair."""

    if isinstance(exc, WalletHistoryTerminalDataQualityError):
        return True
    if isinstance(exc, HttpClientError):
        return False
    if is_sqlite_locked_error(exc):
        return False
    if isinstance(exc, sqlite3.OperationalError):
        return False
    text = str(exc).lower()
    terminal_fragments = (
        "incompatible history data",
        "artifact depth",
        "cannot replace deep history with light history",
    )
    return any(fragment in text for fragment in terminal_fragments)


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
) -> WalletPnlEvidence:
    row = conn.execute(
        "SELECT total_estimated_pnl_usdc, cost_roi_estimate, coverage, "
        "methodology_version, captured_at, official_all_pnl_usdc, "
        "official_all_volume_usdc, official_profit_intensity "
        "FROM wallet_pnl_summaries WHERE wallet = ?",
        (wallet,),
    ).fetchone()
    cache_age = (
        PNL_REFRESH_SECONDS
        if row and row["official_all_pnl_usdc"] is not None
        else PNL_INCOMPLETE_REFRESH_SECONDS
    )
    if (
        row
        and str(row["methodology_version"] or "") == PNL_METHODOLOGY_VERSION
        and int(row["captured_at"] or 0) >= now - cache_age
        and _pnl_cache_satisfies(history_depth, str(row["coverage"] or ""))
    ):
        return WalletPnlEvidence(
            estimated_pnl_usdc=float(row["total_estimated_pnl_usdc"] or 0.0),
            cost_roi_estimate=(
                float(row["cost_roi_estimate"])
                if row["cost_roi_estimate"] is not None
                else None
            ),
            coverage=str(row["coverage"] or "none"),
            official_all_pnl_usdc=_optional_float(row["official_all_pnl_usdc"]),
            official_all_volume_usdc=_optional_float(row["official_all_volume_usdc"]),
            official_profit_intensity=_optional_float(row["official_profit_intensity"]),
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
    official_pnl, official_volume = _fetch_official_all_profit(client, wallet)
    official_profit_intensity = (
        official_pnl / official_volume
        if official_pnl is not None
        and official_volume is not None
        and official_volume > 0
        else None
    )
    current_position_value = sum(
        _float(value.get("value")) for value in values if isinstance(value, dict)
    )
    _persist_pnl_summary(
        conn,
        wallet=wallet,
        estimate=estimate,
        current_position_value=current_position_value,
        coverage=coverage,
        closed_position_limit=(
            DEEP_CLOSED_POSITION_LIMIT
            if history_depth is HistoryDepth.DEEP
            else LIGHT_CLOSED_POSITION_LIMIT
        ),
        official_all_pnl_usdc=official_pnl,
        official_all_volume_usdc=official_volume,
        official_profit_intensity=official_profit_intensity,
        now=now,
    )
    return WalletPnlEvidence(
        estimated_pnl_usdc=estimate.total_estimated_pnl_usdc,
        cost_roi_estimate=estimate.cost_roi_estimate,
        coverage=coverage,
        official_all_pnl_usdc=official_pnl,
        official_all_volume_usdc=official_volume,
        official_profit_intensity=official_profit_intensity,
    )


def _persist_pnl_summary(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    estimate: PnlEstimate,
    current_position_value: float,
    coverage: str,
    closed_position_limit: int,
    official_all_pnl_usdc: float | None,
    official_all_volume_usdc: float | None,
    official_profit_intensity: float | None,
    now: int,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_pnl_summaries(
            wallet, current_position_value_usdc, open_estimated_pnl_usdc,
            closed_realized_pnl_usdc, total_estimated_pnl_usdc,
            capital_basis_usdc, cost_roi_estimate, open_position_count,
            closed_position_count, coverage, methodology_version,
            captured_at, updated_at, official_all_pnl_usdc,
            official_all_volume_usdc, official_profit_intensity,
            evidence_metrics_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            updated_at = excluded.updated_at,
            official_all_pnl_usdc = excluded.official_all_pnl_usdc,
            official_all_volume_usdc = excluded.official_all_volume_usdc,
            official_profit_intensity = excluded.official_profit_intensity,
            evidence_metrics_json = excluded.evidence_metrics_json
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
            PNL_METHODOLOGY_VERSION,
            now,
            now,
            official_all_pnl_usdc,
            official_all_volume_usdc,
            official_profit_intensity,
            json.dumps(
                {
                    "closed_position_limit": int(closed_position_limit),
                    "closed_position_sort": "TIMESTAMP_DESC",
                    "closed_positions_observed": estimate.closed_positions_count,
                    "official_all_time_available": official_all_pnl_usdc is not None,
                },
                sort_keys=True,
            ),
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
            sort_by="TIMESTAMP",
            sort_direction="DESC",
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
        return rows, "deep_recent_bounded"
    return rows, "light_recent_bounded"


def _fetch_official_all_profit(
    client: PublicPolymarketClient,
    wallet: str,
) -> tuple[float | None, float | None]:
    """Fetch the source-neutral official lifetime PnL and turnover cross-check."""

    rows = client.trader_leaderboard(
        category="OVERALL",
        time_period="ALL",
        order_by="PNL",
        limit=1,
        offset=0,
        user=wallet,
    )
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not leaderboard_row_matches_wallet(row, wallet):
            continue
        return _optional_float(row.get("pnl")), _optional_float(
            row.get("vol") if row.get("vol") is not None else row.get("volume")
        )
    return None, None


def _pnl_cache_satisfies(history_depth: HistoryDepth, coverage: str) -> bool:
    if coverage == "complete":
        return True
    if history_depth is HistoryDepth.DEEP:
        return coverage == "deep_recent_bounded"
    return coverage in {
        "light_recent_bounded",
        "deep_recent_bounded",
    }


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
            research_score, diagnostic_score, forward_selection_score,
            score_components_json, forward_score_components_json,
            methodology_version, computed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            diagnostic_score = excluded.diagnostic_score,
            forward_selection_score = excluded.forward_selection_score,
            score_components_json = excluded.score_components_json,
            forward_score_components_json = excluded.forward_score_components_json,
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
            summary.diagnostic_score,
            summary.forward_selection_score,
            json.dumps(summary.score_components, sort_keys=True),
            json.dumps(summary.forward_score_components, sort_keys=True),
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
        "diagnostic_score": summary.diagnostic_score,
        "forward_selection_score": summary.forward_selection_score,
        "score_components": summary.score_components,
        "forward_score_components": summary.forward_score_components,
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
    """Allocate slots across first evidence and refresh work without starvation."""

    bounded_limit = max(0, int(limit))
    if bounded_limit == 0:
        return []
    lanes: dict[str, list[dict[str, Any]]] = {
        "required_depth": [],
        "methodology_upgrade": [],
        "pnl_evidence_refresh": [],
        "new_activity_after_refresh_window": [],
    }
    for row in rows:
        depth = HistoryDepth(_target_depth(row))
        lanes[_history_refresh_reason(row, depth=depth)].append(row)

    lanes["required_depth"] = _round_robin_history_rows(lanes["required_depth"])
    lanes["methodology_upgrade"].sort(key=_methodology_refresh_sort_key)
    lanes["pnl_evidence_refresh"].sort(key=_methodology_refresh_sort_key)
    lanes["new_activity_after_refresh_window"] = _round_robin_history_rows(
        lanes["new_activity_after_refresh_window"]
    )

    # Methodology correctness receives two shares while one share is always
    # reserved for first/required evidence. Optional refresh lanes use the
    # remaining shares when populated.
    schedule = (
        "methodology_upgrade",
        "methodology_upgrade",
        "required_depth",
        "pnl_evidence_refresh",
        "new_activity_after_refresh_window",
    )
    selected: list[dict[str, Any]] = []
    while len(selected) < bounded_limit and any(lanes.values()):
        progressed = False
        for name in schedule:
            lane = lanes[name]
            if lane and len(selected) < bounded_limit:
                selected.append(lane.pop(0))
                progressed = True
        if not progressed:
            break
    return selected


def _limit_l2_trusted_slow_targets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    slow_count = 0
    slow_sources: set[str] = set()
    for row in rows:
        if not _is_l2_trusted_slow_target(row):
            selected.append(row)
            continue
        source_bucket = _source_bucket(str(row.get("sources") or ""))
        if slow_count >= L2_TRUSTED_SLOW_MAX_PER_ROUND:
            continue
        if source_bucket in slow_sources:
            continue
        slow_count += 1
        slow_sources.add(source_bucket)
        selected.append(row)
    return selected


def _is_l2_trusted_slow_target(row: dict[str, Any]) -> bool:
    if str(row.get("level") or "") != WalletLevel.L2.value:
        return False
    source_bucket = _source_bucket(str(row.get("sources") or ""))
    if source_bucket == "stream":
        return False
    return (
        int(row.get("sample_trade_count") or 0) < L2_DEEP_MIN_ACTIVITY_COUNT
        or int(row.get("sample_market_count") or 0) < L2_DEEP_MIN_MARKET_COUNT
        or float(row.get("sample_volume_usdc") or 0.0) < L2_DEEP_MIN_VOLUME_USDC
    )


def _round_robin_history_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            f"{_target_depth(row)}:{str(row.get('level') or '')}:"
            f"{_source_bucket(str(row.get('sources') or ''))}"
        )
        buckets.setdefault(key, []).append(row)
    selected: list[dict[str, Any]] = []
    names = sorted(buckets)
    while names:
        remaining: list[str] = []
        for name in names:
            bucket = buckets[name]
            if bucket:
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
    if str(target.get("current_depth") or "") != depth.value:
        if depth is HistoryDepth.DEEP:
            return 5
        base = 30
        screen_strength = min(5, int(target.get("sample_market_count") or 0))
        screen_strength += min(
            5,
            int(float(target.get("sample_volume_usdc") or 0.0) // 100),
        )
        return max(15, base - screen_strength)
    if int(target.get("methodology_stale") or 0):
        return {
            WalletLevel.L6.value: 0,
            WalletLevel.L5.value: 1,
            WalletLevel.L4.value: 2,
            WalletLevel.L3.value: 3,
            WalletLevel.L2.value: 20,
        }.get(str(target.get("level") or ""), 20)
    if int(target.get("pnl_refresh_needed") or 0):
        return {
            WalletLevel.L6.value: 4,
            WalletLevel.L5.value: 5,
            WalletLevel.L4.value: 6,
        }.get(str(target.get("level") or ""), 10)
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
    if str(target.get("current_depth") or "") != depth.value:
        return "required_depth"
    if int(target.get("methodology_stale") or 0):
        return "methodology_upgrade"
    if int(target.get("activity_refresh_needed") or 0):
        return "new_activity_after_refresh_window"
    if int(target.get("pnl_refresh_needed") or 0):
        return "pnl_evidence_refresh"
    return "new_activity_after_refresh_window"


def _history_job_action(
    target: dict[str, Any],
    *,
    depth: HistoryDepth,
    base_action: str,
    refresh_reason: str,
) -> str:
    """Give each completed snapshot one immutable, deduplicated refresh job."""

    if str(target.get("current_depth") or "") != depth.value:
        return base_action
    summary_updated_at = int(target.get("summary_updated_at") or 0)
    if summary_updated_at <= 0:
        return base_action
    if refresh_reason == "new_activity_after_refresh_window":
        return f"{base_action}:activity:{int(target.get('last_seen_at') or 0)}"
    if refresh_reason == "pnl_evidence_refresh":
        marker = int(target.get("pnl_captured_at") or summary_updated_at)
        return f"{base_action}:pnl:{marker}"
    return f"{base_action}:refresh:{summary_updated_at}"


def _target_rows(depth: HistoryDepth) -> int:
    return DEEP_HISTORY_LIMIT if depth is HistoryDepth.DEEP else LIGHT_HISTORY_LIMIT


def _wallet_shard(wallet: str, shard_count: int) -> int:
    digest = hashlib.sha256(wallet.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


def _float(value: Any) -> float:
    try:
        parsed = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def _optional_float(value: Any) -> float | None:
    try:
        parsed = None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed is not None and math.isfinite(parsed) else None


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
