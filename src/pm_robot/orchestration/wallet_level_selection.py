"""Relative, source-aware selection for L3/L4/L5 wallet promotions."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.orchestration.wallet_history_pipeline import PNL_METHODOLOGY_VERSION
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.wallet_levels import advance_wallet_level
from pm_robot.wallet_levels import HistoryDepth, WalletLevel


SELECTION_POLICY_VERSION = "relative_rank_v6"
OFFICIAL_PNL_MAX_AGE_SECONDS = 86_400
MIN_FAIRNESS_BUCKET_SIZE = 3
_LEVEL_RANK = {level: index for index, level in enumerate(WalletLevel)}

# These are evidence sufficiency floors, not quality-score cutoffs. Relative
# ranking still decides which sufficiently observed wallets advance.
L3_MIN_ACTIVITY_COUNT = 25
L3_MIN_DISTINCT_MARKETS = 2
L3_MIN_VOLUME_USDC = 500.0
L3_SLOW_MIN_ACTIVITY_COUNT = 10
L3_SLOW_MIN_DISTINCT_MARKETS = 1
L3_SLOW_MIN_VOLUME_USDC = 100.0
L3_SLOW_MAX_PROMOTIONS = 2
L4_MIN_ACTIVITY_COUNT = 50
L4_MIN_DISTINCT_MARKETS = 3
L4_MIN_VOLUME_USDC = 500.0
L5_MIN_ACTIVITY_COUNT = 100
L5_MIN_DISTINCT_MARKETS = 5
L5_MIN_VOLUME_USDC = 1_000.0

_REFERENCE_HISTORICAL_SQL = """
SELECT
    decision.rowid AS selection_rowid,
    decision.wallet,
    decision.evidence_artifact_id AS artifact_id,
    decision.forward_selection_score,
    decision.research_score,
    '[]' AS strategy_tags_json,
    decision.updated_at,
    '' AS sources,
    decision.source_bucket,
    decision.strategy_bucket,
    decision.decided_at
FROM wallet_level_selections AS decision
WHERE decision.target_level = ?
  AND decision.policy_version = ?
  AND decision.forward_selection_score IS NOT NULL
  AND decision.score_status = 'valid'
"""


@dataclass(frozen=True)
class WalletLevelSelectionSummary:
    cohorts_processed: int
    decisions_written: int
    promoted_l3: int
    promoted_l4: int
    promoted_l5: int
    status: str


@dataclass(frozen=True)
class _Transition:
    current_level: WalletLevel
    target_level: WalletLevel
    required_depth: HistoryDepth
    fraction: float
    global_fraction: float
    max_promotions: int
    min_activity_count: int
    min_distinct_markets: int
    min_volume_usdc: float
    cooldown_seconds: int


@dataclass(frozen=True)
class _RelativeDecision:
    selected: bool
    eligible: bool
    global_eligible: bool
    rank: int
    reference_size: int


def reconcile_wallet_level_selections(
    conn: sqlite3.Connection,
    *,
    policy_version: str = SELECTION_POLICY_VERSION,
    min_cohort_size: int = 20,
    timeout_min_cohort_size: int = 5,
    max_wait_seconds: int = 3_600,
    l3_fraction: float = 0.25,
    l4_fraction: float = 0.20,
    l5_fraction: float = 0.10,
    l3_max_promotions: int = 12,
    l4_max_promotions: int = 6,
    l5_max_promotions: int = 2,
    now: int | None = None,
    decision_commit_batch_size: int = 50,
) -> WalletLevelSelectionSummary:
    """Select sufficiently observed wallets without an absolute score cutoff."""

    ts = int(time.time()) if now is None else int(now)
    transitions = (
        # Descending order prevents a wallet from moving twice in one reconcile call.
        _Transition(
            WalletLevel.L4,
            WalletLevel.L5,
            HistoryDepth.DEEP,
            _fraction(l5_fraction),
            0.25,
            max(0, int(l5_max_promotions)),
            L5_MIN_ACTIVITY_COUNT,
            L5_MIN_DISTINCT_MARKETS,
            L5_MIN_VOLUME_USDC,
            30 * 86_400,
        ),
        _Transition(
            WalletLevel.L3,
            WalletLevel.L4,
            HistoryDepth.DEEP,
            _fraction(l4_fraction),
            0.50,
            max(0, int(l4_max_promotions)),
            L4_MIN_ACTIVITY_COUNT,
            L4_MIN_DISTINCT_MARKETS,
            L4_MIN_VOLUME_USDC,
            14 * 86_400,
        ),
        _Transition(
            WalletLevel.L2,
            WalletLevel.L3,
            HistoryDepth.LIGHT,
            _fraction(l3_fraction),
            1.00,
            max(0, int(l3_max_promotions)),
            L3_MIN_ACTIVITY_COUNT,
            L3_MIN_DISTINCT_MARKETS,
            L3_MIN_VOLUME_USDC,
            7 * 86_400,
        ),
    )
    cohorts = 0
    decisions = 0
    promoted = {WalletLevel.L3: 0, WalletLevel.L4: 0, WalletLevel.L5: 0}
    for transition in transitions:
        pending_rows = _selection_rows(
            conn,
            transition=transition,
            policy_version=policy_version,
            now=ts,
        )
        if not pending_rows:
            continue
        reference_rows = _reference_rows(
            conn,
            transition=transition,
            policy_version=policy_version,
            now=ts,
        )
        oldest = min(int(row["updated_at"] or 0) for row in pending_rows)
        cohort_ready = len(reference_rows) >= max(1, int(min_cohort_size))
        timed_out = (
            len(reference_rows) >= max(2, int(timeout_min_cohort_size))
            and oldest <= ts - max(0, int(max_wait_seconds))
        )
        if not cohort_ready and not timed_out:
            continue
        cohorts += 1
        relative_decisions = _select_relative(
            pending_rows,
            reference_rows=reference_rows,
            transition=transition,
        )
        cohort_decisions = 0
        commit_batch_size = int(decision_commit_batch_size)
        for row in pending_rows:
            wallet = str(row["wallet"])
            source_bucket = _source_bucket(str(row["sources"] or ""))
            strategy_bucket = _strategy_bucket(str(row["strategy_tags_json"] or "[]"))
            relative = relative_decisions[wallet]
            if relative.selected:
                reason = "relative_rank_selected"
            elif relative.eligible:
                reason = "relative_rank_capacity_limited"
            elif not relative.global_eligible:
                reason = "relative_rank_below_global_baseline"
            else:
                reason = "relative_rank_below_percentile"
            conn.execute(
                """
                INSERT INTO wallet_level_selections(
                    wallet, target_level, evidence_artifact_id, policy_version,
                    selected, rank_in_cohort, cohort_size, source_bucket,
                    strategy_bucket, reason, decided_at, updated_at,
                    research_score, forward_selection_score, score_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'valid')
                ON CONFLICT(wallet, target_level, evidence_artifact_id, policy_version)
                DO UPDATE SET
                    selected = excluded.selected,
                    rank_in_cohort = excluded.rank_in_cohort,
                    cohort_size = excluded.cohort_size,
                    source_bucket = excluded.source_bucket,
                    strategy_bucket = excluded.strategy_bucket,
                    reason = excluded.reason,
                    decided_at = excluded.decided_at,
                    updated_at = excluded.updated_at,
                    research_score = excluded.research_score,
                    forward_selection_score = excluded.forward_selection_score,
                    score_status = excluded.score_status
                """,
                (
                    wallet,
                    transition.target_level.value,
                    str(row["artifact_id"]),
                    policy_version,
                    int(relative.selected),
                    relative.rank,
                    relative.reference_size,
                    source_bucket,
                    strategy_bucket,
                    reason,
                    ts,
                    ts,
                    float(row["research_score"]),
                    float(row["forward_selection_score"]),
                ),
            )
            _update_review_state(
                conn,
                row=row,
                transition=transition,
                selected=relative.selected,
                policy_version=policy_version,
                now=ts,
            )
            decisions += 1
            cohort_decisions += 1
            should_commit_decision_batch = (
                commit_batch_size > 0
                and cohort_decisions % commit_batch_size == 0
            )
            if not relative.selected:
                if should_commit_decision_batch:
                    _commit(conn)
                continue
            current_level = WalletLevel(str(row["current_level"]))
            if _LEVEL_RANK[current_level] >= _LEVEL_RANK[transition.target_level]:
                # Historical L5/L6 wallets still need a current-artifact L5
                # selection record, but relative ranking never demotes them.
                if should_commit_decision_batch:
                    _commit(conn)
                continue
            advance_wallet_level(
                conn,
                wallet,
                to_level=transition.target_level,
                reason=reason,
                policy_version=policy_version,
                facts={
                    "research_score": float(row["research_score"]),
                    "forward_selection_score": float(row["forward_selection_score"]),
                    "rank_in_cohort": relative.rank,
                    "cohort_size": relative.reference_size,
                    "source_bucket": source_bucket,
                    "strategy_bucket": strategy_bucket,
                    "evidence_artifact_id": str(row["artifact_id"]),
                },
                now=ts,
            )
            promoted[transition.target_level] += 1
            if should_commit_decision_batch:
                _commit(conn)
        # Release SQLite's single-writer lock at the cohort boundary.  The
        # research-control loop can process L5/L4/L3 cohorts in one call, and
        # holding the first cohort's transaction until the whole call returns
        # blocks discovery/whale writers touching wallet_levels meanwhile.
        _commit(conn)
    return WalletLevelSelectionSummary(
        cohorts_processed=cohorts,
        decisions_written=decisions,
        promoted_l3=promoted[WalletLevel.L3],
        promoted_l4=promoted[WalletLevel.L4],
        promoted_l5=promoted[WalletLevel.L5],
        status="ok",
    )


def _commit(conn: sqlite3.Connection) -> None:
    conn.commit()


def _selection_rows(
    conn: sqlite3.Connection,
    *,
    transition: _Transition,
    policy_version: str,
    now: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH candidate_levels AS MATERIALIZED (
            SELECT
                levels.wallet,
                levels.level AS current_level,
                levels.level_updated_at,
                COALESCE(observed.sources, '') AS sources,
                review.review_state,
                review.cooldown_until,
                review.policy_version AS review_policy_version,
                review.methodology_version AS review_methodology_version,
                review.last_activity_count,
                review.last_distinct_markets,
                review.last_total_volume_usdc
            FROM wallet_levels AS levels
            LEFT JOIN observed_wallets AS observed ON observed.wallet = levels.wallet
            LEFT JOIN wallet_level_review_state AS review
              ON review.wallet = levels.wallet
             AND review.target_level = ?
            LEFT JOIN wallet_pnl_summaries AS pnl
              ON pnl.wallet = levels.wallet
            WHERE (
                    levels.level = ?
                 OR (
                        ? = 'l5'
                    AND levels.level IN ('l5', 'l6')
                 )
              )
              AND levels.hard_risk_block = 0
              AND (
                    ? != 'l5'
                 OR (
                        pnl.official_all_pnl_usdc > 0
                    AND pnl.official_all_volume_usdc > 0
                    AND pnl.methodology_version = ?
                    AND pnl.captured_at >= ?
                 )
              )
        )
        SELECT
            candidate.wallet,
            candidate.current_level,
            summary.artifact_id,
            summary.research_score,
            summary.forward_selection_score,
            summary.activity_count,
            summary.distinct_markets,
            summary.total_volume_usdc,
            summary.strategy_tags_json,
            summary.updated_at,
            candidate.sources
        FROM candidate_levels AS candidate
        JOIN wallet_history_summaries AS summary ON summary.wallet = candidate.wallet
        WHERE (
                candidate.current_level = ?
             OR (
                    ? = 'l5'
                AND candidate.current_level IN ('l5', 'l6')
                AND summary.updated_at > candidate.level_updated_at
             )
          )
          AND summary.history_depth = ?
          AND summary.methodology_version = ?
          AND summary.forward_selection_score IS NOT NULL
          AND (
                (summary.activity_count >= ?
                 AND summary.distinct_markets >= ?
                 AND summary.total_volume_usdc >= ?)
             OR (
                    ? = 'l3'
                AND summary.activity_count >= ?
                AND summary.distinct_markets >= ?
                AND summary.total_volume_usdc >= ?
                AND (
                       LOWER(candidate.sources) LIKE '%leaderboard%'
                    OR LOWER(candidate.sources) LIKE '%manual%'
                    OR LOWER(candidate.sources) LIKE '%bitget%'
                    OR LOWER(candidate.sources) LIKE '%polydata%'
                )
             )
          )
          AND (
                candidate.review_state IS NULL
             OR candidate.review_state != 'archived'
             OR candidate.review_policy_version != ?
             OR candidate.review_methodology_version != ?
             OR summary.activity_count >= CAST(candidate.last_activity_count * 1.25 AS INTEGER)
             OR summary.total_volume_usdc >= candidate.last_total_volume_usdc * 1.25
             OR summary.distinct_markets > candidate.last_distinct_markets
          )
          AND (
                candidate.review_state IS NULL
             OR candidate.review_state != 'cooldown'
             OR candidate.cooldown_until <= ?
             OR candidate.review_policy_version != ?
             OR candidate.review_methodology_version != ?
             OR summary.activity_count >= CAST(candidate.last_activity_count * 1.25 AS INTEGER)
             OR summary.total_volume_usdc >= candidate.last_total_volume_usdc * 1.25
             OR summary.distinct_markets > candidate.last_distinct_markets
          )
          AND NOT EXISTS (
              SELECT 1
              FROM wallet_level_selections AS decision
              WHERE decision.wallet = candidate.wallet
                AND decision.target_level = ?
                AND decision.evidence_artifact_id = summary.artifact_id
                AND decision.policy_version = ?
          )
        ORDER BY summary.forward_selection_score DESC, summary.wallet ASC
        """,
        (
            transition.target_level.value,
            transition.current_level.value,
            transition.target_level.value,
            transition.target_level.value,
            PNL_METHODOLOGY_VERSION,
            int(now) - OFFICIAL_PNL_MAX_AGE_SECONDS,
            transition.current_level.value,
            transition.target_level.value,
            transition.required_depth.value,
            METHODOLOGY_VERSION,
            transition.min_activity_count,
            transition.min_distinct_markets,
            transition.min_volume_usdc,
            transition.target_level.value,
            L3_SLOW_MIN_ACTIVITY_COUNT,
            L3_SLOW_MIN_DISTINCT_MARKETS,
            L3_SLOW_MIN_VOLUME_USDC,
            policy_version,
            METHODOLOGY_VERSION,
            int(now),
            policy_version,
            METHODOLOGY_VERSION,
            transition.target_level.value,
            policy_version,
        ),
    ).fetchall()
    return [dict(row) for row in rows]


def _reference_rows(
    conn: sqlite3.Connection,
    *,
    transition: _Transition,
    policy_version: str,
    now: int,
) -> list[dict[str, Any]]:
    live_rows = conn.execute(
        """
        WITH candidate_levels AS MATERIALIZED (
            SELECT
                levels.wallet,
                levels.level AS current_level,
                levels.level_updated_at,
                COALESCE(observed.sources, '') AS sources
            FROM wallet_levels AS levels
            LEFT JOIN observed_wallets AS observed ON observed.wallet = levels.wallet
            LEFT JOIN wallet_pnl_summaries AS pnl
              ON pnl.wallet = levels.wallet
            WHERE (
                    levels.level = ?
                 OR (
                        ? = 'l5'
                    AND levels.level IN ('l5', 'l6')
                 )
              )
              AND levels.hard_risk_block = 0
              AND (
                    ? != 'l5'
                 OR (
                        pnl.official_all_pnl_usdc > 0
                    AND pnl.official_all_volume_usdc > 0
                    AND pnl.methodology_version = ?
                    AND pnl.captured_at >= ?
                 )
              )
        )
        SELECT
            candidate.wallet,
            summary.artifact_id,
            summary.research_score,
            summary.forward_selection_score,
            summary.activity_count,
            summary.distinct_markets,
            summary.total_volume_usdc,
            summary.strategy_tags_json,
            summary.updated_at,
            candidate.sources
        FROM candidate_levels AS candidate
        JOIN wallet_history_summaries AS summary ON summary.wallet = candidate.wallet
        WHERE (
                candidate.current_level = ?
             OR (
                    ? = 'l5'
                AND candidate.current_level IN ('l5', 'l6')
                AND summary.updated_at > candidate.level_updated_at
             )
          )
          AND summary.history_depth = ?
          AND summary.methodology_version = ?
          AND summary.forward_selection_score IS NOT NULL
          AND (
                (summary.activity_count >= ?
                 AND summary.distinct_markets >= ?
                 AND summary.total_volume_usdc >= ?)
             OR (
                    ? = 'l3'
                AND summary.activity_count >= ?
                AND summary.distinct_markets >= ?
                AND summary.total_volume_usdc >= ?
                AND (
                       LOWER(candidate.sources) LIKE '%leaderboard%'
                    OR LOWER(candidate.sources) LIKE '%manual%'
                    OR LOWER(candidate.sources) LIKE '%bitget%'
                    OR LOWER(candidate.sources) LIKE '%polydata%'
                )
             )
          )
        ORDER BY summary.forward_selection_score DESC, summary.wallet ASC
        """,
        (
            transition.current_level.value,
            transition.target_level.value,
            transition.target_level.value,
            PNL_METHODOLOGY_VERSION,
            int(now) - OFFICIAL_PNL_MAX_AGE_SECONDS,
            transition.current_level.value,
            transition.target_level.value,
            transition.required_depth.value,
            METHODOLOGY_VERSION,
            transition.min_activity_count,
            transition.min_distinct_markets,
            transition.min_volume_usdc,
            transition.target_level.value,
            L3_SLOW_MIN_ACTIVITY_COUNT,
            L3_SLOW_MIN_DISTINCT_MARKETS,
            L3_SLOW_MIN_VOLUME_USDC,
        ),
    ).fetchall()
    historical_rows = _historical_reference_rows(
        conn,
        target_level=transition.target_level.value,
        policy_version=policy_version,
    )
    # Current transition evidence overrides an older decision snapshot for the
    # same wallet. Promoted wallets remain represented by their immutable score
    # at the time they crossed this transition.
    by_wallet = {str(row["wallet"]): dict(row) for row in historical_rows}
    by_wallet.update({str(row["wallet"]): dict(row) for row in live_rows})
    return sorted(
        by_wallet.values(),
        key=lambda row: (-float(row["forward_selection_score"]), str(row["wallet"])),
    )


def _historical_reference_rows(
    conn: sqlite3.Connection,
    *,
    target_level: str,
    policy_version: str,
) -> list[dict[str, Any]]:
    latest_by_wallet: dict[str, dict[str, Any]] = {}
    for row in conn.execute(_REFERENCE_HISTORICAL_SQL, (target_level, policy_version)):
        wallet = str(row["wallet"])
        current = latest_by_wallet.get(wallet)
        if current is None or (
            int(row["decided_at"] or 0),
            int(row["selection_rowid"] or 0),
        ) > (
            int(current.get("decided_at") or 0),
            int(current.get("selection_rowid") or 0),
        ):
            latest_by_wallet[wallet] = dict(row)
    for row in latest_by_wallet.values():
        row.pop("selection_rowid", None)
        row.pop("decided_at", None)
    return list(latest_by_wallet.values())


def _select_relative(
    pending_rows: list[dict[str, Any]],
    *,
    reference_rows: list[dict[str, Any]],
    transition: _Transition,
) -> dict[str, _RelativeDecision]:
    if transition.max_promotions <= 0 or transition.fraction <= 0:
        return {
            str(row["wallet"]): _RelativeDecision(
                selected=False,
                eligible=False,
                global_eligible=False,
                rank=0,
                reference_size=0,
            )
            for row in pending_rows
        }
    exact_counts: dict[tuple[str, str], int] = {}
    source_counts: dict[str, int] = {}
    for row in reference_rows:
        exact = _row_bucket(row)
        exact_counts[exact] = exact_counts.get(exact, 0) + 1
        source_counts[exact[0]] = source_counts.get(exact[0], 0) + 1

    pending_keys = {
        str(row["wallet"]): _comparison_bucket(
            row,
            exact_counts=exact_counts,
            source_counts=source_counts,
        )
        for row in pending_rows
    }
    global_ranked = sorted(
        reference_rows,
        key=lambda row: (-float(row["forward_selection_score"]), str(row["wallet"])),
    )
    global_quota = min(
        len(global_ranked),
        max(1, math.ceil(len(global_ranked) * transition.global_fraction)),
    )
    global_eligible = {str(row["wallet"]) for row in global_ranked[:global_quota]}
    eligible_by_bucket: dict[tuple[str, str, str], set[str]] = {}
    rank_by_bucket: dict[tuple[str, str, str], dict[str, int]] = {}
    size_by_bucket: dict[tuple[str, str, str], int] = {}
    for key in sorted(set(pending_keys.values())):
        kind, source, strategy = key
        if kind == "exact":
            group = [row for row in reference_rows if _row_bucket(row) == (source, strategy)]
        elif kind == "source":
            group = [row for row in reference_rows if _row_bucket(row)[0] == source]
        else:
            group = list(reference_rows)
        ranked = sorted(
            group,
            key=lambda row: (-float(row["forward_selection_score"]), str(row["wallet"])),
        )
        quota = min(len(ranked), max(1, math.ceil(len(ranked) * transition.fraction)))
        eligible_by_bucket[key] = {str(row["wallet"]) for row in ranked[:quota]}
        rank_by_bucket[key] = {
            str(row["wallet"]): rank
            for rank, row in enumerate(ranked, start=1)
        }
        size_by_bucket[key] = len(ranked)

    candidates: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in pending_rows:
        wallet = str(row["wallet"])
        key = pending_keys[wallet]
        if wallet in eligible_by_bucket.get(key, set()) and wallet in global_eligible:
            candidates.setdefault(key, []).append(row)
    for rows in candidates.values():
        rows.sort(key=lambda row: (-float(row["forward_selection_score"]), str(row["wallet"])))

    selected: set[str] = set()
    slow_selected = 0
    slow_source_selected: set[str] = set()
    keys = sorted(candidates)
    while keys and len(selected) < transition.max_promotions:
        remaining: list[tuple[str, str, str]] = []
        for key in keys:
            bucket = candidates[key]
            if bucket and len(selected) < transition.max_promotions:
                picked_index = _first_selectable_candidate_index(
                    bucket,
                    transition=transition,
                    slow_selected=slow_selected,
                    slow_source_selected=slow_source_selected,
                )
                if picked_index is not None:
                    row = bucket.pop(picked_index)
                    selected.add(str(row["wallet"]))
                    if _is_l3_slow_lane(row, transition=transition):
                        slow_selected += 1
                        slow_source_selected.add(_row_bucket(row)[0])
                else:
                    bucket.clear()
            if bucket:
                remaining.append(key)
        keys = remaining
    return {
        wallet: _RelativeDecision(
            selected=wallet in selected,
            eligible=(
                wallet in eligible_by_bucket.get(key, set())
                and wallet in global_eligible
            ),
            global_eligible=wallet in global_eligible,
            rank=rank_by_bucket.get(key, {}).get(wallet, 0),
            reference_size=size_by_bucket.get(key, 0),
        )
        for wallet, key in pending_keys.items()
    }


def _first_selectable_candidate_index(
    rows: list[dict[str, Any]],
    *,
    transition: _Transition,
    slow_selected: int,
    slow_source_selected: set[str],
) -> int | None:
    for index, row in enumerate(rows):
        if not _is_l3_slow_lane(row, transition=transition):
            return index
        source_bucket = _row_bucket(row)[0]
        if slow_selected >= L3_SLOW_MAX_PROMOTIONS:
            continue
        if source_bucket in slow_source_selected:
            continue
        return index
    return None


def _is_l3_slow_lane(row: dict[str, Any], *, transition: _Transition) -> bool:
    if transition.target_level is not WalletLevel.L3:
        return False
    source_bucket = _row_bucket(row)[0]
    if source_bucket == "stream":
        return False
    return (
        int(row.get("activity_count") or 0) < transition.min_activity_count
        or int(row.get("distinct_markets") or 0) < transition.min_distinct_markets
        or float(row.get("total_volume_usdc") or 0.0) < transition.min_volume_usdc
    )


def _update_review_state(
    conn: sqlite3.Connection,
    *,
    row: dict[str, Any],
    transition: _Transition,
    selected: bool,
    policy_version: str,
    now: int,
) -> None:
    wallet = str(row["wallet"])
    target_level = transition.target_level.value
    if selected:
        conn.execute(
            """
            INSERT INTO wallet_level_review_state(
                wallet, target_level, policy_version, methodology_version,
                review_state, cooldown_until, no_material_improvement_count,
                last_evidence_artifact_id, last_activity_count,
                last_distinct_markets, last_total_volume_usdc,
                last_reviewed_at, updated_at
            ) VALUES (?, ?, ?, ?, 'active', 0, 0, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet, target_level) DO UPDATE SET
                policy_version = excluded.policy_version,
                methodology_version = excluded.methodology_version,
                review_state = excluded.review_state,
                cooldown_until = 0,
                no_material_improvement_count = 0,
                last_evidence_artifact_id = excluded.last_evidence_artifact_id,
                last_activity_count = excluded.last_activity_count,
                last_distinct_markets = excluded.last_distinct_markets,
                last_total_volume_usdc = excluded.last_total_volume_usdc,
                last_reviewed_at = excluded.last_reviewed_at,
                updated_at = excluded.updated_at
            """,
            _review_state_values(row, transition, policy_version, now),
        )
        return

    previous = conn.execute(
        """
        SELECT *
        FROM wallet_level_review_state
        WHERE wallet = ? AND target_level = ?
        """,
        (wallet, target_level),
    ).fetchone()
    prior_count = int(previous["no_material_improvement_count"] or 0) if previous else 0
    improved = (
        previous is not None
        and _materially_improved(row, previous, policy_version=policy_version)
    )
    no_improvement_count = 0 if improved else prior_count + 1
    review_state = "archived" if no_improvement_count >= 3 else "cooldown"
    cooldown_until = 0 if review_state == "archived" else now + transition.cooldown_seconds
    conn.execute(
        """
        INSERT INTO wallet_level_review_state(
            wallet, target_level, policy_version, methodology_version,
            review_state, cooldown_until, no_material_improvement_count,
            last_evidence_artifact_id, last_activity_count,
            last_distinct_markets, last_total_volume_usdc,
            last_reviewed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet, target_level) DO UPDATE SET
            policy_version = excluded.policy_version,
            methodology_version = excluded.methodology_version,
            review_state = excluded.review_state,
            cooldown_until = excluded.cooldown_until,
            no_material_improvement_count = excluded.no_material_improvement_count,
            last_evidence_artifact_id = excluded.last_evidence_artifact_id,
            last_activity_count = excluded.last_activity_count,
            last_distinct_markets = excluded.last_distinct_markets,
            last_total_volume_usdc = excluded.last_total_volume_usdc,
            last_reviewed_at = excluded.last_reviewed_at,
            updated_at = excluded.updated_at
        """,
        (
            wallet,
            target_level,
            policy_version,
            METHODOLOGY_VERSION,
            review_state,
            cooldown_until,
            no_improvement_count,
            str(row["artifact_id"]),
            int(row.get("activity_count") or 0),
            int(row.get("distinct_markets") or 0),
            float(row.get("total_volume_usdc") or 0.0),
            now,
            now,
        ),
    )


def _review_state_values(
    row: dict[str, Any],
    transition: _Transition,
    policy_version: str,
    now: int,
) -> tuple[object, ...]:
    return (
        str(row["wallet"]),
        transition.target_level.value,
        policy_version,
        METHODOLOGY_VERSION,
        str(row["artifact_id"]),
        int(row.get("activity_count") or 0),
        int(row.get("distinct_markets") or 0),
        float(row.get("total_volume_usdc") or 0.0),
        now,
        now,
    )


def _materially_improved(
    row: dict[str, Any],
    previous: sqlite3.Row | None,
    *,
    policy_version: str,
) -> bool:
    if previous is None:
        return True
    if str(previous["policy_version"] or "") != policy_version:
        return True
    if str(previous["methodology_version"] or "") != METHODOLOGY_VERSION:
        return True
    if str(previous["last_evidence_artifact_id"] or "") == str(row["artifact_id"]):
        return False
    activity = int(row.get("activity_count") or 0)
    volume = float(row.get("total_volume_usdc") or 0.0)
    markets = int(row.get("distinct_markets") or 0)
    old_activity = int(previous["last_activity_count"] or 0)
    old_volume = float(previous["last_total_volume_usdc"] or 0.0)
    old_markets = int(previous["last_distinct_markets"] or 0)
    return (
        activity >= max(old_activity + 1, math.ceil(old_activity * 1.25))
        or volume >= max(old_volume + 0.01, old_volume * 1.25)
        or markets > old_markets
    )


def _row_bucket(row: dict[str, Any]) -> tuple[str, str]:
    source_bucket = str(row.get("source_bucket") or "").strip()
    strategy_bucket = str(row.get("strategy_bucket") or "").strip()
    if source_bucket and strategy_bucket:
        return source_bucket, strategy_bucket
    return (
        _source_bucket(str(row["sources"] or "")),
        _strategy_bucket(str(row["strategy_tags_json"] or "[]")),
    )


def _comparison_bucket(
    row: dict[str, Any],
    *,
    exact_counts: dict[tuple[str, str], int],
    source_counts: dict[str, int],
) -> tuple[str, str, str]:
    source, strategy = _row_bucket(row)
    if exact_counts.get((source, strategy), 0) >= MIN_FAIRNESS_BUCKET_SIZE:
        return "exact", source, strategy
    if source_counts.get(source, 0) >= MIN_FAIRNESS_BUCKET_SIZE:
        return "source", source, "*"
    return "global", "*", "*"


def _strategy_bucket(raw_json: str) -> str:
    try:
        payload = json.loads(raw_json or "[]")
    except json.JSONDecodeError:
        payload = []
    tags = sorted(str(tag) for tag in payload if str(tag).strip()) if isinstance(payload, list) else []
    return tags[0] if tags else "general"


def _source_bucket(sources: str) -> str:
    lowered = sources.lower()
    if "leaderboard" in lowered:
        return "leaderboard"
    if "manual" in lowered or "bitget" in lowered:
        return "curated"
    if "polydata" in lowered:
        return "polydata"
    return "stream"


def _fraction(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
