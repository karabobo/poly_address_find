"""Consistency repair for copyability evidence derived from qualified pairs."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Iterator

from pm_robot.pipeline_terms import (
    PENDING_EVIDENCE_JOB_STAGES,
    PENDING_EVIDENCE_STATUSES,
)
from pm_robot.research.copy_backtest import clear_copy_backtest_features
from pm_robot.research.copy_graph import clear_copy_graph_features


SQLITE_IN_CHUNK_SIZE = 400


@dataclass(frozen=True)
class CopyabilityTruthReconcileSummary:
    orphan_leaders: int
    leader_stats_deleted: int
    leader_performance_deleted: int
    backtest_trades_deleted: int
    feature_rows_cleared: int
    score_actions_marked: int
    summary_score_actions_cleared: int = 0


def reconcile_copyability_truth(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
) -> CopyabilityTruthReconcileSummary:
    """Remove validated derivatives that no longer have a qualified pair.

    Unqualified pair rows remain available as candidate diagnostics. This function
    does not change candidate stages or interrupt pending history evidence work.
    """

    reconciled_at = int(time.time()) if now is None else int(now)
    summary_actions_cleared = _clear_unscoreable_truth_actions(
        conn,
        now=reconciled_at,
    )
    rows = conn.execute(
        """
        WITH derivative_leaders AS (
            SELECT leader_wallet FROM copy_leader_stats
            UNION
            SELECT leader_wallet FROM copy_leader_performance
            UNION
            SELECT leader_wallet FROM copy_backtest_trades
            UNION
            SELECT address AS leader_wallet
            FROM wallet_features
            WHERE COALESCE(leader_in_degree, 0) > 0
               OR COALESCE(copy_event_count, 0) > 0
               OR COALESCE(copy_market_count, 0) > 0
               OR COALESCE(containment_pct_median, 0) > 0
               OR ABS(COALESCE(copy_stream_roi, 0)) > 0
               OR edge_retention_pct IS NOT NULL
               OR walk_forward_consistency_pct IS NOT NULL
               OR json_type(
                    CASE
                        WHEN json_valid(COALESCE(extra_json, '{}')) THEN extra_json
                        ELSE '{}'
                    END,
                    '$.copy_graph_qualified_follower_count'
                  ) IS NOT NULL
               OR json_type(
                    CASE
                        WHEN json_valid(COALESCE(extra_json, '{}')) THEN extra_json
                        ELSE '{}'
                    END,
                    '$.copy_backtest_trade_count'
                  ) IS NOT NULL
        )
        SELECT DISTINCT source.leader_wallet
        FROM derivative_leaders source
        WHERE NOT EXISTS (
            SELECT 1
            FROM copy_pair_stats pair
            WHERE pair.leader_wallet = source.leader_wallet
              AND pair.qualifies = 1
        )
        ORDER BY source.leader_wallet
        """
    ).fetchall()
    leaders = [str(row["leader_wallet"]).lower() for row in rows]
    if not leaders:
        return CopyabilityTruthReconcileSummary(
            0,
            0,
            0,
            0,
            0,
            0,
            summary_score_actions_cleared=summary_actions_cleared,
        )

    stats_deleted = 0
    performance_deleted = 0
    backtest_deleted = 0
    feature_rows = 0
    score_actions_marked = 0
    for chunk in _chunks(leaders):
        placeholders = ",".join("?" for _ in chunk)
        feature_rows += int(
            conn.execute(
                f"SELECT COUNT(*) FROM wallet_features WHERE address IN ({placeholders})",
                tuple(chunk),
            ).fetchone()[0]
        )
        backtest_deleted += _delete_for_leaders(
            conn,
            table="copy_backtest_trades",
            leaders=chunk,
        )
        performance_deleted += _delete_for_leaders(
            conn,
            table="copy_leader_performance",
            leaders=chunk,
        )
        stats_deleted += _delete_for_leaders(
            conn,
            table="copy_leader_stats",
            leaders=chunk,
        )
        clear_copy_graph_features(conn, chunk, now=reconciled_at)
        clear_copy_backtest_features(conn, chunk, now=reconciled_at)
        _set_no_qualified_pair_features(conn, leaders=chunk, now=reconciled_at)
        score_actions_marked += _mark_fresh_score_actions(
            conn,
            leaders=chunk,
            now=reconciled_at,
        )

    return CopyabilityTruthReconcileSummary(
        orphan_leaders=len(leaders),
        leader_stats_deleted=stats_deleted,
        leader_performance_deleted=performance_deleted,
        backtest_trades_deleted=backtest_deleted,
        feature_rows_cleared=feature_rows,
        score_actions_marked=score_actions_marked,
        summary_score_actions_cleared=summary_actions_cleared,
    )


def _mark_fresh_score_actions(
    conn: sqlite3.Connection,
    *,
    leaders: list[str],
    now: int,
) -> int:
    leader_placeholders = ",".join("?" for _ in leaders)
    pending_stage_placeholders = ",".join("?" for _ in PENDING_EVIDENCE_JOB_STAGES)
    pending_status_placeholders = ",".join("?" for _ in PENDING_EVIDENCE_STATUSES)
    cursor = conn.execute(
        f"""
        UPDATE wallet_processing_state
        SET next_action = 'score_wallet',
            next_action_at = 0,
            updated_at = ?
        WHERE wallet IN ({leader_placeholders})
          AND COALESCE(next_action, '') NOT IN ({pending_stage_placeholders})
          AND COALESCE(current_stage, '') NOT IN ({pending_stage_placeholders})
          AND COALESCE(evidence_status, '') NOT IN ({pending_status_placeholders})
          AND NOT EXISTS (
              SELECT 1
              FROM wallet_registry registry
              WHERE registry.address = wallet_processing_state.wallet
                AND registry.raw_retention_tier = 'summary_only'
          )
        """,
        (
            now,
            *leaders,
            *PENDING_EVIDENCE_JOB_STAGES,
            *PENDING_EVIDENCE_JOB_STAGES,
            *PENDING_EVIDENCE_STATUSES,
        ),
    )
    return max(int(cursor.rowcount or 0), 0)


def _clear_unscoreable_truth_actions(
    conn: sqlite3.Connection,
    *,
    now: int,
) -> int:
    cursor = conn.execute(
        """
        UPDATE wallet_processing_state
        SET next_action = '',
            next_action_at = 0,
            updated_at = ?
        WHERE next_action = 'score_wallet'
          AND EXISTS (
              SELECT 1
              FROM wallet_registry registry
              WHERE registry.address = wallet_processing_state.wallet
                AND registry.raw_retention_tier = 'summary_only'
          )
          AND EXISTS (
              SELECT 1
              FROM wallet_features feature
              WHERE feature.address = wallet_processing_state.wallet
                AND COALESCE(
                    json_extract(feature.extra_json, '$.copy_stream_roi_source'),
                    ''
                ) = 'copyability_truth_no_qualified_pair'
          )
        """,
        (now,),
    )
    return max(int(cursor.rowcount or 0), 0)


def _set_no_qualified_pair_features(
    conn: sqlite3.Connection,
    *,
    leaders: list[str],
    now: int,
) -> None:
    """Record an explicit zero sample so scoring cannot preserve stale credit."""

    placeholders = ",".join("?" for _ in leaders)
    conn.execute(
        f"""
        UPDATE wallet_features
        SET leader_in_degree = 0,
            copy_event_count = 0,
            copy_market_count = 0,
            containment_pct_median = 0,
            copy_stream_roi = 0,
            extra_json = json_set(
                CASE
                    WHEN json_valid(COALESCE(extra_json, '{{}}')) THEN extra_json
                    ELSE '{{}}'
                END,
                '$.copy_validated_pair_count',
                0,
                '$.copy_stream_roi_source',
                'copyability_truth_no_qualified_pair'
            ),
            updated_at = ?
        WHERE address IN ({placeholders})
        """,
        (now, *leaders),
    )


def _delete_for_leaders(
    conn: sqlite3.Connection,
    *,
    table: str,
    leaders: list[str],
) -> int:
    placeholders = ",".join("?" for _ in leaders)
    cursor = conn.execute(
        f"DELETE FROM {table} WHERE leader_wallet IN ({placeholders})",
        tuple(leaders),
    )
    return max(int(cursor.rowcount or 0), 0)


def _chunks(values: list[str]) -> Iterator[list[str]]:
    for start in range(0, len(values), SQLITE_IN_CHUNK_SIZE):
        yield values[start : start + SQLITE_IN_CHUNK_SIZE]
