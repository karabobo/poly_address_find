"""Current ranked elites and independently validated L6 wallets."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any

from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.l6_validation import L6_VALIDATION_POLICY_VERSION
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION


CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS = 14 * 86_400
HIGH_CONFIDENCE_L6_POLICY_VERSION = "high_confidence_l6_v1"


@dataclass(frozen=True)
class HighConfidenceL6Policy:
    """Current-quality overlay for research handoff; it never enables trading."""

    version: str = HIGH_CONFIDENCE_L6_POLICY_VERSION
    min_active_weeks: int = 8
    min_positive_week_ratio: float = 0.75
    min_official_month_pnl_usdc: float = 0.0
    min_official_week_pnl_usdc: float = 0.0
    max_drawdown_ratio: float = 0.25
    max_top_market_profit_share: float = 0.35
    max_top_day_profit_share: float = 0.35


def current_elite_wallets(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy_version: str = SELECTION_POLICY_VERSION,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> set[str]:
    """Return current score candidates, excluding same-evidence validation failures."""

    cutoff = (int(time.time()) if now is None else int(now)) - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
    requested = tuple(sorted({str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()}))
    wallet_clause = ""
    params: list[object] = [
        policy_version,
        METHODOLOGY_VERSION,
        cutoff,
        validation_policy_version,
    ]
    if requested:
        wallet_clause = f" AND levels.wallet IN ({','.join('?' for _ in requested)})"
        params.extend(requested)
    rows = conn.execute(
        f"""
        SELECT DISTINCT levels.wallet
        FROM wallet_levels AS levels
        JOIN wallet_history_summaries AS summary
          ON summary.wallet = levels.wallet
        JOIN wallet_level_selections AS decision
          ON decision.wallet = levels.wallet
         AND decision.target_level = 'l5'
         AND decision.evidence_artifact_id = summary.artifact_id
         AND decision.policy_version = ?
         AND decision.selected = 1
        WHERE levels.level IN ('l5', 'l6')
          AND levels.hard_risk_block = 0
          AND summary.history_depth = 'deep'
          AND summary.methodology_version = ?
          AND summary.updated_at >= ?
          AND NOT EXISTS (
              SELECT 1
              FROM wallet_l6_validations AS validation
              WHERE validation.validation_id = (
                  SELECT latest.validation_id
                  FROM wallet_l6_validations AS latest
                  WHERE latest.wallet = levels.wallet
                    AND latest.policy_version = ?
                  ORDER BY latest.validated_at DESC, latest.validation_id DESC
                  LIMIT 1
              )
                AND validation.evidence_artifact_id = summary.artifact_id
                AND validation.decision = 'fail'
          )
          {wallet_clause}
        """,
        tuple(params),
    ).fetchall()
    return {str(row[0]) for row in rows}


def current_elite_wallet_count(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy_version: str = SELECTION_POLICY_VERSION,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
) -> int:
    """Count current elites using the same contract as the web surface."""

    return len(
        current_elite_wallets(
            conn,
            now=now,
            policy_version=policy_version,
            validation_policy_version=validation_policy_version,
        )
    )


def current_verified_l6_wallets(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> set[str]:
    """Return L6 wallets current under both ranking and independent validation contracts."""

    cutoff = (int(time.time()) if now is None else int(now)) - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
    requested = tuple(sorted({str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()}))
    wallet_clause = ""
    params: list[object] = [
        selection_policy_version,
        validation_policy_version,
        METHODOLOGY_VERSION,
        cutoff,
        cutoff,
    ]
    if requested:
        wallet_clause = f" AND levels.wallet IN ({','.join('?' for _ in requested)})"
        params.extend(requested)
    rows = conn.execute(
        f"""
        SELECT levels.wallet
        FROM wallet_levels AS levels
        JOIN wallet_history_summaries AS summary
          ON summary.wallet = levels.wallet
        JOIN wallet_level_selections AS selection
          ON selection.wallet = levels.wallet
         AND selection.target_level = 'l5'
         AND selection.evidence_artifact_id = summary.artifact_id
         AND selection.policy_version = ?
         AND selection.selected = 1
        JOIN wallet_l6_validations AS validation
          ON validation.validation_id = (
              SELECT latest.validation_id
              FROM wallet_l6_validations AS latest
              WHERE latest.wallet = levels.wallet
                AND latest.policy_version = ?
              ORDER BY latest.validated_at DESC, latest.validation_id DESC
              LIMIT 1
          )
        WHERE levels.level = 'l6'
          AND levels.hard_risk_block = 0
          AND summary.history_depth = 'deep'
          AND summary.methodology_version = ?
          AND summary.updated_at >= ?
          AND validation.decision = 'pass'
          AND validation.evidence_artifact_id = summary.artifact_id
          AND validation.validated_at >= ?
          {wallet_clause}
        """,
        tuple(params),
    ).fetchall()
    return {str(row[0]) for row in rows}


def current_verified_l6_wallet_count(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
) -> int:
    return len(
        current_verified_l6_wallets(
            conn,
            now=now,
            validation_policy_version=validation_policy_version,
            selection_policy_version=selection_policy_version,
        )
    )


def current_high_confidence_l6_candidates(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy: HighConfidenceL6Policy | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Return current L6 research candidates that also pass recent-quality checks."""

    active_policy = policy or HighConfidenceL6Policy()
    cutoff = (int(time.time()) if now is None else int(now)) - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
    requested = tuple(sorted({str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()}))
    wallet_clause = ""
    params: list[object] = [
        selection_policy_version,
        validation_policy_version,
        METHODOLOGY_VERSION,
        cutoff,
        cutoff,
        active_policy.min_active_weeks,
        active_policy.min_positive_week_ratio,
        active_policy.min_official_month_pnl_usdc,
        active_policy.min_official_week_pnl_usdc,
        active_policy.max_drawdown_ratio,
        active_policy.max_top_market_profit_share,
        active_policy.max_top_day_profit_share,
    ]
    if requested:
        wallet_clause = f" AND levels.wallet IN ({','.join('?' for _ in requested)})"
        params.extend(requested)
    cursor = conn.execute(
        f"""
        SELECT
            levels.wallet,
            summary.artifact_id AS evidence_artifact_id,
            summary.research_score,
            summary.activity_count,
            summary.distinct_markets,
            summary.total_volume_usdc,
            summary.fast_market_share,
            summary.trades_per_day,
            summary.market_volume_top_share,
            summary.strategy_tags_json,
            summary.risk_flags_json,
            summary.updated_at AS evidence_updated_at,
            validation.validated_at,
            validation.active_weeks,
            validation.positive_week_ratio,
            validation.realized_pnl_usdc,
            validation.recent_realized_pnl_usdc,
            validation.max_drawdown_ratio,
            validation.top_market_profit_share,
            validation.top_day_profit_share,
            validation.official_all_pnl_usdc,
            validation.official_all_volume_usdc,
            validation.official_profit_intensity,
            validation.official_month_pnl_usdc,
            validation.official_week_pnl_usdc,
            validation.abnormal_flags_json
        FROM wallet_levels AS levels
        JOIN wallet_history_summaries AS summary
          ON summary.wallet = levels.wallet
        JOIN wallet_level_selections AS selection
          ON selection.wallet = levels.wallet
         AND selection.target_level = 'l5'
         AND selection.evidence_artifact_id = summary.artifact_id
         AND selection.policy_version = ?
         AND selection.selected = 1
        JOIN wallet_l6_validations AS validation
          ON validation.validation_id = (
              SELECT latest.validation_id
              FROM wallet_l6_validations AS latest
              WHERE latest.wallet = levels.wallet
                AND latest.policy_version = ?
              ORDER BY latest.validated_at DESC, latest.validation_id DESC
              LIMIT 1
          )
        WHERE levels.level = 'l6'
          AND levels.hard_risk_block = 0
          AND summary.history_depth = 'deep'
          AND summary.methodology_version = ?
          AND summary.updated_at >= ?
          AND validation.decision = 'pass'
          AND validation.evidence_artifact_id = summary.artifact_id
          AND validation.validated_at >= ?
          AND validation.active_weeks >= ?
          AND validation.positive_week_ratio >= ?
          AND validation.official_month_pnl_usdc > ?
          AND validation.official_week_pnl_usdc > ?
          AND validation.max_drawdown_ratio <= ?
          AND validation.top_market_profit_share <= ?
          AND validation.top_day_profit_share <= ?
          {wallet_clause}
        ORDER BY
            summary.research_score DESC,
            validation.positive_week_ratio DESC,
            validation.official_month_pnl_usdc DESC,
            levels.wallet
        """,
        tuple(params),
    )
    columns = [str(item[0]) for item in cursor.description or ()]
    candidates: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        candidate = dict(zip(columns, row))
        candidate["strategy_tags"] = _json_string_list(candidate.pop("strategy_tags_json", "[]"))
        candidate["risk_flags"] = _json_string_list(candidate.pop("risk_flags_json", "[]"))
        candidate["abnormal_flags"] = _json_string_list(candidate.pop("abnormal_flags_json", "[]"))
        candidates.append(candidate)
    return candidates


def current_high_confidence_l6_wallets(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy: HighConfidenceL6Policy | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> set[str]:
    """Return only wallet addresses from the high-confidence L6 research set."""

    return {
        str(row["wallet"])
        for row in current_high_confidence_l6_candidates(
            conn,
            now=now,
            policy=policy,
            validation_policy_version=validation_policy_version,
            selection_policy_version=selection_policy_version,
            wallets=wallets,
        )
    }


def current_high_confidence_l6_wallet_count(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy: HighConfidenceL6Policy | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
) -> int:
    """Count current high-confidence L6 wallets under the explicit overlay."""

    return len(
        current_high_confidence_l6_candidates(
            conn,
            now=now,
            policy=policy,
            validation_policy_version=validation_policy_version,
            selection_policy_version=selection_policy_version,
        )
    )


def current_high_confidence_l6_snapshot(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy: HighConfidenceL6Policy | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
) -> dict[str, Any]:
    """Build a versioned research handoff artifact without activation semantics."""

    snapshot_at = int(time.time()) if now is None else int(now)
    active_policy = policy or HighConfidenceL6Policy()
    candidates = current_high_confidence_l6_candidates(
        conn,
        now=snapshot_at,
        policy=active_policy,
        validation_policy_version=validation_policy_version,
        selection_policy_version=selection_policy_version,
    )
    return {
        "schema_version": 1,
        "generated_at": snapshot_at,
        "service_scope": "wallet_discovery_research",
        "selection_policy_version": selection_policy_version,
        "validation_policy_version": validation_policy_version,
        "high_confidence_policy": asdict(active_policy),
        "evidence_max_age_seconds": CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS,
        "automatic_trading_activation": False,
        "candidate_count": len(candidates),
        "candidates": candidates,
    }


def _json_string_list(value: object) -> list[str]:
    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(decoded, list):
        return []
    return [str(item) for item in decoded]
