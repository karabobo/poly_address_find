"""Current research score candidates and independently validated L6 wallets."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any

from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.l6_validation import L6_VALIDATION_POLICY_VERSION
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION


CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS = 14 * 86_400
HIGH_CONFIDENCE_L6_POLICY_VERSION = "high_confidence_l6_v6"
L6_EXECUTION_PROFILE_POLICY_VERSION = "l6_execution_profile_v1"
HIGH_CONFIDENCE_L6_SCHEMA_VERSION = 3
HIGH_CONFIDENCE_L6_SOURCE_NAME = "pm_robot.current_high_confidence_l6"
HANDOFF_STATUS_READY = "ready"
HANDOFF_STATUS_WARMING = "warming"
HANDOFF_STATUS_DEGRADED = "degraded"


def _coerce_handoff_readiness(value: Any, *, candidate_count: int) -> tuple[str, bool, dict[str, Any]]:
    if not isinstance(value, dict):
        return (
            HANDOFF_STATUS_DEGRADED,
            False,
            {
                "runtime_ready": False,
                "research_ready": False,
                "planner_ready": False,
                "source": "not_evaluated",
            },
        )
    runtime = value.get("runtime_readiness")
    research = value.get("research_readiness")
    planner_ready = bool(value.get("planner_ready"))
    runtime_ready = bool(runtime.get("ready")) if isinstance(runtime, dict) else False
    research_ready = bool(research.get("ready")) if isinstance(research, dict) else False
    if not (runtime_ready and research_ready):
        return (
            HANDOFF_STATUS_DEGRADED,
            False,
            {
                "runtime_ready": runtime_ready,
                "research_ready": research_ready,
                "planner_ready": planner_ready,
                "source": "runtime_or_research_failed",
            },
        )
    if not planner_ready:
        return (
            HANDOFF_STATUS_WARMING,
            False,
            {
                "runtime_ready": True,
                "research_ready": True,
                "planner_ready": False,
                "source": "planner_not_ready",
            },
        )
    if candidate_count <= 0:
        # A completed, healthy evaluation with no eligible wallet must revoke
        # replaceability instead of emitting the contradictory ready/empty state.
        return (
            HANDOFF_STATUS_DEGRADED,
            False,
            {
                "runtime_ready": True,
                "research_ready": True,
                "planner_ready": True,
                "source": "candidate_set_empty",
            },
        )
    return (
        HANDOFF_STATUS_READY,
        True,
        {
            "runtime_ready": True,
            "research_ready": True,
            "planner_ready": True,
            "source": "evaluated",
        },
    )


@dataclass(frozen=True)
class HighConfidenceL6Policy:
    """Profit-quality overlay for L6 research; execution difficulty is separate."""

    version: str = HIGH_CONFIDENCE_L6_POLICY_VERSION
    min_evidence_active_weeks: int = 6
    min_official_all_pnl_usdc: float = 500.0
    hard_min_official_profit_intensity: float = 0.002
    hard_max_month_loss_ratio: float = 0.10
    hard_max_month_loss_usdc: float = 100.0
    hard_max_week_loss_ratio: float = 0.05
    hard_max_week_loss_usdc: float = 50.0
    hard_max_drawdown_ratio: float = 0.50
    hard_max_top_market_profit_share: float = 0.60
    hard_max_top_day_profit_share: float = 0.50
    max_market_volume_top_share: float = 0.40
    preferred_active_weeks: int = 8
    preferred_positive_week_ratio: float = 0.70
    preferred_official_profit_intensity: float = 0.005
    preferred_month_pnl_usdc: float = 10.0
    preferred_week_pnl_usdc: float = 1.0
    preferred_max_drawdown_ratio: float = 0.25
    preferred_max_top_market_profit_share: float = 0.35
    preferred_max_top_day_profit_share: float = 0.35
    min_quality_signals: int = 6


@dataclass(frozen=True)
class L6ExecutionProfilePolicy:
    """Describe activity and replication difficulty without changing quality level."""

    version: str = L6_EXECUTION_PROFILE_POLICY_VERSION
    min_recent_active_days: int = 3
    max_last_trade_age_seconds: int = 7 * 86_400
    moderate_fast_market_share: float = 0.10
    difficult_fast_market_share: float = 0.25
    moderate_trades_per_day: float = 10.0
    difficult_trades_per_day: float = 20.0
    moderate_same_signal_trades_10_seconds: int = 6
    max_same_signal_trades_10_seconds: int = 12
    blocked_abnormal_flags: tuple[str, ...] = (
        "extreme_burst_frequency",
        "high_turnover_low_net_flow",
        "highly_regular_trade_timing",
        "mechanical_activity_dominance",
    )
    blocked_strategy_tags: tuple[str, ...] = (
        "high_frequency",
        "fast_market_specialist",
    )


def current_score_candidate_wallets(
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
         AND decision.score_status = 'valid'
         AND decision.forward_selection_score IS NOT NULL
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


def current_score_candidate_wallet_count(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy_version: str = SELECTION_POLICY_VERSION,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
) -> int:
    """Count current score candidates using the same contract as the web surface."""

    return len(
        current_score_candidate_wallets(
            conn,
            now=now,
            policy_version=policy_version,
            validation_policy_version=validation_policy_version,
        )
    )


def current_elite_wallets(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy_version: str = SELECTION_POLICY_VERSION,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> set[str]:
    """Legacy alias for current_score_candidate_wallets."""

    return current_score_candidate_wallets(
        conn,
        now=now,
        policy_version=policy_version,
        validation_policy_version=validation_policy_version,
        wallets=wallets,
    )


def current_elite_wallet_count(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy_version: str = SELECTION_POLICY_VERSION,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
) -> int:
    """Legacy alias for current_score_candidate_wallet_count."""

    return current_score_candidate_wallet_count(
        conn,
        now=now,
        policy_version=policy_version,
        validation_policy_version=validation_policy_version,
    )


def current_valid_l6_wallets(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    quality_policy: HighConfidenceL6Policy | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> set[str]:
    """Return L6 wallets that pass both independent validation and current quality checks."""

    cutoff = (int(time.time()) if now is None else int(now)) - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
    active_quality_policy = quality_policy or HighConfidenceL6Policy()
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
        SELECT
            levels.wallet,
            validation.active_weeks,
            validation.positive_week_ratio,
            validation.official_all_pnl_usdc,
            validation.official_profit_intensity,
            validation.official_month_pnl_usdc,
            validation.official_week_pnl_usdc,
            validation.max_drawdown_ratio,
            validation.top_market_profit_share,
            validation.top_day_profit_share,
            validation.abnormal_flags_json,
            validation.evidence_metrics_json,
            summary.fast_market_share,
            summary.trades_per_day,
            summary.market_volume_top_share,
            summary.strategy_tags_json
        FROM wallet_levels AS levels
        JOIN wallet_history_summaries AS summary
          ON summary.wallet = levels.wallet
        JOIN wallet_level_selections AS selection
          ON selection.wallet = levels.wallet
         AND selection.target_level = 'l5'
         AND selection.evidence_artifact_id = summary.artifact_id
         AND selection.policy_version = ?
         AND selection.selected = 1
         AND selection.score_status = 'valid'
         AND selection.forward_selection_score IS NOT NULL
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
    return {
        str(row["wallet"])
        for row in rows
        if not _current_l6_quality_failures(row, active_quality_policy)
    }


def current_valid_l6_wallet_count(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
) -> int:
    return len(
        current_valid_l6_wallets(
            conn,
            now=now,
            validation_policy_version=validation_policy_version,
            selection_policy_version=selection_policy_version,
        )
    )


def l6_promotion_quality_failures(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    evidence_artifact_id: str,
    validation_id: str,
    now: int,
    quality_policy: HighConfidenceL6Policy | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
) -> tuple[str, ...]:
    """Return current L6 quality failures without requiring an existing L6 label."""

    cutoff = int(now) - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
    row = conn.execute(
        """
        SELECT
            validation.active_weeks,
            validation.positive_week_ratio,
            validation.official_all_pnl_usdc,
            validation.official_profit_intensity,
            validation.official_month_pnl_usdc,
            validation.official_week_pnl_usdc,
            validation.max_drawdown_ratio,
            validation.top_market_profit_share,
            validation.top_day_profit_share,
            validation.abnormal_flags_json,
            validation.evidence_metrics_json,
            summary.fast_market_share,
            summary.trades_per_day,
            summary.market_volume_top_share,
            summary.strategy_tags_json
        FROM wallet_history_summaries AS summary
        JOIN wallet_level_selections AS selection
          ON selection.wallet = summary.wallet
         AND selection.target_level = 'l5'
         AND selection.evidence_artifact_id = summary.artifact_id
         AND selection.policy_version = ?
         AND selection.selected = 1
         AND selection.score_status = 'valid'
         AND selection.forward_selection_score IS NOT NULL
        JOIN wallet_l6_validations AS validation
          ON validation.validation_id = ?
         AND validation.wallet = summary.wallet
         AND validation.evidence_artifact_id = summary.artifact_id
         AND validation.policy_version = ?
         AND validation.decision = 'pass'
        WHERE summary.wallet = ?
          AND summary.artifact_id = ?
          AND summary.history_depth = 'deep'
          AND summary.methodology_version = ?
          AND summary.updated_at >= ?
          AND validation.validated_at >= ?
        """,
        (
            selection_policy_version,
            validation_id,
            validation_policy_version,
            str(wallet).strip().lower(),
            evidence_artifact_id,
            METHODOLOGY_VERSION,
            cutoff,
            cutoff,
        ),
    ).fetchone()
    if row is None:
        return ("current_quality_contract_incomplete",)
    return tuple(_current_l6_quality_failures(row, quality_policy or HighConfidenceL6Policy()))


def current_verified_l6_wallets(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> set[str]:
    """Compatibility alias for current_valid_l6_wallets."""

    return current_valid_l6_wallets(
        conn,
        now=now,
        validation_policy_version=validation_policy_version,
        selection_policy_version=selection_policy_version,
        wallets=wallets,
    )


def current_verified_l6_wallet_count(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
) -> int:
    """Compatibility alias for current_valid_l6_wallet_count."""

    return current_valid_l6_wallet_count(
        conn,
        now=now,
        validation_policy_version=validation_policy_version,
        selection_policy_version=selection_policy_version,
    )


def current_high_confidence_l6_candidates(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy: HighConfidenceL6Policy | None = None,
    execution_policy: L6ExecutionProfilePolicy | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Return current valid L6 research candidates that pass quality checks."""

    active_policy = policy or HighConfidenceL6Policy()
    active_execution_policy = execution_policy or L6ExecutionProfilePolicy()
    snapshot_at = int(time.time()) if now is None else int(now)
    cutoff = snapshot_at - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
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
            validation.evidence_metrics_json,
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
         AND selection.score_status = 'valid'
         AND selection.forward_selection_score IS NOT NULL
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
        quality = _l6_quality_assessment(row, active_policy)
        candidate.update(
            {
                "quality_signals": quality["passed_signals"],
                "quality_signal_gaps": quality["failed_signals"],
                "quality_signals_passed": quality["passed_count"],
                "quality_signals_required": active_policy.min_quality_signals,
            }
        )
        candidate.update(_l6_execution_assessment(row, active_execution_policy))
        candidate["strategy_tags"] = _json_string_list(candidate.pop("strategy_tags_json", "[]"))
        candidate["risk_flags"] = _json_string_list(candidate.pop("risk_flags_json", "[]"))
        candidate["abnormal_flags"] = _json_string_list(candidate.pop("abnormal_flags_json", "[]"))
        if not quality["blocking_failures"]:
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
    execution_policy: L6ExecutionProfilePolicy | None = None,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
    readiness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a versioned research handoff artifact without activation semantics."""

    snapshot_at = int(time.time()) if now is None else int(now)
    active_policy = policy or HighConfidenceL6Policy()
    active_execution_policy = execution_policy or L6ExecutionProfilePolicy()
    candidates = current_high_confidence_l6_candidates(
        conn,
        now=snapshot_at,
        policy=active_policy,
        execution_policy=active_execution_policy,
        validation_policy_version=validation_policy_version,
        selection_policy_version=selection_policy_version,
    )
    handoff_status, replace_active_set_allowed, handoff_readiness = _coerce_handoff_readiness(
        readiness,
        candidate_count=len(candidates),
    )
    snapshot: dict[str, Any] = {
        "schema_version": HIGH_CONFIDENCE_L6_SCHEMA_VERSION,
        "source": HIGH_CONFIDENCE_L6_SOURCE_NAME,
        "source_version": _high_confidence_l6_source_version(
            generated_at=snapshot_at,
            selection_policy_version=selection_policy_version,
            validation_policy_version=validation_policy_version,
            high_confidence_policy=asdict(active_policy),
            execution_profile_policy=asdict(active_execution_policy),
            research_only=True,
            not_for_trading=True,
            handoff_status=handoff_status,
            replace_active_set_allowed=replace_active_set_allowed,
        ),
        "generated_at": snapshot_at,
        "service_scope": "wallet_discovery_research",
        "research_only": True,
        "not_for_trading": True,
        "selection_policy_version": selection_policy_version,
        "validation_policy_version": validation_policy_version,
        "methodology_version": METHODOLOGY_VERSION,
        "high_confidence_policy": asdict(active_policy),
        "execution_profile_policy": asdict(active_execution_policy),
        "evidence_max_age_seconds": CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS,
        "automatic_trading_activation": False,
        "handoff_status": handoff_status,
        "replace_active_set_allowed": replace_active_set_allowed,
        "handoff_readiness": handoff_readiness,
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    snapshot["manifest_checksum"] = current_high_confidence_l6_manifest_checksum(snapshot)
    return snapshot


def current_high_confidence_l6_manifest_checksum(manifest: dict[str, Any]) -> str:
    """Return a SHA-256 over the schema-v3 cross-language canonical JSON.

    The contract recursively sorts object keys, preserves array order, omits only
    ``manifest_checksum``, and writes finite floating-point values in plain
    decimal notation (never an exponent), preserving the fractional zeroes that
    distinguish a JSON float such as ``0.0`` from an integer.
    """

    payload = dict(manifest)
    payload.pop("manifest_checksum", None)
    canonical = _canonical_manifest_json(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _canonical_manifest_json(value: Any) -> str:
    """Serialize JSON values using the schema-v3 producer/consumer contract."""

    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("schema-v3 canonical JSON requires finite numbers")
        decimal_value = Decimal(repr(value))
        if decimal_value.is_zero():
            return "0.0"
        return format(decimal_value, "f")
    if isinstance(value, dict):
        return "{" + ",".join(
            _canonical_manifest_json(str(key)) + ":" + _canonical_manifest_json(value[key])
            for key in sorted(value)
        ) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canonical_manifest_json(item) for item in value) + "]"
    raise TypeError(f"unsupported schema-v3 canonical JSON value: {type(value)!r}")


def _high_confidence_l6_source_version(
    *,
    generated_at: int,
    selection_policy_version: str,
    validation_policy_version: str,
    high_confidence_policy: dict[str, Any],
    execution_profile_policy: dict[str, Any],
    research_only: bool,
    not_for_trading: bool,
    handoff_status: str,
    replace_active_set_allowed: bool,
) -> str:
    """Return a short, deterministic version identifier for one export contract."""

    contract = {
        "schema_version": HIGH_CONFIDENCE_L6_SCHEMA_VERSION,
        "source": HIGH_CONFIDENCE_L6_SOURCE_NAME,
        "generated_at": generated_at,
        "methodology_version": METHODOLOGY_VERSION,
        "selection_policy_version": selection_policy_version,
        "validation_policy_version": validation_policy_version,
        "high_confidence_policy": high_confidence_policy,
        "execution_profile_policy": execution_profile_policy,
        "evidence_max_age_seconds": CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS,
        "service_scope": "wallet_discovery_research",
        "automatic_trading_activation": False,
        "research_only": research_only,
        "not_for_trading": not_for_trading,
        "handoff_status": handoff_status,
        "replace_active_set_allowed": replace_active_set_allowed,
    }
    digest = hashlib.sha256(_canonical_manifest_json(contract).encode("utf-8")).hexdigest()[:16]
    return f"hcl6:v{HIGH_CONFIDENCE_L6_SCHEMA_VERSION}:{generated_at}:{digest}"


def _json_string_list(value: object) -> list[str]:
    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(decoded, list):
        return []
    return [str(item) for item in decoded]


def _json_object(value: object) -> dict[str, Any]:
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _nonnegative_int_metric(metrics: dict[str, Any], name: str) -> int | None:
    value = metrics.get(name)
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _current_l6_quality_failures(
    row: sqlite3.Row,
    policy: HighConfidenceL6Policy,
) -> tuple[str, ...]:
    """Apply the final L6 quality contract to an already passing validation row."""

    assessment = _l6_quality_assessment(row, policy)
    return tuple(assessment["blocking_failures"])


def _l6_quality_assessment(
    row: sqlite3.Row,
    policy: HighConfidenceL6Policy,
) -> dict[str, Any]:
    """Separate hard evidence contradictions from a quorum of preferred signals."""

    active_weeks = int(row["active_weeks"] or 0)
    positive_week_ratio = float(row["positive_week_ratio"] or 0)
    all_pnl_value = row["official_all_pnl_usdc"]
    profit_intensity_value = row["official_profit_intensity"]
    month_pnl_value = row["official_month_pnl_usdc"]
    week_pnl_value = row["official_week_pnl_usdc"]
    all_pnl = float(all_pnl_value) if all_pnl_value is not None else None
    profit_intensity = (
        float(profit_intensity_value) if profit_intensity_value is not None else None
    )
    month_pnl = float(month_pnl_value) if month_pnl_value is not None else None
    week_pnl = float(week_pnl_value) if week_pnl_value is not None else None
    drawdown = float(row["max_drawdown_ratio"] or 0)
    top_market = float(row["top_market_profit_share"] or 0)
    top_day = float(row["top_day_profit_share"] or 0)
    market_volume_top = float(row["market_volume_top_share"] or 0)

    hard_failures: list[str] = []
    if active_weeks < policy.min_evidence_active_weeks:
        hard_failures.append("insufficient_active_week_evidence")
    if all_pnl is None or all_pnl < policy.min_official_all_pnl_usdc:
        hard_failures.append("insufficient_official_all_time_pnl")
    if profit_intensity is None or profit_intensity < policy.hard_min_official_profit_intensity:
        hard_failures.append("official_profit_intensity_below_floor")
    if month_pnl is None or week_pnl is None:
        hard_failures.append("recent_official_pnl_incomplete")
    if all_pnl is not None and month_pnl is not None:
        month_loss_limit = max(
            policy.hard_max_month_loss_usdc,
            max(0.0, all_pnl) * policy.hard_max_month_loss_ratio,
        )
        if month_pnl < -month_loss_limit:
            hard_failures.append("severe_official_month_loss")
    if all_pnl is not None and week_pnl is not None:
        week_loss_limit = max(
            policy.hard_max_week_loss_usdc,
            max(0.0, all_pnl) * policy.hard_max_week_loss_ratio,
        )
        if week_pnl < -week_loss_limit:
            hard_failures.append("severe_official_week_loss")
    if drawdown > policy.hard_max_drawdown_ratio:
        hard_failures.append("extreme_drawdown")
    if top_market > policy.hard_max_top_market_profit_share:
        hard_failures.append("extreme_market_profit_concentration")
    if top_day > policy.hard_max_top_day_profit_share:
        hard_failures.append("extreme_day_profit_concentration")
    if market_volume_top > policy.max_market_volume_top_share:
        hard_failures.append("market_volume_concentration")

    signal_checks = (
        (active_weeks >= policy.preferred_active_weeks, "stable_active_weeks"),
        (
            positive_week_ratio >= policy.preferred_positive_week_ratio,
            "strong_positive_week_ratio",
        ),
        (
            profit_intensity is not None
            and profit_intensity >= policy.preferred_official_profit_intensity,
            "strong_official_profit_intensity",
        ),
        (
            month_pnl is not None and month_pnl >= policy.preferred_month_pnl_usdc,
            "positive_official_month_pnl",
        ),
        (
            week_pnl is not None and week_pnl >= policy.preferred_week_pnl_usdc,
            "positive_official_week_pnl",
        ),
        (drawdown <= policy.preferred_max_drawdown_ratio, "controlled_drawdown"),
        (
            top_market <= policy.preferred_max_top_market_profit_share,
            "diversified_market_profit",
        ),
        (top_day <= policy.preferred_max_top_day_profit_share, "diversified_day_profit"),
    )
    passed_signals = [name for passed, name in signal_checks if passed]
    failed_signals = [name for passed, name in signal_checks if not passed]
    blocking_failures = list(dict.fromkeys(hard_failures))
    if len(passed_signals) < policy.min_quality_signals:
        blocking_failures.append("insufficient_quality_signal_quorum")
    return {
        "blocking_failures": blocking_failures,
        "passed_signals": passed_signals,
        "failed_signals": failed_signals,
        "passed_count": len(passed_signals),
    }


def _l6_execution_assessment(
    row: sqlite3.Row,
    policy: L6ExecutionProfilePolicy,
) -> dict[str, Any]:
    """Classify replication difficulty without affecting the wallet's L6 quality."""

    evidence_metrics = _json_object(row["evidence_metrics_json"])
    recent_active_days = _nonnegative_int_metric(evidence_metrics, "recent_active_days")
    last_trade_age_seconds = _nonnegative_int_metric(evidence_metrics, "last_trade_age_seconds")
    max_same_signal_trades = _nonnegative_int_metric(
        evidence_metrics,
        "max_same_signal_trades_10_seconds",
    )
    if recent_active_days is None or last_trade_age_seconds is None:
        activity_state = "unknown"
    elif (
        recent_active_days >= policy.min_recent_active_days
        and last_trade_age_seconds <= policy.max_last_trade_age_seconds
    ):
        activity_state = "active"
    else:
        activity_state = "inactive"

    flags: list[str] = []
    if recent_active_days is None or last_trade_age_seconds is None or max_same_signal_trades is None:
        flags.append("execution_shape_evidence_missing")
    if recent_active_days is not None and recent_active_days < policy.min_recent_active_days:
        flags.append("insufficient_recent_activity")
    if (
        last_trade_age_seconds is not None
        and last_trade_age_seconds > policy.max_last_trade_age_seconds
    ):
        flags.append("stale_execution_activity")

    fast_market_share = float(row["fast_market_share"] or 0)
    trades_per_day = float(row["trades_per_day"] or 0)
    difficult = False
    moderate = False
    if max_same_signal_trades is not None:
        if max_same_signal_trades > policy.max_same_signal_trades_10_seconds:
            flags.append("burst_execution_pattern")
            difficult = True
        elif max_same_signal_trades > policy.moderate_same_signal_trades_10_seconds:
            flags.append("moderate_signal_burst")
            moderate = True
    if fast_market_share > policy.difficult_fast_market_share:
        flags.append("fast_market_dominance")
        difficult = True
    elif fast_market_share > policy.moderate_fast_market_share:
        flags.append("moderate_fast_market_share")
        moderate = True
    if trades_per_day > policy.difficult_trades_per_day:
        flags.append("excessive_trade_rate")
        difficult = True
    elif trades_per_day > policy.moderate_trades_per_day:
        flags.append("moderate_trade_rate")
        moderate = True

    abnormal_flags = set(_json_string_list(row["abnormal_flags_json"]))
    if abnormal_flags.intersection(policy.blocked_abnormal_flags):
        flags.append("execution_pattern_risk")
        difficult = True
    strategy_tags = set(_json_string_list(row["strategy_tags_json"]))
    if strategy_tags.intersection(policy.blocked_strategy_tags):
        flags.append("non_copyable_strategy_shape")
        difficult = True

    if difficult:
        execution_profile = "difficult"
    elif moderate:
        execution_profile = "moderate"
    elif "execution_shape_evidence_missing" in flags:
        execution_profile = "not_assessed"
    else:
        execution_profile = "easy"
    return {
        "activity_state": activity_state,
        "execution_profile": execution_profile,
        "execution_flags": list(dict.fromkeys(flags)),
    }
