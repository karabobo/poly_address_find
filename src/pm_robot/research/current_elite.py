"""Current ranked elites and independently validated L6 wallets."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable

from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.l6_validation import L6_VALIDATION_POLICY_VERSION
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION


CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS = 14 * 86_400


def current_elite_wallets(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    policy_version: str = SELECTION_POLICY_VERSION,
    wallets: Iterable[str] = (),
) -> set[str]:
    """Return historical L5/L6 wallets current under the active ranking contract."""

    cutoff = (int(time.time()) if now is None else int(now)) - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
    requested = tuple(sorted({str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()}))
    wallet_clause = ""
    params: list[object] = [policy_version, METHODOLOGY_VERSION, cutoff]
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
) -> int:
    """Count current elites using the same contract as the web surface."""

    return len(current_elite_wallets(conn, now=now, policy_version=policy_version))


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
