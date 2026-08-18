"""Reconcile historical L6 labels against the current evidence contract."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.research.current_elite import (
    CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS,
    HIGH_CONFIDENCE_L6_POLICY_VERSION,
    current_score_candidate_wallets,
    current_valid_l6_wallets,
    l6_promotion_quality_failures,
)
from pm_robot.research.l6_validation import L6_VALIDATION_POLICY_VERSION
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.wallet_levels import advance_wallet_level, reclassify_wallet_level
from pm_robot.wallet_levels import WalletLevel


L6_RECONCILIATION_POLICY_VERSION = "l6_current_reconciliation_v1"


@dataclass(frozen=True)
class L6LevelReconciliationSummary:
    historical_l6: int
    current_valid_l6: int
    retained_l6: int
    reclassified_l5: int
    reclassified_l2: int
    promoted_l6: int
    dry_run: bool
    status: str


def reconcile_historical_l6_levels(
    conn: sqlite3.Connection,
    *,
    limit: int = 500,
    dry_run: bool = False,
    now: int | None = None,
) -> L6LevelReconciliationSummary:
    """Keep current L6 only; lower stale labels without deleting source evidence."""

    ts = int(time.time()) if now is None else int(now)
    rows = conn.execute(
        "SELECT wallet FROM wallet_levels WHERE level = 'l6' ORDER BY wallet"
    ).fetchall()
    historical = {str(row[0]) for row in rows}
    valid = current_valid_l6_wallets(conn, now=ts, wallets=historical)
    score_candidates = current_score_candidate_wallets(conn, now=ts, wallets=historical)
    stale = sorted(historical - valid)[: max(0, int(limit))]
    l5_count = 0
    l2_count = 0
    for wallet in stale:
        target = WalletLevel.L5 if wallet in score_candidates else WalletLevel.L2
        reason = (
            "l6_current_quality_failed"
            if target is WalletLevel.L5
            else "l6_current_evidence_stale"
        )
        if not dry_run:
            reclassify_wallet_level(
                conn,
                wallet,
                to_level=target,
                reason=reason,
                policy_version=L6_RECONCILIATION_POLICY_VERSION,
                facts={
                    "selection_policy_version": SELECTION_POLICY_VERSION,
                    "validation_policy_version": L6_VALIDATION_POLICY_VERSION,
                    "current_valid_l6": False,
                    "current_score_candidate": wallet in score_candidates,
                },
                now=ts,
            )
        if target is WalletLevel.L5:
            l5_count += 1
        else:
            l2_count += 1

    cutoff = ts - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
    promotion_rows = conn.execute(
        """
        SELECT levels.wallet, summary.artifact_id, validation.validation_id
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
        WHERE levels.level = 'l5'
          AND levels.hard_risk_block = 0
          AND summary.history_depth = 'deep'
          AND summary.methodology_version = ?
          AND summary.updated_at >= ?
          AND validation.decision = 'pass'
          AND validation.evidence_artifact_id = summary.artifact_id
          AND validation.validated_at >= ?
        ORDER BY selection.forward_selection_score DESC, levels.wallet
        LIMIT ?
        """,
        (
            SELECTION_POLICY_VERSION,
            L6_VALIDATION_POLICY_VERSION,
            METHODOLOGY_VERSION,
            cutoff,
            cutoff,
            max(0, int(limit)),
        ),
    ).fetchall()
    promoted = 0
    for row in promotion_rows:
        wallet = str(row["wallet"])
        artifact_id = str(row["artifact_id"])
        validation_id = str(row["validation_id"])
        failures = l6_promotion_quality_failures(
            conn,
            wallet=wallet,
            evidence_artifact_id=artifact_id,
            validation_id=validation_id,
            now=ts,
        )
        if failures:
            continue
        if not dry_run:
            advance_wallet_level(
                conn,
                wallet,
                to_level=WalletLevel.L6,
                reason="l6_current_quality_restored",
                policy_version=L6_RECONCILIATION_POLICY_VERSION,
                facts={
                    "validation_id": validation_id,
                    "evidence_artifact_id": artifact_id,
                    "quality_policy_version": HIGH_CONFIDENCE_L6_POLICY_VERSION,
                    "execution_profile_affects_quality": False,
                },
                now=ts,
            )
        promoted += 1
    return L6LevelReconciliationSummary(
        historical_l6=len(historical),
        current_valid_l6=len(valid),
        retained_l6=len(valid),
        reclassified_l5=l5_count,
        reclassified_l2=l2_count,
        promoted_l6=promoted,
        dry_run=bool(dry_run),
        status="ok",
    )
