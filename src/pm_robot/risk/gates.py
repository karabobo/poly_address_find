"""Risk and eligibility gates derived from the papers."""

from __future__ import annotations

import sqlite3
from typing import Any

from pm_robot.config import threshold
from pm_robot.models import WalletFeatures


HYGIENE_BLOCKS = {"routing_operator", "wash", "wash_trade", "market_maker_taker"}
HYGIENE_INCOMPLETE = {"", "unknown", "incomplete", "unverified"}
MIN_STABLE_READY_OBSERVATIONS = 3
MIN_STABLE_READY_SPAN_SECONDS = 3600


def hygiene_block_reason(features: WalletFeatures, policy: dict[str, Any]) -> str | None:
    status = (features.hygiene_status or "").strip().lower()
    if status in HYGIENE_INCOMPLETE:
        return "hygiene_evidence_incomplete"
    if status in HYGIENE_BLOCKS:
        return f"hygiene_status={status}"
    maker_source = str(features.extra.get("maker_fraction_source") or "")
    if (
        features.maker_fraction is not None
        and maker_source != "public_activity_no_maker_flags_observed"
        and features.maker_fraction > threshold(
            policy, "max_maker_fraction_for_directional_leader", 0.5
        )
    ):
        return "maker_fraction_above_directional_threshold"
    if (features.bot_score or 0.0) >= threshold(policy, "pure_bot_score", 80):
        return "pure_bot_or_hft"
    if features.trades_per_day and features.trades_per_day >= threshold(policy, "max_hft_trades_per_day", 500):
        return "hft_trades_per_day"
    if (
        features.trades_per_day
        and features.trades_per_day >= 100
        and features.median_gap_sec is not None
        and features.median_gap_sec <= threshold(policy, "max_hft_median_gap_sec", 5)
    ):
        return "hft_median_gap"
    fast_market_share = _extra_float(features.extra, "feature_materializer_fast_market_share")
    activity_count = _extra_float(features.extra, "feature_materializer_activity_count")
    if activity_count >= 50 and fast_market_share >= 0.85:
        return "fast_market_dominant"
    if features.single_market_pnl_share and features.single_market_pnl_share > threshold(
        policy, "max_single_market_pnl_share", 0.5
    ):
        return "single_market_pnl_concentration"
    return None


def _extra_float(extra: dict[str, Any], key: str) -> float:
    try:
        return float(extra.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def hedge_block_reason(features: WalletFeatures, policy: dict[str, Any]) -> str | None:
    net_to_gross = features.net_to_gross_exposure
    if net_to_gross is not None and net_to_gross < threshold(
        policy, "min_net_to_gross_exposure", 0.35
    ):
        return "hedge_or_arbitrage_exposure_too_low"
    return None


def live_eligibility_reason(features: WalletFeatures, policy: dict[str, Any]) -> str | None:
    """Return None when a wallet has enough evidence for live eligibility."""
    if hygiene_block_reason(features, policy):
        return "hygiene_block"
    if hedge_block_reason(features, policy):
        return "copyability_block"
    if features.edge_retention_pct is None or features.edge_retention_pct < 60:
        return "insufficient_edge_retention"
    if features.walk_forward_consistency_pct is None or features.walk_forward_consistency_pct < 55:
        return "insufficient_walk_forward_consistency"
    if features.copy_event_count is not None and features.copy_event_count < threshold(policy, "min_copy_events", 5):
        return "copy_event_sample_too_small"
    return None


def stable_readiness_status(conn: sqlite3.Connection, wallet: str) -> dict[str, Any]:
    if not _table_exists(conn, "paper_readiness_observations"):
        return {
            "stable_ready_observations": 0,
            "stable_observation_count": 0,
            "stable_ready_span_seconds": 0,
            "stable_production_ready": 0,
        }
    rows = conn.execute(
        """
        SELECT pro.observed_at, pro.production_ready
        FROM paper_readiness_observations pro
        WHERE pro.wallet = ?
          AND pro.production_ready = 1
          AND NOT EXISTS (
              SELECT 1
              FROM paper_readiness_observations newer_bad
              WHERE newer_bad.wallet = pro.wallet
                AND newer_bad.production_ready = 0
                AND (
                    newer_bad.observed_at > pro.observed_at
                    OR (
                        newer_bad.observed_at = pro.observed_at
                        AND newer_bad.observation_id > pro.observation_id
                    )
                )
          )
        ORDER BY pro.observed_at DESC, pro.observation_id DESC
        """,
        (wallet.lower(),),
    ).fetchall()
    observations = len(rows)
    if rows:
        observed_values = [int(row["observed_at"]) for row in rows]
        span_seconds = max(observed_values) - min(observed_values)
    else:
        span_seconds = 0
    stable_ready = (
        observations >= MIN_STABLE_READY_OBSERVATIONS
        and span_seconds >= MIN_STABLE_READY_SPAN_SECONDS
    )
    return {
        "stable_ready_observations": observations,
        "stable_observation_count": observations,
        "stable_ready_span_seconds": span_seconds,
        "stable_production_ready": 1 if stable_ready else 0,
    }


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None
