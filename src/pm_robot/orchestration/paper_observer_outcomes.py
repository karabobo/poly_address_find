"""Research-only outcome tracking for actionable paper observer quotes."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.execution.market_marks import gamma_market_mark
from pm_robot.pipeline_terms import EXPLORATORY_COPYABILITY_COHORT


@dataclass(frozen=True)
class PaperObserverSettlementSummary:
    generated_at: int
    trials_seen: int
    trials_marked: int
    trials_resolved: int
    missing_market_cache: int
    missing_asset_mark: int
    total_trials: int
    open_trials: int
    resolved_trials: int
    wallets: int
    market_samples: int
    open_markets: int
    resolved_markets: int
    winning_markets: int
    marked_pnl_usd: float
    marked_roi_pct: float
    settled_pnl_usd: float
    settled_roi_pct: float
    win_rate_pct: float
    trial_win_rate_pct: float
    max_market_cost_share_pct: float
    validation_policy: dict[str, Any]
    validation_counts: dict[str, int]
    wallet_summaries: list[dict[str, Any]]


@dataclass(frozen=True)
class ObserverValidationThresholds:
    version: str = "observer_market_sample_v1"
    min_resolved_markets: int = 20
    min_settled_cost_usd: float = 500.0
    min_promising_roi_pct: float = 3.0
    max_negative_roi_pct: float = -5.0
    max_market_cost_share_pct: float = 25.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "min_resolved_markets": self.min_resolved_markets,
            "min_settled_cost_usd": self.min_settled_cost_usd,
            "min_promising_roi_pct": self.min_promising_roi_pct,
            "max_negative_roi_pct": self.max_negative_roi_pct,
            "max_market_cost_share_pct": self.max_market_cost_share_pct,
        }


def settle_paper_observer_trials(
    conn: sqlite3.Connection,
    *,
    limit: int = 1_000,
    now: int | None = None,
    policy: dict[str, Any] | None = None,
) -> PaperObserverSettlementSummary:
    """Mark or resolve research trials without creating orders, fills, or positions."""

    generated_at = int(time.time()) if now is None else int(now)
    safe_limit = min(max(int(limit), 1), 10_000)
    rows = conn.execute(
        """
        SELECT *
        FROM paper_observer_trials
        WHERE status = 'open'
        ORDER BY entry_evaluated_at ASC, signal_id ASC
        LIMIT ?
        """,
        (safe_limit,),
    ).fetchall()
    market_cache: dict[str, sqlite3.Row | None] = {}
    trials_marked = 0
    trials_resolved = 0
    missing_market_cache = 0
    missing_asset_mark = 0

    for trial in rows:
        market_slug = str(trial["market_slug"] or "")
        if market_slug not in market_cache:
            market_cache[market_slug] = conn.execute(
                """
                SELECT *
                FROM gamma_market_cache
                WHERE market_slug = ?
                ORDER BY fetched_at DESC
                LIMIT 1
                """,
                (market_slug,),
            ).fetchone()
        market = market_cache[market_slug]
        if market is None:
            missing_market_cache += 1
            continue
        mark = gamma_market_mark(market, str(trial["asset_id"] or ""))
        if mark is None:
            missing_asset_mark += 1
            continue

        mark_value = float(trial["shares"] or 0) * mark.price
        pnl = mark_value - float(trial["cost_usd"] or 0)
        cost = float(trial["cost_usd"] or 0)
        roi = pnl / cost if cost > 0 else 0.0
        status = "resolved" if mark.is_settlement else "open"
        resolved_at = generated_at if mark.is_settlement else None
        conn.execute(
            """
            UPDATE paper_observer_trials
            SET status = ?,
                mark_price = ?,
                mark_source = ?,
                mark_value_usd = ?,
                pnl_usd = ?,
                roi = ?,
                resolved_at = ?,
                updated_at = ?
            WHERE signal_id = ?
              AND status = 'open'
            """,
            (
                status,
                mark.price,
                mark.source,
                mark_value,
                pnl,
                roi,
                resolved_at,
                generated_at,
                trial["signal_id"],
            ),
        )
        trials_marked += 1
        if mark.is_settlement:
            trials_resolved += 1

    conn.commit()
    summary = paper_observer_trial_summary(conn, policy=policy)
    return PaperObserverSettlementSummary(
        generated_at=generated_at,
        trials_seen=len(rows),
        trials_marked=trials_marked,
        trials_resolved=trials_resolved,
        missing_market_cache=missing_market_cache,
        missing_asset_mark=missing_asset_mark,
        total_trials=int(summary.get("total_trials") or 0),
        open_trials=int(summary.get("open_trials") or 0),
        resolved_trials=int(summary.get("resolved_trials") or 0),
        wallets=int(summary.get("wallets") or 0),
        market_samples=int(summary.get("market_samples") or 0),
        open_markets=int(summary.get("open_markets") or 0),
        resolved_markets=int(summary.get("resolved_markets") or 0),
        winning_markets=int(summary.get("winning_markets") or 0),
        marked_pnl_usd=float(summary.get("marked_pnl_usd") or 0),
        marked_roi_pct=float(summary.get("marked_roi_pct") or 0),
        settled_pnl_usd=float(summary.get("settled_pnl_usd") or 0),
        settled_roi_pct=float(summary.get("settled_roi_pct") or 0),
        win_rate_pct=float(summary.get("win_rate_pct") or 0),
        trial_win_rate_pct=float(summary.get("trial_win_rate_pct") or 0),
        max_market_cost_share_pct=float(summary.get("max_market_cost_share_pct") or 0),
        validation_policy=dict(summary.get("validation_policy") or {}),
        validation_counts=dict(summary.get("validation_counts") or {}),
        wallet_summaries=list(summary.get("wallet_summaries") or []),
    )


def paper_observer_trial_summary(
    conn: sqlite3.Connection,
    *,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize trials by independent wallet-market samples without changing stages."""

    thresholds = observer_validation_thresholds(policy)
    if not _table_exists(conn, "paper_observer_trials"):
        return {
            "available": False,
            "total_trials": 0,
            "open_trials": 0,
            "resolved_trials": 0,
            "wallets": 0,
            "market_samples": 0,
            "open_markets": 0,
            "resolved_markets": 0,
            "winning_markets": 0,
            "validation_policy": thresholds.as_dict(),
            "validation_counts": {},
            "wallet_summaries": [],
            "exploratory_copyability": {
                "available": False,
                "total_trials": 0,
                "wallets": 0,
                "wallet_summaries": [],
            },
        }
    validation = _cohort_trial_summary(conn, thresholds=thresholds, cohort="validation")
    exploratory = _cohort_trial_summary(
        conn,
        thresholds=thresholds,
        cohort=EXPLORATORY_COPYABILITY_COHORT,
    )
    return {
        "available": True,
        **validation,
        "validation_policy": thresholds.as_dict(),
        "exploratory_copyability": {
            "available": True,
            **exploratory,
            "validation_policy": thresholds.as_dict(),
            "counts_toward_formal_validation": False,
        },
    }


def _cohort_trial_summary(
    conn: sqlite3.Connection,
    *,
    thresholds: ObserverValidationThresholds,
    cohort: str,
) -> dict[str, Any]:
    """Summarize one observer cohort so exploratory rows cannot affect formal gates."""

    rows = conn.execute(
        """
        SELECT
            wallet,
            market_slug,
            COUNT(*) AS total_trials,
            SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_trials,
            SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_trials,
            SUM(CASE WHEN mark_price IS NOT NULL THEN 1 ELSE 0 END) AS marked_trials,
            SUM(CASE WHEN status = 'resolved' AND pnl_usd > 0 THEN 1 ELSE 0 END) AS trial_wins,
            SUM(CASE WHEN mark_price IS NOT NULL THEN cost_usd ELSE 0 END) AS marked_cost_usd,
            SUM(CASE WHEN mark_price IS NOT NULL THEN pnl_usd ELSE 0 END) AS marked_pnl_usd,
            SUM(CASE WHEN status = 'resolved' THEN cost_usd ELSE 0 END) AS settled_cost_usd,
            SUM(CASE WHEN status = 'resolved' THEN pnl_usd ELSE 0 END) AS settled_pnl_usd,
            MAX(updated_at) AS latest_updated_at
        FROM paper_observer_trials
        WHERE validation_cohort = ?
        GROUP BY wallet, market_slug
        ORDER BY wallet ASC, market_slug ASC
        """,
        (cohort,),
    ).fetchall()
    market_rows = [dict(row) for row in rows]
    summary = _summarize_market_samples(market_rows)
    by_wallet: dict[str, list[dict[str, Any]]] = {}
    for row in market_rows:
        by_wallet.setdefault(str(row.get("wallet") or ""), []).append(row)
    wallet_summaries = []
    validation_counts: dict[str, int] = {}
    for wallet, wallet_rows in by_wallet.items():
        wallet_summary = {
            "wallet": wallet,
            "validation_cohort": cohort,
            **_summarize_market_samples(wallet_rows),
        }
        wallet_summary.update(_observer_validation(wallet_summary, thresholds))
        status = str(wallet_summary["validation_status"])
        validation_counts[status] = validation_counts.get(status, 0) + 1
        wallet_summaries.append(wallet_summary)
    wallet_summaries.sort(
        key=lambda item: (
            -int(item.get("resolved_markets") or 0),
            -float(item.get("settled_roi_pct") or 0),
            str(item.get("wallet") or ""),
        )
    )
    return {
        **summary,
        "wallets": len(by_wallet),
        "validation_counts": validation_counts,
        "wallet_summaries": wallet_summaries,
    }


def observer_validation_thresholds(policy: dict[str, Any] | None) -> ObserverValidationThresholds:
    section = policy.get("observer_validation", {}) if isinstance(policy, dict) else {}
    if not isinstance(section, dict):
        section = {}
    defaults = ObserverValidationThresholds()
    return ObserverValidationThresholds(
        version=str(section.get("version") or defaults.version),
        min_resolved_markets=max(
            1,
            _safe_int(section.get("min_resolved_markets"), defaults.min_resolved_markets),
        ),
        min_settled_cost_usd=max(
            0.0,
            _safe_float(section.get("min_settled_cost_usd"), defaults.min_settled_cost_usd),
        ),
        min_promising_roi_pct=_safe_float(
            section.get("min_promising_roi_pct"),
            defaults.min_promising_roi_pct,
        ),
        max_negative_roi_pct=_safe_float(
            section.get("max_negative_roi_pct"),
            defaults.max_negative_roi_pct,
        ),
        max_market_cost_share_pct=min(
            100.0,
            max(
                0.0,
                _safe_float(
                    section.get("max_market_cost_share_pct"),
                    defaults.max_market_cost_share_pct,
                ),
            ),
        ),
    )


def _summarize_market_samples(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_trials = sum(int(row.get("total_trials") or 0) for row in rows)
    open_trials = sum(int(row.get("open_trials") or 0) for row in rows)
    resolved_trials = sum(int(row.get("resolved_trials") or 0) for row in rows)
    marked_trials = sum(int(row.get("marked_trials") or 0) for row in rows)
    trial_wins = sum(int(row.get("trial_wins") or 0) for row in rows)
    marked_cost = sum(float(row.get("marked_cost_usd") or 0) for row in rows)
    marked_pnl = sum(float(row.get("marked_pnl_usd") or 0) for row in rows)
    resolved_rows = [
        row
        for row in rows
        if int(row.get("open_trials") or 0) == 0 and int(row.get("resolved_trials") or 0) > 0
    ]
    # A wallet-market sample is final only after every trial in that market resolves.
    settled_cost = sum(float(row.get("settled_cost_usd") or 0) for row in resolved_rows)
    settled_pnl = sum(float(row.get("settled_pnl_usd") or 0) for row in resolved_rows)
    winning_markets = sum(
        1 for row in resolved_rows if float(row.get("settled_pnl_usd") or 0) > 0
    )
    max_market_cost = max(
        (float(row.get("settled_cost_usd") or 0) for row in resolved_rows),
        default=0.0,
    )
    return {
        "total_trials": total_trials,
        "open_trials": open_trials,
        "resolved_trials": resolved_trials,
        "marked_trials": marked_trials,
        "trial_wins": trial_wins,
        "trial_win_rate_pct": _ratio_pct(trial_wins, resolved_trials),
        "market_samples": len(rows),
        "open_markets": sum(1 for row in rows if int(row.get("open_trials") or 0) > 0),
        "resolved_markets": len(resolved_rows),
        "winning_markets": winning_markets,
        "wins": winning_markets,
        "win_rate_pct": _ratio_pct(winning_markets, len(resolved_rows)),
        "marked_cost_usd": round(marked_cost, 6),
        "marked_pnl_usd": round(marked_pnl, 6),
        "marked_roi_pct": _ratio_pct(marked_pnl, marked_cost),
        "settled_cost_usd": round(settled_cost, 6),
        "settled_pnl_usd": round(settled_pnl, 6),
        "settled_roi_pct": _ratio_pct(settled_pnl, settled_cost),
        "max_market_cost_share_pct": _ratio_pct(max_market_cost, settled_cost),
        "latest_updated_at": max(
            (int(row.get("latest_updated_at") or 0) for row in rows),
            default=0,
        ),
    }


def _observer_validation(
    summary: dict[str, Any],
    thresholds: ObserverValidationThresholds,
) -> dict[str, str]:
    resolved_markets = int(summary.get("resolved_markets") or 0)
    settled_cost = float(summary.get("settled_cost_usd") or 0)
    settled_roi = float(summary.get("settled_roi_pct") or 0)
    concentration = float(summary.get("max_market_cost_share_pct") or 0)
    if resolved_markets <= 0:
        direction = "unknown"
    elif settled_roi >= thresholds.min_promising_roi_pct:
        direction = "positive"
    elif settled_roi <= thresholds.max_negative_roi_pct:
        direction = "negative"
    else:
        direction = "mixed"

    if resolved_markets < thresholds.min_resolved_markets:
        status = "collecting_outcomes"
        reason = f"resolved_markets:{resolved_markets}<{thresholds.min_resolved_markets}"
    elif settled_cost < thresholds.min_settled_cost_usd:
        status = "collecting_outcomes"
        reason = f"settled_cost_usd:{settled_cost:.2f}<{thresholds.min_settled_cost_usd:.2f}"
    elif concentration > thresholds.max_market_cost_share_pct:
        status = "validation_concentrated"
        reason = f"max_market_cost_share_pct:{concentration:.2f}>{thresholds.max_market_cost_share_pct:.2f}"
    elif settled_roi >= thresholds.min_promising_roi_pct:
        status = "validated_promising"
        reason = f"settled_roi_pct:{settled_roi:.2f}>={thresholds.min_promising_roi_pct:.2f}"
    elif settled_roi <= thresholds.max_negative_roi_pct:
        status = "validated_negative"
        reason = f"settled_roi_pct:{settled_roi:.2f}<={thresholds.max_negative_roi_pct:.2f}"
    else:
        status = "validated_mixed"
        reason = (
            f"settled_roi_pct:{thresholds.max_negative_roi_pct:.2f}"
            f"<{settled_roi:.2f}<{thresholds.min_promising_roi_pct:.2f}"
        )
    return {
        "validation_status": status,
        "validation_reason": reason,
        "provisional_direction": direction,
    }


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _ratio_pct(numerator: float | int, denominator: float | int) -> float:
    return round((float(numerator) / float(denominator)) * 100, 2) if denominator else 0.0


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
