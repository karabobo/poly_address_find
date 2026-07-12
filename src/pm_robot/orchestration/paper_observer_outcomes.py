"""Research-only outcome tracking for actionable paper observer quotes."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.execution.market_marks import gamma_market_mark


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
    marked_pnl_usd: float
    marked_roi_pct: float
    settled_pnl_usd: float
    settled_roi_pct: float
    win_rate_pct: float
    wallet_summaries: list[dict[str, Any]]


def settle_paper_observer_trials(
    conn: sqlite3.Connection,
    *,
    limit: int = 1_000,
    now: int | None = None,
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
    summary = paper_observer_trial_summary(conn)
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
        marked_pnl_usd=float(summary.get("marked_pnl_usd") or 0),
        marked_roi_pct=float(summary.get("marked_roi_pct") or 0),
        settled_pnl_usd=float(summary.get("settled_pnl_usd") or 0),
        settled_roi_pct=float(summary.get("settled_roi_pct") or 0),
        win_rate_pct=float(summary.get("win_rate_pct") or 0),
        wallet_summaries=list(summary.get("wallet_summaries") or []),
    )


def paper_observer_trial_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    """Summarize durable observer trials without changing candidate or execution state."""

    if not _table_exists(conn, "paper_observer_trials"):
        return {
            "available": False,
            "total_trials": 0,
            "open_trials": 0,
            "resolved_trials": 0,
            "wallets": 0,
            "wallet_summaries": [],
        }
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_trials,
            COUNT(DISTINCT wallet) AS wallets,
            SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_trials,
            SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_trials,
            SUM(CASE WHEN mark_price IS NOT NULL THEN 1 ELSE 0 END) AS marked_trials,
            SUM(CASE WHEN mark_price IS NOT NULL THEN cost_usd ELSE 0 END) AS marked_cost_usd,
            SUM(CASE WHEN mark_price IS NOT NULL THEN pnl_usd ELSE 0 END) AS marked_pnl_usd,
            SUM(CASE WHEN status = 'resolved' THEN cost_usd ELSE 0 END) AS settled_cost_usd,
            SUM(CASE WHEN status = 'resolved' THEN pnl_usd ELSE 0 END) AS settled_pnl_usd,
            SUM(CASE WHEN status = 'resolved' AND pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
            MAX(updated_at) AS latest_updated_at
        FROM paper_observer_trials
        """
    ).fetchone()
    marked_cost = float(row["marked_cost_usd"] or 0) if row else 0.0
    marked_pnl = float(row["marked_pnl_usd"] or 0) if row else 0.0
    settled_cost = float(row["settled_cost_usd"] or 0) if row else 0.0
    settled_pnl = float(row["settled_pnl_usd"] or 0) if row else 0.0
    resolved = int(row["resolved_trials"] or 0) if row else 0
    wins = int(row["wins"] or 0) if row else 0
    return {
        "available": True,
        "total_trials": int(row["total_trials"] or 0) if row else 0,
        "wallets": int(row["wallets"] or 0) if row else 0,
        "open_trials": int(row["open_trials"] or 0) if row else 0,
        "resolved_trials": resolved,
        "marked_trials": int(row["marked_trials"] or 0) if row else 0,
        "marked_cost_usd": round(marked_cost, 6),
        "marked_pnl_usd": round(marked_pnl, 6),
        "marked_roi_pct": _ratio_pct(marked_pnl, marked_cost),
        "settled_cost_usd": round(settled_cost, 6),
        "settled_pnl_usd": round(settled_pnl, 6),
        "settled_roi_pct": _ratio_pct(settled_pnl, settled_cost),
        "wins": wins,
        "win_rate_pct": _ratio_pct(wins, resolved),
        "latest_updated_at": int(row["latest_updated_at"] or 0) if row else 0,
        "wallet_summaries": _wallet_summaries(conn),
    }


def _wallet_summaries(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            wallet,
            COUNT(*) AS total_trials,
            SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_trials,
            SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_trials,
            SUM(CASE WHEN mark_price IS NOT NULL THEN 1 ELSE 0 END) AS marked_trials,
            SUM(CASE WHEN mark_price IS NOT NULL THEN cost_usd ELSE 0 END) AS marked_cost_usd,
            SUM(CASE WHEN mark_price IS NOT NULL THEN pnl_usd ELSE 0 END) AS marked_pnl_usd,
            SUM(CASE WHEN status = 'resolved' THEN cost_usd ELSE 0 END) AS settled_cost_usd,
            SUM(CASE WHEN status = 'resolved' THEN pnl_usd ELSE 0 END) AS settled_pnl_usd,
            SUM(CASE WHEN status = 'resolved' AND pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
            MAX(updated_at) AS latest_updated_at
        FROM paper_observer_trials
        GROUP BY wallet
        ORDER BY resolved_trials DESC, marked_trials DESC, total_trials DESC, wallet ASC
        """
    ).fetchall()
    summaries: list[dict[str, Any]] = []
    for row in rows:
        marked_cost = float(row["marked_cost_usd"] or 0)
        marked_pnl = float(row["marked_pnl_usd"] or 0)
        settled_cost = float(row["settled_cost_usd"] or 0)
        settled_pnl = float(row["settled_pnl_usd"] or 0)
        resolved = int(row["resolved_trials"] or 0)
        wins = int(row["wins"] or 0)
        summaries.append(
            {
                "wallet": str(row["wallet"] or ""),
                "total_trials": int(row["total_trials"] or 0),
                "open_trials": int(row["open_trials"] or 0),
                "resolved_trials": resolved,
                "marked_trials": int(row["marked_trials"] or 0),
                "marked_pnl_usd": round(marked_pnl, 6),
                "marked_roi_pct": _ratio_pct(marked_pnl, marked_cost),
                "settled_pnl_usd": round(settled_pnl, 6),
                "settled_roi_pct": _ratio_pct(settled_pnl, settled_cost),
                "wins": wins,
                "win_rate_pct": _ratio_pct(wins, resolved),
                "latest_updated_at": int(row["latest_updated_at"] or 0),
            }
        )
    return summaries


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _ratio_pct(numerator: float | int, denominator: float | int) -> float:
    return round((float(numerator) / float(denominator)) * 100, 2) if denominator else 0.0
