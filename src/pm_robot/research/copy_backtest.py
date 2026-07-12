"""Copy-stream backtesting from mined leader/follower links."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from statistics import median
from typing import Any

from pm_robot.config import threshold
from pm_robot.models import WalletFeatures
from pm_robot.storage.repository import _feature_from_row, upsert_wallet_feature


@dataclass(frozen=True)
class CopyBacktestSummary:
    trades_written: int
    leader_performance_written: int
    leaders_with_positive_net_roi: int


@dataclass(frozen=True)
class TargetedCopyBacktestSummary:
    leaders_seen: int
    trades_written: int
    leader_performance_written: int
    leaders_with_positive_net_roi: int
    leaders_preserved_on_empty: int = 0


def backtest_copy_stream(conn: sqlite3.Connection, policy: dict[str, Any]) -> CopyBacktestSummary:
    """Backtest qualified leaders using closed episodes or settled markets.

    Unknown settlement and still-open positions do not contribute.
    """

    now = int(time.time())
    stake_usdc = float(threshold(policy, "copy_backtest_stake_usdc", 10))
    friction_bps = float(threshold(policy, "copy_backtest_friction_bps", 150))

    conn.execute("DELETE FROM copy_backtest_trades")
    conn.execute("DELETE FROM copy_leader_performance")
    _clear_copy_stream_features(conn)
    trades = _build_backtest_trades(conn, stake_usdc=stake_usdc, friction_bps=friction_bps, now=now)
    if trades:
        conn.executemany(
            """
            INSERT OR IGNORE INTO copy_backtest_trades(
                leader_wallet, follower_wallet, link_id, leader_activity_id,
                episode_id, market_slug, asset_id, outcome, side,
                leader_ts, copied_ts, lag_seconds, entry_price,
                leader_episode_roi, stake_usdc, gross_pnl_usdc, friction_bps,
                net_pnl_usdc, net_roi, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            trades,
        )
    performance = _build_leader_performance(conn, now)
    if performance:
        conn.executemany(
            """
            INSERT INTO copy_leader_performance(
                leader_wallet, backtest_trade_count, copied_market_count,
                total_stake_usdc, gross_pnl_usdc, net_pnl_usdc, gross_roi,
                net_roi, win_rate, median_lag_seconds, last_backtest_trade_at,
                updated_at, edge_retention_pct, walk_forward_consistency_pct,
                max_drawdown_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            performance,
        )
    _merge_copy_stream_features(conn)
    conn.commit()
    return CopyBacktestSummary(
        trades_written=len(trades),
        leader_performance_written=len(performance),
        leaders_with_positive_net_roi=sum(1 for row in performance if float(row[7]) > 0),
    )


def backtest_copy_stream_for_leaders(
    conn: sqlite3.Connection,
    policy: dict[str, Any],
    leaders: list[str],
    *,
    now: int | None = None,
    commit: bool = True,
    preserve_existing_on_empty: bool = False,
) -> TargetedCopyBacktestSummary:
    """Backtest copy streams for a bounded set of leaders.

    This is the queue-safe variant of :func:`backtest_copy_stream`: it deletes
    and rebuilds only the requested leaders' copy backtest rows.
    """

    leader_wallets = _normalize_wallets(leaders)
    if not leader_wallets:
        return TargetedCopyBacktestSummary(
            leaders_seen=0,
            trades_written=0,
            leader_performance_written=0,
            leaders_with_positive_net_roi=0,
        )
    ts = now or int(time.time())
    stake_usdc = float(threshold(policy, "copy_backtest_stake_usdc", 10))
    friction_bps = float(threshold(policy, "copy_backtest_friction_bps", 150))

    trades = _build_backtest_trades(
        conn,
        stake_usdc=stake_usdc,
        friction_bps=friction_bps,
        now=ts,
        leaders=leader_wallets,
    )
    new_trade_leaders = {str(row[0]) for row in trades}
    preserved_leaders: set[str] = set()
    if preserve_existing_on_empty:
        placeholders = _placeholders(leader_wallets)
        preserved_leaders = {
            str(row["leader_wallet"])
            for row in conn.execute(
                f"""
                SELECT performance.leader_wallet
                FROM copy_leader_performance performance
                WHERE performance.leader_wallet IN ({placeholders})
                  AND EXISTS (
                      SELECT 1
                      FROM copy_pair_stats pair
                      WHERE pair.leader_wallet = performance.leader_wallet
                        AND pair.qualifies = 1
                  )
                """,
                tuple(leader_wallets),
            ).fetchall()
            if str(row["leader_wallet"]) not in new_trade_leaders
        }
    rebuild_leaders = [leader for leader in leader_wallets if leader not in preserved_leaders]
    if not rebuild_leaders:
        if commit:
            conn.commit()
        return TargetedCopyBacktestSummary(
            leaders_seen=len(leader_wallets),
            trades_written=0,
            leader_performance_written=0,
            leaders_with_positive_net_roi=0,
            leaders_preserved_on_empty=len(preserved_leaders),
        )

    placeholders = _placeholders(rebuild_leaders)
    rebuild_trades = [row for row in trades if str(row[0]) in rebuild_leaders]
    conn.execute(
        f"DELETE FROM copy_backtest_trades WHERE leader_wallet IN ({placeholders})",
        tuple(rebuild_leaders),
    )
    if rebuild_trades:
        conn.executemany(
            """
            INSERT OR IGNORE INTO copy_backtest_trades(
                leader_wallet, follower_wallet, link_id, leader_activity_id,
                episode_id, market_slug, asset_id, outcome, side,
                leader_ts, copied_ts, lag_seconds, entry_price,
                leader_episode_roi, stake_usdc, gross_pnl_usdc, friction_bps,
                net_pnl_usdc, net_roi, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rebuild_trades,
        )
    if commit:
        conn.commit()

    performance = _build_leader_performance(conn, ts, leaders=rebuild_leaders)
    conn.execute(
        f"DELETE FROM copy_leader_performance WHERE leader_wallet IN ({placeholders})",
        tuple(rebuild_leaders),
    )
    _clear_copy_stream_features(conn, rebuild_leaders)
    if performance:
        conn.executemany(
            """
            INSERT INTO copy_leader_performance(
                leader_wallet, backtest_trade_count, copied_market_count,
                total_stake_usdc, gross_pnl_usdc, net_pnl_usdc, gross_roi,
                net_roi, win_rate, median_lag_seconds, last_backtest_trade_at,
                updated_at, edge_retention_pct, walk_forward_consistency_pct,
                max_drawdown_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            performance,
        )
    _merge_copy_stream_features(conn, rebuild_leaders)
    if commit:
        conn.commit()
    return TargetedCopyBacktestSummary(
        leaders_seen=len(leader_wallets),
        trades_written=len(rebuild_trades),
        leader_performance_written=len(performance),
        leaders_with_positive_net_roi=sum(1 for row in performance if float(row[7]) > 0),
        leaders_preserved_on_empty=len(preserved_leaders),
    )


def _build_backtest_trades(
    conn: sqlite3.Connection,
    *,
    stake_usdc: float,
    friction_bps: float,
    now: int,
    leaders: list[str] | None = None,
) -> list[tuple[Any, ...]]:
    leader_filter = ""
    params: tuple[Any, ...] = ()
    if leaders:
        leader_filter = f"AND l.leader_wallet IN ({_placeholders(leaders)})"
        params = tuple(leaders)
    rows = conn.execute(
        f"""
        SELECT
            l.link_id,
            l.leader_wallet,
            l.follower_wallet,
            l.leader_activity_id,
            l.follower_ts AS copied_ts,
            l.lag_seconds,
            a.market_slug,
            a.asset_id,
            a.outcome,
            a.side,
            a.timestamp AS leader_ts,
            a.price AS entry_price,
            e.episode_id,
            e.status AS episode_status,
            e.bought_usdc,
            e.sold_usdc,
            e.net_shares,
            e.realized_pnl_est,
            g.closed AS market_closed,
            g.clob_token_ids_json,
            g.outcome_prices_json
        FROM copy_trade_links l
        JOIN copy_pair_stats ps
          ON ps.leader_wallet = l.leader_wallet
         AND ps.follower_wallet = l.follower_wallet
         AND ps.qualifies = 1
        JOIN wallet_activity a
          ON a.activity_id = l.leader_activity_id
        JOIN wallet_episodes e
          ON e.address = l.leader_wallet
         AND COALESCE(e.condition_id, '') = COALESCE(a.condition_id, '')
         AND COALESCE(e.asset_id, '') = COALESCE(a.asset_id, '')
        LEFT JOIN gamma_market_cache g
          ON g.market_slug = a.market_slug
        WHERE UPPER(COALESCE(a.side, '')) = 'BUY'
          AND e.bought_usdc > 0
          AND (e.status = 'closed' OR COALESCE(g.closed, 0) = 1)
          {leader_filter}
        ORDER BY l.leader_wallet, l.follower_ts, l.link_id
        """,
        params,
    ).fetchall()
    out: list[tuple[Any, ...]] = []
    friction = friction_bps / 10_000.0
    for row in rows:
        if row["episode_status"] == "closed":
            leader_roi = float(row["realized_pnl_est"] or 0) / float(row["bought_usdc"] or 1)
        else:
            settlement = _settlement_price(row)
            bought = float(row["bought_usdc"] or 0)
            if settlement is None or bought <= 0:
                continue
            sold = float(row["sold_usdc"] or 0)
            net_shares = float(row["net_shares"] or 0)
            leader_roi = (sold + net_shares * settlement - bought) / bought
        gross_pnl = stake_usdc * leader_roi
        net_roi = leader_roi - friction
        net_pnl = stake_usdc * net_roi
        out.append(
            (
                row["leader_wallet"],
                row["follower_wallet"],
                row["link_id"],
                row["leader_activity_id"],
                row["episode_id"],
                row["market_slug"],
                row["asset_id"],
                row["outcome"],
                row["side"],
                row["leader_ts"],
                row["copied_ts"],
                row["lag_seconds"],
                row["entry_price"],
                leader_roi,
                stake_usdc,
                gross_pnl,
                friction_bps,
                net_pnl,
                net_roi,
                now,
            )
        )
    return out


def _settlement_price(row: sqlite3.Row) -> float | None:
    if int(row["market_closed"] or 0) != 1:
        return None
    try:
        token_ids = [str(item) for item in json.loads(row["clob_token_ids_json"] or "[]")]
        prices = [float(item) for item in json.loads(row["outcome_prices_json"] or "[]")]
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    asset_id = str(row["asset_id"] or "")
    if asset_id not in token_ids:
        return None
    index = token_ids.index(asset_id)
    if index >= len(prices):
        return None
    price = prices[index]
    return price if price <= 0.001 or price >= 0.999 else None


def _build_leader_performance(
    conn: sqlite3.Connection,
    now: int,
    *,
    leaders: list[str] | None = None,
) -> list[tuple[Any, ...]]:
    leader_filter = ""
    params: tuple[Any, ...] = ()
    if leaders:
        leader_filter = f"WHERE leader_wallet IN ({_placeholders(leaders)})"
        params = tuple(leaders)
    rows = conn.execute(
        f"""
        SELECT *
        FROM copy_backtest_trades
        {leader_filter}
        ORDER BY leader_wallet, copied_ts
        """,
        params,
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(row["leader_wallet"], []).append(row)

    out: list[tuple[Any, ...]] = []
    for leader, trades in grouped.items():
        total_stake = sum(float(row["stake_usdc"] or 0) for row in trades)
        gross_pnl = sum(float(row["gross_pnl_usdc"] or 0) for row in trades)
        net_pnl = sum(float(row["net_pnl_usdc"] or 0) for row in trades)
        markets = {row["asset_id"] or row["market_slug"] for row in trades}
        wins = [row for row in trades if float(row["net_pnl_usdc"] or 0) > 0]
        lags = [float(row["lag_seconds"]) for row in trades if row["lag_seconds"] is not None]
        last_trade = max((int(row["copied_ts"] or 0) for row in trades), default=0) or None
        gross_roi = gross_pnl / total_stake if total_stake else 0.0
        net_roi = net_pnl / total_stake if total_stake else 0.0
        edge_retention_pct = (
            max(0.0, min(100.0, net_roi / gross_roi * 100.0))
            if gross_roi > 0
            else 0.0
        )
        walk_forward_consistency_pct = _walk_forward_consistency_pct(trades)
        max_drawdown_pct = _trade_max_drawdown_pct(trades, total_stake)
        out.append(
            (
                leader,
                len(trades),
                len(markets),
                total_stake,
                gross_pnl,
                net_pnl,
                gross_roi,
                net_roi,
                len(wins) / len(trades) if trades else None,
                median(lags) if lags else None,
                last_trade,
                now,
                edge_retention_pct,
                walk_forward_consistency_pct,
                max_drawdown_pct,
            )
        )
    return out


def _merge_copy_stream_features(conn: sqlite3.Connection, leaders: list[str] | None = None) -> None:
    if leaders:
        rows = conn.execute(
            f"SELECT * FROM copy_leader_performance WHERE leader_wallet IN ({_placeholders(leaders)})",
            tuple(leaders),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM copy_leader_performance").fetchall()
    for row in rows:
        existing = conn.execute(
            "SELECT * FROM wallet_features WHERE address = ?",
            (row["leader_wallet"],),
        ).fetchone()
        feature = _feature_from_row(existing) if existing else WalletFeatures(address=row["leader_wallet"])
        merged = WalletFeatures(
            address=row["leader_wallet"],
            cumulative_win_rate=feature.cumulative_win_rate,
            recent_30d_volume_usdc=feature.recent_30d_volume_usdc,
            net_pnl_usdc=feature.net_pnl_usdc,
            total_volume_usdc=feature.total_volume_usdc,
            event_win_rate=feature.event_win_rate,
            trade_win_rate=feature.trade_win_rate,
            avg_dca_entries=feature.avg_dca_entries,
            sell_pct=feature.sell_pct,
            bot_score=feature.bot_score,
            trades_per_day=feature.trades_per_day,
            median_gap_sec=feature.median_gap_sec,
            maker_fraction=feature.maker_fraction,
            leader_in_degree=feature.leader_in_degree,
            copy_event_count=feature.copy_event_count,
            copy_market_count=feature.copy_market_count,
            containment_pct_median=feature.containment_pct_median,
            copy_stream_roi=row["net_roi"],
            edge_retention_pct=row["edge_retention_pct"],
            walk_forward_consistency_pct=row["walk_forward_consistency_pct"],
            survival_score=max(0.0, 100.0 - float(row["max_drawdown_pct"] or 0.0) * 100.0),
            single_market_pnl_share=feature.single_market_pnl_share,
            net_to_gross_exposure=feature.net_to_gross_exposure,
            hygiene_status=feature.hygiene_status,
            primary_category=feature.primary_category,
            last_active_days_ago=feature.last_active_days_ago,
            extra={
                **feature.extra,
                "copy_backtest_trade_count": row["backtest_trade_count"],
                "copy_backtest_net_pnl_usdc": round(float(row["net_pnl_usdc"] or 0), 4),
                "copy_backtest_win_rate": row["win_rate"],
                "copy_backtest_median_lag_seconds": row["median_lag_seconds"],
                "copy_backtest_edge_retention_pct": row["edge_retention_pct"],
                "copy_backtest_walk_forward_consistency_pct": row["walk_forward_consistency_pct"],
                "copy_backtest_max_drawdown_pct": row["max_drawdown_pct"],
            },
        )
        upsert_wallet_feature(conn, merged)


def _clear_copy_stream_features(conn: sqlite3.Connection, leaders: list[str] | None = None) -> None:
    ts = int(time.time())
    if leaders:
        conn.execute(
            f"""
            UPDATE wallet_features
            SET copy_stream_roi = NULL,
                edge_retention_pct = NULL,
                walk_forward_consistency_pct = NULL,
                survival_score = NULL,
                updated_at = ?
            WHERE address IN ({_placeholders(leaders)})
            """,
            (ts, *leaders),
        )
        return
    conn.execute(
        """
        UPDATE wallet_features
        SET copy_stream_roi = NULL,
            edge_retention_pct = NULL,
            walk_forward_consistency_pct = NULL,
            survival_score = NULL,
            updated_at = ?
        WHERE copy_stream_roi IS NOT NULL
           OR edge_retention_pct IS NOT NULL
           OR walk_forward_consistency_pct IS NOT NULL
        """,
        (ts,),
    )


def _walk_forward_consistency_pct(trades: list[sqlite3.Row]) -> float:
    if not trades:
        return 0.0
    fold_count = min(3, len(trades))
    positive = 0
    for fold in range(fold_count):
        start = fold * len(trades) // fold_count
        end = (fold + 1) * len(trades) // fold_count
        pnl = sum(float(row["net_pnl_usdc"] or 0.0) for row in trades[start:end])
        if pnl > 0:
            positive += 1
    return positive / fold_count * 100.0


def _trade_max_drawdown_pct(trades: list[sqlite3.Row], total_stake: float) -> float:
    if total_stake <= 0:
        return 0.0
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for row in trades:
        cumulative += float(row["net_pnl_usdc"] or 0.0)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return max_drawdown / total_stake


def _normalize_wallets(wallets: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for wallet in wallets:
        value = str(wallet or "").strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _placeholders(values: list[Any]) -> str:
    if not values:
        raise ValueError("values must not be empty")
    return ",".join("?" for _ in values)
