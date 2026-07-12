"""Offline copy-leader graph mining from wallet activity."""

from __future__ import annotations

import sqlite3
import time
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from statistics import median
from typing import Any

from pm_robot.config import threshold
from pm_robot.models import WalletFeatures
from pm_robot.orchestration.evidence_readiness import paper_evidence_ready_sql
from pm_robot.storage.repository import _feature_from_row, upsert_wallet_feature


SQLITE_IN_CHUNK_SIZE = 400


@dataclass(frozen=True)
class CopyGraphSummary:
    links_written: int
    pair_stats_written: int
    leader_stats_written: int
    qualified_pairs: int


@dataclass(frozen=True)
class TargetedCopyGraphSummary:
    leaders_seen: int
    links_written: int
    pair_stats_written: int
    leader_stats_written: int
    qualified_pairs: int


def mine_copy_graph(conn: sqlite3.Connection, policy: dict[str, Any]) -> CopyGraphSummary:
    """Mine copy relationships and merge leader graph features.

    The paper uses a 1-block follower window. Public activity rows in this
    framework do not yet carry block numbers, so this implementation uses the
    policy's max_copy_lag_seconds as a conservative timestamp proxy.
    """

    now = int(time.time())
    max_lag_seconds = int(threshold(policy, "max_copy_lag_seconds", 15))
    min_events = int(threshold(policy, "min_copy_events", 5))
    min_markets = int(threshold(policy, "min_copy_markets", 5))
    min_containment = float(threshold(policy, "min_containment_pct", 0.9))
    min_precedes = float(threshold(policy, "min_leader_precedes_pct", 0.9))

    conn.execute("DELETE FROM copy_trade_links")
    links_written = _insert_copy_links(conn, max_lag_seconds=max_lag_seconds, now=now)

    conn.execute("DELETE FROM copy_pair_stats")
    pair_stats = _build_pair_stats(conn, min_events, min_markets, min_containment, min_precedes, now)
    if pair_stats:
        conn.executemany(
            """
            INSERT INTO copy_pair_stats(
                leader_wallet, follower_wallet, copy_event_count, copy_market_count,
                follower_trade_count, containment_pct, leader_precedes_pct,
                median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            pair_stats,
        )

    conn.execute("DELETE FROM copy_leader_stats")
    _clear_leader_features(conn)
    leader_stats = _build_leader_stats(conn, now)
    if leader_stats:
        conn.executemany(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            leader_stats,
        )
    _merge_leader_features(conn)
    prune_unqualified_copy_links_for_leaders(
        conn,
        [str(row[0]) for row in pair_stats],
        commit=False,
    )
    conn.commit()
    return CopyGraphSummary(
        links_written=links_written,
        pair_stats_written=len(pair_stats),
        leader_stats_written=len(leader_stats),
        qualified_pairs=sum(1 for row in pair_stats if row[10]),
    )


def mine_copy_graph_for_leaders(
    conn: sqlite3.Connection,
    policy: dict[str, Any],
    leaders: list[str],
    *,
    max_leader_events: int = 3_000,
    max_followers_per_event: int = 200,
    now: int | None = None,
    commit: bool = True,
) -> TargetedCopyGraphSummary:
    """Refresh copy graph evidence for a bounded set of leaders.

    Unlike :func:`mine_copy_graph`, this function does not clear global copy
    graph tables. It only replaces rows where the requested wallets are the
    leader side, which lets a queue worker refresh promising wallets without
    holding a long full-database write cycle.
    """

    leader_wallets = _normalize_wallets(leaders)
    if not leader_wallets:
        return TargetedCopyGraphSummary(
            leaders_seen=0,
            links_written=0,
            pair_stats_written=0,
            leader_stats_written=0,
            qualified_pairs=0,
        )

    ts = now or int(time.time())
    max_lag_seconds = int(threshold(policy, "max_copy_lag_seconds", 15))
    min_events = int(threshold(policy, "min_copy_events", 5))
    min_markets = int(threshold(policy, "min_copy_markets", 5))
    min_containment = float(threshold(policy, "min_containment_pct", 0.9))
    min_precedes = float(threshold(policy, "min_leader_precedes_pct", 0.9))

    link_rows: list[tuple[Any, ...]] = []
    for leader in leader_wallets:
        link_rows.extend(
            _build_copy_link_rows_for_leader(
                conn,
                leader=leader,
                max_lag_seconds=max_lag_seconds,
                max_leader_events=max_leader_events,
                max_followers_per_event=max_followers_per_event,
                now=ts,
            )
        )

    _delete_for_leaders(conn, "copy_trade_links", "leader_wallet", leader_wallets)
    links_written = _insert_copy_link_rows(conn, link_rows)
    if commit:
        conn.commit()

    pair_stats = _build_pair_stats_for_leaders(
        conn,
        leader_wallets,
        min_events,
        min_markets,
        min_containment,
        min_precedes,
        ts,
    )
    _delete_for_leaders(conn, "copy_pair_stats", "leader_wallet", leader_wallets)
    if pair_stats:
        conn.executemany(
            """
            INSERT INTO copy_pair_stats(
                leader_wallet, follower_wallet, copy_event_count, copy_market_count,
                follower_trade_count, containment_pct, leader_precedes_pct,
                median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            pair_stats,
        )
    if commit:
        conn.commit()

    leader_stats = _build_leader_stats_for_leaders(conn, leader_wallets, ts)
    _delete_for_leaders(conn, "copy_leader_stats", "leader_wallet", leader_wallets)
    _clear_leader_features(conn, leader_wallets)
    if leader_stats:
        conn.executemany(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            leader_stats,
        )
    _merge_leader_features(conn, leader_wallets)
    if commit:
        conn.commit()
    return TargetedCopyGraphSummary(
        leaders_seen=len(leader_wallets),
        links_written=links_written,
        pair_stats_written=len(pair_stats),
        leader_stats_written=len(leader_stats),
        qualified_pairs=sum(1 for row in pair_stats if row[10]),
    )


def prune_unqualified_copy_links_for_leaders(
    conn: sqlite3.Connection,
    leaders: list[str],
    *,
    commit: bool = True,
) -> int:
    """Discard rebuildable raw links after pair summaries have been persisted."""

    leader_wallets = _normalize_wallets(leaders)
    if not leader_wallets:
        return 0
    cursor = conn.execute(
        f"""
        DELETE FROM copy_trade_links
        WHERE leader_wallet IN ({_placeholders(leader_wallets)})
          AND NOT EXISTS (
              SELECT 1
              FROM copy_pair_stats AS pair
              WHERE pair.leader_wallet = copy_trade_links.leader_wallet
                AND pair.follower_wallet = copy_trade_links.follower_wallet
                AND pair.qualifies = 1
          )
        """,
        tuple(leader_wallets),
    )
    deleted = max(0, int(cursor.rowcount or 0))
    if commit:
        conn.commit()
    return deleted


def _insert_copy_links_for_leader(
    conn: sqlite3.Connection,
    *,
    leader: str,
    max_lag_seconds: int,
    max_leader_events: int,
    max_followers_per_event: int,
    now: int,
) -> int:
    return _insert_copy_link_rows(
        conn,
        _build_copy_link_rows_for_leader(
            conn,
            leader=leader,
            max_lag_seconds=max_lag_seconds,
            max_leader_events=max_leader_events,
            max_followers_per_event=max_followers_per_event,
            now=now,
        )
    )


def _insert_copy_links(conn: sqlite3.Connection, *, max_lag_seconds: int, now: int) -> int:
    rows = conn.execute(
        """
        SELECT
            activity_id, address, condition_id, market_slug, asset_id,
            outcome, side, timestamp
        FROM wallet_activity
        WHERE type = 'TRADE'
          AND COALESCE(asset_id, '') != ''
          AND COALESCE(side, '') != ''
          AND timestamp > 0
        ORDER BY asset_id, side, timestamp, activity_id
        """
    ).fetchall()
    link_rows: list[tuple[Any, ...]] = []
    window: list[sqlite3.Row] = []
    current_key: tuple[str, str] | None = None
    for row in rows:
        key = (str(row["asset_id"] or ""), str(row["side"] or ""))
        ts = int(row["timestamp"])
        if key != current_key:
            current_key = key
            window = []
        cutoff = ts - max_lag_seconds
        while window and int(window[0]["timestamp"]) < cutoff:
            window.pop(0)
        for leader in window:
            leader_ts = int(leader["timestamp"])
            if leader_ts >= ts or leader["address"] == row["address"]:
                continue
            link_rows.append(
                (
                    leader["address"],
                    row["address"],
                    leader["activity_id"],
                    row["activity_id"],
                    row["condition_id"],
                    row["market_slug"],
                    row["asset_id"],
                    row["outcome"],
                    row["side"],
                    leader_ts,
                    ts,
                    ts - leader_ts,
                    now,
                )
            )
        window.append(row)

    before = conn.total_changes
    if link_rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO copy_trade_links(
                leader_wallet, follower_wallet, leader_activity_id, follower_activity_id,
                condition_id, market_slug, asset_id, outcome, side,
                leader_ts, follower_ts, lag_seconds, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            link_rows,
        )
    return conn.total_changes - before


def _build_copy_link_rows_for_leader(
    conn: sqlite3.Connection,
    *,
    leader: str,
    max_lag_seconds: int,
    max_leader_events: int,
    max_followers_per_event: int,
    now: int,
) -> list[tuple[Any, ...]]:
    leader_rows = conn.execute(
        """
        SELECT
            activity_id, address, condition_id, market_slug, asset_id,
            outcome, side, timestamp
        FROM wallet_activity
        WHERE address = ?
          AND type = 'TRADE'
          AND COALESCE(asset_id, '') != ''
          AND COALESCE(side, '') != ''
          AND timestamp > 0
        ORDER BY timestamp DESC, activity_id DESC
        LIMIT ?
        """,
        (leader, max(0, int(max_leader_events))),
    ).fetchall()
    leader_rows = sorted(leader_rows, key=lambda row: (int(row["timestamp"] or 0), int(row["activity_id"] or 0)))
    link_rows: list[tuple[Any, ...]] = []
    per_event_limit = max(1, int(max_followers_per_event))
    for row in leader_rows:
        leader_ts = int(row["timestamp"])
        followers = conn.execute(
            """
            SELECT
                activity_id, address, condition_id, market_slug, asset_id,
                outcome, side, timestamp
            FROM wallet_activity
            WHERE type = 'TRADE'
              AND asset_id = ?
              AND side = ?
              AND timestamp > ?
              AND timestamp <= ?
              AND address != ?
            ORDER BY timestamp ASC, activity_id ASC
            LIMIT ?
            """,
            (
                str(row["asset_id"] or ""),
                str(row["side"] or ""),
                leader_ts,
                leader_ts + max_lag_seconds,
                leader,
                per_event_limit,
            ),
        ).fetchall()
        for follower in followers:
            follower_ts = int(follower["timestamp"])
            link_rows.append(
                (
                    leader,
                    follower["address"],
                    row["activity_id"],
                    follower["activity_id"],
                    row["condition_id"] or follower["condition_id"],
                    row["market_slug"] or follower["market_slug"],
                    row["asset_id"],
                    row["outcome"] or follower["outcome"],
                    row["side"],
                    leader_ts,
                    follower_ts,
                    follower_ts - leader_ts,
                    now,
                )
            )

    return link_rows


def _insert_copy_link_rows(conn: sqlite3.Connection, link_rows: list[tuple[Any, ...]]) -> int:
    before = conn.total_changes
    if link_rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO copy_trade_links(
                leader_wallet, follower_wallet, leader_activity_id, follower_activity_id,
                condition_id, market_slug, asset_id, outcome, side,
                leader_ts, follower_ts, lag_seconds, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            link_rows,
        )
    return conn.total_changes - before


def _build_pair_stats(
    conn: sqlite3.Connection,
    min_events: int,
    min_markets: int,
    min_containment: float,
    min_precedes: float,
    now: int,
) -> list[tuple[Any, ...]]:
    forward_rows = conn.execute(
        """
        SELECT
            leader_wallet,
            follower_wallet,
            COUNT(DISTINCT follower_activity_id) AS copy_events,
            COUNT(DISTINCT COALESCE(condition_id, market_slug, asset_id)) AS copy_markets,
            MIN(follower_ts) AS first_copy_ts,
            MAX(follower_ts) AS last_copy_ts,
            GROUP_CONCAT(lag_seconds) AS lags
        FROM copy_trade_links
        GROUP BY leader_wallet, follower_wallet
        """
    ).fetchall()
    follower_trade_timestamps: dict[str, list[int]] = {}
    for row in conn.execute(
        """
        SELECT address, timestamp
        FROM wallet_activity
        WHERE type = 'TRADE' AND timestamp > 0
        ORDER BY address, timestamp, activity_id
        """
    ).fetchall():
        follower_trade_timestamps.setdefault(str(row["address"]), []).append(int(row["timestamp"]))
    reverse_counts = {
        (row["leader_wallet"], row["follower_wallet"]): int(row["copy_events"])
        for row in forward_rows
    }
    evidence_ready_followers = _deep_evidence_ready_wallets(conn)

    out: list[tuple[Any, ...]] = []
    for row in forward_rows:
        leader = row["leader_wallet"]
        follower = row["follower_wallet"]
        copy_events = int(row["copy_events"] or 0)
        copy_markets = int(row["copy_markets"] or 0)
        first_copy_ts = int(row["first_copy_ts"] or 0)
        last_copy_ts = int(row["last_copy_ts"] or first_copy_ts)
        timestamps = follower_trade_timestamps.get(str(follower), [])
        follower_trade_count = (
            bisect_right(timestamps, last_copy_ts) - bisect_left(timestamps, first_copy_ts)
            if timestamps
            else 0
        )
        # Containment is defined over the observed leader/follower overlap
        # window. Dividing by the follower's entire retained history made
        # every long-lived follower look uncontained as backfill grew.
        containment = min(1.0, copy_events / follower_trade_count) if follower_trade_count else 0.0
        reverse_events = reverse_counts.get((follower, leader), 0)
        precedes = copy_events / (copy_events + reverse_events) if copy_events + reverse_events else 0.0
        lags = [int(x) for x in str(row["lags"] or "").split(",") if x != ""]
        # A partial follower history makes the containment denominator unstable.
        qualifies = int(
            str(follower) in evidence_ready_followers
            and copy_events >= min_events
            and copy_markets >= min_markets
            and containment >= min_containment
            and precedes >= min_precedes
        )
        out.append(
            (
                leader,
                follower,
                copy_events,
                copy_markets,
                follower_trade_count,
                containment,
                precedes,
                median(lags) if lags else None,
                row["first_copy_ts"],
                row["last_copy_ts"],
                qualifies,
                now,
            )
        )
    return out


def _build_pair_stats_for_leaders(
    conn: sqlite3.Connection,
    leaders: list[str],
    min_events: int,
    min_markets: int,
    min_containment: float,
    min_precedes: float,
    now: int,
) -> list[tuple[Any, ...]]:
    if not leaders:
        return []
    placeholders = _placeholders(leaders)
    forward_rows = conn.execute(
        f"""
        SELECT
            leader_wallet,
            follower_wallet,
            COUNT(DISTINCT follower_activity_id) AS copy_events,
            COUNT(DISTINCT COALESCE(condition_id, market_slug, asset_id)) AS copy_markets,
            MIN(follower_ts) AS first_copy_ts,
            MAX(follower_ts) AS last_copy_ts,
            GROUP_CONCAT(lag_seconds) AS lags
        FROM copy_trade_links
        WHERE leader_wallet IN ({placeholders})
        GROUP BY leader_wallet, follower_wallet
        """,
        tuple(leaders),
    ).fetchall()
    followers = sorted({str(row["follower_wallet"]) for row in forward_rows})
    follower_trade_timestamps: dict[str, list[int]] = {}
    if followers:
        follower_placeholders = _placeholders(followers)
        for row in conn.execute(
            f"""
            SELECT address, timestamp
            FROM wallet_activity
            WHERE type = 'TRADE'
              AND timestamp > 0
              AND address IN ({follower_placeholders})
            ORDER BY address, timestamp, activity_id
            """,
            tuple(followers),
        ).fetchall():
            follower_trade_timestamps.setdefault(str(row["address"]), []).append(int(row["timestamp"]))

    reverse_counts: dict[tuple[str, str], int] = {}
    if followers:
        reverse_rows = conn.execute(
            f"""
            SELECT leader_wallet, follower_wallet, COUNT(DISTINCT follower_activity_id) AS copy_events
            FROM copy_trade_links
            WHERE leader_wallet IN ({_placeholders(followers)})
              AND follower_wallet IN ({placeholders})
            GROUP BY leader_wallet, follower_wallet
            """,
            tuple(followers) + tuple(leaders),
        ).fetchall()
        reverse_counts = {
            (str(row["leader_wallet"]), str(row["follower_wallet"])): int(row["copy_events"] or 0)
            for row in reverse_rows
        }
        # Unqualified raw links are ephemeral. Their persisted pair summary
        # still supplies reverse-direction evidence on later targeted scans.
        reverse_summary_rows = conn.execute(
            f"""
            SELECT leader_wallet, follower_wallet, copy_event_count
            FROM copy_pair_stats
            WHERE leader_wallet IN ({_placeholders(followers)})
              AND follower_wallet IN ({placeholders})
            """,
            tuple(followers) + tuple(leaders),
        ).fetchall()
        for row in reverse_summary_rows:
            key = (str(row["leader_wallet"]), str(row["follower_wallet"]))
            reverse_counts[key] = max(
                reverse_counts.get(key, 0),
                int(row["copy_event_count"] or 0),
            )
    evidence_ready_followers = _deep_evidence_ready_wallets(conn, followers)

    out: list[tuple[Any, ...]] = []
    for row in forward_rows:
        leader = str(row["leader_wallet"])
        follower = str(row["follower_wallet"])
        copy_events = int(row["copy_events"] or 0)
        copy_markets = int(row["copy_markets"] or 0)
        first_copy_ts = int(row["first_copy_ts"] or 0)
        last_copy_ts = int(row["last_copy_ts"] or first_copy_ts)
        timestamps = follower_trade_timestamps.get(follower, [])
        follower_trade_count = (
            bisect_right(timestamps, last_copy_ts) - bisect_left(timestamps, first_copy_ts)
            if timestamps
            else 0
        )
        containment = min(1.0, copy_events / follower_trade_count) if follower_trade_count else 0.0
        reverse_events = reverse_counts.get((follower, leader), 0)
        precedes = copy_events / (copy_events + reverse_events) if copy_events + reverse_events else 0.0
        lags = [int(x) for x in str(row["lags"] or "").split(",") if x != ""]
        # A partial follower history makes the containment denominator unstable.
        qualifies = int(
            follower in evidence_ready_followers
            and copy_events >= min_events
            and copy_markets >= min_markets
            and containment >= min_containment
            and precedes >= min_precedes
        )
        out.append(
            (
                leader,
                follower,
                copy_events,
                copy_markets,
                follower_trade_count,
                containment,
                precedes,
                median(lags) if lags else None,
                row["first_copy_ts"],
                row["last_copy_ts"],
                qualifies,
                now,
            )
        )
    return out


def _deep_evidence_ready_wallets(
    conn: sqlite3.Connection,
    wallets: list[str] | None = None,
) -> set[str]:
    """Return followers whose retained history can support a stable denominator."""

    readiness_sql = paper_evidence_ready_sql("wps")
    if wallets is None:
        rows = conn.execute(
            f"""
            SELECT wps.wallet
            FROM wallet_processing_state AS wps
            WHERE {readiness_sql}
            """
        ).fetchall()
        return {str(row["wallet"]) for row in rows}

    ready_wallets: set[str] = set()
    normalized_wallets = _normalize_wallets(wallets)
    for offset in range(0, len(normalized_wallets), SQLITE_IN_CHUNK_SIZE):
        chunk = normalized_wallets[offset : offset + SQLITE_IN_CHUNK_SIZE]
        rows = conn.execute(
            f"""
            SELECT wps.wallet
            FROM wallet_processing_state AS wps
            WHERE wps.wallet IN ({_placeholders(chunk)})
              AND {readiness_sql}
            """,
            tuple(chunk),
        ).fetchall()
        ready_wallets.update(str(row["wallet"]) for row in rows)
    return ready_wallets


def _build_leader_stats(conn: sqlite3.Connection, now: int) -> list[tuple[Any, ...]]:
    rows = conn.execute(
        """
        SELECT *
        FROM copy_pair_stats
        WHERE qualifies = 1
        ORDER BY leader_wallet, follower_wallet
        """
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(row["leader_wallet"], []).append(row)

    out: list[tuple[Any, ...]] = []
    for leader, pairs in grouped.items():
        followers = {row["follower_wallet"] for row in pairs}
        event_count = sum(int(row["copy_event_count"] or 0) for row in pairs)
        markets = conn.execute(
            """
            SELECT COUNT(DISTINCT COALESCE(condition_id, market_slug, asset_id))
            FROM copy_trade_links
            WHERE leader_wallet = ?
              AND follower_wallet IN (
                  SELECT follower_wallet
                  FROM copy_pair_stats
                  WHERE leader_wallet = ? AND qualifies = 1
              )
            """,
            (leader, leader),
        ).fetchone()[0]
        containments = [float(row["containment_pct"]) for row in pairs]
        lags = [float(row["median_lag_seconds"]) for row in pairs if row["median_lag_seconds"] is not None]
        last_copy = max((int(row["last_copy_ts"] or 0) for row in pairs), default=0) or None
        out.append(
            (
                leader,
                len(followers),
                event_count,
                int(markets or 0),
                median(containments) if containments else None,
                median(lags) if lags else None,
                len(followers),
                last_copy,
                now,
            )
        )
    return out


def _build_leader_stats_for_leaders(
    conn: sqlite3.Connection,
    leaders: list[str],
    now: int,
) -> list[tuple[Any, ...]]:
    if not leaders:
        return []
    rows = conn.execute(
        f"""
        SELECT *
        FROM copy_pair_stats
        WHERE qualifies = 1
          AND leader_wallet IN ({_placeholders(leaders)})
        ORDER BY leader_wallet, follower_wallet
        """,
        tuple(leaders),
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(row["leader_wallet"], []).append(row)

    out: list[tuple[Any, ...]] = []
    for leader, pairs in grouped.items():
        followers = {row["follower_wallet"] for row in pairs}
        event_count = sum(int(row["copy_event_count"] or 0) for row in pairs)
        markets = conn.execute(
            """
            SELECT COUNT(DISTINCT COALESCE(condition_id, market_slug, asset_id))
            FROM copy_trade_links
            WHERE leader_wallet = ?
              AND follower_wallet IN (
                  SELECT follower_wallet
                  FROM copy_pair_stats
                  WHERE leader_wallet = ? AND qualifies = 1
              )
            """,
            (leader, leader),
        ).fetchone()[0]
        containments = [float(row["containment_pct"]) for row in pairs]
        lags = [float(row["median_lag_seconds"]) for row in pairs if row["median_lag_seconds"] is not None]
        last_copy = max((int(row["last_copy_ts"] or 0) for row in pairs), default=0) or None
        out.append(
            (
                leader,
                len(followers),
                event_count,
                int(markets or 0),
                median(containments) if containments else None,
                median(lags) if lags else None,
                len(followers),
                last_copy,
                now,
            )
        )
    return out


def _merge_leader_features(conn: sqlite3.Connection, leaders: list[str] | None = None) -> None:
    if leaders:
        rows = conn.execute(
            f"SELECT * FROM copy_leader_stats WHERE leader_wallet IN ({_placeholders(leaders)})",
            tuple(leaders),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM copy_leader_stats").fetchall()
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
            leader_in_degree=float(row["leader_in_degree"]),
            copy_event_count=float(row["copy_event_count"]),
            copy_market_count=float(row["copy_market_count"]),
            containment_pct_median=row["containment_pct_median"],
            copy_stream_roi=feature.copy_stream_roi,
            edge_retention_pct=feature.edge_retention_pct,
            walk_forward_consistency_pct=feature.walk_forward_consistency_pct,
            survival_score=feature.survival_score,
            single_market_pnl_share=feature.single_market_pnl_share,
            net_to_gross_exposure=feature.net_to_gross_exposure,
            hygiene_status=feature.hygiene_status,
            primary_category=feature.primary_category,
            last_active_days_ago=feature.last_active_days_ago,
            extra={
                **feature.extra,
                "copy_graph_last_copy_event_at": row["last_copy_event_at"],
                "copy_graph_median_lag_seconds": row["median_lag_seconds"],
                "copy_graph_qualified_follower_count": row["qualified_follower_count"],
            },
        )
        upsert_wallet_feature(conn, merged)


def _clear_leader_features(conn: sqlite3.Connection, leaders: list[str] | None = None) -> None:
    ts = int(time.time())
    if leaders:
        conn.execute(
            f"""
            UPDATE wallet_features
            SET leader_in_degree = NULL,
                copy_event_count = NULL,
                copy_market_count = NULL,
                containment_pct_median = NULL,
                updated_at = ?
            WHERE address IN ({_placeholders(leaders)})
            """,
            (ts, *leaders),
        )
        return
    conn.execute(
        """
        UPDATE wallet_features
        SET leader_in_degree = NULL,
            copy_event_count = NULL,
            copy_market_count = NULL,
            containment_pct_median = NULL,
            updated_at = ?
        WHERE leader_in_degree IS NOT NULL
           OR copy_event_count IS NOT NULL
           OR copy_market_count IS NOT NULL
           OR containment_pct_median IS NOT NULL
        """,
        (ts,),
    )


def _delete_for_leaders(conn: sqlite3.Connection, table: str, column: str, leaders: list[str]) -> None:
    if not leaders:
        return
    conn.execute(
        f"DELETE FROM {table} WHERE {column} IN ({_placeholders(leaders)})",
        tuple(leaders),
    )


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
