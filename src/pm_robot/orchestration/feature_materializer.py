"""Small-batch wallet feature materialization from persisted evidence."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from statistics import median
from typing import Any

from pm_robot.models import WalletFeatures
from pm_robot.storage.db import retry_sqlite_locked
from pm_robot.storage.repository import _feature_from_row, upsert_wallet_feature


MATERIALIZER_VERSION = "2026-07-08-copy-validation-v1"
COPY_CANDIDATE_MIN_EVENTS = 10
COPY_CANDIDATE_MIN_MARKETS = 2
COPY_CANDIDATE_MIN_CONTAINMENT = 0.45
COPY_CANDIDATE_MIN_PRECEDES = 0.55


@dataclass(frozen=True)
class FeatureMaterializeSummary:
    wallets_attempted: int
    wallets_updated: int
    status: str
    error: str = ""


def materialize_wallet_features(
    conn: sqlite3.Connection,
    *,
    limit: int = 5,
    min_activity_events: int = 25,
    now: int | None = None,
) -> FeatureMaterializeSummary:
    ts = now or int(time.time())
    targets = _list_targets(conn, limit=limit, min_activity_events=min_activity_events)
    updated = 0
    error = ""
    status = "ok"
    for wallet in targets:
        try:
            feature = _materialize_wallet(conn, wallet, now=ts)
            if feature is None:
                continue
            _write_materialized_feature(conn, wallet, feature)
            updated += 1
        except Exception as exc:
            status = "partial"
            error = f"{wallet}: {exc}"
    return FeatureMaterializeSummary(
        wallets_attempted=len(targets),
        wallets_updated=updated,
        status=status,
        error=error,
    )


def materialize_wallet_feature(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    now: int | None = None,
    refresh_copyability: bool = False,
    commit: bool = True,
) -> bool:
    """Materialize one wallet immediately.

    The batch materializer deliberately skips many wallets whose existing
    feature fields are already filled. Copyability workers need a stronger
    refresh after targeted copy graph updates, so they can clear just the
    copy-owned fields before recomputing this single wallet.
    """

    ts = now or int(time.time())
    wallet = wallet.lower()
    if refresh_copyability:
        _write_with_retry(
            conn,
            lambda: _clear_copyability_materializer_fields(conn, wallet),
            commit=commit,
        )
    feature = _materialize_wallet(conn, wallet, now=ts)
    if feature is None:
        return False
    _write_materialized_feature(conn, wallet, feature, commit=commit)
    return True


def _write_materialized_feature(
    conn: sqlite3.Connection,
    wallet: str,
    feature: WalletFeatures,
    *,
    commit: bool = True,
) -> None:
    """Write one materialized wallet feature in its own short transaction."""

    def _operation() -> None:
        _clear_materializer_owned_nullable_fields(conn, wallet)
        upsert_wallet_feature(conn, feature)

    _write_with_retry(conn, _operation, commit=commit)


def _write_with_retry(conn: sqlite3.Connection, operation, *, commit: bool = True) -> None:
    if not commit:
        operation()
        return

    def _operation() -> None:
        operation()
        conn.commit()

    retry_sqlite_locked(_operation, rollback=conn.rollback, attempts=4, sleep_seconds=2.0)


def _list_targets(conn: sqlite3.Connection, *, limit: int, min_activity_events: int) -> list[str]:
    rows = conn.execute(
        """
        WITH latest_score AS (
            SELECT ls.address, ls.review_reason
            FROM leader_scores ls
            JOIN (
                SELECT address, MAX(score_id) AS score_id
                FROM leader_scores
                GROUP BY address
            ) latest
              ON latest.score_id = ls.score_id
        ),
        candidate_targets AS (
            SELECT
                cw.address,
                COALESCE(ls.review_reason, '') AS review_reason,
                COALESCE(
                    NULLIF(wps.activity_count, 0),
                    NULLIF(ebb.current_depth, 0),
                    (
                        SELECT COUNT(*)
                        FROM wallet_activity wallet_activity_count
                        WHERE wallet_activity_count.address = cw.address
                    ),
                    0
                ) AS wallet_activity_count,
                COALESCE(pwq.total_roi, -999.0) AS paper_roi,
                COALESCE(pwq.orders, 0) AS paper_orders,
                COALESCE(pwq.settled_positions, 0) AS settled_positions,
                COALESCE(wps.evidence_status, '') AS evidence_status,
                COALESCE(wps.current_stage, '') AS current_stage,
                COALESCE(wps.next_action, '') AS next_action,
                COALESCE(wps.priority, 100) AS pipeline_priority,
                COALESCE(ebb.stage, '') AS backfill_stage,
                wf.maker_fraction,
                wf.bot_score,
                wf.leader_in_degree,
                wf.copy_event_count,
                wf.copy_market_count,
                wf.copy_stream_roi,
                wf.single_market_pnl_share,
                wf.net_to_gross_exposure,
                wf.extra_json,
                COALESCE(cw.updated_at, 0) AS candidate_updated_at
            FROM candidate_wallets cw
            LEFT JOIN latest_score ls
              ON ls.address = cw.address
            LEFT JOIN wallet_features wf
              ON wf.address = cw.address
            LEFT JOIN paper_wallet_quality pwq
              ON pwq.wallet = cw.address
            LEFT JOIN wallet_processing_state wps
              ON wps.wallet = cw.address
            LEFT JOIN evidence_backfill_budget ebb
              ON ebb.wallet = cw.address
            LEFT JOIN wallet_registry wr
              ON wr.address = cw.address
            WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
              AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
        )
        SELECT
            address
        FROM candidate_targets
        WHERE (
               wallet_activity_count >= ?
            OR EXISTS (
                SELECT 1 FROM copy_pair_stats qualified_pair
                WHERE qualified_pair.leader_wallet = candidate_targets.address
                  AND qualified_pair.qualifies = 1
            )
        )
           AND (
               (maker_fraction IS NULL AND instr(COALESCE(extra_json, '{}'), ?) = 0)
            OR leader_in_degree IS NULL
            OR copy_event_count IS NULL
            OR copy_market_count IS NULL
            OR copy_stream_roi IS NULL
            OR single_market_pnl_share IS NULL
            OR net_to_gross_exposure IS NULL
            OR instr(COALESCE(extra_json, '{}'), 'paper_roi_after_slippage') = 0
           )
        ORDER BY
            CASE
                WHEN review_reason LIKE 'missing_required_score_components:%'
                 AND (
                        bot_score IS NULL
                     OR leader_in_degree IS NULL
                     OR copy_event_count IS NULL
                     OR copy_market_count IS NULL
                     OR copy_stream_roi IS NULL
                     OR single_market_pnl_share IS NULL
                     OR net_to_gross_exposure IS NULL
                 ) THEN 0
                WHEN next_action = 'score_wallet' AND current_stage = 'deep_done' THEN 1
                WHEN next_action = 'score_wallet' AND current_stage = 'medium_done' THEN 2
                WHEN next_action = 'score_wallet' THEN 3
                WHEN evidence_status = 'summary_ready' THEN 4
                ELSE 5
            END ASC,
            pipeline_priority ASC,
            CASE WHEN EXISTS (
                SELECT 1 FROM copy_pair_stats qualified_pair
                WHERE qualified_pair.leader_wallet = candidate_targets.address
                  AND qualified_pair.qualifies = 1
            ) THEN 0 ELSE 1 END ASC,
            CASE WHEN paper_roi > 0 THEN 0 ELSE 1 END ASC,
            paper_orders DESC,
            settled_positions DESC,
            CASE backfill_stage
                WHEN 'medium_pending' THEN 0
                WHEN 'medium_done' THEN 1
                WHEN 'light_done' THEN 2
                WHEN 'light_pending' THEN 3
                ELSE 4
            END ASC,
            wallet_activity_count DESC,
            candidate_updated_at DESC,
            address ASC
        LIMIT ?
        """,
        (min_activity_events, MATERIALIZER_VERSION, limit),
    ).fetchall()
    return [str(row["address"]) for row in rows]


def _materialize_wallet(conn: sqlite3.Connection, wallet: str, *, now: int) -> WalletFeatures | None:
    wallet = wallet.lower()
    existing_row = conn.execute("SELECT * FROM wallet_features WHERE address = ?", (wallet,)).fetchone()
    base = _feature_from_row(existing_row) if existing_row else WalletFeatures(address=wallet)
    activity = _activity_stats(conn, wallet)
    if not activity:
        return None
    episodes = _episode_stats(conn, wallet)
    copy = _copy_stats(conn, wallet)
    paper = _paper_stats(conn, wallet)
    extra = {
        **base.extra,
        "feature_materializer_version": MATERIALIZER_VERSION,
        "feature_materialized_at": now,
        "feature_materializer_activity_count": activity["trade_count"],
        "feature_materializer_distinct_markets": activity["distinct_markets"],
        "feature_materializer_non_fast_trade_count": activity["non_fast_trade_count"],
        "feature_materializer_fast_market_share": activity["fast_market_share"],
    }
    if paper:
        extra["paper_roi_after_slippage"] = paper["paper_roi_after_slippage"]
        extra["paper_orders"] = paper["orders"]
        extra["paper_settled_positions"] = paper["settled_positions"]
    if copy.get("copy_stream_roi_source"):
        extra["copy_stream_roi_source"] = copy["copy_stream_roi_source"]
    if copy.get("copy_candidate_pair_count") is not None:
        extra["copy_candidate_thresholds"] = {
            "min_events": COPY_CANDIDATE_MIN_EVENTS,
            "min_markets": COPY_CANDIDATE_MIN_MARKETS,
            "min_containment": COPY_CANDIDATE_MIN_CONTAINMENT,
            "min_leader_precedes": COPY_CANDIDATE_MIN_PRECEDES,
        }
        extra["copy_candidate_pair_count"] = copy["copy_candidate_pair_count"]
        extra["copy_candidate_follower_count"] = copy["copy_candidate_follower_count"]
        extra["copy_candidate_event_count"] = copy["copy_candidate_event_count"]
        extra["copy_candidate_market_count"] = copy["copy_candidate_market_count"]
        extra["copy_candidate_containment_median"] = copy["copy_candidate_containment_median"]
        extra["copy_candidate_precedes_median"] = copy["copy_candidate_precedes_median"]
        extra["copy_validated_pair_count"] = copy.get("copy_validated_pair_count", 0)
    official_role_evidence = str(base.extra.get("maker_fraction_source") or "").startswith(
        "polymarket_data_api_trades"
    )
    if activity.get("maker_fraction_source") and not official_role_evidence:
        extra["maker_fraction_source"] = activity["maker_fraction_source"]

    event_win_rate = _first_not_none(base.event_win_rate, episodes.get("event_win_rate"))
    trade_win_rate = _first_not_none(base.trade_win_rate, episodes.get("trade_win_rate"), event_win_rate)
    return WalletFeatures(
        address=wallet,
        cumulative_win_rate=_first_not_none(base.cumulative_win_rate, event_win_rate, trade_win_rate),
        recent_30d_volume_usdc=_first_not_none(base.recent_30d_volume_usdc, activity["recent_30d_volume_usdc"]),
        net_pnl_usdc=_first_not_none(base.net_pnl_usdc, episodes.get("net_pnl_usdc")),
        total_volume_usdc=_first_not_none(base.total_volume_usdc, activity["total_volume_usdc"], episodes.get("total_volume_usdc")),
        event_win_rate=event_win_rate,
        trade_win_rate=trade_win_rate,
        avg_dca_entries=_first_not_none(base.avg_dca_entries, episodes.get("avg_dca_entries")),
        sell_pct=_first_not_none(base.sell_pct, activity["sell_pct"]),
        bot_score=_first_not_none(base.bot_score, activity["bot_score"]),
        trades_per_day=_first_not_none(base.trades_per_day, activity["trades_per_day"]),
        median_gap_sec=_first_not_none(base.median_gap_sec, activity["median_gap_sec"]),
        maker_fraction=(
            base.maker_fraction if official_role_evidence else activity["maker_fraction"]
        ),
        leader_in_degree=_first_not_none(base.leader_in_degree, copy["leader_in_degree"]),
        copy_event_count=_first_not_none(base.copy_event_count, copy["copy_event_count"]),
        copy_market_count=_first_not_none(base.copy_market_count, copy["copy_market_count"]),
        containment_pct_median=_first_not_none(base.containment_pct_median, copy["containment_pct_median"]),
        copy_stream_roi=_first_not_none(base.copy_stream_roi, copy["copy_stream_roi"]),
        edge_retention_pct=base.edge_retention_pct,
        walk_forward_consistency_pct=base.walk_forward_consistency_pct,
        survival_score=base.survival_score,
        single_market_pnl_share=_first_not_none(base.single_market_pnl_share, episodes.get("single_market_pnl_share")),
        net_to_gross_exposure=_first_not_none(base.net_to_gross_exposure, episodes.get("net_to_gross_exposure")),
        hygiene_status=_hygiene_status(base, activity),
        primary_category=base.primary_category,
        last_active_days_ago=_first_not_none(base.last_active_days_ago, activity["last_active_days_ago"]),
        extra=extra,
    )


def _activity_stats(conn: sqlite3.Connection, wallet: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT timestamp, side, usdc_size, raw_json, market_slug
        FROM wallet_activity
        WHERE address = ? AND type = 'TRADE'
        ORDER BY timestamp ASC, activity_id ASC
        """,
        (wallet,),
    ).fetchall()
    if not rows:
        return {}
    timestamps = [int(row["timestamp"] or 0) for row in rows if int(row["timestamp"] or 0) > 0]
    latest = max(timestamps) if timestamps else 0
    oldest = min(timestamps) if timestamps else latest
    recent_cutoff = latest - 30 * 86_400
    total_volume = sum(float(row["usdc_size"] or 0.0) for row in rows)
    recent_volume = sum(
        float(row["usdc_size"] or 0.0)
        for row in rows
        if int(row["timestamp"] or 0) >= recent_cutoff
    )
    buy_count = sum(1 for row in rows if str(row["side"] or "").upper() == "BUY")
    sell_count = sum(1 for row in rows if str(row["side"] or "").upper() == "SELL")
    gaps = [b - a for a, b in zip(timestamps, timestamps[1:]) if b >= a]
    days = max((latest - oldest) / 86_400, 1.0) if latest and oldest else 1.0
    fast_share = _fast_market_share(rows)
    markets = [str(row["market_slug"] or "") for row in rows]
    distinct_markets = len({market for market in markets if market})
    non_fast_trade_count = sum(1 for market in markets if market and not _is_fast_market(market))
    maker_fraction, maker_source = _maker_fraction(rows)
    median_gap = median(gaps) if gaps else None
    trades_per_day = len(rows) / days
    bot_score = _bot_score(trades_per_day=trades_per_day, median_gap=median_gap, fast_share=fast_share)
    return {
        "trade_count": len(rows),
        "distinct_markets": distinct_markets,
        "non_fast_trade_count": non_fast_trade_count,
        "fast_market_share": fast_share,
        "recent_30d_volume_usdc": recent_volume,
        "total_volume_usdc": total_volume,
        "sell_pct": (sell_count / max(buy_count + sell_count, 1)) * 100.0,
        "median_gap_sec": median_gap,
        "trades_per_day": trades_per_day,
        "bot_score": bot_score,
        "maker_fraction": maker_fraction,
        "maker_fraction_source": maker_source,
        "last_active_days_ago": 0.0,
    }


def _episode_stats(conn: sqlite3.Connection, wallet: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT *
        FROM wallet_episodes
        WHERE address = ?
        """,
        (wallet,),
    ).fetchall()
    if not rows:
        return {
            "single_market_pnl_share": 0.0,
            "net_to_gross_exposure": 1.0,
        }
    total_bought = sum(float(row["bought_usdc"] or 0.0) for row in rows)
    total_sold = sum(float(row["sold_usdc"] or 0.0) for row in rows)
    gross = total_bought + total_sold
    net = abs(total_bought - total_sold)
    pnl_by_market: dict[str, float] = {}
    for row in rows:
        market = str(row["market_slug"] or row["condition_id"] or row["asset_id"] or "")
        pnl_by_market[market] = pnl_by_market.get(market, 0.0) + float(row["realized_pnl_est"] or 0.0)
    total_abs_pnl = sum(abs(value) for value in pnl_by_market.values())
    top_abs_pnl = max((abs(value) for value in pnl_by_market.values()), default=0.0)
    closed = [row for row in rows if row["status"] == "closed"]
    wins = [row for row in closed if float(row["realized_pnl_est"] or 0.0) > 0]
    buy_count = sum(int(row["buy_count"] or 0) for row in rows)
    sell_count = sum(int(row["sell_count"] or 0) for row in rows)
    profitable_sells = sum(
        1
        for row in rows
        if int(row["sell_count"] or 0) > 0 and float(row["realized_pnl_est"] or 0.0) > 0
    )
    return {
        "net_pnl_usdc": sum(float(row["realized_pnl_est"] or 0.0) for row in rows),
        "total_volume_usdc": gross,
        "event_win_rate": (len(wins) / len(closed)) if closed else None,
        "trade_win_rate": profitable_sells / max(buy_count + sell_count, 1),
        "avg_dca_entries": buy_count / max(len(rows), 1),
        "single_market_pnl_share": (top_abs_pnl / total_abs_pnl) if total_abs_pnl else 0.0,
        "net_to_gross_exposure": (net / gross) if gross else 1.0,
    }


def _copy_stats(conn: sqlite3.Connection, wallet: str) -> dict[str, Any]:
    perf = conn.execute(
        "SELECT * FROM copy_leader_performance WHERE leader_wallet = ?",
        (wallet,),
    ).fetchone()
    if perf:
        return {
            "leader_in_degree": float(perf["backtest_trade_count"] or 0),
            "copy_event_count": float(perf["backtest_trade_count"] or 0),
            "copy_market_count": float(perf["copied_market_count"] or 0),
            "containment_pct_median": None,
            "copy_stream_roi": float(perf["net_roi"] or 0.0),
            "copy_stream_roi_source": "copy_leader_performance",
        }
    rows = conn.execute(
        """
        SELECT *
        FROM copy_pair_stats
        WHERE leader_wallet = ?
          AND copy_event_count >= ?
          AND copy_market_count >= ?
          AND containment_pct >= ?
          AND leader_precedes_pct >= ?
        ORDER BY copy_event_count DESC
        """,
        (
            wallet,
            COPY_CANDIDATE_MIN_EVENTS,
            COPY_CANDIDATE_MIN_MARKETS,
            COPY_CANDIDATE_MIN_CONTAINMENT,
            COPY_CANDIDATE_MIN_PRECEDES,
        ),
    ).fetchall()
    if not rows:
        return {
            "leader_in_degree": 0.0,
            "copy_event_count": 0.0,
            "copy_market_count": 0.0,
            "containment_pct_median": 0.0,
            "copy_stream_roi": 0.0,
            "copy_stream_roi_source": "no_copy_backtest_default_zero",
            "copy_candidate_pair_count": 0,
            "copy_candidate_follower_count": 0,
            "copy_candidate_event_count": 0,
            "copy_candidate_market_count": 0,
            "copy_candidate_containment_median": 0.0,
            "copy_candidate_precedes_median": 0.0,
            "copy_validated_pair_count": 0,
        }
    qualified_rows = [row for row in rows if int(row["qualifies"] or 0) == 1]
    followers = {row["follower_wallet"] for row in rows}
    containments = [float(row["containment_pct"] or 0.0) for row in rows]
    precedes = [float(row["leader_precedes_pct"] or 0.0) for row in rows]
    event_count = float(sum(int(row["copy_event_count"] or 0) for row in rows))
    market_count = float(sum(int(row["copy_market_count"] or 0) for row in rows))
    if not qualified_rows:
        return {
            "leader_in_degree": 0.0,
            "copy_event_count": 0.0,
            "copy_market_count": 0.0,
            "containment_pct_median": 0.0,
            "copy_stream_roi": 0.0,
            "copy_stream_roi_source": "copy_candidate_pair_stats_unvalidated_default_zero",
            "copy_candidate_pair_count": len(rows),
            "copy_candidate_follower_count": len(followers),
            "copy_candidate_event_count": event_count,
            "copy_candidate_market_count": market_count,
            "copy_candidate_containment_median": median(containments) if containments else 0.0,
            "copy_candidate_precedes_median": median(precedes) if precedes else 0.0,
            "copy_validated_pair_count": 0,
        }
    qualified_followers = {row["follower_wallet"] for row in qualified_rows}
    qualified_containments = [float(row["containment_pct"] or 0.0) for row in qualified_rows]
    qualified_event_count = float(sum(int(row["copy_event_count"] or 0) for row in qualified_rows))
    qualified_market_count = float(sum(int(row["copy_market_count"] or 0) for row in qualified_rows))
    return {
        "leader_in_degree": float(len(qualified_followers)),
        "copy_event_count": qualified_event_count,
        "copy_market_count": qualified_market_count,
        "containment_pct_median": median(qualified_containments) if qualified_containments else 0.0,
        "copy_stream_roi": 0.0,
        "copy_stream_roi_source": "copy_qualified_pair_stats_without_backtest_default_zero",
        "copy_candidate_pair_count": len(rows),
        "copy_candidate_follower_count": len(followers),
        "copy_candidate_event_count": event_count,
        "copy_candidate_market_count": market_count,
        "copy_candidate_containment_median": median(containments) if containments else 0.0,
        "copy_candidate_precedes_median": median(precedes) if precedes else 0.0,
        "copy_validated_pair_count": len(qualified_rows),
    }


def _paper_stats(conn: sqlite3.Connection, wallet: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM paper_wallet_quality WHERE wallet = ?", (wallet,)).fetchone()
    if not row:
        return {}
    total_roi = float(row["total_roi"] or 0.0)
    return {
        "orders": int(row["orders"] or 0),
        "settled_positions": int(row["settled_positions"] or 0),
        "paper_roi_after_slippage": total_roi - 0.015,
    }


def _maker_fraction(rows: list[sqlite3.Row]) -> tuple[float, str]:
    observed = 0
    maker = 0
    for row in rows:
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except json.JSONDecodeError:
            raw = {}
        for key in ("maker", "isMaker", "is_maker", "makerSide"):
            if key not in raw:
                continue
            observed += 1
            value = raw.get(key)
            if value is True or str(value).lower() in {"true", "maker", "yes", "1"}:
                maker += 1
            break
    if observed:
        return maker / observed, "raw_activity_maker_flags"
    return None, "public_activity_no_maker_flags_observed"


def _hygiene_status(base: WalletFeatures, activity: dict[str, Any]) -> str:
    existing = (base.hygiene_status or "").strip().lower()
    if existing in {"routing_operator", "wash", "wash_trade", "market_maker_taker"}:
        return existing
    official_role_evidence = str(base.extra.get("maker_fraction_source") or "").startswith(
        "polymarket_data_api_trades"
    )
    if official_role_evidence and existing in {"clean", "screened"} and base.maker_fraction is not None:
        return existing
    if existing in {"clean", "screened"}:
        return existing
    source = str(activity.get("maker_fraction_source") or "")
    if source == "raw_activity_maker_flags" and activity.get("maker_fraction") is not None:
        return "screened"
    if int(activity.get("trade_count") or 0) >= 25:
        return "screened"
    return existing or "incomplete"


def _clear_materializer_owned_nullable_fields(conn: sqlite3.Connection, wallet: str) -> None:
    """Allow recomputation to clear stale optimistic values."""
    conn.execute(
        """
        UPDATE wallet_features
        SET maker_fraction = NULL,
            hygiene_status = CASE
                WHEN lower(hygiene_status) IN (
                    'routing_operator', 'wash', 'wash_trade', 'market_maker_taker'
                ) THEN hygiene_status
                ELSE hygiene_status
            END
        WHERE address = ?
          AND COALESCE(json_extract(extra_json, '$.maker_fraction_source'), '')
              NOT LIKE 'polymarket_data_api_trades%'
        """,
        (wallet.lower(),),
    )


def _clear_copyability_materializer_fields(conn: sqlite3.Connection, wallet: str) -> None:
    conn.execute(
        """
        UPDATE wallet_features
        SET leader_in_degree = NULL,
            copy_event_count = NULL,
            copy_market_count = NULL,
            containment_pct_median = NULL,
            copy_stream_roi = NULL,
            updated_at = ?
        WHERE address = ?
        """,
        (int(time.time()), wallet.lower()),
    )


def _fast_market_share(rows: list[sqlite3.Row]) -> float:
    if not rows:
        return 0.0
    fast = 0
    for row in rows:
        if _is_fast_market(str(row["market_slug"] or "")):
            fast += 1
    return fast / len(rows)


def _is_fast_market(market_slug: str) -> bool:
    value = market_slug.lower()
    return "updown-5m" in value or "btc-up-or-down-5m" in value or value.startswith("btc-updown-5m")


def _bot_score(*, trades_per_day: float, median_gap: float | None, fast_share: float) -> float:
    score = 0.0
    if trades_per_day >= 500:
        score += 45
    elif trades_per_day >= 100:
        score += 25
    elif trades_per_day >= 25:
        score += 10
    if median_gap is not None:
        if median_gap <= 5:
            score += 35
        elif median_gap <= 60:
            score += 20
        elif median_gap <= 300:
            score += 8
    if fast_share >= 0.85:
        score += 25
    elif fast_share >= 0.5:
        score += 10
    return min(score, 100.0)


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None
