"""Paper portfolio accounting and mark-to-market summaries."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from pm_robot.execution.paper_broker import PAPER_MAX_PRICE, PAPER_MIN_PRICE
from pm_robot.risk.gates import stable_readiness_status
from pm_robot.storage.repository import apply_paper_quality_blocks


@dataclass(frozen=True)
class PaperPortfolioSummary:
    fills_created: int
    positions_written: int
    settlements_written: int
    marks_written: int
    wallets_written: int
    quality_rows_written: int
    wallets_blocked: int
    missing_marks: int


@dataclass(frozen=True)
class Mark:
    price: float
    source: str
    is_settlement: bool = False


MIN_PRODUCTION_ORDERS = 200
MIN_PRODUCTION_SETTLED_POSITIONS = 30
MIN_PRODUCTION_MARK_COVERAGE = 0.70
MIN_PRODUCTION_VALIDATION_DAYS = 14
MAX_PRODUCTION_DRAWDOWN_PCT = 0.15
MAX_PRODUCTION_MARKET_EXPOSURE_SHARE = 0.25
MARK_HEARTBEAT_SECONDS = 21_600
READINESS_HEARTBEAT_SECONDS = 21_600


def settle_paper_portfolio(conn: sqlite3.Connection, *, now: int | None = None) -> PaperPortfolioSummary:
    settled_at = now or int(time.time())
    _delete_portfolio_ineligible_fills(conn)
    fills_created = _create_missing_fills(conn, settled_at)
    positions = _position_rows(conn)
    mark_cache: dict[str, sqlite3.Row | None] = {}
    marks_written = 0
    missing_marks = 0

    conn.execute("DELETE FROM paper_positions")
    conn.execute("DELETE FROM paper_settlements")
    for position in positions:
        mark = _mark_position(conn, position, mark_cache)
        if mark.source == "entry_price_fallback":
            missing_marks += 1
        mark_value = position["shares"] * mark.price
        realized = mark_value - position["cost_usd"] if mark.is_settlement else 0.0
        unrealized = 0.0 if mark.is_settlement else mark_value - position["cost_usd"]
        status = "resolved" if mark.is_settlement else "open"
        conn.execute(
            """
            INSERT INTO paper_positions(
                wallet, market_slug, asset_id, outcome, shares, cost_usd, avg_price,
                mark_price, mark_value_usd, unrealized_pnl_usd, realized_pnl_usd,
                status, opened_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position["wallet"],
                position["market_slug"],
                position["asset_id"],
                position["outcome"],
                position["shares"],
                position["cost_usd"],
                position["avg_price"],
                mark.price,
                mark_value,
                unrealized,
                realized,
                status,
                position["opened_at"],
                settled_at,
            ),
        )
        if mark.is_settlement:
            conn.execute(
                """
                INSERT INTO paper_settlements(
                    wallet, market_slug, asset_id, outcome, shares, cost_usd,
                    settlement_price, payout_usd, realized_pnl_usd,
                    settlement_source, settled_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position["wallet"],
                    position["market_slug"],
                    position["asset_id"],
                    position["outcome"],
                    position["shares"],
                    position["cost_usd"],
                    mark.price,
                    mark_value,
                    realized,
                    mark.source,
                    settled_at,
                    settled_at,
                ),
            )
        if _should_write_mark(
            conn,
            wallet=str(position["wallet"]),
            asset_id=str(position["asset_id"]),
            price=mark.price,
            source=mark.source,
            marked_at=settled_at,
        ):
            conn.execute(
                """
                INSERT INTO paper_marks(wallet, market_slug, asset_id, mark_price, mark_source, marked_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    position["wallet"],
                    position["market_slug"],
                    position["asset_id"],
                    mark.price,
                    mark.source,
                    settled_at,
                ),
            )
            marks_written += 1

    wallets_written = _write_wallet_performance(conn, settled_at)
    quality_rows_written = _write_wallet_quality(conn, settled_at)
    _write_readiness_observations(conn, settled_at)
    wallets_blocked = apply_paper_quality_blocks(conn, now=settled_at)
    conn.commit()
    return PaperPortfolioSummary(
        fills_created=fills_created,
        positions_written=len(positions),
        settlements_written=_count_table(conn, "paper_settlements"),
        marks_written=marks_written,
        wallets_written=wallets_written,
        quality_rows_written=quality_rows_written,
        wallets_blocked=wallets_blocked,
        missing_marks=missing_marks,
    )


def _create_missing_fills(conn: sqlite3.Connection, filled_at: int) -> int:
    rows = conn.execute(
        """
        SELECT po.*
        FROM paper_orders po
        LEFT JOIN paper_fills pf ON pf.order_id = po.order_id
        WHERE po.accepted = 1
          AND pf.order_id IS NULL
          AND po.price >= ?
          AND po.price <= ?
        ORDER BY po.created_at ASC, po.order_id ASC
        """,
        (PAPER_MIN_PRICE, PAPER_MAX_PRICE),
    ).fetchall()
    for row in rows:
        side = str(row["side"]).upper()
        shares = row["stake_usd"] / row["price"]
        if side == "SELL":
            shares = -shares
        conn.execute(
            """
            INSERT INTO paper_fills(
                order_id, wallet, market_slug, asset_id, outcome, side,
                fill_price, stake_usd, shares, filled_at, source_order_created_at,
                leader_price, fee_usd, slippage_bps, validation_cohort
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["order_id"],
                row["wallet"],
                row["market_slug"],
                row["asset_id"],
                row["outcome"],
                side,
                row["price"],
                row["stake_usd"],
                shares,
                filled_at,
                row["created_at"],
                row["leader_price"],
                row["fee_usd"],
                row["slippage_bps"],
                row["validation_cohort"],
            ),
        )
    return len(rows)


def _delete_portfolio_ineligible_fills(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        DELETE FROM paper_fills
        WHERE order_id IN (
            SELECT order_id
            FROM paper_orders
            WHERE price < ? OR price > ? OR accepted != 1
        )
        """,
        (PAPER_MIN_PRICE, PAPER_MAX_PRICE),
    )


def _position_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            wallet,
            market_slug,
            asset_id,
            outcome,
            SUM(shares) AS shares,
            SUM(
                CASE WHEN side = 'BUY'
                    THEN stake_usd + fee_usd
                    ELSE -stake_usd + fee_usd
                END
            ) AS cost_usd,
            CASE
                WHEN SUM(shares) = 0 THEN 0
                ELSE SUM(
                    CASE WHEN side = 'BUY'
                        THEN stake_usd + fee_usd
                        ELSE -stake_usd + fee_usd
                    END
                ) / SUM(shares)
            END AS avg_price,
            MIN(source_order_created_at) AS opened_at
        FROM paper_fills
        WHERE validation_cohort = 'validation'
        GROUP BY wallet, asset_id
        HAVING ABS(SUM(shares)) > 0.0000001
        ORDER BY wallet ASC, asset_id ASC
        """
    ).fetchall()


def _mark_position(
    conn: sqlite3.Connection,
    position: sqlite3.Row,
    mark_cache: dict[str, sqlite3.Row | None],
) -> Mark:
    market_slug = position["market_slug"]
    if market_slug not in mark_cache:
        mark_cache[market_slug] = conn.execute(
            """
            SELECT *
            FROM gamma_market_cache
            WHERE market_slug = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (market_slug,),
        ).fetchone()
    gamma_row = mark_cache[market_slug]
    gamma_mark = _gamma_asset_mark(gamma_row, str(position["asset_id"])) if gamma_row else None
    if gamma_mark is not None:
        is_settlement = _is_settlement_mark(gamma_row, gamma_mark)
        return Mark(
            price=gamma_mark,
            source="gamma_settlement" if is_settlement else "gamma_outcome_price",
            is_settlement=is_settlement,
        )
    return Mark(price=float(position["avg_price"]), source="entry_price_fallback")


def _gamma_asset_mark(row: sqlite3.Row, asset_id: str) -> float | None:
    token_ids = [str(item) for item in _json_list(row["clob_token_ids_json"])]
    prices = [_to_float(item) for item in _json_list(row["outcome_prices_json"])]
    if asset_id in token_ids:
        idx = token_ids.index(asset_id)
        if idx < len(prices) and prices[idx] is not None:
            return prices[idx]

    raw = _json_object(row["raw_json"])
    tokens = raw.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            token_id = str(token.get("token_id") or token.get("tokenId") or token.get("id") or "")
            if token_id != asset_id:
                continue
            price = _to_float(token.get("price") or token.get("last_price") or token.get("lastPrice"))
            if price is not None:
                return price
    return None


def _is_settlement_mark(row: sqlite3.Row | None, price: float) -> bool:
    if row is None or int(row["closed"] or 0) != 1:
        return False
    return price <= 0.001 or price >= 0.999


def _write_wallet_performance(conn: sqlite3.Connection, updated_at: int) -> int:
    conn.execute("DELETE FROM paper_wallet_performance")
    rows = conn.execute(
        """
        SELECT
            orders.wallet AS wallet,
            orders.orders AS orders,
            COALESCE(positions.open_positions, 0) AS open_positions,
            COALESCE(positions.total_cost_usd, 0) AS total_cost_usd,
            COALESCE(positions.mark_value_usd, 0) AS mark_value_usd,
            COALESCE(positions.unrealized_pnl_usd, 0) AS unrealized_pnl_usd,
            COALESCE(positions.realized_pnl_usd, 0) AS realized_pnl_usd
        FROM (
            SELECT wallet, COUNT(*) AS orders
            FROM paper_fills
            WHERE validation_cohort = 'validation'
            GROUP BY wallet
        ) orders
        LEFT JOIN (
            SELECT
                wallet,
                SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_positions,
                SUM(cost_usd) AS total_cost_usd,
                SUM(mark_value_usd) AS mark_value_usd,
                SUM(unrealized_pnl_usd) AS unrealized_pnl_usd,
                SUM(realized_pnl_usd) AS realized_pnl_usd
            FROM paper_positions
            GROUP BY wallet
        ) positions ON positions.wallet = orders.wallet
        """
    ).fetchall()
    for row in rows:
        total_pnl = row["unrealized_pnl_usd"] + row["realized_pnl_usd"]
        roi = total_pnl / row["total_cost_usd"] if row["total_cost_usd"] else 0.0
        conn.execute(
            """
            INSERT INTO paper_wallet_performance(
                wallet, orders, open_positions, total_cost_usd, mark_value_usd,
                unrealized_pnl_usd, realized_pnl_usd, total_pnl_usd, roi, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["wallet"],
                row["orders"],
                row["open_positions"],
                row["total_cost_usd"],
                row["mark_value_usd"],
                row["unrealized_pnl_usd"],
                row["realized_pnl_usd"],
                total_pnl,
                roi,
                updated_at,
            ),
        )
    return len(rows)


def _write_wallet_quality(conn: sqlite3.Connection, updated_at: int) -> int:
    conn.execute("DELETE FROM paper_wallet_quality")
    rows = conn.execute(
        """
        WITH latest_marks AS (
            SELECT pm.wallet, pm.asset_id, pm.mark_source
            FROM paper_marks pm
            WHERE pm.mark_id = (
                SELECT pm2.mark_id
                FROM paper_marks pm2
                WHERE pm2.wallet = pm.wallet
                  AND pm2.asset_id = pm.asset_id
                ORDER BY pm2.marked_at DESC, pm2.mark_id DESC
                LIMIT 1
            )
        ),
        position_stats AS (
            SELECT
                pp.wallet AS wallet,
                SUM(CASE WHEN pp.status = 'open' THEN 1 ELSE 0 END) AS open_positions,
                SUM(CASE WHEN pp.status = 'resolved' THEN 1 ELSE 0 END) AS settled_positions,
                SUM(CASE WHEN lm.mark_source IN ('gamma_outcome_price', 'gamma_settlement') THEN 1 ELSE 0 END)
                    AS gamma_marked_positions,
                SUM(CASE WHEN lm.mark_source = 'entry_price_fallback' THEN 1 ELSE 0 END)
                    AS fallback_marked_positions,
                COUNT(*) AS total_positions,
                SUM(CASE WHEN pp.status = 'resolved' THEN pp.cost_usd ELSE 0 END) AS settled_cost_usd,
                SUM(CASE WHEN pp.status = 'resolved' THEN pp.realized_pnl_usd ELSE 0 END) AS settled_pnl_usd,
                SUM(pp.cost_usd) AS total_cost_usd,
                SUM(pp.unrealized_pnl_usd + pp.realized_pnl_usd) AS total_pnl_usd
            FROM paper_positions pp
            LEFT JOIN latest_marks lm
              ON lm.wallet = pp.wallet
             AND lm.asset_id = pp.asset_id
            GROUP BY pp.wallet
        ),
        orders AS (
            SELECT
                wallet,
                COUNT(*) AS orders,
                MIN(source_order_created_at) AS first_order_at,
                MAX(source_order_created_at) AS last_order_at
            FROM paper_fills
            WHERE validation_cohort = 'validation'
            GROUP BY wallet
        )
        SELECT
            orders.wallet AS wallet,
            orders.orders AS orders,
            COALESCE(position_stats.open_positions, 0) AS open_positions,
            COALESCE(position_stats.settled_positions, 0) AS settled_positions,
            COALESCE(position_stats.gamma_marked_positions, 0) AS gamma_marked_positions,
            COALESCE(position_stats.fallback_marked_positions, 0) AS fallback_marked_positions,
            COALESCE(position_stats.total_positions, 0) AS total_positions,
            COALESCE(position_stats.settled_cost_usd, 0) AS settled_cost_usd,
            COALESCE(position_stats.settled_pnl_usd, 0) AS settled_pnl_usd,
            COALESCE(position_stats.total_cost_usd, 0) AS total_cost_usd,
            COALESCE(position_stats.total_pnl_usd, 0) AS total_pnl_usd,
            COALESCE(orders.first_order_at, 0) AS first_order_at,
            COALESCE(orders.last_order_at, 0) AS last_order_at
        FROM orders
        LEFT JOIN position_stats ON position_stats.wallet = orders.wallet
        """,
        (),
    ).fetchall()
    for row in rows:
        mark_coverage = (
            float(row["gamma_marked_positions"]) / float(row["total_positions"])
            if row["total_positions"]
            else 0.0
        )
        settled_roi = row["settled_pnl_usd"] / row["settled_cost_usd"] if row["settled_cost_usd"] else 0.0
        total_roi = row["total_pnl_usd"] / row["total_cost_usd"] if row["total_cost_usd"] else 0.0
        max_drawdown_pct = _max_drawdown_pct(conn, str(row["wallet"]))
        max_market_exposure_share = _max_market_exposure_share(conn, str(row["wallet"]))
        validation_days = max(
            0.0,
            (float(row["last_order_at"]) - float(row["first_order_at"])) / 86_400.0,
        )
        blockers = _quality_blockers(
            row,
            mark_coverage,
            settled_roi,
            total_roi,
            max_drawdown_pct,
            max_market_exposure_share,
            validation_days,
        )
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at,
                max_drawdown_pct, max_market_exposure_share, validation_days
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["wallet"],
                row["orders"],
                row["open_positions"],
                row["settled_positions"],
                row["gamma_marked_positions"],
                row["fallback_marked_positions"],
                mark_coverage,
                row["settled_cost_usd"],
                row["settled_pnl_usd"],
                settled_roi,
                row["total_pnl_usd"],
                total_roi,
                0 if blockers else 1,
                json.dumps(blockers, ensure_ascii=False),
                updated_at,
                max_drawdown_pct,
                max_market_exposure_share,
                validation_days,
            ),
        )
    return len(rows)


def _write_readiness_observations(conn: sqlite3.Connection, observed_at: int) -> int:
    if not _table_exists(conn, "paper_readiness_observations"):
        return 0
    rows = conn.execute("SELECT * FROM paper_wallet_quality").fetchall()
    written = 0
    for row in rows:
        if not _should_write_readiness_observation(conn, row, observed_at):
            continue
        conn.execute(
            """
            INSERT INTO paper_readiness_observations(
                wallet, observed_at, orders, settled_positions, mark_coverage,
                settled_roi, total_roi, production_ready, blockers_json
                , max_drawdown_pct, max_market_exposure_share, validation_days
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["wallet"],
                observed_at,
                row["orders"],
                row["settled_positions"],
                row["mark_coverage"],
                row["settled_roi"],
                row["total_roi"],
                row["production_ready"],
                row["blockers_json"],
                row["max_drawdown_pct"],
                row["max_market_exposure_share"],
                row["validation_days"],
            ),
        )
        written += 1
    return written


def _quality_blockers(
    row: sqlite3.Row,
    mark_coverage: float,
    settled_roi: float,
    total_roi: float,
    max_drawdown_pct: float,
    max_market_exposure_share: float,
    validation_days: float,
) -> list[str]:
    blockers: list[str] = []
    if row["orders"] < MIN_PRODUCTION_ORDERS:
        blockers.append("insufficient_paper_orders")
    if row["settled_positions"] < MIN_PRODUCTION_SETTLED_POSITIONS:
        blockers.append("insufficient_settled_positions")
    if mark_coverage < MIN_PRODUCTION_MARK_COVERAGE:
        blockers.append("insufficient_mark_coverage")
    if total_roi <= 0:
        blockers.append("non_positive_total_roi")
    if row["settled_positions"] > 0 and settled_roi <= 0:
        blockers.append("non_positive_settled_roi")
    if row["settled_positions"] == 0:
        blockers.append("missing_settled_roi")
    if max_drawdown_pct > MAX_PRODUCTION_DRAWDOWN_PCT:
        blockers.append("max_drawdown_exceeded")
    if max_market_exposure_share > MAX_PRODUCTION_MARKET_EXPOSURE_SHARE:
        blockers.append("market_concentration_exceeded")
    if validation_days < MIN_PRODUCTION_VALIDATION_DAYS:
        blockers.append("insufficient_validation_period")
    return blockers


def _max_market_exposure_share(conn: sqlite3.Connection, wallet: str) -> float:
    rows = conn.execute(
        """
        SELECT market_slug, SUM(cost_usd) AS cost_usd
        FROM paper_positions
        WHERE wallet = ?
        GROUP BY market_slug
        """,
        (wallet,),
    ).fetchall()
    costs = [max(float(row["cost_usd"] or 0.0), 0.0) for row in rows]
    total = sum(costs)
    return max(costs, default=0.0) / total if total > 0 else 0.0


def _max_drawdown_pct(conn: sqlite3.Connection, wallet: str) -> float:
    rows = conn.execute(
        """
        SELECT realized_pnl_usd, cost_usd
        FROM paper_positions
        WHERE wallet = ? AND status = 'resolved'
        ORDER BY opened_at ASC, asset_id ASC
        """,
        (wallet,),
    ).fetchall()
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    total_cost = 0.0
    for row in rows:
        total_cost += max(float(row["cost_usd"] or 0.0), 0.0)
        cumulative += float(row["realized_pnl_usd"] or 0.0)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return max_drawdown / total_cost if total_cost > 0 else 0.0


def _should_write_mark(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    asset_id: str,
    price: float,
    source: str,
    marked_at: int,
) -> bool:
    row = conn.execute(
        """
        SELECT mark_price, mark_source, marked_at
        FROM paper_marks
        WHERE wallet = ? AND asset_id = ?
        ORDER BY marked_at DESC, mark_id DESC
        LIMIT 1
        """,
        (wallet, asset_id),
    ).fetchone()
    if row is None:
        return True
    changed = abs(float(row["mark_price"]) - float(price)) > 0.0001 or str(row["mark_source"]) != source
    return changed or marked_at - int(row["marked_at"] or 0) >= MARK_HEARTBEAT_SECONDS


def _should_write_readiness_observation(
    conn: sqlite3.Connection,
    quality: sqlite3.Row,
    observed_at: int,
) -> bool:
    row = conn.execute(
        """
        SELECT *
        FROM paper_readiness_observations
        WHERE wallet = ?
        ORDER BY observed_at DESC, observation_id DESC
        LIMIT 1
        """,
        (quality["wallet"],),
    ).fetchone()
    if row is None:
        return True
    fields = (
        "orders",
        "settled_positions",
        "production_ready",
        "blockers_json",
        "mark_coverage",
        "settled_roi",
        "total_roi",
        "max_drawdown_pct",
        "max_market_exposure_share",
        "validation_days",
    )
    changed = any(row[field] != quality[field] for field in fields)
    return changed or observed_at - int(row["observed_at"] or 0) >= READINESS_HEARTBEAT_SECONDS


def paper_readiness_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM paper_wallet_quality
        ORDER BY production_ready DESC, total_roi DESC, orders DESC
        """
    ).fetchall()
    out: list[dict[str, Any]] = []
    has_observations = _table_exists(conn, "paper_readiness_observations")
    for row in rows:
        item = dict(row)
        if has_observations:
            stable = stable_readiness_status(conn, row["wallet"])
            item.update(stable)
        out.append(item)
    return out


def _count_table(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _json_list(value: Any) -> list[Any]:
    parsed = _json_value(value)
    if isinstance(parsed, list):
        return parsed
    return []


def _json_object(value: Any) -> dict[str, Any]:
    parsed = _json_value(value)
    if isinstance(parsed, dict):
        return parsed
    return {}


def _json_value(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, str):
        try:
            return json.loads(parsed)
        except json.JSONDecodeError:
            return parsed
    return parsed


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number < 0 or number > 1:
        return None
    return number
