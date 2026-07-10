"""Collect wallet-level maker/taker evidence from the official trades API."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass

from pm_robot.clients.polymarket_public import PublicPolymarketClient


TRADE_SAMPLE_LIMIT = 10_000
MIN_SCREENING_TRADES = 100
MAX_DIRECTIONAL_MAKER_FRACTION = 0.5


@dataclass(frozen=True)
class TradeRoleIngestSummary:
    wallets_attempted: int
    wallets_succeeded: int
    wallets_screened: int
    status: str
    error: str = ""


def ingest_trade_role_evidence(
    conn: sqlite3.Connection,
    *,
    limit: int = 5,
    client: PublicPolymarketClient | None = None,
    now: int | None = None,
) -> TradeRoleIngestSummary:
    client = client or PublicPolymarketClient(conn=conn)
    fetched_at = now or int(time.time())
    wallets = _targets(conn, limit)
    succeeded = 0
    screened = 0
    error = ""
    for wallet in wallets:
        try:
            all_trades = client.wallet_trades(
                wallet,
                limit=TRADE_SAMPLE_LIMIT,
                taker_only=False,
            )
            taker_trades = client.wallet_trades(
                wallet,
                limit=TRADE_SAMPLE_LIMIT,
                taker_only=True,
            )
            all_keys = {_trade_key(item) for item in all_trades}
            taker_keys = {_trade_key(item) for item in taker_trades}
            all_keys.discard("")
            taker_keys.discard("")
            total = len(all_keys) if all_keys else len(all_trades)
            taker = len(all_keys & taker_keys) if all_keys and taker_keys else min(len(taker_trades), total)
            maker = max(total - taker, 0)
            maker_fraction = maker / total if total else None
            complete = len(all_trades) < TRADE_SAMPLE_LIMIT and len(taker_trades) < TRADE_SAMPLE_LIMIT
            status = _screening_status(
                conn,
                wallet,
                total=total,
                maker_fraction=maker_fraction,
                complete=complete,
            )
            _persist(
                conn,
                wallet,
                total=total,
                taker=taker,
                maker=maker,
                maker_fraction=maker_fraction,
                complete=complete,
                status=status,
                fetched_at=fetched_at,
            )
            succeeded += 1
            screened += int(status == "screened")
        except Exception as exc:
            error = f"{wallet}: {exc}"
            _persist_error(conn, wallet, str(exc), fetched_at)
    conn.commit()
    return TradeRoleIngestSummary(
        wallets_attempted=len(wallets),
        wallets_succeeded=succeeded,
        wallets_screened=screened,
        status="ok" if succeeded == len(wallets) else "partial",
        error=error,
    )


def _targets(conn: sqlite3.Connection, limit: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT cw.address
        FROM candidate_wallets cw
        JOIN wallet_features wf ON wf.address = cw.address
        LEFT JOIN wallet_trade_role_evidence wtre ON wtre.wallet = cw.address
        WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene')
          AND COALESCE(wf.extra_json, '{}') != ''
        ORDER BY
            CASE WHEN EXISTS (
                SELECT 1 FROM copy_pair_stats cps
                WHERE cps.leader_wallet = cw.address
                  AND cps.qualifies = 1
            ) THEN 0 ELSE 1 END,
            CASE WHEN wtre.wallet IS NULL THEN 0 ELSE 1 END,
            COALESCE(wtre.fetched_at, 0) ASC,
            cw.updated_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [str(row["address"]) for row in rows]


def _screening_status(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    total: int,
    maker_fraction: float | None,
    complete: bool,
) -> str:
    if not complete or total < MIN_SCREENING_TRADES or maker_fraction is None:
        return "incomplete"
    if maker_fraction > MAX_DIRECTIONAL_MAKER_FRACTION:
        return "market_maker_taker"
    row = conn.execute(
        """
        SELECT bot_score, net_to_gross_exposure, single_market_pnl_share
        FROM wallet_features WHERE address = ?
        """,
        (wallet,),
    ).fetchone()
    if row is None:
        return "incomplete"
    if row["bot_score"] is None or row["net_to_gross_exposure"] is None:
        return "incomplete"
    if float(row["bot_score"]) >= 80:
        return "incomplete"
    if float(row["net_to_gross_exposure"]) < 0.35:
        return "incomplete"
    if row["single_market_pnl_share"] is not None and float(row["single_market_pnl_share"]) > 0.5:
        return "incomplete"
    return "screened"


def _persist(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    total: int,
    taker: int,
    maker: int,
    maker_fraction: float | None,
    complete: bool,
    status: str,
    fetched_at: int,
) -> None:
    source = "polymarket_data_api_trades_takerOnly_comparison"
    conn.execute(
        """
        INSERT INTO wallet_trade_role_evidence(
            wallet, total_trades, taker_trades, maker_trades, maker_fraction,
            sample_complete, sample_limit, evidence_source, error, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', ?)
        ON CONFLICT(wallet) DO UPDATE SET
            total_trades=excluded.total_trades,
            taker_trades=excluded.taker_trades,
            maker_trades=excluded.maker_trades,
            maker_fraction=excluded.maker_fraction,
            sample_complete=excluded.sample_complete,
            sample_limit=excluded.sample_limit,
            evidence_source=excluded.evidence_source,
            error='',
            fetched_at=excluded.fetched_at
        """,
        (
            wallet,
            total,
            taker,
            maker,
            maker_fraction,
            1 if complete else 0,
            TRADE_SAMPLE_LIMIT,
            source,
            fetched_at,
        ),
    )
    conn.execute(
        """
        UPDATE wallet_features
        SET maker_fraction = ?,
            hygiene_status = ?,
            extra_json = json_set(
                COALESCE(extra_json, '{}'),
                '$.maker_fraction_source', ?,
                '$.trade_role_total_trades', ?,
                '$.trade_role_taker_trades', ?,
                '$.trade_role_sample_complete', ?
            ),
            updated_at = ?
        WHERE address = ?
        """,
        (
            maker_fraction,
            status,
            source,
            total,
            taker,
            1 if complete else 0,
            fetched_at,
            wallet,
        ),
    )


def _persist_error(conn: sqlite3.Connection, wallet: str, error: str, fetched_at: int) -> None:
    conn.execute(
        """
        INSERT INTO wallet_trade_role_evidence(
            wallet, total_trades, taker_trades, maker_trades, maker_fraction,
            sample_complete, sample_limit, evidence_source, error, fetched_at
        ) VALUES (?, 0, 0, 0, NULL, 0, ?, 'polymarket_data_api_trades', ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET error=excluded.error, fetched_at=excluded.fetched_at
        """,
        (wallet, TRADE_SAMPLE_LIMIT, error[:1000], fetched_at),
    )
    conn.execute(
        """
        UPDATE wallet_features
        SET maker_fraction = NULL,
            hygiene_status = 'incomplete',
            extra_json = json_set(
                COALESCE(extra_json, '{}'),
                '$.maker_fraction_source', 'polymarket_data_api_trades_error',
                '$.trade_role_error', ?
            ),
            updated_at = ?
        WHERE address = ?
        """,
        (error[:1000], fetched_at, wallet),
    )


def _trade_key(item: dict) -> str:
    tx = str(item.get("transactionHash") or item.get("transaction_hash") or "")
    asset = str(item.get("asset") or "")
    timestamp = str(item.get("timestamp") or "")
    side = str(item.get("side") or "")
    size = str(item.get("size") or "")
    price = str(item.get("price") or "")
    return json.dumps([tx, asset, timestamp, side, size, price], separators=(",", ":"))
