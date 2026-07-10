"""Gamma metadata cache ingestor."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

from pm_robot.clients.http import HttpClientError
from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.orchestration.retry_policy import is_upstream_scheduling_error
from pm_robot.storage.repository import (
    finish_ingest_run,
    list_gamma_market_backfill_targets,
    rebuild_wallet_episodes,
    start_ingest_run,
    upsert_gamma_market_cache,
    upsert_gamma_market_failure,
)


@dataclass(frozen=True)
class GammaIngestSummary:
    run_id: int
    markets_attempted: int
    markets_succeeded: int
    failures_cached: int
    rows_written: int
    episodes_rebuilt: int
    status: str
    error: str = ""


def ingest_gamma_markets(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    ttl_seconds: int = 21_600,
    failure_ttl_seconds: int = 604_800,
    sleep_seconds: float = 0.1,
    paper_only: bool = False,
    max_episode_rebuild_wallets: int = 250,
    client: PublicPolymarketClient | None = None,
) -> GammaIngestSummary:
    client = client or PublicPolymarketClient(conn=conn)
    run_id = start_ingest_run(conn, "gamma_markets")
    slugs = list_gamma_market_backfill_targets(conn, limit=limit, paper_only=paper_only)
    attempted = 0
    succeeded = 0
    failures_cached = 0
    rows_written = 0
    closed_slugs: set[str] = set()
    episodes_rebuilt = 0
    error = ""
    status = "ok"
    try:
        for idx, slug in enumerate(slugs):
            attempted += 1
            if idx > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            try:
                market = client.market_by_slug(slug)
                if not market:
                    error = f"{slug}: empty_market_response"
                    upsert_gamma_market_failure(
                        conn,
                        market_slug=slug,
                        error="empty_market_response",
                        fetched_at=int(time.time()),
                        ttl_seconds=failure_ttl_seconds,
                    )
                    failures_cached += 1
                    continue
                upsert_gamma_market_cache(
                    conn,
                    market_slug=slug,
                    market=market,
                    fetched_at=int(time.time()),
                    ttl_seconds=ttl_seconds,
                )
                if bool(market.get("closed") or market.get("resolved")):
                    closed_slugs.add(slug)
                succeeded += 1
                rows_written += 1
            except HttpClientError as exc:
                error = f"{slug}: {exc}"
                if is_upstream_scheduling_error(exc):
                    status = "partial"
                    break
                if _should_cache_failure(exc):
                    upsert_gamma_market_failure(
                        conn,
                        market_slug=slug,
                        error=str(exc),
                        fetched_at=int(time.time()),
                        ttl_seconds=failure_ttl_seconds,
                    )
                    failures_cached += 1
            except Exception as exc:
                error = f"{slug}: {exc}"
                if _should_cache_failure(exc):
                    upsert_gamma_market_failure(
                        conn,
                        market_slug=slug,
                        error=str(exc),
                        fetched_at=int(time.time()),
                        ttl_seconds=failure_ttl_seconds,
                    )
                    failures_cached += 1
        episodes_rebuilt = _rebuild_closed_market_wallets(
            conn,
            closed_slugs,
            max_wallets=max_episode_rebuild_wallets,
        )
        return GammaIngestSummary(
            run_id,
            attempted,
            succeeded,
            failures_cached,
            rows_written,
            episodes_rebuilt,
            status,
            error,
        )
    except Exception as exc:
        status = "failed"
        error = str(exc)
        return GammaIngestSummary(
            run_id,
            attempted,
            succeeded,
            failures_cached,
            rows_written,
            episodes_rebuilt,
            status,
            error,
        )
    finally:
        finish_ingest_run(
            conn,
            run_id,
            status=status,
            wallets_attempted=attempted,
            wallets_succeeded=succeeded,
            rows_written=rows_written,
            error=error,
        )


def _should_cache_failure(exc: Exception) -> bool:
    text = str(exc).lower()
    return "404" in text or "not found" in text


def _rebuild_closed_market_wallets(conn: sqlite3.Connection, slugs: set[str], *, max_wallets: int) -> int:
    if not slugs:
        return 0
    if max_wallets <= 0:
        return 0
    placeholders = ",".join("?" for _ in slugs)
    slug_params = tuple(sorted(slugs))
    rows = conn.execute(
        f"""
        SELECT address
        FROM (
            SELECT
                address,
                MAX(COALESCE(last_ts, first_ts, 0)) AS recent_ts
            FROM wallet_episodes
            WHERE market_slug IN ({placeholders})
              AND status = 'open'
            GROUP BY address
            UNION ALL
            SELECT
                address,
                MAX(timestamp) AS recent_ts
            FROM wallet_activity
            WHERE market_slug IN ({placeholders})
              AND type = 'TRADE'
            GROUP BY address, condition_id, asset_id
            HAVING SUM(
                CASE
                    WHEN UPPER(COALESCE(side, '')) = 'BUY' THEN COALESCE(size, 0)
                    WHEN UPPER(COALESCE(side, '')) = 'SELL' THEN -COALESCE(size, 0)
                    ELSE 0
                END
            ) > 1e-9
        )
        GROUP BY address
        ORDER BY MAX(recent_ts) DESC, address ASC
        LIMIT ?
        """,
        (*slug_params, *slug_params, max_wallets),
    ).fetchall()
    rebuilt = 0
    for row in rows:
        rebuild_wallet_episodes(conn, str(row["address"]))
        rebuilt += 1
    return rebuilt
