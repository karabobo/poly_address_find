"""Activity ingestion and episode reconstruction."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.storage.repository import (
    activity_event_key,
    activity_watermark,
    finish_ingest_run,
    list_activity_backfill_targets,
    list_ingest_targets,
    list_paper_activity_targets,
    persist_wallet_activity,
    rebuild_wallet_episodes,
    start_ingest_run,
)


@dataclass(frozen=True)
class ActivityIngestSummary:
    run_id: int
    wallets_attempted: int
    wallets_succeeded: int
    events_written: int
    episodes_rebuilt: int
    status: str
    error: str = ""


def ingest_activity(
    conn: sqlite3.Connection,
    *,
    wallet_limit: int = 10,
    page_limit: int = 100,
    max_events_per_wallet: int = 200,
    target_events_per_wallet: int = 0,
    paper_stage_only: bool = False,
    sleep_seconds: float = 0.25,
    client: PublicPolymarketClient | None = None,
) -> ActivityIngestSummary:
    client = client or PublicPolymarketClient(conn=conn)
    run_type = "paper_activity" if paper_stage_only else "activity"
    run_id = start_ingest_run(conn, run_type)
    if paper_stage_only:
        wallets = list_paper_activity_targets(conn, limit=wallet_limit)
        activity_source = "paper_wallet_activity"
    else:
        wallets = (
            list_activity_backfill_targets(
                conn,
                limit=wallet_limit,
                target_events_per_wallet=target_events_per_wallet,
            )
            if target_events_per_wallet > 0
            else list_ingest_targets(conn, limit=wallet_limit)
        )
        activity_source = "wallet_activity_poll"
    succeeded = 0
    events_written = 0
    episodes_rebuilt = 0
    error = ""
    status = "ok"
    try:
        for idx, wallet in enumerate(wallets):
            if idx > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            try:
                watermark = activity_watermark(conn, wallet)
                events = _fetch_wallet_activity(
                    client,
                    wallet,
                    page_limit=page_limit,
                    max_events=max_events_per_wallet,
                    sleep_seconds=sleep_seconds,
                    stop_at_timestamp=int(watermark.get("newest_timestamp") or 0),
                    stop_at_key=str(watermark.get("newest_activity_key") or ""),
                )
                now = int(time.time())
                events_written += persist_wallet_activity(
                    conn,
                    wallet,
                    events,
                    ingested_at=now,
                    source=activity_source,
                )
                episodes_rebuilt += rebuild_wallet_episodes(conn, wallet)
                succeeded += 1
            except Exception as exc:
                error = f"{wallet}: {exc}"
        return ActivityIngestSummary(
            run_id, len(wallets), succeeded, events_written, episodes_rebuilt, status, error
        )
    except Exception as exc:
        status = "failed"
        error = str(exc)
        return ActivityIngestSummary(
            run_id, len(wallets), succeeded, events_written, episodes_rebuilt, status, error
        )
    finally:
        finish_ingest_run(
            conn,
            run_id,
            status=status,
            wallets_attempted=len(wallets),
            wallets_succeeded=succeeded,
            rows_written=events_written,
            error=error,
        )


def _fetch_wallet_activity(
    client: PublicPolymarketClient,
    wallet: str,
    *,
    page_limit: int,
    max_events: int,
    sleep_seconds: float,
    stop_at_timestamp: int = 0,
    stop_at_key: str = "",
) -> list[dict]:
    out: list[dict] = []
    offset = 0
    while len(out) < max_events:
        batch = client.activity(wallet, limit=page_limit, offset=offset)
        if not batch:
            break
        fresh = []
        reached_watermark = False
        for event in batch:
            if _event_matches_watermark(event, stop_at_timestamp=stop_at_timestamp, stop_at_key=stop_at_key):
                reached_watermark = True
                break
            if stop_at_timestamp > 0 and int(event.get("timestamp") or 0) < stop_at_timestamp:
                reached_watermark = True
                break
            fresh.append(event)
        remaining = max_events - len(out)
        out.extend(fresh[:remaining])
        if reached_watermark or len(batch) < page_limit:
            break
        offset += page_limit
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return out


def _event_matches_watermark(event: dict, *, stop_at_timestamp: int, stop_at_key: str) -> bool:
    if stop_at_timestamp <= 0:
        return False
    if int(event.get("timestamp") or 0) != stop_at_timestamp:
        return False
    if not stop_at_key:
        return True
    return activity_event_key(event) == stop_at_key
