"""Position snapshot ingestion for candidate wallets."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.storage.repository import (
    finish_ingest_run,
    list_ingest_targets,
    persist_wallet_positions,
    start_ingest_run,
)


@dataclass(frozen=True)
class IngestSummary:
    run_id: int
    wallets_attempted: int
    wallets_succeeded: int
    rows_written: int
    status: str
    error: str = ""


def ingest_positions(
    conn: sqlite3.Connection,
    *,
    limit: int = 25,
    size_threshold: float = 0.0,
    sleep_seconds: float = 0.25,
    client: PublicPolymarketClient | None = None,
) -> IngestSummary:
    client = client or PublicPolymarketClient(conn=conn)
    run_id = start_ingest_run(conn, "positions")
    wallets = list_ingest_targets(conn, limit=limit)
    succeeded = 0
    rows_written = 0
    error = ""
    status = "ok"
    try:
        for idx, wallet in enumerate(wallets):
            if idx > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            try:
                positions = client.positions(wallet, size_threshold=size_threshold)
                captured_at = int(time.time())
                rows_written += persist_wallet_positions(
                    conn,
                    wallet,
                    positions,
                    captured_at=captured_at,
                )
                succeeded += 1
            except Exception as exc:
                # Keep running; a single bad wallet/API response should not stop the batch.
                error = f"{wallet}: {exc}"
        return IngestSummary(run_id, len(wallets), succeeded, rows_written, status, error)
    except Exception as exc:
        status = "failed"
        error = str(exc)
        return IngestSummary(run_id, len(wallets), succeeded, rows_written, status, error)
    finally:
        finish_ingest_run(
            conn,
            run_id,
            status=status,
            wallets_attempted=len(wallets),
            wallets_succeeded=succeeded,
            rows_written=rows_written,
            error=error,
        )
