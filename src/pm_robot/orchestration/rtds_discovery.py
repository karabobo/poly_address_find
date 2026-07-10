"""Real-time candidate discovery from Polymarket RTDS activity trades."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Protocol

from pm_robot.clients.websocket import SimpleWebSocketClient
from pm_robot.orchestration.activity_discovery import ActivityDiscoverySummary, process_activity_rows
from pm_robot.pipeline_terms import PAPER_ELIGIBLE_CANDIDATE_STAGES, PROVISIONAL_CANDIDATE_STAGES
from pm_robot.storage.repository import persist_wallet_activity, record_runtime_heartbeat


RTDS_ENDPOINT = "wss://ws-live-data.polymarket.com"
RTDS_HEARTBEAT_MIN_SECONDS = 60.0
RTDS_SQLITE_LOCK_RETRY_DELAYS = (0.25, 0.5, 1.0, 2.0)
RTDS_WALLET_KEYS = ("proxyWallet", "proxy_wallet", "user", "address", "wallet", "trader")
RTDS_WATCH_ACTIVITY_SOURCE = "polymarket_rtds_watch_activity"
DEFAULT_RTDS_WATCH_MIN_SCORE = 65.0


class TextWebSocket(Protocol):
    def send_text(self, text: str) -> None: ...
    def recv_text(self, *, timeout: float | None = None) -> str: ...


@dataclass(frozen=True)
class RTDSActivityDiscoverySummary:
    connections_attempted: int
    connections_succeeded: int
    messages_seen: int
    trades_seen: int
    trades_selected: int
    batches_flushed: int
    wallets_seen: int
    candidates_inserted_or_updated: int
    features_updated: int
    observed_wallets: int
    promoted_wallets: int
    paper_activity_wallets: int
    paper_activity_events_written: int
    paper_rows_seen: int
    paper_rows_with_wallet: int
    paper_activity_matches: int
    paper_eligible_wallets: int
    paper_wallet_field_counts: dict[str, int]
    watch_activity_wallets: int
    watch_activity_events_written: int
    watch_activity_matches: int
    watch_eligible_wallets: int
    reconnects: int
    status: str
    error: str = ""


def run_rtds_activity_discovery(
    conn: sqlite3.Connection,
    *,
    endpoint: str = RTDS_ENDPOINT,
    min_trade_usdc: float = 500.0,
    paper_min_trade_usdc: float = 0.0,
    batch_size: int = 25,
    flush_interval: float = 10.0,
    ping_interval: float = 5.0,
    receive_timeout: float = 1.0,
    reconnect_sleep: float = 5.0,
    max_runtime_seconds: float = 0.0,
    max_messages: int = 0,
    max_reconnects: int = 0,
    watch_min_score: float = DEFAULT_RTDS_WATCH_MIN_SCORE,
    websocket_factory: Any | None = None,
) -> RTDSActivityDiscoverySummary:
    deadline = time.monotonic() + max_runtime_seconds if max_runtime_seconds > 0 else None
    attempted = 0
    succeeded = 0
    messages_seen = 0
    trades_seen = 0
    trades_selected = 0
    batches = 0
    wallets_seen = 0
    candidates = 0
    features = 0
    observed = 0
    promoted = 0
    paper_wallets: set[str] = set()
    paper_events_written = 0
    paper_rows_seen = 0
    paper_rows_with_wallet = 0
    paper_activity_matches = 0
    paper_eligible_wallets = 0
    paper_wallet_field_counts: dict[str, int] = {}
    watch_wallets: set[str] = set()
    watch_events_written = 0
    watch_activity_matches = 0
    watch_eligible_wallets = 0
    reconnects = 0
    error = ""
    status = "ok"
    websocket_factory = websocket_factory or (lambda url: SimpleWebSocketClient(url))
    last_heartbeat = 0.0

    while True:
        if _deadline_reached(deadline) or (max_messages > 0 and messages_seen >= max_messages):
            break
        if max_reconnects > 0 and reconnects > max_reconnects:
            status = "partial" if succeeded else "failed"
            break
        attempted += 1
        discovery_batch: list[dict[str, Any]] = []
        paper_batch: list[dict[str, Any]] = []
        last_flush = time.monotonic()
        last_ping = 0.0
        try:
            with websocket_factory(endpoint) as ws:
                succeeded += 1
                _subscribe_activity_trades(ws)
                _record_rtds_heartbeat(
                    conn,
                    messages_seen=messages_seen,
                    trades_seen=trades_seen,
                    trades_selected=trades_selected,
                    batches_flushed=batches,
                    paper_activity_wallets=len(paper_wallets),
                    paper_activity_events_written=paper_events_written,
                    paper_rows_seen=paper_rows_seen,
                    paper_rows_with_wallet=paper_rows_with_wallet,
                    paper_activity_matches=paper_activity_matches,
                    paper_eligible_wallets=paper_eligible_wallets,
                    paper_wallet_field_counts=paper_wallet_field_counts,
                    watch_activity_wallets=len(watch_wallets),
                    watch_activity_events_written=watch_events_written,
                    watch_activity_matches=watch_activity_matches,
                    watch_eligible_wallets=watch_eligible_wallets,
                )
                last_heartbeat = time.monotonic()
                while True:
                    now = time.monotonic()
                    if _deadline_reached(deadline) or (max_messages > 0 and messages_seen >= max_messages):
                        break
                    if now - last_ping >= ping_interval:
                        ws.send_text("PING")
                        last_ping = now
                    try:
                        raw = ws.recv_text(timeout=receive_timeout)
                    except TimeoutError:
                        if (discovery_batch or paper_batch) and now - last_flush >= flush_interval:
                            result, paper_result, watch_result = _flush_realtime_batch(
                                conn,
                                discovery_batch,
                                paper_batch,
                                min_trade_usdc=min_trade_usdc,
                                max_candidates=batch_size,
                                watch_min_score=watch_min_score,
                            )
                            rows_written = _rtds_flush_rows_written(result, paper_result)
                            meaningful_flush = bool(discovery_batch) or rows_written > 0
                            if meaningful_flush:
                                batches += 1
                            wallets_seen += result.wallets_seen
                            candidates += result.candidates_inserted_or_updated
                            features += result.features_updated
                            observed += result.observed_wallets
                            promoted += result.promoted_wallets
                            paper_wallets.update(paper_result["wallets"])
                            paper_events_written += int(paper_result["events_written"])
                            paper_rows_seen += int(paper_result["rows_seen"])
                            paper_rows_with_wallet += int(paper_result["rows_with_wallet"])
                            paper_activity_matches += int(paper_result["rows_matched"])
                            paper_eligible_wallets = max(paper_eligible_wallets, int(paper_result["eligible_wallets"]))
                            _merge_wallet_field_counts(paper_wallet_field_counts, paper_result["wallet_field_counts"])
                            watch_wallets.update(watch_result["wallets"])
                            watch_events_written += int(watch_result["events_written"])
                            watch_activity_matches += int(watch_result["rows_matched"])
                            watch_eligible_wallets = max(watch_eligible_wallets, int(watch_result["eligible_wallets"]))
                            discovery_batch.clear()
                            paper_batch.clear()
                            last_flush = time.monotonic()
                            if meaningful_flush:
                                _record_rtds_heartbeat(
                                    conn,
                                    rows_written=rows_written,
                                    messages_seen=messages_seen,
                                    trades_seen=trades_seen,
                                    trades_selected=trades_selected,
                                    batches_flushed=batches,
                                    paper_activity_wallets=len(paper_wallets),
                                    paper_activity_events_written=paper_events_written,
                                    paper_rows_seen=paper_rows_seen,
                                    paper_rows_with_wallet=paper_rows_with_wallet,
                                    paper_activity_matches=paper_activity_matches,
                                    paper_eligible_wallets=paper_eligible_wallets,
                                    paper_wallet_field_counts=paper_wallet_field_counts,
                                    watch_activity_wallets=len(watch_wallets),
                                    watch_activity_events_written=watch_events_written,
                                    watch_activity_matches=watch_activity_matches,
                                    watch_eligible_wallets=watch_eligible_wallets,
                                )
                                last_heartbeat = time.monotonic()
                        elif now - last_heartbeat >= max(RTDS_HEARTBEAT_MIN_SECONDS, flush_interval):
                            _record_rtds_heartbeat(
                                conn,
                                messages_seen=messages_seen,
                                trades_seen=trades_seen,
                                trades_selected=trades_selected,
                                batches_flushed=batches,
                                paper_activity_wallets=len(paper_wallets),
                                paper_activity_events_written=paper_events_written,
                                paper_rows_seen=paper_rows_seen,
                                paper_rows_with_wallet=paper_rows_with_wallet,
                                paper_activity_matches=paper_activity_matches,
                                paper_eligible_wallets=paper_eligible_wallets,
                                paper_wallet_field_counts=paper_wallet_field_counts,
                                watch_activity_wallets=len(watch_wallets),
                                watch_activity_events_written=watch_events_written,
                                watch_activity_matches=watch_activity_matches,
                                watch_eligible_wallets=watch_eligible_wallets,
                            )
                            last_heartbeat = now
                        continue
                    if raw in {"PING", "PONG", ""}:
                        continue
                    message = _json_message(raw)
                    if not message:
                        continue
                    messages_seen += 1
                    trade = rtds_trade_to_activity_row(message)
                    if not trade:
                        continue
                    trades_seen += 1
                    trade_usdc = _trade_usdc(trade)
                    if trade_usdc >= paper_min_trade_usdc:
                        paper_batch.append(trade)
                    if trade_usdc >= min_trade_usdc:
                        trades_selected += 1
                        discovery_batch.append(trade)
                    if len(discovery_batch) >= batch_size or len(paper_batch) >= batch_size:
                        result, paper_result, watch_result = _flush_realtime_batch(
                            conn,
                            discovery_batch,
                            paper_batch,
                            min_trade_usdc=min_trade_usdc,
                            max_candidates=batch_size,
                            watch_min_score=watch_min_score,
                        )
                        rows_written = _rtds_flush_rows_written(result, paper_result)
                        meaningful_flush = bool(discovery_batch) or rows_written > 0
                        if meaningful_flush:
                            batches += 1
                        wallets_seen += result.wallets_seen
                        candidates += result.candidates_inserted_or_updated
                        features += result.features_updated
                        observed += result.observed_wallets
                        promoted += result.promoted_wallets
                        paper_wallets.update(paper_result["wallets"])
                        paper_events_written += int(paper_result["events_written"])
                        paper_rows_seen += int(paper_result["rows_seen"])
                        paper_rows_with_wallet += int(paper_result["rows_with_wallet"])
                        paper_activity_matches += int(paper_result["rows_matched"])
                        paper_eligible_wallets = max(paper_eligible_wallets, int(paper_result["eligible_wallets"]))
                        _merge_wallet_field_counts(paper_wallet_field_counts, paper_result["wallet_field_counts"])
                        watch_wallets.update(watch_result["wallets"])
                        watch_events_written += int(watch_result["events_written"])
                        watch_activity_matches += int(watch_result["rows_matched"])
                        watch_eligible_wallets = max(watch_eligible_wallets, int(watch_result["eligible_wallets"]))
                        discovery_batch.clear()
                        paper_batch.clear()
                        last_flush = time.monotonic()
                        if meaningful_flush:
                            _record_rtds_heartbeat(
                                conn,
                                rows_written=rows_written,
                                messages_seen=messages_seen,
                                trades_seen=trades_seen,
                                trades_selected=trades_selected,
                                batches_flushed=batches,
                                paper_activity_wallets=len(paper_wallets),
                                paper_activity_events_written=paper_events_written,
                                paper_rows_seen=paper_rows_seen,
                                paper_rows_with_wallet=paper_rows_with_wallet,
                                paper_activity_matches=paper_activity_matches,
                                paper_eligible_wallets=paper_eligible_wallets,
                                paper_wallet_field_counts=paper_wallet_field_counts,
                                watch_activity_wallets=len(watch_wallets),
                                watch_activity_events_written=watch_events_written,
                                watch_activity_matches=watch_activity_matches,
                                watch_eligible_wallets=watch_eligible_wallets,
                            )
                            last_heartbeat = time.monotonic()
                if discovery_batch or paper_batch:
                    result, paper_result, watch_result = _flush_realtime_batch(
                        conn,
                        discovery_batch,
                        paper_batch,
                        min_trade_usdc=min_trade_usdc,
                        max_candidates=batch_size,
                        watch_min_score=watch_min_score,
                    )
                    rows_written = _rtds_flush_rows_written(result, paper_result)
                    meaningful_flush = bool(discovery_batch) or rows_written > 0
                    if meaningful_flush:
                        batches += 1
                    wallets_seen += result.wallets_seen
                    candidates += result.candidates_inserted_or_updated
                    features += result.features_updated
                    observed += result.observed_wallets
                    promoted += result.promoted_wallets
                    paper_wallets.update(paper_result["wallets"])
                    paper_events_written += int(paper_result["events_written"])
                    paper_rows_seen += int(paper_result["rows_seen"])
                    paper_rows_with_wallet += int(paper_result["rows_with_wallet"])
                    paper_activity_matches += int(paper_result["rows_matched"])
                    paper_eligible_wallets = max(paper_eligible_wallets, int(paper_result["eligible_wallets"]))
                    _merge_wallet_field_counts(paper_wallet_field_counts, paper_result["wallet_field_counts"])
                    watch_wallets.update(watch_result["wallets"])
                    watch_events_written += int(watch_result["events_written"])
                    watch_activity_matches += int(watch_result["rows_matched"])
                    watch_eligible_wallets = max(watch_eligible_wallets, int(watch_result["eligible_wallets"]))
                    if meaningful_flush:
                        _record_rtds_heartbeat(
                            conn,
                            rows_written=rows_written,
                            messages_seen=messages_seen,
                            trades_seen=trades_seen,
                            trades_selected=trades_selected,
                            batches_flushed=batches,
                            paper_activity_wallets=len(paper_wallets),
                            paper_activity_events_written=paper_events_written,
                            paper_rows_seen=paper_rows_seen,
                            paper_rows_with_wallet=paper_rows_with_wallet,
                            paper_activity_matches=paper_activity_matches,
                            paper_eligible_wallets=paper_eligible_wallets,
                            paper_wallet_field_counts=paper_wallet_field_counts,
                            watch_activity_wallets=len(watch_wallets),
                            watch_activity_events_written=watch_events_written,
                            watch_activity_matches=watch_activity_matches,
                            watch_eligible_wallets=watch_eligible_wallets,
                        )
                        last_heartbeat = time.monotonic()
                if _deadline_reached(deadline) or (max_messages > 0 and messages_seen >= max_messages):
                    break
        except Exception as exc:
            error = str(exc)
            status = "partial" if succeeded else "failed"
            _record_rtds_heartbeat(
                conn,
                status=status,
                error=error,
                messages_seen=messages_seen,
                trades_seen=trades_seen,
                trades_selected=trades_selected,
                batches_flushed=batches,
                paper_activity_wallets=len(paper_wallets),
                paper_activity_events_written=paper_events_written,
                paper_rows_seen=paper_rows_seen,
                paper_rows_with_wallet=paper_rows_with_wallet,
                paper_activity_matches=paper_activity_matches,
                paper_eligible_wallets=paper_eligible_wallets,
                paper_wallet_field_counts=paper_wallet_field_counts,
                watch_activity_wallets=len(watch_wallets),
                watch_activity_events_written=watch_events_written,
                watch_activity_matches=watch_activity_matches,
                watch_eligible_wallets=watch_eligible_wallets,
            )
            reconnects += 1
            if _deadline_reached(deadline) or (max_reconnects > 0 and reconnects > max_reconnects):
                break
            if reconnect_sleep > 0:
                time.sleep(reconnect_sleep)

    if status == "ok":
        _record_rtds_heartbeat(
            conn,
            status=status,
            messages_seen=messages_seen,
            trades_seen=trades_seen,
            trades_selected=trades_selected,
            batches_flushed=batches,
            paper_activity_wallets=len(paper_wallets),
            paper_activity_events_written=paper_events_written,
            paper_rows_seen=paper_rows_seen,
            paper_rows_with_wallet=paper_rows_with_wallet,
            paper_activity_matches=paper_activity_matches,
            paper_eligible_wallets=paper_eligible_wallets,
            paper_wallet_field_counts=paper_wallet_field_counts,
            watch_activity_wallets=len(watch_wallets),
            watch_activity_events_written=watch_events_written,
            watch_activity_matches=watch_activity_matches,
            watch_eligible_wallets=watch_eligible_wallets,
        )

    return RTDSActivityDiscoverySummary(
        connections_attempted=attempted,
        connections_succeeded=succeeded,
        messages_seen=messages_seen,
        trades_seen=trades_seen,
        trades_selected=trades_selected,
        batches_flushed=batches,
        wallets_seen=wallets_seen,
        candidates_inserted_or_updated=candidates,
        features_updated=features,
        observed_wallets=observed,
        promoted_wallets=promoted,
        paper_activity_wallets=len(paper_wallets),
        paper_activity_events_written=paper_events_written,
        paper_rows_seen=paper_rows_seen,
        paper_rows_with_wallet=paper_rows_with_wallet,
        paper_activity_matches=paper_activity_matches,
        paper_eligible_wallets=paper_eligible_wallets,
        paper_wallet_field_counts=dict(sorted(paper_wallet_field_counts.items())),
        watch_activity_wallets=len(watch_wallets),
        watch_activity_events_written=watch_events_written,
        watch_activity_matches=watch_activity_matches,
        watch_eligible_wallets=watch_eligible_wallets,
        reconnects=reconnects,
        status=status,
        error=error,
    )


def _record_rtds_heartbeat(
    conn: sqlite3.Connection,
    *,
    status: str = "ok",
    rows_written: int = 0,
    error: str = "",
    messages_seen: int = 0,
    trades_seen: int = 0,
    trades_selected: int = 0,
    batches_flushed: int = 0,
    paper_activity_wallets: int = 0,
    paper_activity_events_written: int = 0,
    paper_rows_seen: int = 0,
    paper_rows_with_wallet: int = 0,
    paper_activity_matches: int = 0,
    paper_eligible_wallets: int = 0,
    paper_wallet_field_counts: dict[str, int] | None = None,
    watch_activity_wallets: int = 0,
    watch_activity_events_written: int = 0,
    watch_activity_matches: int = 0,
    watch_eligible_wallets: int = 0,
) -> None:
    try:
        details = _rtds_heartbeat_details(
            messages_seen=messages_seen,
            trades_seen=trades_seen,
            trades_selected=trades_selected,
            batches_flushed=batches_flushed,
            paper_activity_wallets=paper_activity_wallets,
            paper_activity_events_written=paper_activity_events_written,
            paper_rows_seen=paper_rows_seen,
            paper_rows_with_wallet=paper_rows_with_wallet,
            paper_activity_matches=paper_activity_matches,
            paper_eligible_wallets=paper_eligible_wallets,
            paper_wallet_field_counts=paper_wallet_field_counts or {},
            watch_activity_wallets=watch_activity_wallets,
            watch_activity_events_written=watch_activity_events_written,
            watch_activity_matches=watch_activity_matches,
            watch_eligible_wallets=watch_eligible_wallets,
        )
        record_runtime_heartbeat(
            conn,
            "loop_rtds_discovery",
            status=status,
            rows_written=rows_written,
            error=error or details,
        )
    except sqlite3.Error:
        pass


def _rtds_heartbeat_details(
    *,
    messages_seen: int,
    trades_seen: int,
    trades_selected: int,
    batches_flushed: int,
    paper_activity_wallets: int,
    paper_activity_events_written: int,
    paper_rows_seen: int,
    paper_rows_with_wallet: int,
    paper_activity_matches: int,
    paper_eligible_wallets: int,
    paper_wallet_field_counts: dict[str, int],
    watch_activity_wallets: int,
    watch_activity_events_written: int,
    watch_activity_matches: int,
    watch_eligible_wallets: int,
) -> str:
    key_text = _wallet_field_counts_text(paper_wallet_field_counts)
    return (
        f"messages={int(messages_seen)} "
        f"trades={int(trades_seen)} "
        f"selected={int(trades_selected)} "
        f"batches={int(batches_flushed)} "
        f"paper_wallets={int(paper_activity_wallets)} "
        f"paper_events={int(paper_activity_events_written)} "
        f"paper_rows={int(paper_rows_seen)} "
        f"paper_wallet_rows={int(paper_rows_with_wallet)} "
        f"paper_matches={int(paper_activity_matches)} "
        f"paper_eligible={int(paper_eligible_wallets)} "
        f"paper_wallet_keys={key_text} "
        f"watch_wallets={int(watch_activity_wallets)} "
        f"watch_events={int(watch_activity_events_written)} "
        f"watch_matches={int(watch_activity_matches)} "
        f"watch_eligible={int(watch_eligible_wallets)}"
    )


def rtds_trade_to_activity_row(message: dict[str, Any]) -> dict[str, Any] | None:
    if str(message.get("topic") or "") != "activity":
        return None
    if str(message.get("type") or "") != "trades":
        return None
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None
    row = dict(payload)
    if "timestamp" not in row and message.get("timestamp") is not None:
        try:
            row["timestamp"] = int(float(message["timestamp"]) / 1000)
        except (TypeError, ValueError):
            pass
    if "usdcSize" not in row and "usdc_size" not in row:
        size = _float(row.get("size")) or 0.0
        price = _float(row.get("price")) or 0.0
        row["usdcSize"] = size * price
    row.setdefault("type", "TRADE")
    row["source"] = "polymarket_rtds_activity"
    return row


def _subscribe_activity_trades(ws: TextWebSocket) -> None:
    ws.send_text(
        json.dumps(
            {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "activity",
                        "type": "trades",
                    }
                ],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _flush_batch(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    min_trade_usdc: float,
    max_candidates: int,
):
    return process_activity_rows(
        conn,
        rows,
        source="polymarket_rtds_activity",
        labels="realtime_trade_activity",
        status_prefix="rtds_activity_discovered",
        min_trade_usdc=min_trade_usdc,
        max_candidates=max_candidates,
        target_depth=200,
    )


def _flush_realtime_batch(
    conn: sqlite3.Connection,
    discovery_rows: list[dict[str, Any]],
    paper_rows: list[dict[str, Any]],
    *,
    min_trade_usdc: float,
    max_candidates: int,
    watch_min_score: float,
) -> tuple[ActivityDiscoverySummary, dict[str, Any], dict[str, Any]]:
    """Persist one RTDS batch, retrying brief SQLite writer contention."""

    for attempt, delay in enumerate((*RTDS_SQLITE_LOCK_RETRY_DELAYS, None)):
        try:
            result = (
                _flush_batch(
                    conn,
                    discovery_rows,
                    min_trade_usdc=min_trade_usdc,
                    max_candidates=max_candidates,
                )
                if discovery_rows
                else _empty_activity_discovery_summary()
            )
            ingested_at = int(time.time())
            paper_result = _persist_paper_stage_activity(conn, paper_rows, ingested_at=ingested_at)
            watch_result = _persist_watch_scope_activity(
                conn,
                paper_rows,
                ingested_at=ingested_at,
                min_score=watch_min_score,
            )
            return result, paper_result, watch_result
        except sqlite3.OperationalError as exc:
            if not _sqlite_lock_error(exc) or delay is None:
                raise
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            time.sleep(delay)
    raise RuntimeError("unreachable rtds flush retry state")


def _rtds_flush_rows_written(result: ActivityDiscoverySummary, paper_result: dict[str, Any]) -> int:
    return (
        int(result.observed_wallets)
        + int(result.candidates_inserted_or_updated)
        + int(paper_result.get("events_written") or 0)
    )


def _empty_activity_discovery_summary() -> ActivityDiscoverySummary:
    return ActivityDiscoverySummary(
        pages_attempted=0,
        pages_succeeded=0,
        events_seen=0,
        wallets_seen=0,
        candidates_inserted_or_updated=0,
        features_updated=0,
        observed_wallets=0,
        promoted_wallets=0,
        status="ok",
    )


def _sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def _persist_paper_stage_activity(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    ingested_at: int,
) -> dict[str, Any]:
    """Persist RTDS rows only for wallets already approved for paper observation."""

    result = _empty_paper_activity_result()
    result["rows_seen"] = len(rows)
    for row in rows:
        wallet_keys = _wallet_keys_from_activity_row(row)
        for key in wallet_keys:
            result["wallet_field_counts"][key] = int(result["wallet_field_counts"].get(key, 0)) + 1
        if _wallet_from_activity_row(row):
            result["rows_with_wallet"] += 1
    if not rows:
        return result
    placeholders = ",".join("?" for _ in PAPER_ELIGIBLE_CANDIDATE_STAGES)
    eligible = {
        str(row["address"]).lower()
        for row in conn.execute(
            f"""
            SELECT address
            FROM candidate_wallets
            WHERE candidate_stage IN ({placeholders})
            """,
            PAPER_ELIGIBLE_CANDIDATE_STAGES,
        ).fetchall()
    }
    result["eligible_wallets"] = len(eligible)
    if not eligible:
        return result
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        wallet = _wallet_from_activity_row(row)
        if wallet not in eligible:
            continue
        result["rows_matched"] += 1
        grouped.setdefault(wallet, []).append(row)
    events_written = 0
    for wallet, events in grouped.items():
        events_written += persist_wallet_activity(
            conn,
            wallet,
            events,
            ingested_at=ingested_at,
            source="polymarket_rtds_activity",
        )
    result["wallets"] = set(grouped)
    result["events_written"] = events_written
    return result


def _persist_watch_scope_activity(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    ingested_at: int,
    min_score: float,
) -> dict[str, Any]:
    """Persist RTDS rows for near-paper research wallets without paper approval."""

    result = _empty_paper_activity_result()
    result["rows_seen"] = len(rows)
    if not rows or min_score <= 0:
        return result
    placeholders = ",".join("?" for _ in PROVISIONAL_CANDIDATE_STAGES)
    eligible = {
        str(row["address"]).lower()
        for row in conn.execute(
            f"""
            WITH latest AS (
                SELECT ls.address, ls.leader_score
                FROM leader_scores ls
                JOIN (
                    SELECT address, MAX(score_id) AS score_id
                    FROM leader_scores
                    GROUP BY address
                ) x
                  ON x.address = ls.address
                 AND x.score_id = ls.score_id
            )
            SELECT cw.address
            FROM candidate_wallets cw
            JOIN latest
              ON latest.address = cw.address
            WHERE cw.candidate_stage IN ({placeholders})
              AND latest.leader_score >= ?
            """,
            (*PROVISIONAL_CANDIDATE_STAGES, float(min_score)),
        ).fetchall()
    }
    result["eligible_wallets"] = len(eligible)
    if not eligible:
        return result
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        wallet = _wallet_from_activity_row(row)
        if not wallet:
            continue
        result["rows_with_wallet"] += 1
        if wallet not in eligible:
            continue
        result["rows_matched"] += 1
        grouped.setdefault(wallet, []).append(row)
    events_written = 0
    for wallet, events in grouped.items():
        watch_events = [dict(event, source=RTDS_WATCH_ACTIVITY_SOURCE) for event in events]
        events_written += persist_wallet_activity(
            conn,
            wallet,
            watch_events,
            ingested_at=ingested_at,
            source=RTDS_WATCH_ACTIVITY_SOURCE,
        )
    result["wallets"] = set(grouped)
    result["events_written"] = events_written
    return result


def _wallet_from_activity_row(row: dict[str, Any]) -> str:
    for key in RTDS_WALLET_KEYS:
        value = str(row.get(key) or "").lower().strip()
        if value.startswith("0x") and len(value) == 42:
            return value
    return ""


def _wallet_keys_from_activity_row(row: dict[str, Any]) -> list[str]:
    keys = []
    for key in RTDS_WALLET_KEYS:
        value = str(row.get(key) or "").lower().strip()
        if value.startswith("0x") and len(value) == 42:
            keys.append(key)
    if not keys:
        keys.append("none")
    return keys


def _empty_paper_activity_result() -> dict[str, Any]:
    return {
        "wallets": set(),
        "events_written": 0,
        "rows_seen": 0,
        "rows_with_wallet": 0,
        "rows_matched": 0,
        "eligible_wallets": 0,
        "wallet_field_counts": {},
    }


def _merge_wallet_field_counts(target: dict[str, int], source: dict[str, int]) -> None:
    for key, count in (source or {}).items():
        target[str(key)] = int(target.get(str(key), 0)) + int(count or 0)


def _wallet_field_counts_text(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    items = sorted(counts.items(), key=lambda item: (-int(item[1]), str(item[0])))
    return ",".join(f"{key}:{int(count)}" for key, count in items[:6])


def _json_message(raw: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _trade_usdc(row: dict[str, Any]) -> float:
    explicit = _float(row.get("usdcSize") or row.get("usdc_size"))
    if explicit is not None:
        return explicit
    return (_float(row.get("size")) or 0.0) * (_float(row.get("price")) or 0.0)


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _deadline_reached(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline
