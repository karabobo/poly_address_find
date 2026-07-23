"""Real-time wallet discovery from Polymarket RTDS trade events."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Protocol

from pm_robot.clients.websocket import SimpleWebSocketClient
from pm_robot.orchestration.activity_discovery import (
    ActivityDiscoverySummary,
    process_activity_rows,
)
from pm_robot.storage.repository import record_runtime_heartbeat


RTDS_ENDPOINT = "wss://ws-live-data.polymarket.com"
RTDS_HEARTBEAT_MIN_SECONDS = 60.0
DEFAULT_RTDS_MAX_IDLE_SECONDS = 300.0
RTDS_SQLITE_LOCK_RETRY_DELAYS = (0.25, 0.5, 1.0, 2.0)
RTDS_WALLET_KEYS = (
    "proxyWallet",
    "proxy_wallet",
    "user",
    "address",
    "wallet",
    "trader",
)


class TextWebSocket(Protocol):
    def send_text(self, text: str) -> None: ...

    def recv_text(self, *, timeout: float | None = None) -> str: ...


class RTDSStreamIdleError(RuntimeError):
    """Raised when a connected RTDS stream stops delivering data messages."""


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
    reconnects: int
    status: str
    error: str = ""


@dataclass
class _DiscoveryCounters:
    messages_seen: int = 0
    trades_seen: int = 0
    trades_selected: int = 0
    batches_flushed: int = 0
    wallets_seen: int = 0
    candidates_inserted_or_updated: int = 0
    features_updated: int = 0
    observed_wallets: int = 0
    promoted_wallets: int = 0

    def absorb(self, result: ActivityDiscoverySummary) -> None:
        self.wallets_seen += int(result.wallets_seen)
        self.candidates_inserted_or_updated += int(result.candidates_inserted_or_updated)
        self.features_updated += int(result.features_updated)
        self.observed_wallets += int(result.observed_wallets)
        self.promoted_wallets += int(result.promoted_wallets)


def run_rtds_activity_discovery(
    conn: sqlite3.Connection,
    *,
    endpoint: str = RTDS_ENDPOINT,
    min_trade_usdc: float = 1.0,
    batch_size: int = 25,
    flush_interval: float = 10.0,
    ping_interval: float = 5.0,
    receive_timeout: float = 1.0,
    max_idle_seconds: float = DEFAULT_RTDS_MAX_IDLE_SECONDS,
    reconnect_sleep: float = 5.0,
    max_runtime_seconds: float = 0.0,
    max_messages: int = 0,
    max_reconnects: int = 0,
    websocket_factory: Any | None = None,
) -> RTDSActivityDiscoverySummary:
    """Consume RTDS trades and route selected rows into the shared wallet ingress."""
    deadline = time.monotonic() + max_runtime_seconds if max_runtime_seconds > 0 else None
    effective_batch_size = max(1, int(batch_size))
    counters = _DiscoveryCounters()
    attempted = 0
    succeeded = 0
    reconnects = 0
    status = "ok"
    error = ""
    websocket_factory = websocket_factory or (lambda url: SimpleWebSocketClient(url))
    last_heartbeat = 0.0

    while not _stop_requested(deadline, counters.messages_seen, max_messages):
        if max_reconnects > 0 and reconnects > max_reconnects:
            status = "partial" if succeeded else "failed"
            break

        attempted += 1
        discovery_batch: list[dict[str, Any]] = []
        last_flush = time.monotonic()
        last_ping = 0.0
        stream_idle_error: RTDSStreamIdleError | None = None

        try:
            with websocket_factory(endpoint) as ws:
                succeeded += 1
                _subscribe_activity_trades(ws)
                _record_rtds_heartbeat(conn, counters=counters)
                last_heartbeat = time.monotonic()
                last_message_at = last_heartbeat

                while not _stop_requested(deadline, counters.messages_seen, max_messages):
                    now = time.monotonic()
                    if ping_interval > 0 and now - last_ping >= ping_interval:
                        ws.send_text("PING")
                        last_ping = now

                    try:
                        raw = ws.recv_text(timeout=receive_timeout)
                    except TimeoutError:
                        timeout_now = time.monotonic()
                        if discovery_batch and timeout_now - last_flush >= flush_interval:
                            rows_written = _flush_pending_batch(
                                conn,
                                discovery_batch,
                                counters=counters,
                                min_trade_usdc=min_trade_usdc,
                                max_candidates=effective_batch_size,
                            )
                            last_flush = time.monotonic()
                            if _rtds_heartbeat_due(
                                last_heartbeat=last_heartbeat,
                                now=last_flush,
                            ):
                                _record_rtds_heartbeat(
                                    conn,
                                    counters=counters,
                                    rows_written=rows_written,
                                )
                                last_heartbeat = last_flush
                        elif timeout_now - last_heartbeat >= max(
                            RTDS_HEARTBEAT_MIN_SECONDS,
                            flush_interval,
                        ):
                            _record_rtds_heartbeat(conn, counters=counters)
                            last_heartbeat = timeout_now

                        stream_idle_error = _rtds_stream_idle_error(
                            last_message_at=last_message_at,
                            now=timeout_now,
                            max_idle_seconds=max_idle_seconds,
                        )
                        if stream_idle_error is not None:
                            break
                        continue

                    message_now = time.monotonic()
                    if raw in {"PING", "PONG", ""}:
                        stream_idle_error = _rtds_stream_idle_error(
                            last_message_at=last_message_at,
                            now=message_now,
                            max_idle_seconds=max_idle_seconds,
                        )
                        if stream_idle_error is not None:
                            break
                        continue

                    message = _json_message(raw)
                    if message is None:
                        stream_idle_error = _rtds_stream_idle_error(
                            last_message_at=last_message_at,
                            now=message_now,
                            max_idle_seconds=max_idle_seconds,
                        )
                        if stream_idle_error is not None:
                            break
                        continue

                    last_message_at = message_now
                    counters.messages_seen += 1
                    trade = rtds_trade_to_activity_row(message)
                    if trade is None:
                        continue
                    counters.trades_seen += 1
                    if _trade_usdc(trade) < min_trade_usdc:
                        continue

                    counters.trades_selected += 1
                    discovery_batch.append(trade)
                    if len(discovery_batch) >= effective_batch_size:
                        rows_written = _flush_pending_batch(
                            conn,
                            discovery_batch,
                            counters=counters,
                            min_trade_usdc=min_trade_usdc,
                            max_candidates=effective_batch_size,
                        )
                        last_flush = time.monotonic()
                        if _rtds_heartbeat_due(
                            last_heartbeat=last_heartbeat,
                            now=last_flush,
                        ):
                            _record_rtds_heartbeat(
                                conn,
                                counters=counters,
                                rows_written=rows_written,
                            )
                            last_heartbeat = last_flush

                if discovery_batch:
                    rows_written = _flush_pending_batch(
                        conn,
                        discovery_batch,
                        counters=counters,
                        min_trade_usdc=min_trade_usdc,
                        max_candidates=effective_batch_size,
                    )
                    heartbeat_now = time.monotonic()
                    if _rtds_heartbeat_due(
                        last_heartbeat=last_heartbeat,
                        now=heartbeat_now,
                    ):
                        _record_rtds_heartbeat(
                            conn,
                            counters=counters,
                            rows_written=rows_written,
                        )
                        last_heartbeat = heartbeat_now

                if stream_idle_error is not None:
                    raise stream_idle_error
                if _stop_requested(deadline, counters.messages_seen, max_messages):
                    break
        except Exception as exc:
            error = str(exc)
            status = "partial" if succeeded else "failed"
            _record_rtds_heartbeat(
                conn,
                counters=counters,
                status=status,
                error=error,
            )
            reconnects += 1
            if _deadline_reached(deadline) or (
                max_reconnects > 0 and reconnects > max_reconnects
            ):
                break
            if reconnect_sleep > 0:
                time.sleep(reconnect_sleep)

    if status == "ok":
        _record_rtds_heartbeat(conn, counters=counters, status=status)

    return RTDSActivityDiscoverySummary(
        connections_attempted=attempted,
        connections_succeeded=succeeded,
        messages_seen=counters.messages_seen,
        trades_seen=counters.trades_seen,
        trades_selected=counters.trades_selected,
        batches_flushed=counters.batches_flushed,
        wallets_seen=counters.wallets_seen,
        candidates_inserted_or_updated=counters.candidates_inserted_or_updated,
        features_updated=counters.features_updated,
        observed_wallets=counters.observed_wallets,
        promoted_wallets=counters.promoted_wallets,
        reconnects=reconnects,
        status=status,
        error=error,
    )


def _flush_pending_batch(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    counters: _DiscoveryCounters,
    min_trade_usdc: float,
    max_candidates: int,
) -> int:
    """Flush one discovery batch and update counters; no other state is touched."""

    if not rows:
        return 0
    result = _flush_realtime_batch(
        conn,
        rows,
        min_trade_usdc=min_trade_usdc,
        max_candidates=max_candidates,
    )
    rows.clear()
    counters.batches_flushed += 1
    counters.absorb(result)
    return _discovery_rows_written(result)


def _flush_realtime_batch(
    conn: sqlite3.Connection,
    discovery_rows: list[dict[str, Any]],
    *,
    min_trade_usdc: float,
    max_candidates: int,
) -> ActivityDiscoverySummary:
    """Persist one discovery batch, retrying brief SQLite writer contention."""

    for delay in (*RTDS_SQLITE_LOCK_RETRY_DELAYS, None):
        try:
            return _flush_batch(
                conn,
                discovery_rows,
                min_trade_usdc=min_trade_usdc,
                max_candidates=max_candidates,
            )
        except sqlite3.OperationalError as exc:
            if not _sqlite_lock_error(exc) or delay is None:
                raise
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            time.sleep(delay)
    raise RuntimeError("unreachable rtds flush retry state")


def _flush_batch(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    min_trade_usdc: float,
    max_candidates: int,
) -> ActivityDiscoverySummary:
    return process_activity_rows(
        conn,
        rows,
        source="polymarket_rtds_activity",
        labels="realtime_trade_activity",
        status_prefix="rtds_activity_discovered",
        min_trade_usdc=min_trade_usdc,
        max_candidates=max_candidates,
    )


def rtds_trade_to_activity_row(message: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize one verified trade message for the shared discovery ingress."""

    if str(message.get("topic") or "") != "activity":
        return None
    if str(message.get("type") or "") != "trades":
        return None
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None

    row = dict(payload)
    wallet = _wallet_from_activity_row(row)
    if not wallet:
        return None
    row["proxyWallet"] = wallet
    if "timestamp" not in row and message.get("timestamp") is not None:
        try:
            row["timestamp"] = int(float(message["timestamp"]) / 1000)
        except (TypeError, ValueError):
            pass
    row["usdcSize"] = _trade_usdc(row)
    row.setdefault("type", "TRADE")
    row["source"] = "polymarket_rtds_activity"
    return row


def _subscribe_activity_trades(ws: TextWebSocket) -> None:
    ws.send_text(
        json.dumps(
            {
                "action": "subscribe",
                "subscriptions": [{"topic": "activity", "type": "trades"}],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _rtds_stream_idle_error(
    *,
    last_message_at: float,
    now: float,
    max_idle_seconds: float,
) -> RTDSStreamIdleError | None:
    """Return a reconnect signal after prolonged data-message silence."""

    if max_idle_seconds <= 0:
        return None
    idle_seconds = max(0.0, now - last_message_at)
    if idle_seconds >= max_idle_seconds:
        return RTDSStreamIdleError(
            f"rtds stream idle for {idle_seconds:.1f}s "
            f"(limit {max_idle_seconds:.1f}s)"
        )
    return None


def _record_rtds_heartbeat(
    conn: sqlite3.Connection,
    *,
    counters: _DiscoveryCounters,
    status: str = "ok",
    rows_written: int = 0,
    error: str = "",
) -> None:
    try:
        details = _rtds_heartbeat_details(counters)
        heartbeat_error = f"{error} | {details}" if error else details
        record_runtime_heartbeat(
            conn,
            "loop_rtds_discovery",
            status=status,
            rows_written=rows_written,
            error=heartbeat_error,
        )
    except sqlite3.Error:
        pass


def _rtds_heartbeat_due(*, last_heartbeat: float, now: float) -> bool:
    """Throttle healthy heartbeats while preserving reconnect and exit events."""

    return last_heartbeat <= 0 or now - last_heartbeat >= RTDS_HEARTBEAT_MIN_SECONDS


def _rtds_heartbeat_details(counters: _DiscoveryCounters) -> str:
    return (
        f"messages={counters.messages_seen} "
        f"trades={counters.trades_seen} "
        f"selected={counters.trades_selected} "
        f"batches={counters.batches_flushed} "
        f"wallets={counters.wallets_seen} "
        f"candidates={counters.candidates_inserted_or_updated} "
        f"observed={counters.observed_wallets} "
        f"promoted={counters.promoted_wallets}"
    )


def _discovery_rows_written(result: ActivityDiscoverySummary) -> int:
    return int(result.observed_wallets) + int(result.candidates_inserted_or_updated)


def _wallet_from_activity_row(row: dict[str, Any]) -> str:
    for key in RTDS_WALLET_KEYS:
        value = str(row.get(key) or "").lower().strip()
        if value.startswith("0x") and len(value) == 42:
            return value
    return ""


def _json_message(raw: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def _trade_usdc(row: dict[str, Any]) -> float:
    for key in ("usdcSize", "usdc_size"):
        explicit = _float(row.get(key))
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


def _sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def _stop_requested(
    deadline: float | None,
    messages_seen: int,
    max_messages: int,
) -> bool:
    return _deadline_reached(deadline) or (
        max_messages > 0 and messages_seen >= max_messages
    )


def _deadline_reached(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline
