"""Real-time wallet discovery from Polymarket RTDS trade events."""

from __future__ import annotations

import json
import math
import os
import sqlite3
import time
import heapq
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Protocol

from pm_robot.clients.websocket import SimpleWebSocketClient
from pm_robot.orchestration.activity_discovery import (
    ActivityDiscoverySummary,
    process_activity_rows,
)
from pm_robot.storage.repository import record_runtime_heartbeat
from pm_robot.storage.wallet_levels import try_normalize_wallet
from pm_robot.wallet_levels import (
    RECENT_SAMPLE_TRADE_LIMIT,
    RECENT_SAMPLE_VOLUME_GATE_USDC,
)


RTDS_ENDPOINT = "wss://ws-live-data.polymarket.com"
DEFAULT_RTDS_MIN_TRADE_USDC = 10.0
RTDS_HEARTBEAT_MIN_SECONDS = 60.0
DEFAULT_RTDS_MAX_IDLE_SECONDS = 300.0
DEFAULT_RTDS_L0_BUFFER_TTL_SECONDS = 86_400.0
DEFAULT_RTDS_L0_BUFFER_MAX_WALLETS = 50_000
DEFAULT_RTDS_PERSIST_COOLDOWN_SECONDS = 300.0
DEFAULT_RTDS_DUE_DRAIN_MAX_WALLETS = 1_000
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
    buffered_wallets: int
    qualified_flushes: int
    deferred_low_value: int
    evicted: int
    backpressure: int
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
    buffered_wallets: int = 0
    qualified_flushes: int = 0
    deferred_low_value: int = 0
    evicted: int = 0
    backpressure: int = 0

    def absorb(self, result: ActivityDiscoverySummary) -> None:
        self.wallets_seen += int(result.wallets_seen)
        self.candidates_inserted_or_updated += int(result.candidates_inserted_or_updated)
        self.features_updated += int(result.features_updated)
        self.observed_wallets += int(result.observed_wallets)
        self.promoted_wallets += int(result.promoted_wallets)


@dataclass
class _BufferedWalletSample:
    wallet: str
    first_seen_monotonic: float
    updated_monotonic: float
    rows_by_key: OrderedDict[str, dict[str, Any]] = field(default_factory=OrderedDict)

    def add(self, row: dict[str, Any], *, now: float) -> None:
        trade_key = _activity_trade_key(row)
        if not trade_key:
            return
        self.rows_by_key[trade_key] = row
        self.updated_monotonic = now
        self._trim()

    def _trim(self) -> None:
        rows = sorted(
            self.rows_by_key.values(),
            key=lambda row: (
                int(_float(row.get("timestamp")) or 0),
                _activity_trade_key(row),
            ),
            reverse=True,
        )[:RECENT_SAMPLE_TRADE_LIMIT]
        self.rows_by_key = OrderedDict((_activity_trade_key(row), row) for row in rows)

    @property
    def rows(self) -> list[dict[str, Any]]:
        return list(self.rows_by_key.values())

    @property
    def volume_usdc(self) -> float:
        return sum(_trade_usdc(row) for row in self.rows)

    @property
    def keys(self) -> set[str]:
        return set(self.rows_by_key)


@dataclass(frozen=True)
class _ObservedSnapshot:
    recent_usdc_total: float
    recent_trade_usdc_by_key: dict[str, float]
    updated_at: int

    @property
    def recent_trade_keys(self) -> set[str]:
        return set(self.recent_trade_usdc_by_key)


class _RTDSL0QualificationBuffer:
    """Process-local L0 gate for RTDS.

    Sub-threshold RTDS samples are intentionally transient: they survive
    WebSocket reconnects in this process, but a process restart may drop samples
    that have not yet reached the L0 to L1 recent-volume gate.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float,
        max_wallets: int,
        persist_cooldown_seconds: float,
    ) -> None:
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self.max_wallets = max(1, int(max_wallets))
        self.persist_cooldown_seconds = max(0.0, float(persist_cooldown_seconds))
        self.wallets: OrderedDict[str, _BufferedWalletSample] = OrderedDict()
        self.qualified_pending_wallets: set[str] = set()
        self.last_persisted_monotonic: dict[str, float] = {}
        self.next_due_at: dict[str, float] = {}
        self.due_heap: list[tuple[float, str]] = []
        self.expires_at: dict[str, float] = {}
        self.expiry_heap: list[tuple[float, str]] = []

    def consume_qualified_rows(
        self,
        conn: sqlite3.Connection,
        rows: list[dict[str, Any]],
        *,
        now: float,
        counters: _DiscoveryCounters,
    ) -> list[dict[str, Any]]:
        self._prune_persisted(now=now)
        self._expire(now=now, counters=counters)
        touched: set[str] = set()
        incoming_count_by_wallet: dict[str, int] = {}
        for row in rows:
            wallet = _wallet_from_activity_row(row)
            if not wallet:
                continue
            touched.add(wallet)
            incoming_count_by_wallet[wallet] = incoming_count_by_wallet.get(wallet, 0) + 1
            sample = self.wallets.get(wallet)
            if sample is None:
                if not self._make_room(counters):
                    counters.backpressure += 1
                    continue
                sample = _BufferedWalletSample(
                    wallet=wallet,
                    first_seen_monotonic=now,
                    updated_monotonic=now,
                )
            self.wallets[wallet] = sample
            sample.add(row, now=now)
            self._schedule_expiry(wallet, sample.updated_monotonic + self.ttl_seconds)

        if not touched:
            counters.buffered_wallets = len(self.wallets)
            return []

        qualified_rows = self._qualified_rows_for_wallets(
            conn,
            touched,
            now=now,
            counters=counters,
            incoming_count_by_wallet=incoming_count_by_wallet,
        )
        counters.buffered_wallets = len(self.wallets)
        return qualified_rows

    def due_rows(
        self,
        conn: sqlite3.Connection,
        *,
        now: float,
        counters: _DiscoveryCounters,
        max_wallets: int | None = None,
    ) -> list[dict[str, Any]]:
        self._prune_persisted(now=now)
        due_wallets = self._peek_due_wallets(now=now, max_wallets=max_wallets)
        if not due_wallets:
            self._expire(now=now, counters=counters)
            counters.buffered_wallets = len(self.wallets)
            return []
        rows = self._qualified_rows_for_wallets(
            conn,
            due_wallets,
            now=now,
            counters=counters,
            incoming_count_by_wallet={},
        )
        self._consume_due_wallets(due_wallets)
        counters.buffered_wallets = len(self.wallets)
        return rows

    def _qualified_rows_for_wallets(
        self,
        conn: sqlite3.Connection,
        wallets: set[str],
        *,
        now: float,
        counters: _DiscoveryCounters,
        incoming_count_by_wallet: dict[str, int],
    ) -> list[dict[str, Any]]:
        existing_candidates = _candidate_wallets_snapshot(conn, wallets)
        observed = _observed_wallets_snapshot(conn, wallets)
        qualified_wallets: set[str] = set()
        for wallet in sorted(wallets):
            sample = self.wallets.get(wallet)
            if sample is None:
                continue
            observed_snapshot = observed.get(wallet)
            existing_sample_total = _non_duplicate_observed_total(
                observed_snapshot,
                sample,
            )
            reaches_gate = (
                sample.volume_usdc + existing_sample_total
                >= RECENT_SAMPLE_VOLUME_GATE_USDC
            )
            persisted_identity = wallet in existing_candidates or wallet in observed
            self._hydrate_db_cooldown(
                wallet,
                now=now,
                db_updated_at=max(
                    existing_candidates.get(wallet, 0),
                    observed_snapshot.updated_at if observed_snapshot is not None else 0,
                ),
            )
            cooldown_active = self._cooldown_active(wallet, now=now)
            existing_candidate = wallet in existing_candidates
            if (
                wallet in self.qualified_pending_wallets
                or (reaches_gate and not existing_candidate)
                or (persisted_identity and not cooldown_active)
            ):
                qualified_wallets.add(wallet)
            else:
                if persisted_identity and cooldown_active:
                    self._schedule_due(
                        wallet,
                        self.last_persisted_monotonic[wallet]
                        + self.persist_cooldown_seconds,
                    )
                counters.deferred_low_value += incoming_count_by_wallet.get(wallet, 0)

        qualified_rows: list[dict[str, Any]] = []
        for wallet in sorted(qualified_wallets):
            sample = self.wallets.get(wallet)
            if sample is None:
                continue
            qualified_rows.extend(sample.rows)
            self.qualified_pending_wallets.add(wallet)
            self._schedule_due(wallet, now)

        if qualified_rows:
            counters.qualified_flushes += 1
        return qualified_rows

    def mark_persisted(self, rows: list[dict[str, Any]], *, now: float) -> None:
        self._prune_persisted(now=now)
        for row in rows:
            wallet = _wallet_from_activity_row(row)
            if not wallet:
                continue
            self.wallets.pop(wallet, None)
            self.qualified_pending_wallets.discard(wallet)
            self.next_due_at.pop(wallet, None)
            self.expires_at.pop(wallet, None)
            self.last_persisted_monotonic[wallet] = now
        self._prune_persisted(now=now)

    def _cooldown_active(self, wallet: str, *, now: float) -> bool:
        last = self.last_persisted_monotonic.get(wallet)
        return (
            last is not None
            and self.persist_cooldown_seconds > 0
            and now - last < self.persist_cooldown_seconds
        )

    def _hydrate_db_cooldown(
        self,
        wallet: str,
        *,
        now: float,
        db_updated_at: int,
    ) -> None:
        if wallet in self.last_persisted_monotonic:
            return
        if self.persist_cooldown_seconds <= 0 or db_updated_at <= 0:
            return
        age_seconds = max(0.0, time.time() - float(db_updated_at))
        if age_seconds >= self.persist_cooldown_seconds:
            return
        persisted_monotonic = now - age_seconds
        self.last_persisted_monotonic[wallet] = persisted_monotonic

    def _expire(self, *, now: float, counters: _DiscoveryCounters) -> None:
        if self.ttl_seconds <= 0:
            return
        while self.expiry_heap and self.expiry_heap[0][0] <= now:
            expires_at, wallet = heapq.heappop(self.expiry_heap)
            if self.expires_at.get(wallet) != expires_at:
                continue
            if wallet in self.qualified_pending_wallets or wallet in self.next_due_at:
                continue
            self.wallets.pop(wallet, None)
            self.next_due_at.pop(wallet, None)
            self.expires_at.pop(wallet, None)
            counters.evicted += 1

    def _make_room(self, counters: _DiscoveryCounters) -> bool:
        if len(self.wallets) < self.max_wallets:
            return True
        for wallet in list(self.wallets):
            if wallet in self.qualified_pending_wallets or wallet in self.next_due_at:
                continue
            self.wallets.pop(wallet, None)
            self.next_due_at.pop(wallet, None)
            self.expires_at.pop(wallet, None)
            counters.evicted += 1
            return True
        return False

    def _prune_persisted(self, *, now: float) -> None:
        if not self.last_persisted_monotonic:
            return
        expired_before = now - self.persist_cooldown_seconds
        for wallet, persisted_at in list(self.last_persisted_monotonic.items()):
            if self.persist_cooldown_seconds <= 0 or (
                persisted_at <= expired_before
                and wallet not in self.wallets
                and wallet not in self.next_due_at
            ):
                self.last_persisted_monotonic.pop(wallet, None)

    def _schedule_due(self, wallet: str, due_at: float) -> None:
        existing = self.next_due_at.get(wallet)
        if existing is not None and existing <= due_at:
            return
        self.next_due_at[wallet] = due_at
        heapq.heappush(self.due_heap, (due_at, wallet))

    def _schedule_expiry(self, wallet: str, expires_at: float) -> None:
        if self.ttl_seconds <= 0:
            return
        self.expires_at[wallet] = expires_at
        heapq.heappush(self.expiry_heap, (expires_at, wallet))

    def _peek_due_wallets(self, *, now: float, max_wallets: int | None = None) -> set[str]:
        while self.due_heap:
            due_at, wallet = self.due_heap[0]
            if self.next_due_at.get(wallet) != due_at or wallet not in self.wallets:
                heapq.heappop(self.due_heap)
                continue
            break
        if not self.due_heap or self.due_heap[0][0] > now:
            return set()
        due_wallets: set[str] = set()
        limit = max(1, int(max_wallets)) if max_wallets is not None else None
        for due_at, wallet in sorted(self.due_heap):
            if due_at > now:
                break
            if self.next_due_at.get(wallet) != due_at or wallet not in self.wallets:
                continue
            due_wallets.add(wallet)
            if limit is not None and len(due_wallets) >= limit:
                break
        return due_wallets

    def _consume_due_wallets(self, wallets: set[str]) -> None:
        for wallet in wallets:
            self.next_due_at.pop(wallet, None)

    def restore_due_for_rows(self, rows: list[dict[str, Any]], *, due_at: float) -> None:
        for row in rows:
            wallet = _wallet_from_activity_row(row)
            if wallet and wallet in self.wallets:
                self._schedule_due(wallet, due_at)


def run_rtds_activity_discovery(
    conn: sqlite3.Connection,
    *,
    endpoint: str = RTDS_ENDPOINT,
    min_trade_usdc: float = DEFAULT_RTDS_MIN_TRADE_USDC,
    batch_size: int = 25,
    flush_interval: float = 10.0,
    ping_interval: float = 5.0,
    receive_timeout: float = 1.0,
    max_idle_seconds: float = DEFAULT_RTDS_MAX_IDLE_SECONDS,
    reconnect_sleep: float = 5.0,
    max_runtime_seconds: float = 0.0,
    max_messages: int = 0,
    max_reconnects: int = 0,
    l0_buffer_ttl_seconds: float | None = None,
    l0_buffer_max_wallets: int | None = None,
    persist_cooldown_seconds: float | None = None,
    due_drain_max_wallets: int | None = None,
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
    factory = websocket_factory or (lambda url: SimpleWebSocketClient(url))
    effective_due_drain_max_wallets = _env_int(
        "PM_ROBOT_RTDS_DUE_DRAIN_MAX_WALLETS",
        min(DEFAULT_RTDS_DUE_DRAIN_MAX_WALLETS, effective_batch_size),
        explicit=due_drain_max_wallets,
    )
    last_heartbeat = 0.0
    l0_buffer = _RTDSL0QualificationBuffer(
        ttl_seconds=_env_float(
            "PM_ROBOT_RTDS_L0_BUFFER_TTL_SECONDS",
            DEFAULT_RTDS_L0_BUFFER_TTL_SECONDS,
            explicit=l0_buffer_ttl_seconds,
        ),
        max_wallets=_env_int(
            "PM_ROBOT_RTDS_L0_BUFFER_MAX_WALLETS",
            DEFAULT_RTDS_L0_BUFFER_MAX_WALLETS,
            explicit=l0_buffer_max_wallets,
        ),
        persist_cooldown_seconds=_env_float(
            "PM_ROBOT_RTDS_PERSIST_COOLDOWN_SECONDS",
            DEFAULT_RTDS_PERSIST_COOLDOWN_SECONDS,
            explicit=persist_cooldown_seconds,
        ),
    )

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
            with factory(endpoint) as ws:
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
                                l0_buffer=l0_buffer,
                                counters=counters,
                                min_trade_usdc=min_trade_usdc,
                                max_candidates=effective_batch_size,
                            )
                            last_flush = time.monotonic()
                        else:
                            rows_written = _flush_due_buffer(
                                conn,
                                l0_buffer,
                                counters=counters,
                                min_trade_usdc=min_trade_usdc,
                                max_candidates=effective_batch_size,
                                max_due_wallets=effective_due_drain_max_wallets,
                            )
                            last_flush = time.monotonic() if rows_written else last_flush
                        if rows_written and _rtds_heartbeat_due(
                            last_heartbeat=last_heartbeat,
                            now=last_flush,
                        ):
                            _record_rtds_heartbeat(
                                conn,
                                counters=counters,
                                rows_written=rows_written,
                            )
                            last_heartbeat = last_flush
                        if not rows_written:
                            if _rtds_heartbeat_due(
                                last_heartbeat=last_heartbeat,
                                now=timeout_now,
                            ):
                                _record_rtds_heartbeat(
                                    conn,
                                    counters=counters,
                                )
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
                        rows_written = _flush_due_buffer(
                            conn,
                            l0_buffer,
                            counters=counters,
                            min_trade_usdc=min_trade_usdc,
                            max_candidates=effective_batch_size,
                            max_due_wallets=effective_due_drain_max_wallets,
                        )
                        if rows_written:
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
                            l0_buffer=l0_buffer,
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
                        l0_buffer=l0_buffer,
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
            error = _flush_pending_after_exception(
                conn,
                discovery_batch,
                l0_buffer=l0_buffer,
                counters=counters,
                min_trade_usdc=min_trade_usdc,
                max_candidates=effective_batch_size,
                original_error=str(exc),
            )
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
        buffered_wallets=counters.buffered_wallets,
        qualified_flushes=counters.qualified_flushes,
        deferred_low_value=counters.deferred_low_value,
        evicted=counters.evicted,
        backpressure=counters.backpressure,
        reconnects=reconnects,
        status=status,
        error=error,
    )


def _flush_pending_batch(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    l0_buffer: _RTDSL0QualificationBuffer | None = None,
    counters: _DiscoveryCounters,
    min_trade_usdc: float,
    max_candidates: int,
) -> int:
    """Flush one qualified discovery batch and update counters."""

    if not rows:
        return 0
    flush_now = time.monotonic()
    if l0_buffer is not None:
        qualified_rows = l0_buffer.consume_qualified_rows(
            conn,
            rows,
            now=flush_now,
            counters=counters,
        )
        rows.clear()
        if not qualified_rows:
            return 0
        rows = qualified_rows
    result = _flush_realtime_batch(
        conn,
        rows,
        min_trade_usdc=min_trade_usdc,
        max_candidates=max_candidates,
    )
    if l0_buffer is not None:
        l0_buffer.mark_persisted(rows, now=flush_now)
        counters.buffered_wallets = len(l0_buffer.wallets)
    rows.clear()
    counters.batches_flushed += 1
    counters.absorb(result)
    return _discovery_rows_written(result)


def _flush_due_buffer(
    conn: sqlite3.Connection,
    l0_buffer: _RTDSL0QualificationBuffer,
    *,
    counters: _DiscoveryCounters,
    min_trade_usdc: float,
    max_candidates: int,
    max_due_wallets: int | None = None,
) -> int:
    flush_now = time.monotonic()
    rows = l0_buffer.due_rows(
        conn,
        now=flush_now,
        counters=counters,
        max_wallets=max_due_wallets,
    )
    if not rows:
        return 0
    try:
        result = _flush_realtime_batch(
            conn,
            rows,
            min_trade_usdc=min_trade_usdc,
            max_candidates=max_candidates,
        )
    except Exception:
        l0_buffer.restore_due_for_rows(rows, due_at=flush_now)
        raise
    l0_buffer.mark_persisted(rows, now=flush_now)
    counters.buffered_wallets = len(l0_buffer.wallets)
    counters.batches_flushed += 1
    counters.absorb(result)
    return _discovery_rows_written(result)


def _flush_pending_after_exception(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    *,
    l0_buffer: _RTDSL0QualificationBuffer,
    counters: _DiscoveryCounters,
    min_trade_usdc: float,
    max_candidates: int,
    original_error: str,
) -> str:
    if not rows:
        return original_error
    try:
        _flush_pending_batch(
            conn,
            rows,
            l0_buffer=l0_buffer,
            counters=counters,
            min_trade_usdc=min_trade_usdc,
            max_candidates=max_candidates,
        )
    except Exception as flush_exc:
        return (
            f"{original_error} | pending_flush_failed={flush_exc}"
            if original_error
            else f"pending_flush_failed={flush_exc}"
        )
    return original_error


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
        except (TypeError, ValueError, OverflowError):
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
        f"promoted={counters.promoted_wallets} "
        f"buffered_wallets={counters.buffered_wallets} "
        f"qualified_flushes={counters.qualified_flushes} "
        f"deferred_low_value={counters.deferred_low_value} "
        f"evicted={counters.evicted} "
        f"backpressure={counters.backpressure}"
    )


def _discovery_rows_written(result: ActivityDiscoverySummary) -> int:
    return int(result.observed_wallets) + int(result.candidates_inserted_or_updated)


def _candidate_wallets_snapshot(
    conn: sqlite3.Connection,
    addresses: set[str],
) -> dict[str, int]:
    normalized = sorted({wallet for wallet in addresses if wallet})
    if not normalized:
        return {}
    snapshot: dict[str, int] = {}
    for chunk in _chunks(normalized, _sqlite_variable_limit(conn)):
        placeholders = ", ".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT address, updated_at FROM candidate_wallets WHERE address IN ({placeholders})",
            tuple(chunk),
        ).fetchall()
        snapshot.update({str(row["address"]): int(row["updated_at"] or 0) for row in rows})
    return snapshot


def _observed_wallets_snapshot(
    conn: sqlite3.Connection,
    addresses: set[str],
) -> dict[str, _ObservedSnapshot]:
    normalized = sorted({wallet for wallet in addresses if wallet})
    if not normalized:
        return {}
    snapshot: dict[str, _ObservedSnapshot] = {}
    for chunk in _chunks(normalized, _sqlite_variable_limit(conn)):
        placeholders = ", ".join("?" for _ in chunk)
        rows = conn.execute(
            """
            SELECT wallet, recent_usdc_total, recent_trades_json, updated_at
            FROM observed_wallets
            WHERE wallet IN (
            """
            + placeholders
            + ")",
            tuple(chunk),
        ).fetchall()
        for row in rows:
            trades = _json_list(str(row["recent_trades_json"] or "[]"))
            snapshot[str(row["wallet"])] = _ObservedSnapshot(
                recent_usdc_total=float(row["recent_usdc_total"] or 0.0),
                recent_trade_usdc_by_key=_trade_usdc_by_key(trades),
                updated_at=int(row["updated_at"] or 0),
            )
    return snapshot


def _non_duplicate_observed_total(
    observed: _ObservedSnapshot | None,
    sample: _BufferedWalletSample,
) -> float:
    if observed is None:
        return 0.0
    if sample.keys.isdisjoint(observed.recent_trade_keys):
        return observed.recent_usdc_total
    return sum(
        usdc
        for key, usdc in observed.recent_trade_usdc_by_key.items()
        if key not in sample.keys
    )


def _trade_usdc_by_key(trades: list[Any]) -> dict[str, float]:
    result: dict[str, float] = {}
    for item in trades:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        if not key:
            continue
        usdc = _float(item.get("usdc_size"))
        result[key] = max(0.0, usdc or 0.0)
    return result


def _activity_trade_key(row: dict[str, Any]) -> str:
    timestamp = int(_float(row.get("timestamp")) or 0)
    market = str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "").strip()
    side = str(row.get("side") or "").strip().upper()
    usdc_size = _trade_usdc(row)
    tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or "").strip()
    return "|".join(
        [
            tx_hash,
            str(timestamp),
            market,
            side,
            f"{usdc_size:.8f}",
        ]
    )


def _json_list(raw: str) -> list[Any]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    return value if isinstance(value, list) else []


def _chunks(items: list[str], size: int) -> list[list[str]]:
    chunk_size = max(1, int(size))
    return [items[offset : offset + chunk_size] for offset in range(0, len(items), chunk_size)]


def _sqlite_variable_limit(conn: sqlite3.Connection) -> int:
    try:
        rows = conn.execute("PRAGMA compile_options").fetchall()
    except sqlite3.Error:
        return 999
    for row in rows:
        option = str(row[0])
        if option.startswith("MAX_VARIABLE_NUMBER="):
            try:
                return max(1, int(option.partition("=")[2]))
            except ValueError:
                return 999
    return 999


def _env_float(name: str, default: float, *, explicit: float | None = None) -> float:
    if explicit is not None:
        return float(explicit)
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int, *, explicit: int | None = None) -> int:
    if explicit is not None:
        return int(explicit)
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _wallet_from_activity_row(row: dict[str, Any]) -> str:
    for key in RTDS_WALLET_KEYS:
        value = try_normalize_wallet(row.get(key))
        if value:
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
            return max(0.0, explicit)
    total = (_float(row.get("size")) or 0.0) * (_float(row.get("price")) or 0.0)
    return max(0.0, total) if math.isfinite(total) else 0.0


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


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
