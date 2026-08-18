import json
import sqlite3

from pm_robot.orchestration import rtds_discovery as rtds_module
from pm_robot.orchestration.activity_discovery import ActivityDiscoverySummary
from pm_robot.orchestration.rtds_discovery import (
    rtds_trade_to_activity_row,
    run_rtds_activity_discovery,
)
from pm_robot.models import CandidateAddress
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import upsert_candidate


def _table_exists(conn, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        is not None
    )


class FakeWebSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def send_text(self, text):
        self.sent.append(text)

    def recv_text(self, *, timeout=None):
        if not self.messages:
            raise TimeoutError("done")
        item = self.messages.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class ClockedWebSocket(FakeWebSocket):
    def __init__(self, messages, clock, *, step=1.0):
        super().__init__(messages)
        self.clock = clock
        self.step = step

    def recv_text(self, *, timeout=None):
        self.clock["now"] += self.step
        return super().recv_text(timeout=timeout)


class ScriptedClockWebSocket(FakeWebSocket):
    def __init__(self, messages, clock, *, advances, timeout_step=1.0):
        super().__init__(messages)
        self.clock = clock
        self.advances = list(advances)
        self.timeout_step = timeout_step

    def recv_text(self, *, timeout=None):
        if self.advances:
            self.clock["now"] += self.advances.pop(0)
        elif not self.messages:
            self.clock["now"] += self.timeout_step
        return super().recv_text(timeout=timeout)


def _rtds_message(
    wallet: str,
    tx: str,
    *,
    size: float,
    price: float = 0.5,
    market: str = "market-1",
) -> str:
    return json.dumps(
        {
            "topic": "activity",
            "type": "trades",
            "timestamp": 1_000_000,
            "payload": {
                "proxyWallet": wallet,
                "transactionHash": tx,
                "slug": market,
                "asset": "asset-1",
                "outcome": "YES",
                "side": "BUY",
                "price": price,
                "size": size,
            },
        }
    )


def test_rtds_trade_to_activity_row_extracts_wallet_and_usdc_size():
    wallet = "0x" + "1" * 40

    row = rtds_trade_to_activity_row(
        {
            "topic": "activity",
            "type": "trades",
            "timestamp": 1_000_000,
            "payload": {
                "trader": wallet.upper(),
                "price": 0.5,
                "size": 1200,
                "side": "BUY",
                "slug": "market-1",
                "transactionHash": "0xabc",
            },
        }
    )

    assert row is not None
    assert row["proxyWallet"] == wallet
    assert row["timestamp"] == 1_000
    assert row["usdcSize"] == 600
    assert row["source"] == "polymarket_rtds_activity"


def test_rtds_trade_to_activity_row_rejects_non_trade_or_missing_wallet():
    assert rtds_trade_to_activity_row({"topic": "status", "type": "trades", "payload": {}}) is None
    assert rtds_trade_to_activity_row(
        {
            "topic": "activity",
            "type": "trades",
            "payload": {"size": 1000, "price": 1},
        }
    ) is None
    assert rtds_trade_to_activity_row(
        {
            "topic": "activity",
            "type": "trades",
            "payload": {
                "proxyWallet": "0x" + "g" * 40,
                "size": 1000,
                "price": 1,
            },
        }
    ) is None


def test_run_rtds_activity_discovery_routes_verified_trade_to_l1(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    ws = FakeWebSocket(["PONG", _rtds_message(wallet, "0x1", size=1200)])
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        candidate = conn.execute(
            "SELECT * FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()
        observed = conn.execute(
            "SELECT * FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        level = conn.execute(
            "SELECT * FROM wallet_levels WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        source = conn.execute(
            """
            SELECT * FROM candidate_source_events
            WHERE address = ? AND source = 'polymarket_rtds_activity'
            """,
            (wallet,),
        ).fetchone()
        heartbeat = conn.execute(
            """
            SELECT * FROM runtime_heartbeats
            WHERE name = 'loop_rtds_discovery'
            ORDER BY heartbeat_id DESC
            LIMIT 1
            """
        ).fetchone()

        assert summary.status == "ok"
        assert summary.connections_succeeded == 1
        assert summary.messages_seen == 1
        assert summary.trades_seen == 1
        assert summary.trades_selected == 1
        assert summary.batches_flushed == 1
        assert summary.observed_wallets == 1
        assert summary.promoted_wallets == 0
        assert candidate is None
        assert observed["recent_max_trade_usdc"] == 600
        assert observed["recent_trade_count"] == 1
        assert observed["promotion_reason"] == ""
        assert level["level"] == "l0"
        assert source is None
        assert not _table_exists(conn, "wallet_activity")
        assert heartbeat is not None
        assert "messages=1" in heartbeat["error"]
        assert "selected=1" in heartbeat["error"]
        assert "promoted=0" in heartbeat["error"]
        assert any("activity" in item for item in ws.sent)
    finally:
        conn.close()


def test_rtds_high_volume_batches_do_not_write_one_heartbeat_per_batch(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "a" * 40
    ws = FakeWebSocket(
        [_rtds_message(wallet, f"0x{index}", size=1200) for index in range(5)]
    )
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            batch_size=1,
            max_messages=5,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        heartbeat_count = conn.execute(
            "SELECT COUNT(*) FROM runtime_heartbeats WHERE name = 'loop_rtds_discovery'"
        ).fetchone()[0]
        assert summary.batches_flushed == 2
        assert summary.deferred_low_value == 3
        assert heartbeat_count == 2
    finally:
        conn.close()


def test_rtds_buffers_independent_low_value_wallets_without_discovery_writes(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + digit * 40 for digit in ("b", "c", "d")]
    ws = FakeWebSocket(
        [_rtds_message(wallet, f"0xlow-{idx}", size=40) for idx, wallet in enumerate(wallets)]
    )
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=10,
            batch_size=1,
            max_messages=3,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )
        heartbeat = conn.execute(
            """
            SELECT error FROM runtime_heartbeats
            WHERE name = 'loop_rtds_discovery'
            ORDER BY heartbeat_id DESC
            LIMIT 1
            """
        ).fetchone()

        assert summary.trades_selected == 3
        assert summary.batches_flushed == 0
        assert summary.observed_wallets == 0
        assert summary.candidates_inserted_or_updated == 0
        assert summary.buffered_wallets == 3
        assert summary.deferred_low_value == 3
        assert conn.execute("SELECT COUNT(*) FROM observed_wallets").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM candidate_wallets").fetchone()[0] == 0
        assert "buffered_wallets=3" in heartbeat["error"]
        assert "deferred_low_value=3" in heartbeat["error"]
    finally:
        conn.close()


def test_rtds_accumulates_wallet_across_flushes_before_single_persist(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "e" * 40
    ws = FakeWebSocket(
        [
            _rtds_message(wallet, "0xpart-1", size=80),
            _rtds_message(wallet, "0xpart-2", size=60),
            _rtds_message(wallet, "0xpart-3", size=60),
        ]
    )
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=10,
            batch_size=1,
            max_messages=3,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )
        observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total "
            "FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.batches_flushed == 1
        assert summary.qualified_flushes == 1
        assert summary.deferred_low_value == 2
        assert summary.promoted_wallets == 1
        assert dict(observed) == {
            "observed_trade_count": 3,
            "recent_trade_count": 3,
            "recent_usdc_total": 100.0,
        }
    finally:
        conn.close()


def test_rtds_l0_buffer_deduplicates_current_sample_by_trade_key(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "f" * 40
    ws = FakeWebSocket(
        [
            _rtds_message(wallet, "0xduplicate", size=120),
            _rtds_message(wallet, "0xduplicate", size=120),
            _rtds_message(wallet, "0xunique", size=100),
        ]
    )
    try:
        run_migrations(conn)

        run_rtds_activity_discovery(
            conn,
            min_trade_usdc=10,
            batch_size=1,
            max_messages=3,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )
        observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total "
            "FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert dict(observed) == {
            "observed_trade_count": 2,
            "recent_trade_count": 2,
            "recent_usdc_total": 110.0,
        }
    finally:
        conn.close()


def test_rtds_existing_observed_plus_incoming_sample_promotes(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "1" * 39 + "0"
    ws = FakeWebSocket([_rtds_message(wallet, "0xincoming", size=120)])
    try:
        run_migrations(conn)
        rtds_module._flush_batch(
            conn,
            [{"proxyWallet": wallet, "transactionHash": "0xseed", "timestamp": 1_000, "slug": "seed", "side": "BUY", "usdcSize": 40}],
            min_trade_usdc=10,
            max_candidates=0,
        )

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=10,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )
        observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total, promotion_reason "
            "FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.batches_flushed == 1
        assert summary.promoted_wallets == 1
        assert dict(observed) == {
            "observed_trade_count": 2,
            "recent_trade_count": 2,
            "recent_usdc_total": 100.0,
            "promotion_reason": "observed_resource_gate",
        }
    finally:
        conn.close()


def test_rtds_existing_candidate_repeated_activity_obeys_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 39 + "0"
    ws = FakeWebSocket(
        [
            _rtds_message(wallet, "0xcooldown-1", size=1200),
            _rtds_message(wallet, "0xcooldown-2", size=1200),
        ]
    )
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual"), now=1)
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=10,
            batch_size=1,
            max_messages=2,
            reconnect_sleep=0,
            persist_cooldown_seconds=300,
            websocket_factory=lambda endpoint: ws,
        )
        observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total "
            "FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.batches_flushed == 1
        assert summary.deferred_low_value == 1
        assert summary.buffered_wallets == 1
        assert dict(observed) == {
            "observed_trade_count": 1,
            "recent_trade_count": 1,
            "recent_usdc_total": 600.0,
        }
    finally:
        conn.close()


def test_rtds_restart_uses_db_fresh_candidate_updated_at_for_zero_persist(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 39 + "9"
    ws = FakeWebSocket([_rtds_message(wallet, "0xdb-fresh", size=1200)])
    monkeypatch.setattr(rtds_module.time, "time", lambda: 1_000.0)
    monkeypatch.setattr(rtds_module.time, "monotonic", lambda: 10.0)
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual"), now=1_000)
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=10,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            persist_cooldown_seconds=300,
            websocket_factory=lambda endpoint: ws,
        )

        assert summary.batches_flushed == 0
        assert summary.observed_wallets == 0
        assert summary.candidates_inserted_or_updated == 0
        assert summary.deferred_low_value == 1
        assert summary.buffered_wallets == 1
        assert conn.execute(
            "SELECT 1 FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone() is None
    finally:
        conn.close()


def test_rtds_timer_drain_persists_candidate_after_cooldown_without_new_trade(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 39 + "1"
    clock = {"now": 100.0}
    ws = ScriptedClockWebSocket(
        [
            _rtds_message(wallet, "0xtimer-1", size=1200),
            _rtds_message(wallet, "0xtimer-2", size=1200),
        ],
        clock,
        advances=[0.0, 1.0],
        timeout_step=301.0,
    )
    monkeypatch.setattr(rtds_module.time, "monotonic", lambda: clock["now"])
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual"), now=1)
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=10,
            batch_size=1,
            flush_interval=60,
            max_idle_seconds=0,
            max_runtime_seconds=400,
            reconnect_sleep=0,
            persist_cooldown_seconds=300,
            websocket_factory=lambda endpoint: ws,
        )
        observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total "
            "FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.batches_flushed == 2
        assert summary.buffered_wallets == 0
        assert dict(observed) == {
            "observed_trade_count": 2,
            "recent_trade_count": 2,
            "recent_usdc_total": 1200.0,
        }
    finally:
        conn.close()


def test_rtds_timer_drain_uses_db_cooldown_after_restart_without_new_trade(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 39 + "8"
    clock = {"now": 100.0}
    ws = ScriptedClockWebSocket(
        [_rtds_message(wallet, "0xdb-timer", size=1200)],
        clock,
        advances=[0.0],
        timeout_step=301.0,
    )
    monkeypatch.setattr(rtds_module.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(rtds_module.time, "time", lambda: 900.0 + clock["now"])
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual"), now=1_000)
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=10,
            batch_size=1,
            flush_interval=60,
            max_idle_seconds=0,
            max_runtime_seconds=400,
            reconnect_sleep=0,
            persist_cooldown_seconds=300,
            websocket_factory=lambda endpoint: ws,
        )
        observed = conn.execute(
            "SELECT observed_trade_count, recent_trade_count, recent_usdc_total "
            "FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone()

        assert summary.batches_flushed == 1
        assert summary.deferred_low_value == 1
        assert summary.buffered_wallets == 0
        assert dict(observed) == {
            "observed_trade_count": 1,
            "recent_trade_count": 1,
            "recent_usdc_total": 600.0,
        }
    finally:
        conn.close()


def test_rtds_transport_exception_flushes_pending_batch_before_reconnect(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 39 + "2"
    clock = {"now": 100.0}
    ws = ScriptedClockWebSocket(
        [_rtds_message(wallet, "0xpending-before-transport", size=1200), RuntimeError("transport down")],
        clock,
        advances=[0.0, 2.0],
    )
    monkeypatch.setattr(rtds_module.time, "monotonic", lambda: clock["now"])
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            batch_size=25,
            max_runtime_seconds=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        assert summary.status == "partial"
        assert summary.error == "transport down"
        assert summary.batches_flushed == 1
        assert conn.execute(
            "SELECT 1 FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone() is not None
    finally:
        conn.close()


def test_rtds_sqlite_failure_protects_qualified_pending_from_capacity_eviction(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    counters = rtds_module._DiscoveryCounters()
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=1,
        max_wallets=1,
        persist_cooldown_seconds=300,
    )
    original_flush = rtds_module._flush_batch

    def locked_flush(conn_arg, rows, *, min_trade_usdc, max_candidates):
        raise sqlite3.OperationalError("database is locked")

    try:
        run_migrations(conn)
        monkeypatch.setattr(rtds_module, "_flush_batch", locked_flush)
        monkeypatch.setattr(rtds_module.time, "sleep", lambda _seconds: None)

        try:
            rtds_module._flush_pending_batch(
                conn,
                [{"proxyWallet": "0x" + "6" * 40, "transactionHash": "0xqualified", "timestamp": 1, "usdcSize": 150}],
                l0_buffer=buffer,
                counters=counters,
                min_trade_usdc=10,
                max_candidates=1,
            )
        except sqlite3.OperationalError:
            pass
        else:
            raise AssertionError("expected sqlite lock")

        assert set(buffer.wallets) == {"0x" + "6" * 40}
        assert buffer.qualified_pending_wallets == {"0x" + "6" * 40}

        buffer.consume_qualified_rows(
            conn,
            [{"proxyWallet": "0x" + "7" * 40, "transactionHash": "0xlow", "timestamp": 2, "usdcSize": 20}],
            now=2.0,
            counters=counters,
        )

        assert set(buffer.wallets) == {"0x" + "6" * 40}
        assert counters.backpressure == 1
        assert counters.evicted == 0

        monkeypatch.setattr(rtds_module, "_flush_batch", original_flush)
        rows_written = rtds_module._flush_due_buffer(
            conn,
            buffer,
            counters=counters,
            min_trade_usdc=10,
            max_candidates=1,
        )

        assert rows_written == 1
        assert buffer.wallets == {}
        assert buffer.qualified_pending_wallets == set()
    finally:
        conn.close()


def test_rtds_cooldown_map_prunes_only_expired_entries():
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=60,
        max_wallets=2,
        persist_cooldown_seconds=10,
    )
    expired_wallet = "0x" + "8" * 40
    kept_wallet = "0x" + "9" * 40
    newest_wallet = "0x" + "a" * 40
    buffer.last_persisted_monotonic = {
        expired_wallet: 1.0,
        kept_wallet: 3.0,
        newest_wallet: 4.0,
    }

    buffer._prune_persisted(now=11.0)

    assert buffer.last_persisted_monotonic == {
        kept_wallet: 3.0,
        newest_wallet: 4.0,
    }


def test_rtds_noop_timer_ticks_do_not_snapshot_unknown_low_value_buffer(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    counters = rtds_module._DiscoveryCounters()
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=3600,
        max_wallets=50_000,
        persist_cooldown_seconds=300,
    )

    def forbidden_snapshot(*_args, **_kwargs):
        raise AssertionError("timer tick must not query discovery identity snapshots")

    try:
        run_migrations(conn)
        buffer.consume_qualified_rows(
            conn,
            [
                {
                    "proxyWallet": "0x" + f"{idx:040x}"[-40:],
                    "transactionHash": f"0xlow-{idx}",
                    "timestamp": idx,
                    "usdcSize": 10,
                }
                for idx in range(50)
            ],
            now=1.0,
            counters=counters,
        )
        monkeypatch.setattr(rtds_module, "_candidate_wallets_snapshot", forbidden_snapshot)
        monkeypatch.setattr(rtds_module, "_observed_wallets_snapshot", forbidden_snapshot)

        for now in (2.0, 3.0, 4.0):
            assert buffer.due_rows(conn, now=now, counters=counters) == []

        assert counters.buffered_wallets == 50
    finally:
        conn.close()


def test_rtds_timer_drain_snapshots_only_due_wallets(monkeypatch, tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    counters = rtds_module._DiscoveryCounters()
    due_wallet = "0x" + "b" * 40
    low_wallet = "0x" + "c" * 40
    seen_candidate_queries = []
    seen_observed_queries = []
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=3600,
        max_wallets=10,
        persist_cooldown_seconds=300,
    )

    def candidate_snapshot(conn_arg, addresses):
        seen_candidate_queries.append(set(addresses))
        return {due_wallet: 1}

    def observed_snapshot(conn_arg, addresses):
        seen_observed_queries.append(set(addresses))
        return {}

    try:
        run_migrations(conn)
        buffer.consume_qualified_rows(
            conn,
            [
                {
                    "proxyWallet": due_wallet,
                    "transactionHash": "0xdue",
                    "timestamp": 1,
                    "usdcSize": 10,
                },
                {
                    "proxyWallet": low_wallet,
                    "transactionHash": "0xlow",
                    "timestamp": 1,
                    "usdcSize": 10,
                },
            ],
            now=1.0,
            counters=counters,
        )
        buffer.last_persisted_monotonic[due_wallet] = 1.0
        buffer._schedule_due(due_wallet, 301.0)
        monkeypatch.setattr(rtds_module, "_candidate_wallets_snapshot", candidate_snapshot)
        monkeypatch.setattr(rtds_module, "_observed_wallets_snapshot", observed_snapshot)

        rows = buffer.due_rows(conn, now=301.0, counters=counters)

        assert [row["proxyWallet"] for row in rows] == [due_wallet]
        assert seen_candidate_queries == [{due_wallet}]
        assert seen_observed_queries == [{due_wallet}]
    finally:
        conn.close()


def test_rtds_due_drain_budget_keeps_unprocessed_due_wallets(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    counters = rtds_module._DiscoveryCounters()
    wallets = ["0x" + digit * 40 for digit in ("a", "b", "c")]
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=3600,
        max_wallets=10,
        persist_cooldown_seconds=300,
    )
    try:
        run_migrations(conn)
        for wallet in wallets:
            upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual"), now=1)
        conn.commit()
        buffer.consume_qualified_rows(
            conn,
            [
                {
                    "proxyWallet": wallet,
                    "transactionHash": f"0xdue-budget-{idx}",
                    "timestamp": idx,
                    "usdcSize": 10,
                }
                for idx, wallet in enumerate(wallets, start=1)
            ],
            now=1.0,
            counters=counters,
        )
        for wallet in wallets:
            buffer.last_persisted_monotonic[wallet] = 1.0
            buffer._schedule_due(wallet, 301.0)
        monkeypatch.setattr(rtds_module.time, "time", lambda: 302.0)

        rows = buffer.due_rows(conn, now=301.0, counters=counters, max_wallets=2)

        drained_wallets = {row["proxyWallet"] for row in rows}
        assert len(drained_wallets) == 2
        assert set(buffer.next_due_at) == set(wallets) - drained_wallets
        assert all(item[1] in wallets for item in buffer.due_heap)
    finally:
        conn.close()


def test_rtds_due_drain_keeps_schedule_when_snapshot_lock_recovers(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    counters = rtds_module._DiscoveryCounters()
    wallet = "0x" + "f" * 40
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=3600,
        max_wallets=10,
        persist_cooldown_seconds=300,
    )
    original_candidate_snapshot = rtds_module._candidate_wallets_snapshot
    calls = {"candidate_snapshot": 0}

    def flaky_candidate_snapshot(conn_arg, addresses):
        calls["candidate_snapshot"] += 1
        if calls["candidate_snapshot"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return original_candidate_snapshot(conn_arg, addresses)

    try:
        run_migrations(conn)
        buffer.consume_qualified_rows(
            conn,
            [
                {
                    "proxyWallet": wallet,
                    "transactionHash": "0xdue-lock",
                    "timestamp": 1,
                    "usdcSize": 10,
                }
            ],
            now=1.0,
            counters=counters,
        )
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual"), now=1)
        conn.commit()
        buffer.last_persisted_monotonic[wallet] = 1.0
        buffer._schedule_due(wallet, 301.0)
        monkeypatch.setattr(rtds_module, "_candidate_wallets_snapshot", flaky_candidate_snapshot)
        monkeypatch.setattr(rtds_module.time, "monotonic", lambda: 301.0)

        try:
            rtds_module._flush_due_buffer(
                conn,
                buffer,
                counters=counters,
                min_trade_usdc=10,
                max_candidates=10,
            )
        except sqlite3.OperationalError as exc:
            assert "locked" in str(exc)
        else:
            raise AssertionError("expected snapshot lock")

        assert buffer.next_due_at == {wallet: 301.0}
        assert any(item == (301.0, wallet) for item in buffer.due_heap)

        rows_written = rtds_module._flush_due_buffer(
            conn,
            buffer,
            counters=counters,
            min_trade_usdc=10,
            max_candidates=10,
        )

        assert rows_written == 2
        assert buffer.wallets == {}
        assert buffer.next_due_at == {}
        assert conn.execute(
            "SELECT 1 FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone() is not None
    finally:
        conn.close()


def test_rtds_due_drain_reschedules_after_write_lock_recovery(
    monkeypatch,
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    counters = rtds_module._DiscoveryCounters()
    wallet = "0x" + "f" * 39 + "1"
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=3600,
        max_wallets=10,
        persist_cooldown_seconds=300,
    )
    original_flush = rtds_module._flush_batch
    calls = {"flush": 0}

    def flaky_flush(conn_arg, rows, *, min_trade_usdc, max_candidates):
        calls["flush"] += 1
        if calls["flush"] <= len(rtds_module.RTDS_SQLITE_LOCK_RETRY_DELAYS) + 1:
            raise sqlite3.OperationalError("database is locked")
        return original_flush(
            conn_arg,
            rows,
            min_trade_usdc=min_trade_usdc,
            max_candidates=max_candidates,
        )

    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="manual"), now=1)
        conn.commit()
        buffer.consume_qualified_rows(
            conn,
            [
                {
                    "proxyWallet": wallet,
                    "transactionHash": "0xdue-write-lock",
                    "timestamp": 1,
                    "usdcSize": 10,
                }
            ],
            now=1.0,
            counters=counters,
        )
        buffer.last_persisted_monotonic[wallet] = 1.0
        buffer._schedule_due(wallet, 301.0)
        monkeypatch.setattr(rtds_module, "_flush_batch", flaky_flush)
        monkeypatch.setattr(rtds_module.time, "monotonic", lambda: 301.0)
        monkeypatch.setattr(rtds_module.time, "sleep", lambda _seconds: None)

        try:
            rtds_module._flush_due_buffer(
                conn,
                buffer,
                counters=counters,
                min_trade_usdc=10,
                max_candidates=10,
            )
        except sqlite3.OperationalError as exc:
            assert "locked" in str(exc)
        else:
            raise AssertionError("expected sqlite lock")

        assert buffer.next_due_at == {wallet: 301.0}

        rows_written = rtds_module._flush_due_buffer(
            conn,
            buffer,
            counters=counters,
            min_trade_usdc=10,
            max_candidates=10,
        )

        assert rows_written == 2
        assert calls["flush"] == len(rtds_module.RTDS_SQLITE_LOCK_RETRY_DELAYS) + 2
        assert buffer.wallets == {}
        assert buffer.next_due_at == {}
    finally:
        monkeypatch.setattr(rtds_module, "_flush_batch", original_flush)
        conn.close()


def test_rtds_cooldown_prune_does_not_drop_unexpired_over_max_entries():
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=60,
        max_wallets=2,
        persist_cooldown_seconds=300,
    )
    wallets = ["0x" + digit * 40 for digit in ("1", "2", "3")]
    buffer.last_persisted_monotonic = {
        wallets[0]: 1.0,
        wallets[1]: 2.0,
        wallets[2]: 3.0,
    }

    buffer._prune_persisted(now=100.0)

    assert buffer.last_persisted_monotonic == {
        wallets[0]: 1.0,
        wallets[1]: 2.0,
        wallets[2]: 3.0,
    }


def test_rtds_capacity_protects_due_candidate_from_unknown_replacement(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    counters = rtds_module._DiscoveryCounters()
    due_wallet = "0x" + "d" * 40
    unknown_wallet = "0x" + "e" * 40
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=3600,
        max_wallets=1,
        persist_cooldown_seconds=300,
    )
    try:
        run_migrations(conn)
        buffer.consume_qualified_rows(
            conn,
            [
                {
                    "proxyWallet": due_wallet,
                    "transactionHash": "0xdue",
                    "timestamp": 1,
                    "usdcSize": 10,
                }
            ],
            now=1.0,
            counters=counters,
        )
        buffer.last_persisted_monotonic[due_wallet] = 1.0
        buffer._schedule_due(due_wallet, 301.0)

        rows = buffer.consume_qualified_rows(
            conn,
            [
                {
                    "proxyWallet": unknown_wallet,
                    "transactionHash": "0xunknown",
                    "timestamp": 2,
                    "usdcSize": 10,
                }
            ],
            now=2.0,
            counters=counters,
        )

        assert rows == []
        assert set(buffer.wallets) == {due_wallet}
        assert buffer.next_due_at == {due_wallet: 301.0}
        assert counters.backpressure == 1
    finally:
        conn.close()


def test_rtds_l0_buffer_ttl_and_capacity_eviction(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    counters = rtds_module._DiscoveryCounters()
    buffer = rtds_module._RTDSL0QualificationBuffer(
        ttl_seconds=1,
        max_wallets=2,
        persist_cooldown_seconds=300,
    )
    try:
        run_migrations(conn)
        buffer.consume_qualified_rows(
            conn,
            [
                {"proxyWallet": "0x" + "3" * 40, "transactionHash": "0x1", "timestamp": 1, "usdcSize": 20},
                {"proxyWallet": "0x" + "4" * 40, "transactionHash": "0x2", "timestamp": 2, "usdcSize": 20},
                {"proxyWallet": "0x" + "5" * 40, "transactionHash": "0x3", "timestamp": 3, "usdcSize": 20},
            ],
            now=10.0,
            counters=counters,
        )
        assert counters.evicted == 1
        assert counters.buffered_wallets == 2

        buffer.consume_qualified_rows(conn, [], now=12.0, counters=counters)

        assert counters.evicted == 3
        assert counters.buffered_wallets == 0
    finally:
        conn.close()


def test_rtds_does_not_read_stage_or_persist_observer_activity(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    ws = FakeWebSocket([_rtds_message(wallet, "0xexisting", size=1200)])
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                first_seen_at, updated_at
            ) VALUES (?, 'legacy', '', '', '', 'active', 1, 1)
            """,
            (wallet,),
        )
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        assert summary.trades_selected == 1
        assert not {
            row["name"]
            for row in conn.execute("PRAGMA table_info(candidate_wallets)")
        }.intersection({"candidate_stage"})
        assert not _table_exists(conn, "wallet_activity")
    finally:
        conn.close()


def test_run_rtds_activity_discovery_filters_small_realtime_trades(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    ws = FakeWebSocket([_rtds_message(wallet, "0xsmall", size=100)])
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        assert summary.trades_seen == 1
        assert summary.trades_selected == 0
        assert summary.batches_flushed == 0
        assert conn.execute(
            "SELECT 1 FROM observed_wallets WHERE wallet = ?",
            (wallet,),
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone() is None
        assert not _table_exists(conn, "wallet_activity")
    finally:
        conn.close()


def test_rtds_reconnects_when_control_frames_mask_idle_stream(monkeypatch, tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    clock = {"now": 100.0}
    idle_ws = ClockedWebSocket(["PONG", "PONG", "PONG"], clock)
    live_ws = ClockedWebSocket([_rtds_message(wallet, "0xafter-idle", size=1200)], clock)
    sockets = iter((idle_ws, live_ws))
    monkeypatch.setattr(rtds_module.time, "monotonic", lambda: clock["now"])
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            batch_size=1,
            ping_interval=5,
            max_idle_seconds=3,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: next(sockets),
        )

        assert summary.connections_attempted == 2
        assert summary.connections_succeeded == 2
        assert summary.reconnects == 1
        assert summary.messages_seen == 1
        assert summary.trades_selected == 1
        assert summary.status == "partial"
        assert "rtds stream idle for 3.0s" in summary.error
        assert conn.execute(
            "SELECT 1 FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone() is None
    finally:
        conn.close()


def test_rtds_data_messages_reset_idle_timer(monkeypatch, tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    clock = {"now": 100.0}
    ws = ClockedWebSocket(
        [
            '{"topic":"status","type":"subscribed"}',
            "PONG",
            _rtds_message(wallet, "0xafter-progress", size=1200),
        ],
        clock,
        step=2.0,
    )
    monkeypatch.setattr(rtds_module.time, "monotonic", lambda: clock["now"])
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            batch_size=1,
            ping_interval=5,
            max_idle_seconds=3,
            max_messages=2,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        assert summary.connections_attempted == 1
        assert summary.reconnects == 0
        assert summary.messages_seen == 2
        assert summary.trades_selected == 1
        assert summary.status == "ok"
    finally:
        conn.close()


def test_rtds_idle_reconnect_can_be_disabled():
    assert rtds_module._rtds_stream_idle_error(
        last_message_at=10.0,
        now=10_000.0,
        max_idle_seconds=0,
    ) is None


def test_rtds_flushes_pending_rows_before_idle_reconnect(monkeypatch, tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    first_wallet = "0x" + "7" * 40
    second_wallet = "0x" + "8" * 40
    clock = {"now": 100.0}
    idle_ws = ClockedWebSocket(
        [
            _rtds_message(first_wallet, "0xbefore-idle", size=1200),
            "PONG",
            "PONG",
            "PONG",
        ],
        clock,
    )
    live_ws = ClockedWebSocket(
        [_rtds_message(second_wallet, "0xafter-reconnect", size=1200)],
        clock,
    )
    sockets = iter((idle_ws, live_ws))
    monkeypatch.setattr(rtds_module.time, "monotonic", lambda: clock["now"])
    try:
        run_migrations(conn)

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            batch_size=25,
            ping_interval=5,
            max_idle_seconds=3,
            max_messages=2,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: next(sockets),
        )

        assert summary.reconnects == 1
        assert summary.trades_selected == 2
        assert summary.batches_flushed == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM observed_wallets WHERE wallet IN (?, ?)",
            (first_wallet, second_wallet),
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_rtds_flush_retries_short_sqlite_locks(monkeypatch, tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    calls = {"flush": 0, "sleep": []}

    def flaky_flush(conn_arg, rows, *, min_trade_usdc, max_candidates):
        calls["flush"] += 1
        if calls["flush"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return ActivityDiscoverySummary(0, 0, len(rows), 1, 1, 1, 1, 1, "ok")

    monkeypatch.setattr(rtds_module, "_flush_batch", flaky_flush)
    monkeypatch.setattr(
        rtds_module.time,
        "sleep",
        lambda seconds: calls["sleep"].append(seconds),
    )
    try:
        result = rtds_module._flush_realtime_batch(
            conn,
            [{"proxyWallet": "0x" + "9" * 40}],
            min_trade_usdc=500,
            max_candidates=20,
        )

        assert calls["flush"] == 2
        assert calls["sleep"] == [0.25]
        assert result.candidates_inserted_or_updated == 1
    finally:
        conn.close()
