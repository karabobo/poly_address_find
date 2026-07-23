import json
import sqlite3

from pm_robot.orchestration import rtds_discovery as rtds_module
from pm_robot.orchestration.activity_discovery import ActivityDiscoverySummary
from pm_robot.orchestration.rtds_discovery import (
    rtds_trade_to_activity_row,
    run_rtds_activity_discovery,
)
from pm_robot.storage.db import connect, run_migrations


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
        assert summary.promoted_wallets == 1
        assert candidate["sources"] == "polymarket_rtds_activity"
        assert "realtime_trade_activity" in candidate["labels"]
        assert observed["recent_max_trade_usdc"] == 600
        assert observed["recent_trade_count"] == 1
        assert observed["promotion_reason"] == "observed_sample_volume_at_least_100_usdc"
        assert level["level"] == "l1"
        assert source is not None
        assert not _table_exists(conn, "wallet_activity")
        assert heartbeat is not None
        assert "messages=1" in heartbeat["error"]
        assert "selected=1" in heartbeat["error"]
        assert "promoted=1" in heartbeat["error"]
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
        assert summary.batches_flushed == 5
        assert heartbeat_count == 2
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
        ).fetchone() is not None
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
