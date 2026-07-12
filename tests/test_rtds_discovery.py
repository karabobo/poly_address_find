import sqlite3

from pm_robot.orchestration import rtds_discovery as rtds_module
from pm_robot.orchestration.activity_discovery import ActivityDiscoverySummary
from pm_robot.orchestration.rtds_discovery import (
    rtds_trade_to_activity_row,
    run_rtds_activity_discovery,
)
from pm_robot.pipeline_terms import COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import get_wallet_features


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


def _rtds_message(wallet: str, tx: str, *, size: float, price: float = 0.5, market: str = "market-1") -> str:
    return (
        "{"
        '"topic":"activity",'
        '"type":"trades",'
        '"timestamp":1000000,'
        '"payload":{'
        f'"proxyWallet":"{wallet}",'
        f'"transactionHash":"{tx}",'
        f'"slug":"{market}",'
        '"asset":"asset-1",'
        '"outcome":"YES",'
        '"side":"BUY",'
        f'"price":{price},'
        f'"size":{size}'
        "}"
        "}"
    )


def test_rtds_trade_to_activity_row_normalizes_payload_usdc_size():
    wallet = "0x" + "1" * 40
    row = rtds_trade_to_activity_row(
        {
            "topic": "activity",
            "type": "trades",
            "timestamp": 1_000_000,
            "payload": {
                "proxyWallet": wallet,
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


def test_run_rtds_activity_discovery_writes_realtime_candidate(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "2" * 40
    ws = FakeWebSocket(
        [
            "PONG",
            _rtds_message(wallet, "0x1", size=1200, price=0.5),
        ]
    )
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

        candidate = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()
        observed = conn.execute("SELECT * FROM observed_wallets WHERE wallet = ?", (wallet,)).fetchone()
        source = conn.execute(
            "SELECT * FROM candidate_source_events WHERE address = ? AND source = 'polymarket_rtds_activity'",
            (wallet,),
        ).fetchone()
        features = get_wallet_features(conn)[wallet]
        heartbeat = conn.execute(
            """
            SELECT * FROM ingest_runs
            WHERE ingest_type = 'loop_rtds_discovery'
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()

        assert summary.status == "ok"
        assert summary.connections_succeeded == 1
        assert summary.messages_seen == 1
        assert summary.trades_seen == 1
        assert summary.trades_selected == 1
        assert summary.batches_flushed == 1
        assert summary.paper_activity_events_written == 0
        assert candidate["sources"] == "polymarket_rtds_activity"
        assert "realtime_trade_activity" in candidate["labels"]
        assert observed["recent_max_trade_usdc"] == 600
        assert observed["promotion_reason"] == "single_trade_usdc>=100"
        assert source is not None
        assert features.recent_30d_volume_usdc == 600
        assert heartbeat is not None
        assert "messages=1" in heartbeat["error"]
        assert "trades=1" in heartbeat["error"]
        assert "selected=1" in heartbeat["error"]
        assert "paper_events=0" in heartbeat["error"]
        assert any("activity" in item for item in ws.sent)
    finally:
        conn.close()


def test_rtds_reconnects_when_heartbeat_frames_mask_an_idle_stream(monkeypatch, tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "7" * 40
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


def test_rtds_data_messages_reset_the_idle_timer(monkeypatch, tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "8" * 40
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
    first_wallet = "0x" + "9" * 40
    second_wallet = "0x" + "a" * 40
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
        assert summary.messages_seen == 2
        assert summary.trades_selected == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM candidate_wallets WHERE address IN (?, ?)",
            (first_wallet, second_wallet),
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_run_rtds_activity_discovery_persists_paper_stage_activity(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    ws = FakeWebSocket([_rtds_message(wallet, "0xpaper", size=1200, price=0.5, market="paper-market")])
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'paper_approved', 1, 1)
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

        activity = conn.execute("SELECT * FROM wallet_activity WHERE address = ?", (wallet,)).fetchone()
        candidate = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()

        assert summary.status == "ok"
        assert summary.paper_activity_wallets == 1
        assert summary.paper_activity_events_written == 1
        assert summary.paper_rows_seen == 1
        assert summary.paper_rows_with_wallet == 1
        assert summary.paper_activity_matches == 1
        assert summary.paper_eligible_wallets == 1
        assert summary.paper_wallet_field_counts == {"proxyWallet": 1}
        assert activity is not None
        assert activity["market_slug"] == "paper-market"
        assert activity["asset_id"] == "asset-1"
        assert activity["outcome"] == "YES"
        assert activity["transaction_hash"] == "0xpaper"
        assert candidate["candidate_stage"] == "paper_approved"
    finally:
        conn.close()


def test_rtds_persists_small_paper_stage_activity_without_discovery_promotion(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    ws = FakeWebSocket([_rtds_message(wallet, "0xsmallpaper", size=100, price=0.5, market="small-paper")])
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'paper_approved', 1, 1)
            """,
            (wallet,),
        )
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            paper_min_trade_usdc=0,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        activity = conn.execute("SELECT * FROM wallet_activity WHERE address = ?", (wallet,)).fetchone()
        source = conn.execute(
            "SELECT * FROM candidate_source_events WHERE address = ? AND source = 'polymarket_rtds_activity'",
            (wallet,),
        ).fetchone()

        assert summary.status == "ok"
        assert summary.trades_seen == 1
        assert summary.trades_selected == 0
        assert summary.candidates_inserted_or_updated == 0
        assert summary.paper_activity_wallets == 1
        assert summary.paper_activity_events_written == 1
        assert summary.paper_rows_seen == 1
        assert summary.paper_rows_with_wallet == 1
        assert summary.paper_activity_matches == 1
        assert activity is not None
        assert activity["transaction_hash"] == "0xsmallpaper"
        assert source is None
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

    def paper_activity(conn_arg, rows, *, ingested_at):
        return {"wallets": {"0x" + "4" * 40}, "events_written": 1}

    def watch_activity(conn_arg, rows, *, ingested_at, min_score):
        return {"wallets": set(), "events_written": 0, "rows_matched": 0, "eligible_wallets": 0}

    monkeypatch.setattr(rtds_module, "_flush_batch", flaky_flush)
    monkeypatch.setattr(rtds_module, "_persist_paper_stage_activity", paper_activity)
    monkeypatch.setattr(rtds_module, "_persist_watch_scope_activity", watch_activity)
    monkeypatch.setattr(rtds_module.time, "sleep", lambda seconds: calls["sleep"].append(seconds))

    try:
        result, paper_result, watch_result = rtds_module._flush_realtime_batch(
            conn,
            [{"proxyWallet": "0x" + "4" * 40}],
            [{"proxyWallet": "0x" + "4" * 40}],
            min_trade_usdc=500,
            max_candidates=20,
            watch_min_score=65,
        )

        assert calls["flush"] == 2
        assert calls["sleep"] == [0.25]
        assert result.candidates_inserted_or_updated == 1
        assert paper_result["events_written"] == 1
        assert watch_result["events_written"] == 0
    finally:
        conn.close()


def test_run_rtds_activity_discovery_filters_small_realtime_trades(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "3" * 40
    ws = FakeWebSocket([_rtds_message(wallet, "0x1", size=100, price=0.5)])
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

        candidate = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()
        observed = conn.execute("SELECT * FROM observed_wallets WHERE wallet = ?", (wallet,)).fetchone()

        assert summary.status == "ok"
        assert summary.trades_seen == 1
        assert summary.trades_selected == 0
        assert summary.batches_flushed == 0
        assert candidate is None
        assert observed is None
    finally:
        conn.close()


def test_rtds_persists_near_paper_watch_activity_without_stage_promotion(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    ws = FakeWebSocket([_rtds_message(wallet, "0xwatch", size=100, price=0.5, market="watch-market")])
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'needs_manual_review', 1, 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 69.5, 'needs_manual_review', 'near paper',
                      '{}', '{}', 'test', 1)
            """,
            (wallet,),
        )
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            paper_min_trade_usdc=0,
            watch_min_score=65,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        activity = conn.execute("SELECT * FROM wallet_activity WHERE address = ?", (wallet,)).fetchone()
        candidate = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (wallet,)).fetchone()

        assert summary.trades_selected == 0
        assert summary.watch_activity_wallets == 1
        assert summary.watch_activity_events_written == 1
        assert summary.watch_activity_matches == 1
        assert summary.watch_eligible_wallets == 1
        assert summary.paper_activity_wallets == 0
        assert summary.paper_activity_events_written == 0
        assert activity is not None
        assert activity["market_slug"] == "watch-market"
        assert "polymarket_rtds_watch_activity" in activity["raw_json"]
        assert candidate["candidate_stage"] == "needs_manual_review"
    finally:
        conn.close()


def test_rtds_persists_low_score_deep_copyability_near_miss_activity(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    ws = FakeWebSocket([_rtds_message(wallet, "0xnear-miss", size=100, price=0.5)])
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'needs_manual_review', 1, 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 50, 'needs_manual_review', 'watchlist_score',
                      '{}', '{}', 'test', 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10,
                      'deep_done', 'score_wallet', 0, 1000, 500, 20, 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(address, extra_json, updated_at)
            VALUES (?, '{"copy_candidate_event_count":12,"copy_candidate_market_count":3}', 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 10, 0, 'done',
                      NULL, 0, 1, 3, 0, '{"graph_scan_mode":"deep"}',
                      '{"graph_scan_mode":"deep"}', '', 1, 1, 1)
            """,
            (wallet,),
        )
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            paper_min_trade_usdc=0,
            watch_min_score=65,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        activity = conn.execute(
            "SELECT raw_json FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert summary.trades_selected == 0
        assert summary.watch_activity_wallets == 1
        assert summary.watch_activity_events_written == 1
        assert summary.watch_activity_matches == 1
        assert summary.watch_eligible_wallets == 1
        assert activity is not None
        assert "polymarket_rtds_watch_activity" in activity["raw_json"]
    finally:
        conn.close()


def test_rtds_watch_includes_rescannable_needs_data_near_miss(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "6" * 40
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'needs_data', 1, 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 44, 'needs_data', ?, '{}', '{}', 'test', 1)
            """,
            (wallet, COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10,
                      'deep_done', 'score_wallet', 0, 1000, 500, 20, 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(address, extra_json, updated_at)
            VALUES (?, '{"copy_candidate_event_count":12,"copy_candidate_market_count":5}', 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 10, 0, 'done',
                      NULL, 0, 1, 3, 0, '{"graph_scan_mode":"deep"}',
                      '{"graph_scan_mode":"deep"}', '', 1, 1, 1)
            """,
            (wallet,),
        )
        conn.commit()

        assert rtds_module._rtds_watch_wallets(conn, min_score=65) == {wallet}
    finally:
        conn.close()


def test_rtds_keeps_low_score_deep_rescan_wallet_in_watch_scope(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "4" * 40
    ws = FakeWebSocket([_rtds_message(wallet, "0xactive-rescan", size=100, price=0.5)])
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'needs_manual_review', 1, 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 50, 'needs_manual_review', 'watchlist_score',
                      '{}', '{}', 'test', 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, non_fast_trade_count,
                distinct_markets, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10,
                      'deep_done', 'score_wallet', 0, 1000, 500, 20, 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(address, extra_json, updated_at)
            VALUES (?, '{"copy_candidate_event_count":12,"copy_candidate_market_count":3}', 1)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 10, 0, 'queued',
                      NULL, 0, 0, 3, 0, '{"graph_scan_mode":"deep"}',
                      '{}', '', 1, 1, NULL)
            """,
            (wallet,),
        )
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            paper_min_trade_usdc=0,
            watch_min_score=65,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        assert summary.watch_activity_wallets == 1
        assert summary.watch_activity_events_written == 1
        assert summary.watch_eligible_wallets == 1
    finally:
        conn.close()


def test_rtds_paper_diagnostics_count_unmatched_wallet_fields(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    paper_wallet = "0x" + "8" * 40
    other_wallet = "0x" + "9" * 40
    ws = FakeWebSocket([_rtds_message(other_wallet, "0xother", size=100, price=0.5)])
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'paper_approved', 1, 1)
            """,
            (paper_wallet,),
        )
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            paper_min_trade_usdc=0,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )
        heartbeat = conn.execute(
            """
            SELECT * FROM ingest_runs
            WHERE ingest_type = 'loop_rtds_discovery'
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()

        assert summary.paper_activity_wallets == 0
        assert summary.paper_activity_events_written == 0
        assert summary.paper_rows_seen == 1
        assert summary.paper_rows_with_wallet == 1
        assert summary.paper_activity_matches == 0
        assert summary.paper_eligible_wallets == 1
        assert summary.paper_wallet_field_counts == {"proxyWallet": 1}
        assert heartbeat is not None
        assert "paper_rows=1" in heartbeat["error"]
        assert "paper_wallet_rows=1" in heartbeat["error"]
        assert "paper_matches=0" in heartbeat["error"]
        assert "paper_eligible=1" in heartbeat["error"]
        assert "paper_wallet_keys=proxyWallet:1" in heartbeat["error"]
    finally:
        conn.close()


def test_rtds_paper_diagnostics_count_missing_wallet_fields(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "a" * 40
    message = (
        "{"
        '"topic":"activity",'
        '"type":"trades",'
        '"timestamp":1000000,'
        '"payload":{'
        '"transactionHash":"0xmissingwallet",'
        '"slug":"market-1",'
        '"asset":"asset-1",'
        '"outcome":"YES",'
        '"side":"BUY",'
        '"price":0.5,'
        '"size":100'
        "}"
        "}"
    )
    ws = FakeWebSocket([message])
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'paper_approved', 1, 1)
            """,
            (wallet,),
        )
        conn.commit()

        summary = run_rtds_activity_discovery(
            conn,
            min_trade_usdc=500,
            paper_min_trade_usdc=0,
            batch_size=1,
            max_messages=1,
            reconnect_sleep=0,
            websocket_factory=lambda endpoint: ws,
        )

        assert summary.paper_rows_seen == 1
        assert summary.paper_rows_with_wallet == 0
        assert summary.paper_activity_matches == 0
        assert summary.paper_wallet_field_counts == {"none": 1}
    finally:
        conn.close()
