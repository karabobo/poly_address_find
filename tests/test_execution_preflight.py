import json

from pm_robot.execution.preflight import (
    execution_preflight_status,
    paper_realtime_audit_status,
    parse_compose_rows,
    rtds_watch_audit_status,
)
from pm_robot.storage.db import connect, run_migrations


NOW = 1_800_000_000


def _db(tmp_path):
    conn = connect(tmp_path / "pm_robot.sqlite")
    run_migrations(conn)
    return conn


def _seed_candidate(conn, wallet="0xabc0000000000000000000000000000000000001", stage="paper_approved"):
    conn.execute(
        """
        INSERT INTO candidate_wallets(
            address, sources, labels, notes, links, status,
            candidate_stage, first_seen_at, updated_at
        ) VALUES (?, 'test', '', '', '', 'active', ?, ?, ?)
        """,
        (wallet, stage, NOW - 100, NOW),
    )
    conn.commit()
    return wallet


def _seed_buy(conn, wallet, ts=NOW - 20):
    conn.execute(
        """
        INSERT INTO wallet_activity(
            address, timestamp, market_slug, asset_id, outcome, type,
            side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
        ) VALUES (?, ?, 'market-1', 'asset-1', 'YES', 'TRADE',
                  'BUY', 0.55, 100, 55, ?, '{}', ?)
        """,
        (wallet, ts, "0xtx%s" % ts, ts + 2),
    )
    conn.commit()


def _seed_buy_with_source(conn, wallet, *, source, ts=NOW - 20, ingested_at=NOW - 10):
    conn.execute(
        """
        INSERT INTO wallet_activity(
            address, timestamp, market_slug, asset_id, outcome, type,
            side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
        ) VALUES (?, ?, 'market-1', 'asset-1', 'YES', 'TRADE',
                  'BUY', 0.55, 100, 55, ?, ?, ?)
        """,
        (
            wallet,
            ts,
            "0xtx-source-%s-%s" % (source, ts),
            json.dumps({"source": source}),
            ingested_at,
        ),
    )
    conn.commit()


def _seed_observer(conn, wallet, *, actionable, accepted=1, evaluated_at=NOW - 10):
    conn.execute(
        """
        INSERT INTO paper_signal_evaluations(
            signal_id, wallet, candidate_stage, validation_cohort, market_slug,
            asset_id, outcome, side, detected_at, signal_age_sec,
            max_actionable_signal_age_sec, leader_price,
            requested_stake_usd, best_ask, executable_price,
            fillable_stake_usd, quote_snapshot_at, quote_latency_ms,
            quote_source, accepted, actionable, actionability_reason,
            decision_reason, stake_usd, route,
            fee_usd, slippage_bps, leader_score, copy_event_count,
            hygiene_status, evaluated_at, raw_json
        ) VALUES (
            ?, ?, 'paper_approved', 'validation', 'market-1',
            'asset-1', 'YES', 'BUY', ?, 20,
            300, 0.55, 40, 0.56, 0.56,
            40, ?, 20,
            'polymarket_clob_book', ?, ?, ?,
            'paper_clob_vwap', 40, 'paper_clob_vwap',
            0.1, 100, 83, 5,
            'clean', ?, '{}'
        )
        """,
        (
            "signal-%s-%s" % (wallet[-4:], actionable),
            wallet,
            NOW - 20,
            evaluated_at,
            accepted,
            1 if actionable else 0,
            "actionable_quote" if actionable else "slippage_too_high",
            evaluated_at,
        ),
    )
    conn.commit()


def test_execution_preflight_reports_no_paper_wallets(tmp_path):
    conn = _db(tmp_path)
    try:
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    assert payload["state"] == "no_paper_stage_wallets"
    assert payload["ready_to_start_execution"] is False
    assert payload["wallets"]["paper_stage_wallets"] == 0


def test_execution_preflight_waits_for_fresh_buy_on_paper_wallet(tmp_path):
    conn = _db(tmp_path)
    try:
        _seed_candidate(conn)
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    assert payload["state"] == "waiting_fresh_buy_signal"
    assert payload["recent_paper_stage_buy"]["events"] == 0
    assert "空转" in payload["recommended_action"]
    assert payload["paper_realtime_coverage"]["state"] == "no_paper_buy_24h"


def test_execution_preflight_waits_for_observer_after_recent_buy(tmp_path):
    conn = _db(tmp_path)
    try:
        wallet = _seed_candidate(conn)
        _seed_buy(conn, wallet)
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    assert payload["state"] == "recent_buy_waiting_quote_evaluation"
    assert payload["recent_paper_stage_buy"]["events"] == 1
    assert payload["observer"]["evaluations"] == 0
    assert payload["paper_realtime_coverage"]["state"] == "timely_non_rtds_buy_seen"
    assert payload["paper_realtime_coverage"]["timely_buy_events"] == 1


def test_execution_preflight_requires_actionable_observer_signal(tmp_path):
    conn = _db(tmp_path)
    try:
        wallet = _seed_candidate(conn)
        _seed_buy(conn, wallet)
        _seed_observer(conn, wallet, actionable=False)
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    assert payload["state"] == "recent_buy_not_actionable"
    assert payload["ready_to_start_execution"] is False
    assert payload["observer"]["evaluations"] == 1
    assert payload["observer"]["actionable"] == 0


def test_execution_preflight_is_ready_only_with_actionable_signal(tmp_path):
    conn = _db(tmp_path)
    try:
        wallet = _seed_candidate(conn)
        _seed_buy(conn, wallet)
        _seed_observer(conn, wallet, actionable=True)
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    assert payload["state"] == "ready_to_start_execution"
    assert payload["ready_to_start_execution"] is True
    assert payload["observer"]["actionable"] == 1


def test_execution_preflight_does_not_count_legacy_orders_as_current_paper_orders(tmp_path):
    conn = _db(tmp_path)
    try:
        _seed_candidate(conn)
        conn.execute(
            """
            INSERT INTO paper_orders(
                order_id, signal_id, wallet, market_slug, asset_id, outcome,
                side, price, stake_usd, route, accepted, reason, created_at
            ) VALUES ('order-1', 'signal-1', '0xdef0000000000000000000000000000000000002',
                      'market-legacy', 'asset-legacy', 'YES', 'BUY',
                      0.5, 20, 'legacy', 1, 'accepted', ?)
            """,
            (NOW - 10,),
        )
        conn.commit()
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    assert payload["paper_orders"]["orders"] == 1
    assert payload["paper_orders"]["paper_stage_orders"] == 0
    assert payload["wallets"]["paper_stage_wallets"] == 1


def test_execution_preflight_reports_running_execution_profile(tmp_path):
    conn = _db(tmp_path)
    try:
        _seed_candidate(conn)
        rows = parse_compose_rows(
            json.dumps([
                {"Service": "paper-runner-loop", "State": "running"},
                {"Service": "web", "State": "running"},
            ])
        )
        payload = execution_preflight_status(conn, now=NOW, compose_rows=rows)
    finally:
        conn.close()

    assert payload["state"] == "execution_already_running"
    assert payload["execution_profile"]["running_services"] == ["paper-runner-loop"]


def test_execution_preflight_realtime_coverage_reports_rtds_hits(tmp_path):
    conn = _db(tmp_path)
    try:
        wallet = _seed_candidate(conn)
        _seed_buy_with_source(conn, wallet, source="polymarket_rtds_activity")
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    coverage = payload["paper_realtime_coverage"]
    assert coverage["state"] == "rtds_current_buy_seen"
    assert coverage["current_rtds_buy_events"] == 1
    assert coverage["rtds_buy_events_24h"] == 1
    assert coverage["timely_buy_events"] == 1


def test_execution_preflight_realtime_coverage_reports_delayed_poll_buy(tmp_path):
    conn = _db(tmp_path)
    try:
        wallet = _seed_candidate(conn)
        _seed_buy_with_source(
            conn,
            wallet,
            source="paper_wallet_activity",
            ts=NOW - 20,
            ingested_at=NOW + 400,
        )
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    coverage = payload["paper_realtime_coverage"]
    assert coverage["state"] == "current_buy_delayed_ingest"
    assert coverage["current_buy_events"] == 1
    assert coverage["timely_buy_events"] == 0
    assert coverage["delayed_current_buy_events"] == 1
    assert coverage["timely_buy_events_24h"] == 0
    assert coverage["delayed_buy_events_24h"] == 1


def test_execution_preflight_realtime_coverage_reports_poll_only_history(tmp_path):
    conn = _db(tmp_path)
    try:
        wallet = _seed_candidate(conn)
        _seed_buy_with_source(
            conn,
            wallet,
            source="paper_wallet_activity",
            ts=NOW - 3_600,
            ingested_at=NOW - 200,
        )
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    coverage = payload["paper_realtime_coverage"]
    assert coverage["state"] == "paper_buy_delayed_without_rtds"
    assert coverage["buy_events_24h"] == 1
    assert coverage["timely_buy_events_24h"] == 0
    assert coverage["delayed_buy_events_24h"] == 1
    assert coverage["rtds_buy_events_24h"] == 0


def test_paper_realtime_audit_explains_each_paper_wallet_blocker(tmp_path):
    conn = _db(tmp_path)
    try:
        rtds_wallet = _seed_candidate(conn, wallet="0xaaa0000000000000000000000000000000000001")
        poll_wallet = _seed_candidate(conn, wallet="0xbbb0000000000000000000000000000000000002")
        quiet_wallet = _seed_candidate(conn, wallet="0xccc0000000000000000000000000000000000003")
        _seed_buy_with_source(conn, rtds_wallet, source="polymarket_rtds_activity")
        _seed_buy_with_source(
            conn,
            poll_wallet,
            source="paper_wallet_activity",
            ts=NOW - 3_600,
            ingested_at=NOW - 200,
        )
        payload = paper_realtime_audit_status(conn, now=NOW)
    finally:
        conn.close()

    by_wallet = {row["address"]: row for row in payload["wallets"]}
    assert payload["schema_version"] == "paper_realtime_audit_v1"
    assert payload["wallet_count"] == 3
    assert by_wallet[rtds_wallet]["realtime_blocker"] == "rtds_buy_waiting_observer"
    assert by_wallet[rtds_wallet]["current_rtds_buy_events"] == 1
    assert by_wallet[poll_wallet]["realtime_blocker"] == "paper_buy_delayed_without_rtds"
    assert by_wallet[poll_wallet]["buy_events_24h"] == 1
    assert by_wallet[poll_wallet]["timely_buy_events_24h"] == 0
    assert by_wallet[poll_wallet]["delayed_buy_events_24h"] == 1
    assert by_wallet[poll_wallet]["avg_buy_ingest_lag_sec"] == 3400.0
    assert by_wallet[poll_wallet]["current_buy_events"] == 0
    assert by_wallet[quiet_wallet]["realtime_blocker"] == "no_buy_24h"
    blockers = {row["blocker"]: row["count"] for row in payload["blocker_counts"]}
    assert blockers["rtds_buy_waiting_observer"] == 1
    assert blockers["paper_buy_delayed_without_rtds"] == 1
    assert blockers["no_buy_24h"] == 1


def test_paper_realtime_audit_flags_actionable_signal_as_ready(tmp_path):
    conn = _db(tmp_path)
    try:
        wallet = _seed_candidate(conn)
        _seed_buy_with_source(conn, wallet, source="polymarket_rtds_activity")
        _seed_observer(conn, wallet, actionable=True)
        payload = paper_realtime_audit_status(conn, now=NOW)
    finally:
        conn.close()

    assert payload["wallets"][0]["realtime_blocker"] == "ready_actionable_signal"
    assert payload["wallets"][0]["observer_actionable"] == 1


def test_execution_preflight_structures_rtds_runtime_diagnostics(tmp_path):
    conn = _db(tmp_path)
    try:
        _seed_candidate(conn)
        conn.execute(
            """
            INSERT INTO ingest_runs(
                ingest_type, started_at, finished_at, status,
                wallets_attempted, wallets_succeeded, rows_written, error
            ) VALUES (
                'loop_rtds_discovery', ?, ?, 'ok',
                0, 0, 4,
                'messages=8040 trades=8040 selected=22 batches=15 paper_wallets=0 paper_events=0 paper_rows=8040 paper_wallet_rows=8040 paper_matches=0 paper_eligible=2 paper_wallet_keys=proxyWallet:8040'
            )
            """,
            (NOW - 10, NOW - 5),
        )
        conn.commit()
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    rtds = payload["rtds_runtime_diagnostics"]
    assert rtds["state"] == "no_paper_wallet_match"
    assert rtds["paper_rows"] == 8040
    assert rtds["paper_wallet_rows"] == 8040
    assert rtds["paper_matches"] == 0
    assert rtds["paper_eligible"] == 2
    assert rtds["paper_wallet_keys"] == "proxyWallet:8040"
    assert rtds["heartbeat_fresh"] is True
    assert rtds["heartbeat_age_sec"] == 5
    assert rtds["stream_state"] == "insufficient_samples"


def test_execution_preflight_reports_rtds_recent_progress(tmp_path):
    conn = _db(tmp_path)
    try:
        _seed_candidate(conn)
        for finished_at, messages, selected in (
            (NOW - 90, 1000, 10),
            (NOW - 10, 2200, 42),
        ):
            conn.execute(
                """
                INSERT INTO ingest_runs(
                    ingest_type, started_at, finished_at, status,
                    wallets_attempted, wallets_succeeded, rows_written, error
                ) VALUES (
                    'loop_rtds_discovery', ?, ?, 'ok',
                    0, 0, 4,
                    ?
                )
                """,
                (
                    finished_at - 1,
                    finished_at,
                    (
                        f"messages={messages} trades={messages} selected={selected} batches=2 "
                        "paper_wallets=0 paper_events=0 paper_rows=100 paper_wallet_rows=100 "
                        "paper_matches=0 paper_eligible=1 paper_wallet_keys=proxyWallet:100 "
                        "watch_wallets=0 watch_events=0 watch_matches=0 watch_eligible=1"
                    ),
                ),
            )
        conn.commit()
        payload = execution_preflight_status(conn, now=NOW)
    finally:
        conn.close()

    rtds = payload["rtds_runtime_diagnostics"]
    assert rtds["stream_state"] == "stream_progressing"
    assert rtds["progress_samples"] == 2
    assert rtds["message_delta"] == 1200
    assert rtds["selected_delta"] == 32


def test_rtds_watch_audit_lists_near_paper_wallets_and_hits(tmp_path):
    conn = _db(tmp_path)
    wallet = "0xddd0000000000000000000000000000000000004"
    try:
        _seed_candidate(conn, wallet=wallet, stage="needs_manual_review")
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 69.48, 'needs_manual_review',
                      'validated_copy_stream_below_paper_score',
                      '{}', '{}', 'test', ?)
            """,
            (wallet, NOW),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(
                address, copy_event_count, copy_market_count, copy_stream_roi,
                hygiene_status, net_pnl_usdc, total_volume_usdc,
                extra_json, updated_at
            ) VALUES (?, 31, 1, 0.289, 'clean', 225.42, 2462.47, '{}', ?)
            """,
            (wallet, NOW),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l2_medium', 'summary_ready', 701,
                      0.8, 20, 'deep_done', 'score_wallet',
                      0, 701, 284, 701, ?)
            """,
            (wallet, NOW),
        )
        _seed_buy_with_source(
            conn,
            wallet,
            source="polymarket_rtds_watch_activity",
            ts=NOW - 20,
            ingested_at=NOW - 10,
        )
        payload = rtds_watch_audit_status(conn, now=NOW)
    finally:
        conn.close()

    assert payload["schema_version"] == "rtds_watch_audit_v1"
    assert payload["wallet_count"] == 1
    row = payload["wallets"][0]
    assert row["address"] == wallet
    assert row["leader_score"] == 69.48
    assert row["watch_state"] == "current_watch_hit"
    assert row["watch_events_24h"] == 1
    assert row["current_watch_events"] == 1
    assert row["copy_market_count"] == 1
    assert payload["state_counts"] == [{"state": "current_watch_hit", "count": 1}]
