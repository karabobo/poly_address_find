import csv
import io
import json
from pathlib import Path
import time

import pm_robot.web as web_module
from pm_robot.config import RobotSettings
from pm_robot.storage.db import connect, run_migrations
from pm_robot.web import (
    _badge,
    _copyability_lane_panel,
    _localized_cell,
    _render_dashboard,
    _render_wallet_detail,
    _render_wallets,
    _paper_candidate_thresholds,
    _paper_pool_expansion_panel,
    _paper_pool_expansion_state,
    _format_cell,
    _runtime_build_info,
    _storage_maintenance_panel,
    _storage_maintenance_summary,
    _wallet_pipeline_diagnostic,
    dashboard_data,
    discovery_data,
    execution_preflight_data,
    paper_realtime_audit_data,
    paper_handoff_data,
    paper_handoff_csv,
    paper_observer_evaluation_data,
    paper_observer_preview_data,
    paper_pool_expansion_data,
    rtds_watch_audit_data,
    wallet_detail_data,
    wallet_table_rows,
)


def _settings(tmp_path):
    return RobotSettings(db_path=tmp_path / "pm_robot.sqlite", execution_mode="research")


def test_web_console_binds_before_starting_dashboard_prewarm(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    settings.db_path.touch()
    events = []

    class FakeServer:
        def __init__(self, address, handler):
            events.append(("bind", address, handler))

        def serve_forever(self):
            events.append(("serve",))

    monkeypatch.setattr(web_module, "ThreadingHTTPServer", FakeServer)
    monkeypatch.setattr(web_module, "_start_dashboard_cache_prewarm", lambda _settings: events.append(("prewarm",)))

    web_module.run_web_console(web_module.WebConsoleConfig(settings=settings, host="127.0.0.1", port=8787))

    assert [event[0] for event in events] == ["bind", "prewarm", "serve"]


def _insert_l3_evidence(conn, wallet: str, *, updated_at: int) -> None:
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, distinct_markets,
            non_fast_trade_count, updated_at
        ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done',
                  'score_wallet', 0, 1000, 20, 200, ?)
        """,
        (wallet, updated_at),
    )


def _seed(settings):
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        now = 1_800_000_000
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0xabc0000000000000000000000000000000000001",
                "polymarket_trades_global",
                "probe",
                "seeded for test",
                "",
                "active",
                "needs_manual_review",
                now - 100,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links,
                evidence_json, observed_at, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0xabc0000000000000000000000000000000000001",
                "polymarket_trades_global",
                "active",
                "probe",
                "",
                "",
                "{}",
                now - 90,
                now - 80,
            ),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(
                address, net_pnl_usdc, total_volume_usdc, leader_in_degree,
                copy_event_count, copy_market_count, containment_pct_median,
                copy_stream_roi, hygiene_status, primary_category,
                extra_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0xabc0000000000000000000000000000000000001",
                120.0,
                2500.0,
                2,
                16,
                3,
                0.51,
                0.0,
                "ok",
                "politics",
                json.dumps({"materialized": True}),
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0xabc0000000000000000000000000000000000001",
                55.5,
                "needs_manual_review",
                "thin but promising",
                "{}",
                "{}",
                "test",
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO wallet_activity(
                address, timestamp, market_slug, asset_id, outcome, type,
                side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0xabc0000000000000000000000000000000000001",
                now - 10,
                "test-market",
                "asset-1",
                "Yes",
                "TRADE",
                "BUY",
                0.62,
                10.0,
                6.2,
                "0xtx",
                "{}",
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO evidence_backfill_budget(
                wallet, source, priority, stage, target_depth, current_depth,
                next_attempt_at, evidence_json, created_at, updated_at
            ) VALUES (?, ?, 10, 'light_pending', 200, 1, 0, '{}', ?, ?)
            """,
            ("0xabc0000000000000000000000000000000000001", "test", now, now),
        )
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready,
                blockers_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0xabc0000000000000000000000000000000000001",
                3,
                1,
                1,
                2,
                0,
                1.0,
                40.0,
                8.0,
                0.2,
                9.0,
                0.225,
                0,
                "[]",
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_paper_threshold_policy_error_is_visible(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "pm_robot.sqlite",
        policy_path=tmp_path / "missing-policy.json",
        execution_mode="research",
    )

    thresholds = _paper_candidate_thresholds(settings)
    html = _paper_pool_expansion_panel(
        {
            "wallet_count": 0,
            "near_paper_count": 0,
            "copyability_needed_count": 0,
            "best_score": 0,
            "wallets": [],
            "scope": {"watch_min_score": 65, "paper_min_score": thresholds["min_score"]},
            "policy_loaded": thresholds["policy_loaded"],
            "policy_error": thresholds["policy_error"],
        }
    )

    assert thresholds["policy_loaded"] is False
    assert "FileNotFoundError" in thresholds["policy_error"]
    assert "评分策略加载失败" in html
    assert "故障回退值" in html


def test_dashboard_data_reads_research_summaries(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    data = dashboard_data(settings)

    assert data["total_candidates"] == 1
    assert data["activity_coverage"]["total_events"] == 1
    assert data["source_counts"][0]["name"] == "polymarket_trades_global"
    assert data["source_quality"][0]["source"] == "polymarket_trades_global"
    assert data["source_quality"][0]["wallets"] == 1
    assert data["source_quality"][0]["review_wallets"] == 1
    assert data["source_quality"][0]["max_score"] == 55.5
    assert data["paper_quality"]["wallets"] == 1
    assert len(data["runtime"]["source_fingerprint"]) == 12
    assert data["runtime"]["source_fingerprint"] == data["ops_health"]["runtime"]["source_fingerprint"]
    assert data["runtime"]["source_delivery"] in {"bind_mount", "image_source", "local_source"}
    assert data["runtime"]["source_root"]
    assert data["production_readiness"]["state"] == "needs_better_sources"
    assert data["production_readiness"]["paper_stage_wallets"] == 0
    assert data["production_readiness"]["needs_manual_review"] == 1
    assert data["production_readiness"]["max_manual_score"] == 55.5
    assert data["production_readiness"]["score_gap_to_paper"] == 14.5
    assert data["score_policy"]["state"] == "stale_low_scores_deferred"
    assert data["score_policy"]["latest_scores"] == 1
    assert data["score_policy"]["stale_policy_scores"] == 1
    assert data["score_policy"]["max_stale_score"] == 55.5
    assert data["production_readiness"]["manual_review_actions"][0]["blocker"] == "历史证据偏薄"
    assert data["execution_preflight"]["state"] == "no_paper_stage_wallets"
    assert data["execution_preflight"]["ready_to_start_execution"] is False
    assert data["execution_preflight"]["wallets"]["paper_stage_wallets"] == 0
    assert data["paper_realtime_audit"]["schema_version"] == "paper_realtime_audit_v1"
    assert data["paper_realtime_audit"]["wallet_count"] == 0
    assert data["rtds_watch_audit"]["schema_version"] == "rtds_watch_audit_v1"
    assert data["rtds_watch_audit"]["wallet_count"] == 0
    assert data["paper_pool_expansion"]["schema_version"] == "paper_pool_expansion_v1"
    assert data["paper_pool_expansion"]["wallet_count"] == 1
    assert data["paper_pool_expansion"]["wallets"][0]["address"] == "0xabc0000000000000000000000000000000000001"
    assert data["paper_pool_expansion"]["wallets"][0]["expansion_state"] == "needs_more_evidence"
    assert paper_pool_expansion_data(settings)["wallets"][0]["expansion_state"] == "needs_more_evidence"
    assert data["manual_review_actions"][0]["blocker"] == "历史证据偏薄"
    assert data["ops_health"]["storage"]["db_bytes"] > 0
    assert data["storage_maintenance"]["db_bytes"] > 0
    assert data["storage_maintenance"]["safe_command"] == "./pmrobot-nas.sh wal-truncate-window"
    assert data["storage_maintenance"]["idle_window_command"] == "./pmrobot-nas.sh wal-truncate-when-idle 7200 900 30"
    assert data["ops_health"]["address_quality"]["invalid_address_rows"] == 0
    assert data["ops_health"]["upstream_request_budget"]["active_cooldowns"] == 0
    assert data["top_review_candidates"][0]["address"] == "0xabc0000000000000000000000000000000000001"
    assert data["top_review_candidates"][0]["leader_score"] == 55.5
    assert data["top_review_candidates"][0]["blocker_label"] == "历史证据偏薄"
    assert data["top_review_blockers"][0]["blocker"] == "历史证据偏薄"
    assert data["top_review_blockers"][0]["count"] == 1


def test_paper_pool_expansion_uses_evidence_and_policy_thresholds():
    def classify(**overrides):
        row = {
            "leader_score": 69.5,
            "copy_signal_events": 8,
            "copy_signal_markets": 6,
            "activity_count": 800,
            "evidence_status": "summary_ready",
        }
        row.update(overrides)
        return _paper_pool_expansion_state(
            row,
            paper_min_score=70,
            watch_min_score=65,
            min_copy_events=5,
            min_copy_markets=5,
        )[0]

    assert classify(activity_count=199) == "needs_more_evidence"
    assert classify(evidence_status="queued") == "needs_more_evidence"
    assert classify(copy_signal_events=4) == "near_paper_waiting_copy_events"
    assert classify(copy_signal_markets=4) == "near_paper_waiting_copy_markets"
    assert classify() == "near_paper_score_gap"
    assert classify(leader_score=70) == "near_paper_manual_gate"
    assert classify(leader_score=55, copy_signal_events=4) == "watchlist_needs_copyability"
    assert classify(leader_score=55) == "watchlist_score_gap"
    assert classify(leader_score=45, copy_signal_markets=4) == "missing_copyability_signal"
    assert classify(leader_score=45) == "score_gap_large"


def test_dashboard_shows_paper_handoff_without_implying_auto_execution(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000002"
    now = 1_800_000_200
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'copyability', 'paper', '', '', 'active', 'paper_approved', ?, ?)
            """,
            (wallet, now - 100, now),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 83.11, 'paper_approved', 'score_and_validation_present', '{}', '{}', 'test', ?)
            """,
            (wallet, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 1, 'deep_done',
                      'score_wallet', 0, 1200, 51, 900, ?)
            """,
            (wallet, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(
                address, net_pnl_usdc, total_volume_usdc, copy_stream_roi,
                hygiene_status, extra_json, updated_at
            ) VALUES (?, 27212.03, 184882.82, 0.2073, 'ok', '{}', ?)
            """,
            (wallet, now),
        )
        conn.execute(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, 348, 348, 51, 0.2, 66, 3, ?, ?)
            """,
            (wallet, now - 10, now),
        )
        conn.execute(
            """
            INSERT INTO copy_leader_performance(
                leader_wallet, backtest_trade_count, copied_market_count,
                total_stake_usdc, gross_pnl_usdc, net_pnl_usdc,
                gross_roi, net_roi, win_rate, median_lag_seconds,
                last_backtest_trade_at, updated_at,
                edge_retention_pct, walk_forward_consistency_pct, max_drawdown_pct
            ) VALUES (?, 348, 51, 3480, 800, 721.42,
                      0.23, 0.2073, 0.59, 66, ?, ?, 93.25, 66.67, -0.12)
            """,
            (wallet, now - 5, now),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    html = _render_dashboard(settings)
    detail = wallet_detail_data(settings, wallet)
    detail_html = _render_wallet_detail(settings, wallet)

    handoff = data["paper_handoff"]
    handoff_api = paper_handoff_data(settings)
    assert handoff["research_only"] is True
    assert handoff["schema_version"] == "paper_handoff_v1"
    assert handoff["runtime_mode"] == "research"
    assert handoff["execution_boundary"] == "research_handoff_only"
    assert handoff["nas_paper_loop_enabled"] is False
    assert handoff["paper_loop_status"] == "not_started_in_nas_research_stack"
    assert handoff["candidate_count"] == 1
    assert handoff["visible_wallet_count"] == 1
    assert handoff["visible_research_ready"] == 1
    assert handoff["stage_counts"] == [{"stage": "paper_approved", "count": 1}]
    assert handoff["wallets"][0]["address"] == wallet
    assert handoff["wallets"][0]["handoff_state"] == "awaiting_actionable_signal"
    assert handoff["wallets"][0]["observer_evaluations"] == 0
    assert handoff["wallets"][0]["observer_actionable_signals"] == 0
    assert handoff["wallets"][0]["observer_quality_state"] == "no_observer_quality"
    assert handoff["wallets"][0]["observer_quality_evaluations"] == 0
    assert handoff["wallets"][0]["observer_quality_actionable_rate_pct"] == 0.0
    assert handoff["wallets"][0]["research_ready"] is True
    assert handoff["wallets"][0]["research_check_passed"] == 4
    assert handoff["wallets"][0]["research_check_total"] == 4
    assert handoff["wallets"][0]["research_check_summary"] == "研究证据完整"
    assert handoff["wallets"][0]["paper_execution_state"] == "not_started_on_nas"
    assert "runtime_research_only" in handoff["wallets"][0]["formal_blocker_list"]
    assert "missing_paper_wallet_quality" in handoff["wallets"][0]["formal_blocker_list"]
    assert "no_paper_orders" in handoff["wallets"][0]["formal_blocker_list"]
    assert handoff["wallets"][0]["formal_next_action"] == "当前 NAS 只做 research/scoring；正式化需要独立 paper/settle/publish 运行面。"
    assert [check["key"] for check in handoff["wallets"][0]["research_checks"]] == [
        "score_ready",
        "l3_summary",
        "hygiene_clean",
        "copyability_validated",
    ]
    assert handoff["wallets"][0]["next_action"] == "研究侧已批准；等待 paper observer 捕捉可及时跟的 BUY 信号。"
    assert handoff["state_counts"] == [{"state": "awaiting_actionable_signal", "count": 1}]
    assert data["production_readiness"]["state"] == "paper_candidates_waiting_actionable_signals"
    assert data["production_readiness"]["observer_actionable_signals"] == 0
    publish_gate = data["production_readiness"]["formal_publish_gate"]
    assert publish_gate["state"] == "research_mode_publish_disabled"
    assert publish_gate["active_published_leaders"] == 0
    assert publish_gate["current_formal_status"] == "当前正式钱包为 0"
    assert publish_gate["root_formal_blocker"] == "runtime_research_only"
    assert publish_gate["active_formal_wallets"] == []
    assert publish_gate["live_eligible_wallets"] == 0
    assert publish_gate["paper_stage_wallets"] == 1
    assert publish_gate["paper_stage_missing_quality"] == 1
    assert publish_gate["publish_loop_enabled"] is False
    assert publish_gate["paper_stage_gap_wallets"][0]["address"] == wallet
    assert publish_gate["paper_stage_gap_wallets"][0]["formal_next_action"] == "当前 NAS 只做 research/scoring；正式化需要独立 paper/settle/publish 运行面。"
    assert "runtime_research_only" in publish_gate["paper_stage_gap_wallets"][0]["formal_blocker_list"]
    publish_blockers = {row["blocker"]: row for row in publish_gate["formal_blocker_rows"]}
    assert publish_gate["top_formal_blocker"] == "missing_paper_wallet_quality"
    assert publish_blockers["runtime_research_only"]["count"] == 1
    assert publish_blockers["stage_not_live_eligible"]["count"] == 1
    assert publish_blockers["missing_paper_wallet_quality"]["count"] == 1
    assert publish_blockers["no_paper_orders"]["count"] == 1
    assert publish_blockers["publish_not_active"]["count"] == 1
    assert data["execution_preflight"]["state"] == "waiting_fresh_buy_signal"
    assert data["execution_preflight"]["ready_to_start_execution"] is False
    assert data["execution_preflight"]["wallets"]["paper_stage_wallets"] == 1
    assert data["execution_preflight"]["paper_orders"]["paper_stage_orders"] == 0
    assert execution_preflight_data(settings)["state"] == "waiting_fresh_buy_signal"
    assert data["paper_realtime_audit"]["wallet_count"] == 1
    assert data["paper_realtime_audit"]["wallets"][0]["address"] == wallet
    assert data["paper_realtime_audit"]["wallets"][0]["realtime_blocker"] == "no_buy_24h"
    assert paper_realtime_audit_data(settings)["wallets"][0]["realtime_blocker"] == "no_buy_24h"
    assert rtds_watch_audit_data(settings)["wallet_count"] == 0
    assert handoff_api["schema_version"] == "paper_handoff_v1"
    assert handoff_api["wallets"][0]["address"] == wallet
    assert handoff_api["wallets"][0]["paper_orders"] == 0
    assert handoff_api["wallets"][0]["publish_status"] == ""
    csv_rows = list(csv.DictReader(io.StringIO(paper_handoff_csv(settings))))
    assert csv_rows[0]["address"] == wallet
    assert csv_rows[0]["candidate_stage"] == "paper_approved"
    assert csv_rows[0]["handoff_state"] == "awaiting_actionable_signal"
    assert csv_rows[0]["observer_actionable_signals"] == "0"
    assert csv_rows[0]["observer_quality_state"] == "no_observer_quality"
    assert csv_rows[0]["observer_quality_evaluations"] == "0"
    assert csv_rows[0]["research_ready"] == "True"
    assert csv_rows[0]["paper_execution_state"] == "not_started_on_nas"
    assert "missing_paper_wallet_quality" in csv_rows[0]["formal_blockers"]
    assert csv_rows[0]["formal_next_action"] == "当前 NAS 只做 research/scoring；正式化需要独立 paper/settle/publish 运行面。"
    assert detail["paper_handoff"]["address"] == wallet
    assert detail["paper_handoff"]["research_ready"] is True
    assert detail["paper_handoff"]["research_check_summary"] == "研究证据完整"
    assert detail["paper_handoff"]["paper_execution_state"] == "not_started_on_nas"
    assert detail["paper_handoff"]["observer_actionable_signals"] == 0
    assert detail["paper_handoff"]["observer_quality_state"] == "no_observer_quality"
    assert "runtime_research_only" in detail["paper_handoff"]["formal_blocker_list"]
    assert "Paper 交接观察" in html
    assert "NAS research/scoring" in html
    assert "paper 标签不代表 NAS 已自动跟单" in html
    assert "正式发布门槛" in html
    assert "当前正式钱包" in html
    assert "Paper 到正式缺口" in html
    assert "Execution Preflight 执行前检查" in html
    assert "Paper 实时钱包审计" in html
    assert "Paper 钱包逐个卡点" in html
    assert "延迟补进" in html
    assert "paper-realtime-audit" in html
    assert "RTDS Watch 近 Paper 审计" in html
    assert "rtds-watch-audit" in html
    assert "Paper 候选扩池审计" in html
    assert "paper-pool-expansion" in html
    assert "RTDS Paper 匹配" in html
    assert "RTDS 流进度" in html
    assert "RTDS Watch 匹配" in html
    assert "rtds_runtime_state" in html
    assert "rtds_message_delta" in html
    assert "rtds_paper_matches" in html
    assert "rtds_watch_matches" in html
    assert "execution-preflight" in html
    assert "启动 execution 只会空转" in html
    assert "write_boundary" in html
    assert "正式阻塞分布" in html
    assert "当前正式钱包为 0" in html
    assert "missing_paper_wallet_quality" in html
    assert "正式阻塞" in detail_html
    assert "只读质量" in detail_html
    assert "NAS Paper Loop" in html
    assert "未启用" in html
    assert "not_started_in_nas_research_stack" in html
    assert "研究证据完整" in html
    assert "等待新信号" in html
    assert "actionable" in html
    assert "not_started_on_nas" in html
    assert "JSON 交接出口" in html
    assert "CSV 表格出口" in html
    assert "awaiting_actionable_signal" in html
    assert "Paper 交接审计" in detail_html
    assert "研究证据完整" in detail_html
    assert "not_started_on_nas" in detail_html
    assert "research/scoring 不自动下单" in detail_html


def test_paper_handoff_ignores_stale_actionable_observer_rows(tmp_path):
    settings = _settings(tmp_path)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        wallet = "0xabc00000000000000000000000000000000000aa"
        now = int(time.time())
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'observer-test', '', '', '', 'active', 'paper_approved', ?, ?)
            """,
            (wallet, now - 1200, now),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 83.11, 'paper_approved', 'observer_ready', '{}', '{}', 'test', ?)
            """,
            (wallet, now),
        )
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
                'activity-stale-actionable', ?, 'paper_approved', 'validation', 'old-market',
                'old-asset', 'YES', 'BUY', ?, 60,
                300, 0.54, 40, 0.55, 0.55,
                40, ?, 42,
                'polymarket_clob_book', 1, 1, 'actionable_quote',
                'paper_clob_vwap', 40, 'paper_clob_vwap',
                0.4, 185.18, 83.11, 5,
                'clean', ?, '{}'
            )
            """,
            (wallet, now - 960, now - 900, now - 900),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    handoff = data["paper_handoff"]
    evaluation_history = data["paper_observer_evaluation"]["history"]

    assert evaluation_history["actionable"] == 1
    assert handoff["observer_current_window_sec"] == 600
    assert handoff["wallets"][0]["observer_actionable_signals"] == 0
    assert handoff["wallets"][0]["handoff_state"] == "awaiting_actionable_signal"
    assert handoff["state_counts"] == [{"state": "awaiting_actionable_signal", "count": 1}]
    assert data["production_readiness"]["observer_current_window_sec"] == 600
    assert data["production_readiness"]["observer_actionable_signals"] == 0
    assert data["production_readiness"]["state"] == "paper_candidates_waiting_actionable_signals"


def test_paper_handoff_route_is_a_dedicated_lightweight_export():
    source = Path("src/pm_robot/web.py").read_text(encoding="utf-8")
    route_start = source.index('if parsed.path == "/api/paper-handoff":')
    route_end = source.index('if parsed.path == "/api/paper-observer-preview":')
    route = source[route_start:route_end]

    assert "paper_handoff_data" in route
    assert "paper_handoff_csv" in route
    assert "dashboard_data" not in route


def test_paper_handoff_csv_exports_header_when_empty(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    rows = list(csv.DictReader(io.StringIO(paper_handoff_csv(settings))))
    header = paper_handoff_csv(settings).splitlines()[0].split(",")

    assert rows == []
    assert "address" in header
    assert "research_check_summary" in header


def test_paper_handoff_export_cli_writes_json_and_csv_without_db_writes(
    tmp_path,
    monkeypatch,
    capsys,
):
    from pm_robot.cli import main

    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000012"
    now = 1_800_000_200
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'paper-export-test', '', '', '', 'active', 'paper_approved', ?, ?)
            """,
            (wallet, now - 100, now),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 83.11, 'paper_approved', 'score_and_validation_present', '{}', '{}', 'test', ?)
            """,
            (wallet, now),
        )
        conn.commit()
    finally:
        conn.close()

    json_out = tmp_path / "reports" / "paper_handoff.json"
    csv_out = tmp_path / "reports" / "paper_handoff.csv"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(settings.db_path),
            "paper-handoff-export",
            "--out",
            str(json_out),
            "--csv-out",
            str(csv_out),
        ],
    )

    assert main() == 0
    captured = json.loads(capsys.readouterr().out)
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    rows = list(csv.DictReader(io.StringIO(csv_out.read_text(encoding="utf-8"))))
    conn = connect(settings.db_path)
    try:
        order_count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]
    finally:
        conn.close()

    assert captured["schema_version"] == "paper_handoff_v1"
    assert payload["wallets"][0]["address"] == wallet
    assert rows[0]["address"] == wallet
    assert rows[0]["candidate_stage"] == "paper_approved"
    assert order_count == 0


def test_paper_handoff_total_count_is_not_limited_by_visible_rows(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    now = 1_800_000_200
    conn = connect(settings.db_path)
    try:
        rows = [
            (f"0xdef000000000000000000000000000000000{i:04x}", "paper_approved" if i % 2 else "paper_candidate", now - i, now)
            for i in range(12)
        ]
        conn.executemany(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'paper-test', '', '', '', 'active', ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    handoff = paper_handoff_data(settings, limit=5)

    assert handoff["candidate_count"] == 12
    assert handoff["visible_wallet_count"] == 5
    assert handoff["visible_research_ready"] == 0
    assert handoff["visible_research_incomplete"] == 5
    assert handoff["incomplete_research_wallets"][0]["missing"].startswith("缺 ")
    assert handoff["stage_counts"] == [
        {"stage": "paper_approved", "count": 6},
        {"stage": "paper_candidate", "count": 6},
    ]


def test_dashboard_shows_readonly_paper_observer_preview(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000003"
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'observer-test', '', '', '', 'active', 'paper_approved', ?, ?)
            """,
            (wallet, now - 3600, now),
        )
        conn.execute(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links,
                evidence_json, observed_at, recorded_at
            ) VALUES (?, 'observer-test', 'active', '', '', '', '{}', ?, ?)
            """,
            (wallet, now - 3600, now - 3500),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(
                address, copy_event_count, hygiene_status, extra_json, updated_at
            ) VALUES (?, 5, 'clean', '{}', ?)
            """,
            (wallet, now),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 83.11, 'paper_approved', 'observer_ready', '{}', '{}', 'test', ?)
            """,
            (wallet, now),
        )
        _insert_l3_evidence(conn, wallet, updated_at=now)
        rows = []
        for idx in range(99):
            rows.append((wallet, now - 100_000 - idx, "old-market", f"old-asset-{idx}", "YES", "TRADE", "BUY", 0.54, 10, 5.4, f"0xold{idx}", "{}", now))
        rows.append((wallet, now - 60, "fresh-market", "fresh-asset", "YES", "TRADE", "BUY", 0.54, 10, 5.4, "0xfresh", "{}", now))
        conn.executemany(
            """
            INSERT INTO wallet_activity(
                address, timestamp, market_slug, asset_id, outcome, type,
                side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
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
                'activity-1', ?, 'paper_approved', 'validation', 'fresh-market',
                'fresh-asset', 'YES', 'BUY', ?, 60,
                300, 0.54, 40, 0.55, 0.55,
                40, ?, 42,
                'polymarket_clob_book', 1, 1, 'actionable_quote',
                'paper_clob_vwap', 40, 'paper_clob_vwap',
                0.4, 185.18, 83.11, 5,
                'clean', ?, '{}'
            )
            """,
            (wallet, now - 60, now, now),
        )
        conn.commit()
    finally:
        conn.close()

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    (reports_dir / "paper_observer_evaluation.json").write_text(
        json.dumps(
            {
                "schema_version": "paper_observer_evaluation_v1",
                "generated_at": now,
                "max_signal_age_sec": 21_600,
                "max_actionable_signal_age_sec": 300,
                "max_stake_usd": 40,
                "signals_seen": 1,
                "quotes_attempted": 1,
                "quotes_succeeded": 1,
                "accepted_signals": 1,
                "actionable_signals": 1,
                "stale_signal_rejections": 0,
                "actionable_rate_pct": 100.0,
                "rejected_signals": 0,
                "quote_error_signals": 0,
                "average_slippage_bps": 185.18,
                "average_latency_ms": 42,
                "evaluations": [
                    {
                        "signal_id": "activity-1",
                        "wallet": wallet,
                        "market_slug": "fresh-market",
                        "outcome": "YES",
                        "side": "BUY",
                        "leader_price": 0.54,
                        "best_ask": 0.55,
                        "executable_price": 0.55,
                        "slippage_bps": 185.18,
                        "accepted": True,
                        "actionable": True,
                        "actionability_reason": "actionable_quote",
                        "decision_reason": "paper_clob_vwap",
                        "quote_latency_ms": 42,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    data = dashboard_data(settings)
    preview = paper_observer_preview_data(settings, limit=5)
    evaluation = paper_observer_evaluation_data(settings)
    html = _render_dashboard(settings)
    conn = connect(settings.db_path)
    try:
        order_count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]
    finally:
        conn.close()

    assert data["paper_observer_preview"]["schema_version"] == "paper_observer_preview_v1"
    assert data["paper_observer_preview"]["read_only"] is True
    assert data["paper_observer_preview"]["write_scope"] == "no_writes"
    assert data["paper_observer_preview"]["signals_seen"] == 1
    assert data["paper_observer_preview"]["signals"][0]["wallet"] == wallet
    assert data["paper_observer_preview"]["signals"][0]["ingest_lag_sec"] == 60
    assert data["paper_observer_preview"]["paper_stage_wallets"] == 1
    assert data["paper_observer_preview"]["recent_buy_events"] == 1
    assert data["paper_observer_preview"]["latest_buy_ts"] == now - 60
    assert data["paper_observer_preview"]["latest_buy_ingested_at"] == now
    assert data["paper_observer_preview"]["recent_buy_max_ingest_lag_sec"] == 60
    assert data["paper_observer_preview"]["no_signal_reason"] == ""
    windows = {row["window_label"]: row for row in data["paper_observer_preview"]["window_diagnostics"]}
    assert windows["6h"]["eligible_signals"] == 1
    assert windows["6h"]["max_ingest_lag_sec"] == 60
    assert windows["6h"]["max_ingest_lag"] == "1 分钟"
    assert windows["6h"]["no_signal_reason"] == ""
    assert data["paper_observer_preview"]["suggested_window"]["window_label"] == "6h"
    assert data["paper_observer_preview"]["suggested_window"]["mode"] == "live_window"
    assert data["paper_observer_evaluation"]["schema_version"] == "paper_observer_evaluation_v1"
    assert data["paper_observer_evaluation"]["state"] == "current"
    assert data["paper_observer_evaluation"]["accepted_signals"] == 1
    assert data["paper_observer_evaluation"]["actionable_signals"] == 1
    assert data["paper_observer_evaluation"]["history"]["total_evaluations"] == 1
    assert data["paper_observer_evaluation"]["history"]["actionable"] == 1
    assert data["paper_observer_evaluation"]["history"]["wallets_summary"][0]["wallet"] == wallet
    assert data["paper_observer_evaluation"]["history"]["wallets_summary"][0]["actionable"] == 1
    assert data["production_readiness"]["state"] == "paper_candidates_present"
    assert data["production_readiness"]["observer_actionable_signals"] == 1
    assert data["production_readiness"]["observer_actionable_wallets"] == 1
    assert data["paper_handoff"]["wallets"][0]["handoff_state"] == "awaiting_external_paper"
    assert data["paper_handoff"]["wallets"][0]["observer_actionable_signals"] == 1
    assert data["paper_handoff"]["wallets"][0]["observer_quality_state"] == "actionable_seen"
    assert data["paper_handoff"]["wallets"][0]["observer_quality_evaluations"] == 1
    assert data["paper_handoff"]["wallets"][0]["observer_quality_actionable_rate_pct"] == 100.0
    assert data["paper_handoff"]["wallets"][0]["observer_quality_avg_signal_age_sec"] == 60.0
    assert preview["signals_seen"] == 1
    assert preview["signals"][0]["market_slug"] == "fresh-market"
    assert evaluation["accepted_signals"] == 1
    assert evaluation["actionable_signals"] == 1
    assert order_count == 0
    assert "Paper Observer 预览" in html
    assert "Paper Observer 报价评估" in html
    assert "JSON 报价评估" in html
    assert "只读盘口评估" in html
    assert "长期报价证据" in html
    assert "可及时跟" in html
    assert "actionable_seen" in html
    assert "quality 1 eval" in html
    assert "入库延迟" in html
    assert "ingest lag" in html
    assert "JSON 观察预览" in html
    assert "只读 paper observer 预览" in html
    assert "窗口内 BUY" in html
    assert "最近 BUY" in html
    assert "观察窗口对比" in html


def test_dashboard_explains_no_paper_observer_signal_when_latest_buy_is_stale(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000004"
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'observer-test', '', '', '', 'active', 'paper_approved', ?, ?)
            """,
            (wallet, now - 100_000, now),
        )
        conn.execute(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links,
                evidence_json, observed_at, recorded_at
            ) VALUES (?, 'observer-test', 'active', '', '', '', '{}', ?, ?)
            """,
            (wallet, now - 100_000, now - 99_000),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(
                address, copy_event_count, hygiene_status, extra_json, updated_at
            ) VALUES (?, 5, 'clean', '{}', ?)
            """,
            (wallet, now),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 83.11, 'paper_approved', 'observer_ready', '{}', '{}', 'test', ?)
            """,
            (wallet, now),
        )
        _insert_l3_evidence(conn, wallet, updated_at=now)
        rows = [
            (wallet, now - 90_000 - idx, "old-market", f"old-asset-{idx}", "YES", "TRADE", "BUY", 0.54, 10, 5.4, f"0xstale{idx}", "{}", now)
            for idx in range(100)
        ]
        conn.executemany(
            """
            INSERT INTO wallet_activity(
                address, timestamp, market_slug, asset_id, outcome, type,
                side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    preview = paper_observer_preview_data(settings, limit=5, max_signal_age_sec=3600)
    html = _render_dashboard(settings)
    conn = connect(settings.db_path)
    try:
        order_count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]
    finally:
        conn.close()

    assert preview["signals_seen"] == 0
    assert preview["paper_stage_wallets"] == 1
    assert preview["recent_buy_events"] == 0
    assert preview["latest_buy_ingested_at"] == now
    assert preview["latest_buy_age_sec"] > 3600
    assert preview["no_signal_reason"] == "latest_buy_outside_window"
    assert preview["next_action"] == "默认窗口暂无信号；最短可复盘窗口是 72h，仅用于历史 paper 观察复盘，不代表实时跟单。"
    windows = {row["window_label"]: row for row in preview["window_diagnostics"]}
    assert windows["6h"]["eligible_signals"] == 0
    assert windows["24h"]["eligible_signals"] == 0
    assert windows["72h"]["eligible_signals"] > 0
    assert windows["72h"]["max_ingest_lag_sec"] > 86_000
    assert windows["72h"]["max_ingest_lag"] != "无"
    assert windows["168h"]["eligible_signals"] > 0
    assert preview["suggested_window"]["window_label"] == "72h"
    assert preview["suggested_window"]["mode"] == "historical_review_window"
    assert order_count == 0
    assert "最短可复盘窗口是 72h" in html
    assert "打开 72h 复盘" in html
    assert "latest_buy_outside_window" in html
    assert "最大入库延迟" in html


def test_paper_observer_preview_route_is_a_dedicated_lightweight_export():
    source = Path("src/pm_robot/web.py").read_text(encoding="utf-8")
    route_start = source.index('if parsed.path == "/api/paper-observer-preview":')
    route_end = source.index('if parsed.path == "/api/paper-observer-evaluation":')
    route = source[route_start:route_end]

    assert "paper_observer_preview_data" in route
    assert "dashboard_data" not in route


def test_paper_observer_evaluation_route_serves_export_file():
    source = Path("src/pm_robot/web.py").read_text(encoding="utf-8")
    route_start = source.index('if parsed.path == "/api/paper-observer-evaluation":')
    route_end = source.index('if parsed.path == "/api/discovery":')
    route = source[route_start:route_end]

    assert "paper_observer_evaluation_data" in route
    assert "evaluate_paper_observer" not in route
    assert "dashboard_data" not in route


def test_dashboard_prioritizes_actionable_manual_blocker_over_archived_blocks(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    blocked = "0xdef0000000000000000000000000000000000002"
    conn = connect(settings.db_path)
    try:
        now = 1_800_000_100
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'blocked_copyability', ?, ?)
            """,
            (blocked, now - 10, now),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 99.0, 'blocked_copyability', 'copyability_scan_no_signal', '{}', '{}', 'test', ?)
            """,
            (blocked, now),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)

    assert data["production_readiness"]["blocked_copyability"] == 1
    assert data["production_readiness"]["top_blocker_key"] == "thin_evidence"
    assert data["production_readiness"]["top_blocker"] == "历史证据偏薄"
    assert data["top_review_candidates"][0]["address"] == "0xabc0000000000000000000000000000000000001"
    assert all(row["candidate_stage"] != "blocked_copyability" for row in data["top_review_candidates"])
    assert data["top_review_blockers"][0]["key"] == "thin_evidence"


def test_web_runtime_route_is_lightweight():
    source = Path("src/pm_robot/web.py").read_text(encoding="utf-8")
    route_start = source.index('if parsed.path == "/api/runtime":')
    route_end = source.index('if parsed.path == "/api/summary":')
    route = source[route_start:route_end]

    assert "_runtime_build_info()" in route
    assert "dashboard_data" not in route


def test_dashboard_flags_near_threshold_stale_scores_for_rescore(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            UPDATE leader_scores
            SET leader_score = 68.0, scored_at = ?
            WHERE address = ?
            """,
            (1_800_000_100, "0xabc0000000000000000000000000000000000001"),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)

    assert data["score_policy"]["state"] == "stale_scores_need_rescore"
    assert data["score_policy"]["stale_near_threshold_scores"] == 1
    assert data["score_policy"]["stale_paper_threshold_scores"] == 0


def test_dashboard_source_quality_uses_latest_score_only(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    completed_wallet = "0xabc0000000000000000000000000000000000002"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, ?, ?, ?, '{}', '{}', 'test', ?)
            """,
            (wallet, 12.0, "needs_data", "latest lower score", 1_800_000_010),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)

    assert data["source_quality"][0]["source"] == "polymarket_trades_global"
    assert data["source_quality"][0]["max_score"] == 12.0


def test_candidate_source_wallet_latest_snapshot_tracks_event_changes(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        snapshot = conn.execute(
            "SELECT * FROM candidate_source_wallet_latest WHERE source = ? AND address = ?",
            ("polymarket_trades_global", wallet),
        ).fetchone()
        assert snapshot["event_count"] == 1

        conn.execute(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links,
                evidence_json, observed_at, recorded_at
            ) VALUES (?, 'polymarket_trades_global', 'active', 'second', '', '',
                      '{}', ?, ?)
            """,
            (wallet, 1_800_000_020, 1_800_000_030),
        )
        event_id = conn.execute(
            "SELECT MAX(event_id) AS event_id FROM candidate_source_events WHERE address = ?",
            (wallet,),
        ).fetchone()["event_id"]
        snapshot = conn.execute(
            "SELECT event_count, latest_recorded_at FROM candidate_source_wallet_latest WHERE source = ? AND address = ?",
            ("polymarket_trades_global", wallet),
        ).fetchone()
        assert snapshot["event_count"] == 2
        assert snapshot["latest_recorded_at"] == 1_800_000_030

        conn.execute(
            "UPDATE candidate_source_events SET source = 'manual_seed', recorded_at = ? WHERE event_id = ?",
            (1_800_000_040, event_id),
        )
        old_snapshot = conn.execute(
            "SELECT event_count, latest_recorded_at FROM candidate_source_wallet_latest WHERE source = ? AND address = ?",
            ("polymarket_trades_global", wallet),
        ).fetchone()
        new_snapshot = conn.execute(
            "SELECT event_count, latest_recorded_at FROM candidate_source_wallet_latest WHERE source = ? AND address = ?",
            ("manual_seed", wallet),
        ).fetchone()
        assert old_snapshot["event_count"] == 1
        assert new_snapshot["event_count"] == 1
        assert new_snapshot["latest_recorded_at"] == 1_800_000_040

        conn.execute("DELETE FROM candidate_source_events WHERE event_id = ?", (event_id,))
        assert conn.execute(
            "SELECT * FROM candidate_source_wallet_latest WHERE source = ? AND address = ?",
            ("manual_seed", wallet),
        ).fetchone() is None
    finally:
        conn.close()


def test_leader_latest_scores_snapshot_tracks_score_changes(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 12.0, 'needs_data', 'latest lower score', '{}', '{}', 'test', ?)
            """,
            (wallet, 1_800_000_010),
        )
        latest_id = conn.execute(
            "SELECT MAX(score_id) AS score_id FROM leader_scores WHERE address = ?",
            (wallet,),
        ).fetchone()["score_id"]
        assert conn.execute(
            "SELECT leader_score FROM leader_latest_scores WHERE address = ?",
            (wallet,),
        ).fetchone()["leader_score"] == 12.0

        conn.execute(
            "UPDATE leader_scores SET leader_score = 71.0, review_stage = 'paper_candidate' WHERE score_id = ?",
            (latest_id,),
        )
        latest = conn.execute(
            "SELECT leader_score, review_stage FROM leader_latest_scores WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert latest["leader_score"] == 71.0
        assert latest["review_stage"] == "paper_candidate"

        conn.execute("DELETE FROM leader_scores WHERE score_id = ?", (latest_id,))
        latest = conn.execute(
            "SELECT leader_score, review_stage FROM leader_latest_scores WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert latest["leader_score"] == 55.5
        assert latest["review_stage"] == "needs_manual_review"
    finally:
        conn.close()


def test_wallet_dashboard_snapshot_tracks_dashboard_inputs(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        snapshot = conn.execute(
            "SELECT * FROM wallet_dashboard_snapshot WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert snapshot["candidate_stage"] == "needs_manual_review"
        assert snapshot["activity_count"] == 1
        assert snapshot["leader_score"] == 55.5

        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 3,
                      'deep_done', 'score_wallet', 0, 1000, 12, 800, ?)
            """,
            (wallet, 1_800_000_050),
        )
        snapshot = conn.execute(
            "SELECT activity_count, discovery_tier, next_action FROM wallet_dashboard_snapshot WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert snapshot["activity_count"] == 1000
        assert snapshot["discovery_tier"] == "l3_deep"
        assert snapshot["next_action"] == "score_wallet"

        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 71.0, 'paper_candidate', 'snapshot score', '{}', '{}', 'test', ?)
            """,
            (wallet, 1_800_000_060),
        )
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'paper_candidate', updated_at = ? WHERE address = ?",
            (1_800_000_061, wallet),
        )
        snapshot = conn.execute(
            "SELECT candidate_stage, leader_score FROM wallet_dashboard_snapshot WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert snapshot["candidate_stage"] == "paper_candidate"
        assert snapshot["leader_score"] == 71.0

        conn.execute("DELETE FROM wallet_processing_state WHERE wallet = ?", (wallet,))
        snapshot = conn.execute(
            "SELECT activity_count, discovery_tier, next_action FROM wallet_dashboard_snapshot WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert snapshot["activity_count"] == 1
        assert snapshot["discovery_tier"] == ""
        assert snapshot["next_action"] == ""
    finally:
        conn.close()


def test_dashboard_copyability_lane_reports_queue_and_worker_progress(tmp_path, monkeypatch):
    monkeypatch.setenv("PM_ROBOT_COPYABILITY_PLANNER_MAX_ACTIVE_JOBS", "2")
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    completed_wallet = "0xabc0000000000000000000000000000000000002"
    weak_follower = "0xabc0000000000000000000000000000000000003"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'needs_data', 1800000000, 1800000000)
            """,
            (weak_follower,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 10, 0,
                      'queued', NULL, 0, 0, 3, 0, ?, '{}', '', ?, ?)
            """,
            (
                wallet,
                json.dumps(
                    {
                        "activity_count": 1000,
                        "max_pair_events": 8,
                        "max_pair_markets": 3,
                        "graph_scan_mode": "deep",
                    }
                ),
                1_800_000_000,
                1_800_000_010,
            ),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 10, 0,
                      'done', NULL, 0, 1, 3, 0, '{}', '{}', '', ?, ?, ?)
            """,
            (completed_wallet, 1_800_000_000, 1_800_000_020, 1_800_000_020),
        )
        conn.execute(
            """
            INSERT INTO copy_pair_stats(
                leader_wallet, follower_wallet, copy_event_count, copy_market_count,
                follower_trade_count, containment_pct, leader_precedes_pct,
                median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
            ) VALUES (?, ?, 21, 5, 100, 0.10, 1.0, 4, 1800000000, 1800000100, 0, 1800000100)
            """,
            (wallet, weak_follower),
        )
        conn.execute(
            """
            INSERT INTO ingest_runs(
                ingest_type, started_at, finished_at, status,
                wallets_attempted, wallets_succeeded, rows_written, error
            ) VALUES ('copyability_evidence_worker_0_test', ?, ?, 'ok', 1, 1, 25, '')
            """,
            (1_800_000_020, 1_800_000_050),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    lane = data["copyability_lane"]
    html = _render_dashboard(settings)

    assert lane["queued"] == 1
    assert lane["running"] == 0
    assert lane["active"] == 1
    assert lane["max_active_jobs"] == 2
    assert lane["available_slots"] == 1
    assert lane["queue_utilization_pct"] == 50.0
    assert lane["queue_waterline_reached"] is False
    assert lane["high_priority_active"] == 1
    assert lane["completed_1h"] == 1
    assert lane["completed_6h"] == 1
    assert lane["completed_24h"] == 1
    assert lane["recent_rate_per_hour"] > 0
    assert lane["eta_label"]
    pair_quality = lane["pair_quality"]
    buckets = {row["bucket"]: row for row in pair_quality["bucket_rows"]}
    assert pair_quality["thresholds"]["min_containment"] == 0.2
    assert buckets["miss_containment"]["count"] == 1
    assert pair_quality["near_miss_leaders"][0]["leader_wallet"] == wallet
    assert pair_quality["near_miss_leaders"][0]["copy_events"] == 21
    light_data = dashboard_data(settings, include_pair_quality=False)
    assert light_data["copyability_lane"]["pair_quality"] == {}
    assert lane["active_by_priority"][0]["priority"] == 10
    assert lane["active_by_priority"][0]["candidate_stage"] == "needs_manual_review"
    assert lane["top_active_jobs"][0]["wallet"] == wallet
    assert lane["top_active_jobs"][0]["activity_count"] == 1000
    assert lane["top_active_jobs"][0]["max_pair_events"] == 8
    assert lane["recent_runs"][0]["run_type"] == "copyability_evidence_worker_0_test"
    assert lane["recent_runs"][0]["duration_seconds"] == 30
    assert "Copyability 证据通道" in html
    assert "队列水位" in html
    assert "1/2" in html
    assert "粗略剩余" in html
    assert "估算/小时" in html
    assert "Pair 质量诊断" in html
    assert "弱信号钱包" in html
    assert "队列前排" in html
    assert "最近 Worker" in html

    monkeypatch.setenv("PM_ROBOT_COPYABILITY_PLANNER_MAX_ACTIVE_JOBS", "1")
    saturated = dashboard_data(settings)["copyability_lane"]
    saturated_html = _copyability_lane_panel(saturated)
    assert saturated["available_slots"] == 0
    assert saturated["queue_utilization_pct"] == 100.0
    assert saturated["queue_waterline_reached"] is True
    assert "已达到活动水位" in saturated_html


def test_dashboard_discovery_freshness_reports_recent_source_and_observed_flow(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    _seed(settings)
    monkeypatch.setenv("PM_ROBOT_RTDS_PAPER_MIN_TRADE_USDC", "0")
    observed_wallet = "0xabc00000000000000000000000000000000000f1"
    promoted_wallet = "0xabc00000000000000000000000000000000000f2"
    paper_wallet = "0xabc00000000000000000000000000000000000f3"
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO observed_wallets(
                wallet, sources, labels, notes, links, status,
                observed_trade_count, recent_trade_count, recent_usdc_total,
                recent_max_trade_usdc, recent_trades_json, promoted_at,
                promotion_reason, first_seen_at, updated_at
            ) VALUES (?, 'polymarket_rtds_activity', '', '', '', 'observed',
                      3, 3, 450.0, 180.0, '[]', NULL, '', ?, ?)
            """,
            (observed_wallet, now - 120, now - 60),
        )
        conn.execute(
            """
            INSERT INTO observed_wallets(
                wallet, sources, labels, notes, links, status,
                observed_trade_count, recent_trade_count, recent_usdc_total,
                recent_max_trade_usdc, recent_trades_json, promoted_at,
                promotion_reason, first_seen_at, updated_at
            ) VALUES (?, 'polymarket_trades_global', '', '', '', 'promoted',
                      5, 5, 1200.0, 600.0, '[]', ?, 'recent size gate', ?, ?)
            """,
            (promoted_wallet, now - 50, now - 180, now - 40),
        )
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'polymarket_rtds_activity', '', '', '', 'active',
                      'needs_data', ?, ?)
            """,
            (promoted_wallet, now - 45, now - 40),
        )
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'polymarket_rtds_activity', '', '', '', 'active',
                      'paper_approved', ?, ?)
            """,
            (paper_wallet, now - 240, now - 30),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 83.5, 'paper_approved', 'score_and_validation_present',
                      '{}', '{}', 'test', ?)
            """,
            (paper_wallet, now - 30),
        )
        conn.execute(
            """
            INSERT INTO wallet_activity(
                address, timestamp, market_slug, asset_id, outcome, type,
                side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
            ) VALUES (?, ?, 'rtds-paper-market', 'asset-rtds', 'Yes', 'TRADE',
                      'BUY', 0.58, 200.0, 116.0, '0xrtdspaper',
                      ?, ?)
            """,
            (
                paper_wallet,
                now - 120,
                json.dumps({"source": "polymarket_rtds_activity"}),
                now - 60,
            ),
        )
        conn.execute(
            """
            INSERT INTO wallet_activity(
                address, timestamp, market_slug, asset_id, outcome, type,
                side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
            ) VALUES (?, ?, 'rtds-paper-market', 'asset-rtds', 'Yes', 'REDEEM',
                      '', 1.0, 84.0, 84.0, '0xrtdspaperredeem',
                      ?, ?)
            """,
            (
                paper_wallet,
                now - 100,
                json.dumps({"source": "polymarket_rtds_activity"}),
                now - 20,
            ),
        )
        conn.execute(
            """
            INSERT INTO wallet_activity(
                address, timestamp, market_slug, asset_id, outcome, type,
                side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
            ) VALUES (?, ?, 'poll-late-buy-market', 'asset-late-buy', 'Yes', 'TRADE',
                      'BUY', 0.61, 100.0, 61.0, '0xlatepollbuy',
                      '{}', ?)
            """,
            (paper_wallet, now - 7200, now - 10),
        )
        conn.execute(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links,
                evidence_json, observed_at, recorded_at
            ) VALUES (?, 'polymarket_rtds_activity', 'active', '', '', '',
                      '{}', ?, ?)
            """,
            (promoted_wallet, now - 45, now - 40),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    freshness = data["discovery_freshness"]
    html = _render_dashboard(settings)
    detailed_html = web_module._discovery_freshness_panel(freshness)
    sources = {row["source"]: row for row in freshness["source_rows"]}
    observed_sources = {row["source"]: row for row in freshness["observed_sources"]}
    stages = {row["candidate_stage"]: row for row in freshness["stage_rows"]}
    pulse = freshness["paper_activity_pulse"]
    bridge = freshness["paper_rtds_bridge"]

    assert freshness["state"] == "fresh"
    assert freshness["source_events_24h"] >= 1
    assert freshness["candidates_24h"] >= 1
    assert freshness["observed_seen_24h"] == 2
    assert freshness["promoted_24h"] == 1
    assert bridge["state"] == "fresh_buy_activity"
    assert bridge["paper_stage_wallets"] == 1
    assert bridge["rtds_activity_events"] == 2
    assert bridge["rtds_activity_wallets"] == 1
    assert bridge["rtds_activity_events_fresh"] == 2
    assert bridge["rtds_activity_events_24h"] == 2
    assert bridge["rtds_trade_events"] == 1
    assert bridge["rtds_buy_events"] == 1
    assert bridge["rtds_buy_events_fresh"] == 1
    assert bridge["rtds_buy_events_24h"] == 1
    assert bridge["rtds_redeem_events"] == 1
    assert bridge["rtds_non_buy_events"] == 1
    assert bridge["rtds_max_ingest_lag_sec"] == 80
    assert bridge["paper_min_trade_usdc"] == 0.0
    assert bridge["wallet_rows"][0]["wallet"] == paper_wallet
    assert bridge["wallet_rows"][0]["rtds_buy_events"] == 1
    assert bridge["wallet_rows"][0]["rtds_redeem_events"] == 1
    assert bridge["wallet_rows"][0]["latest_rtds_event_type"] == "REDEEM"
    assert sources["polymarket_rtds_activity"]["events_24h"] == 1
    assert observed_sources["polymarket_trades_global"]["promoted_24h"] == 1
    assert stages["paper_approved"]["new_24h"] == 1
    assert stages["needs_data"]["new_24h"] >= 1
    assert pulse["state"] == "timely_buy_activity"
    assert pulse["paper_stage_wallets"] == 1
    assert pulse["events_24h"] == 3
    assert pulse["buy_events_24h"] == 2
    assert pulse["timely_buy_events"] == 1
    assert pulse["stale_buy_events_24h"] == 1
    assert pulse["non_buy_events_24h"] == 1
    assert pulse["max_buy_ingest_lag_sec"] == 7190
    assert pulse["latest_buy_ingest_lag_sec"] == 60
    pulse_sources = {row["source"]: row for row in pulse["source_rows"]}
    assert pulse_sources["polymarket_rtds_activity"]["buy_events_24h"] == 1
    assert pulse_sources["wallet_activity_poll"]["buy_events_24h"] == 1
    assert pulse["wallet_rows"][0]["latest_source"] == "wallet_activity_poll"
    assert "发现活水" in html
    assert "首屏仅展示最近 24/72 小时关键指标" in html
    assert "Paper-stage 活动脉冲" in detailed_html
    assert "可及时 BUY" in detailed_html
    assert "Paper 活动来源" in detailed_html
    assert "RTDS→Paper 实时桥接" in detailed_html
    assert "RTDS BUY" in detailed_html
    assert "最大入库延迟" in detailed_html
    assert "Paper RTDS阈值" in detailed_html
    assert "wallet_activity" in detailed_html
    assert "来源事件新鲜度" in detailed_html
    assert "观察池晋级" in detailed_html


def test_dashboard_evidence_pipeline_reports_l1_l2_l3_queue_progress(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    _seed(settings)
    monkeypatch.setenv("PM_ROBOT_PIPELINE_PRIORITY_AGING_SECONDS", "1800")
    wallet = "0xabc0000000000000000000000000000000000001"
    deep_wallet = "0xabc00000000000000000000000000000000000d3"
    done_wallet = "0xabc00000000000000000000000000000000000d4"
    stalled_wallet = "0xabc00000000000000000000000000000000000d5"
    exhausted_wallet = "0xabc00000000000000000000000000000000000d6"
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        for address in (deep_wallet, done_wallet, stalled_wallet):
            conn.execute(
                """
                INSERT INTO candidate_wallets(
                    address, sources, labels, notes, links, status,
                    candidate_stage, first_seen_at, updated_at
                ) VALUES (?, 'test', '', '', '', 'active', 'needs_data', ?, ?)
                """,
                (address, 1_800_000_000, 1_800_000_000),
            )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l1_light', 'queued', 200, 0.7, 5, 'light_done',
                      'medium_pending', 0, 240, 6, 180, ?)
            """,
            (wallet, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1600, 1.0, 30, 'deep_done',
                      'score_wallet', 0, 1600, 24, 1200, ?)
            """,
            (deep_wallet, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l2_medium', 'queued', 800, 0.8, 50, 'medium_done',
                      'deep_pending', 0, 820, 18, 620, ?)
            """,
            (stalled_wallet, now - 60),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES ('wallet_evidence_backfill', ?, 'medium_pending', 'l1_light',
                      5, 0, 'queued', NULL, 0, 0, 3, 0, ?, '{}', '', ?, ?)
            """,
            (
                wallet,
                json.dumps({"stage": "medium_pending", "target_depth": 1000}),
                now - 2_000,
                now - 1_900,
            ),
        )
        conn.execute(
            """
            INSERT INTO pipeline_scheduler_state(
                job_type, subject_key, current_weight, last_selected_at, updated_at
            ) VALUES ('wallet_evidence_backfill', 'medium_pending', -5, ?, ?)
            """,
            (now - 60, now - 60),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES ('wallet_evidence_backfill', ?, 'deep_pending', 'l2_medium',
                      99, 0, 'queued', NULL, 0, 3, 3, 0, '{}', '{}', 'retry exhausted', ?, ?)
            """,
            (exhausted_wallet, now - 3_000, now - 3_000),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('wallet_evidence_backfill', ?, 'light_pending', 'l0_discovered',
                      8, 0, 'done', NULL, 0, 1, 3, 0, '{}', ?, '', ?, ?, ?)
            """,
            (
                done_wallet,
                json.dumps(
                    {
                        "stage": "light_pending",
                        "next_stage": "medium_pending",
                        "target_depth": 200,
                        "activity_count": 220,
                        "state": {
                            "discovery_tier": "l1_light",
                            "evidence_status": "queued",
                            "next_action": "medium_pending",
                        },
                    }
                ),
                now - 1_800,
                now - 1_800,
                now - 1_800,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    pipeline = data["evidence_pipeline"]
    html = _render_dashboard(settings)

    assert pipeline["queued"] == 2
    assert pipeline["running"] == 0
    assert pipeline["active"] == 2
    assert pipeline["completed_1h"] == 1
    assert pipeline["completed_24h"] == 1
    assert pipeline["eta_label"]
    assert pipeline["total_due_backlog"] == 2
    assert pipeline["total_eta_label"]
    assert pipeline["l3_wallets"] == 1
    assert pipeline["summary_ready_wallets"] == 1
    assert pipeline["active_by_action"][0]["job_action"] == "medium_pending"
    assert pipeline["active_by_action"][0]["job_scope"] == "l1_light"
    assert pipeline["aged_queued_jobs"] == 1
    assert pipeline["due_queued_jobs"] == 1
    assert pipeline["deferred_queued_jobs"] == 0
    assert pipeline["exhausted_queued_jobs"] == 1
    assert pipeline["oldest_claimable_wait_seconds"] >= 1_900
    stage_schedule = {row["job_action"]: row for row in pipeline["stage_schedule"]}
    assert stage_schedule["medium_pending"]["configured_weight"] == 20
    assert stage_schedule["medium_pending"]["queued_count"] == 1
    assert stage_schedule["medium_pending"]["aged_queued_count"] == 1
    assert stage_schedule["medium_pending"]["current_weight"] == -5
    assert pipeline["pending_state_without_active_job"] == 1
    assert pipeline["high_priority_pending_state_without_active_job"] == 0
    assert pipeline["pending_state_by_action"][0]["next_action"] == "deep_pending"
    assert pipeline["pending_state_by_action"][0]["count"] == 1
    assert pipeline["top_active_jobs"][0]["wallet"] == wallet
    assert pipeline["top_active_jobs"][0]["target_depth"] == 1000
    assert pipeline["recent_completed_jobs"][0]["wallet"] == done_wallet
    assert pipeline["recent_completed_jobs"][0]["evidence_tier"] == "l1_light"
    assert "L1/L2/L3 证据流水线" in html
    assert "钱包证据层级" in html
    assert "分层调度状态" in html
    assert 'class="scheduler-table"' in html
    assert "调度权重" in html
    assert "到期" in html
    assert "退避" in html
    assert "耗尽" in html
    assert "老化排队" in html
    assert "最久等待" in html
    assert "尝试耗尽" in html
    assert "维护循环将标记失败并释放水位" in html
    assert "执行队列前排" in html
    assert "待调度候选" in html
    assert "总到期待补" in html
    assert "总 ETA" in html
    assert "最近完成证据任务" in html


def test_dashboard_distinguishes_completed_copyability_with_no_signal(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            "UPDATE leader_scores SET leader_score = 69.0, review_reason = 'near threshold' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            "UPDATE wallet_features SET copy_event_count = 0, copy_market_count = 0 WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 3, 'deep_done',
                      'score_wallet', 0, 1000, 12, 800, 1800000000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 3, 0,
                      'done', NULL, 0, 1, 3, 0, '{}', '{}', '', 1800000000,
                      1800000100, 1800000100)
            """,
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    rows = wallet_table_rows(settings, signal="review_copy_no_signal")
    discovery = discovery_data(settings)
    signal_counts = {row["signal"]: row["count"] for row in discovery["signal_counts"]}

    assert data["top_review_candidates"][0]["blocker_key"] == "copyability_no_signal"
    assert data["top_review_candidates"][0]["blocker_label"] == "copyability 无跟随信号"
    assert data["production_readiness"]["state"] == "near_threshold_no_copy_signal"
    assert data["production_readiness"]["top_blocker_key"] == "copyability_no_signal"
    assert data["production_readiness"]["manual_review_actions"][0]["blocker"] == "copyability 无跟随信号"
    assert signal_counts["review_copy_no_signal"] == 1
    assert len(rows) == 1
    assert rows[0]["copyability_status"] == "done"


def test_dashboard_distinguishes_light_copyability_no_signal(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            "UPDATE leader_scores SET leader_score = 69.0, review_reason = 'near threshold' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            "UPDATE wallet_features SET copy_event_count = 0, copy_market_count = 0 WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l2_medium', 'summary_ready', 700, 0.8, 3, 'medium_done',
                      'score_wallet', 0, 700, 12, 650, 1800000000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 3, 0,
                      'done', NULL, 0, 1, 3, 0,
                      '{"graph_scan_mode":"light_missing_copyability"}',
                      '{"graph_scan_mode":"light_missing_copyability"}',
                      '', 1800000000, 1800000100, 1800000100)
            """,
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    light_rows = wallet_table_rows(settings, signal="review_copy_light_no_signal")
    deep_rows = wallet_table_rows(settings, signal="review_copy_no_signal")
    discovery = discovery_data(settings)
    signal_counts = {row["signal"]: row["count"] for row in discovery["signal_counts"]}

    assert data["top_review_candidates"][0]["blocker_key"] == "copyability_light_no_signal"
    assert data["top_review_candidates"][0]["blocker_label"] == "copyability 轻扫无信号"
    assert data["top_review_candidates"][0]["copyability_scan_mode"] == "light_missing_copyability"
    assert signal_counts["review_copy_light_no_signal"] == 1
    assert signal_counts["review_copy_no_signal"] == 0
    assert len(light_rows) == 1
    assert len(deep_rows) == 0


def test_dashboard_surfaces_blocked_copyability_no_signal_watchlist(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc00000000000000000000000000000000000c0"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'test', '', '', '', 'active', 'blocked_copyability', ?, ?)
            """,
            (wallet, 1_800_000_000, 1_800_000_100),
        )
        conn.execute(
            """
            INSERT INTO wallet_features(
                address, recent_30d_volume_usdc, net_pnl_usdc, total_volume_usdc,
                leader_in_degree, copy_event_count, copy_market_count,
                single_market_pnl_share, net_to_gross_exposure,
                hygiene_status, primary_category, extra_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)
            """,
            (
                wallet,
                42_000.0,
                12_500.0,
                95_000.0,
                0,
                0,
                0,
                0.18,
                0.72,
                "ok",
                "macro",
                1_800_000_100,
            ),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 82.4, 'blocked_copyability', 'copyability_scan_no_signal',
                      '{}', '{}', 'test', ?)
            """,
            (wallet, 1_800_000_100),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    html = _render_dashboard(settings)
    watchlist = data["copyability_no_signal"]

    assert watchlist["wallet_count"] == 1
    assert watchlist["high_score_wallets"] == 1
    assert watchlist["high_pnl_wallets"] == 1
    assert watchlist["clean_wallets"] == 1
    assert watchlist["rows"][0]["address"] == wallet
    assert watchlist["rows"][0]["leader_score"] == 82.4
    assert watchlist["rows"][0]["candidate_stage"] == "blocked_copyability"
    assert "Copyability 无信号高潜池" in html
    assert "不是放行队列" in html
    assert "copyability_scan_no_signal" in html


def test_dashboard_distinguishes_completed_deep_copyability_near_miss(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            "UPDATE leader_scores SET leader_score = 69.0, review_reason = 'near threshold' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 3, 'deep_done',
                      'score_wallet', 0, 1000, 12, 800, 1800000000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, 1, 18, 4, 0.41, 15, 0, 1800000000, 1800000000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability', 3, 0,
                      'done', NULL, 0, 1, 3, 0,
                      '{"graph_scan_mode":"deep"}', '{"graph_scan_mode":"deep"}', '', 1800000000,
                      1800000100, 1800000100)
            """,
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    rows = wallet_table_rows(settings, signal="review_copy_near_miss")
    discovery = discovery_data(settings)
    signal_counts = {row["signal"]: row["count"] for row in discovery["signal_counts"]}

    assert data["top_review_candidates"][0]["blocker_key"] == "copyability_near_miss"
    assert data["top_review_candidates"][0]["blocker_label"] == "深扫近失，暂未达标"
    assert data["top_review_candidates"][0]["review_handling"] == "watch"
    assert data["top_review_candidates"][0]["operator_required"] is False
    assert data["production_readiness"]["state"] == "near_threshold_copyability_near_miss"
    assert data["production_readiness"]["top_blocker_key"] == "copyability_near_miss"
    assert data["production_readiness"]["manual_review_actions"][0]["blocker"] == "深扫近失，暂未达标"
    assert data["production_readiness"]["watch_review_wallets"] == 1
    assert data["production_readiness"]["operator_review_wallets"] == 0
    assert data["paper_pool_expansion"]["watch_count"] == 1
    assert data["paper_pool_expansion"]["operator_required_count"] == 0
    assert "自动" in data["production_readiness"]["next_action"]
    assert signal_counts["review_copy_near_miss"] == 1
    assert signal_counts["review_copy_unvalidated"] == 0
    assert len(rows) == 1
    assert rows[0]["leader_copy_events"] == 18


def test_dashboard_keeps_unfinished_copyability_signal_in_validation_bucket(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            "UPDATE leader_scores SET leader_score = 60.0, review_reason = 'watchlist_score' WHERE address = ?",
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 3, 'deep_done',
                      'score_wallet', 0, 1000, 12, 800, 1800000000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, 1, 9, 3, 0.35, 25, 0, 1800000000, 1800000000)
            """,
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    rows = wallet_table_rows(settings, signal="review_copy_unvalidated")
    near_miss_rows = wallet_table_rows(settings, signal="review_copy_near_miss")
    signal_counts = {row["signal"]: row["count"] for row in discovery_data(settings)["signal_counts"]}

    assert data["top_review_candidates"][0]["blocker_key"] == "copyability_unvalidated"
    assert data["top_review_candidates"][0]["review_handling"] == "automatic"
    assert signal_counts["review_copy_unvalidated"] == 1
    assert signal_counts["review_copy_near_miss"] == 0
    assert len(rows) == 1
    assert near_miss_rows == []


def test_dashboard_groups_paper_evidence_incomplete_review(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            UPDATE leader_scores
            SET leader_score = 71.91,
                review_reason = 'paper_evidence_tier_incomplete'
            WHERE address = ?
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l2_medium', 'summary_ready', 525, 0.8, 3, 'deep_done',
                      'score_wallet', 0, 525, 12, 400, 1800000000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO copy_leader_performance(
                leader_wallet, backtest_trade_count, copied_market_count,
                total_stake_usdc, gross_pnl_usdc, net_pnl_usdc, gross_roi,
                net_roi, win_rate, median_lag_seconds, last_backtest_trade_at,
                updated_at
            ) VALUES (?, 5, 3, 1000, 120, 90, 0.12, 0.09, 0.6, 30,
                      1800000000, 1800000000)
            """,
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    html = _render_dashboard(settings)
    rows = wallet_table_rows(settings, signal="review_paper_evidence_incomplete")
    discovery = discovery_data(settings)
    signal_counts = {row["signal"]: row["count"] for row in discovery["signal_counts"]}

    assert data["top_review_candidates"][0]["blocker_key"] == "paper_evidence_incomplete"
    assert data["top_review_candidates"][0]["blocker_label"] == "L3 证据未完成"
    assert data["production_readiness"]["manual_review_actions"][0]["blocker"] == "L3 证据未完成"
    assert data["production_readiness"]["manual_review_actions"][0]["next_action"] == "保持复核；L3 未达 summary_ready，不进入 paper"
    assert "保持复核；L3 未达 summary_ready，不进入 paper" in html
    assert signal_counts["review_paper_evidence_incomplete"] == 1
    assert len(rows) == 1
    assert rows[0]["evidence_tier"] == "l2_medium"


def test_dashboard_does_not_group_bounded_deep_summary_as_paper_incomplete(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            UPDATE leader_scores
            SET leader_score = 71.91,
                review_reason = 'paper_evidence_tier_incomplete'
            WHERE address = ?
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l2_medium', 'summary_ready', 525, 0.9, 3, 'deep_done',
                      'score_wallet', 0, 525, 81, 524, 1800000000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO copy_leader_performance(
                leader_wallet, backtest_trade_count, copied_market_count,
                total_stake_usdc, gross_pnl_usdc, net_pnl_usdc, gross_roi,
                net_roi, win_rate, median_lag_seconds, last_backtest_trade_at,
                updated_at
            ) VALUES (?, 5, 3, 1000, 120, 90, 0.12, 0.09, 0.6, 30,
                      1800000000, 1800000000)
            """,
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    rows = wallet_table_rows(settings, signal="review_paper_evidence_incomplete")
    discovery = discovery_data(settings)
    signal_counts = {row["signal"]: row["count"] for row in discovery["signal_counts"]}

    assert signal_counts.get("review_paper_evidence_incomplete", 0) == 0
    assert rows == []


def test_dashboard_groups_needs_data_reasons_into_operator_actions(tmp_path):
    settings = _settings(tmp_path)
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        now = 1_800_000_000
        seed_rows = [
            (
                "0x1000000000000000000000000000000000000001",
                "missing_required_score_components:leader_in_degree,copy_event_count,copy_market_count",
            ),
            (
                "0x1000000000000000000000000000000000000002",
                "insufficient_net_pnl_usdc:20.00<50.00",
            ),
            (
                "0x1000000000000000000000000000000000000003",
                "insufficient_recent_30d_volume_usdc:100.00<500.00",
            ),
            (
                "0x1000000000000000000000000000000000000004",
                "missing_required_score_components:bot_score,leader_in_degree,copy_event_count,copy_market_count,single_market_pnl_share,net_to_gross_exposure",
            ),
        ]
        for address, reason in seed_rows:
            conn.execute(
                """
                INSERT INTO candidate_wallets(
                    address, sources, labels, notes, links, status,
                    candidate_stage, first_seen_at, updated_at
                ) VALUES (?, 'reason_source', '', '', '', 'active',
                          'needs_data', ?, ?)
                """,
                (address, now - 100, now),
            )
            conn.execute(
                """
                INSERT INTO leader_scores(
                    address, leader_score, review_stage, review_reason,
                    components_json, penalties_json, policy_version, scored_at
                ) VALUES (?, 0.0, 'needs_data', ?, '{}', '{}', 'test', ?)
                """,
                (address, reason, now),
            )
            conn.execute(
                """
                INSERT INTO wallet_processing_state(
                    wallet, discovery_tier, evidence_status, evidence_depth,
                    evidence_confidence, priority, current_stage, next_action,
                    next_action_at, activity_count, distinct_markets,
                    non_fast_trade_count, updated_at
                ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10,
                          'deep_done', 'score_wallet', 0, 1000, 10, 900, ?)
                """,
                (address, now),
            )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    reasons = {row["key"]: row for row in data["needs_data_reasons"]}
    html = _render_dashboard(settings)

    assert reasons["missing_copyability_components"]["count"] == 1
    assert reasons["missing_copyability_components"]["reason"] == "缺 copyability 组件"
    assert reasons["missing_core_score_components"]["count"] == 1
    assert reasons["missing_core_score_components"]["reason"] == "缺基础评分组件"
    assert reasons["low_net_pnl"]["reason"] == "净收益不足"
    assert reasons["low_recent_volume"]["reason"] == "近期交易量不足"
    assert "Needs Data 原因" in html
    assert "补 copyability 证据并重评" in html
    assert "先物化 wallet_features，再分流补 copyability/hygiene" in html


def test_dashboard_ops_health_warns_on_invalid_wallet_addresses(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    conn = connect(settings.db_path)
    try:
        short_wallet = "0x" + "d" * 39
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'bad_source', '', '', '', 'manual_research_seed', 'needs_data', 10, 10)
            """,
            (short_wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    html = _render_dashboard(settings)

    assert data["ops_health"]["health"] == "attention"
    assert data["ops_health"]["address_quality"]["invalid_address_rows"] == 1
    assert "地址质量" in html
    assert "非标准钱包地址" in html


def test_dashboard_ops_health_reports_stale_and_active_pipeline_jobs(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 0, ?, 'worker', ?, 1, 3, 0, '{}', '{}', '', ?, ?)
            """,
            [
                ("wallet_evidence_backfill", "0x" + "2" * 40, "light_pending", "l0_discovered", 5, "queued", 0, 1_800_000_000, 1_800_000_000),
                ("copyability_evidence", "0x" + "3" * 40, "copyability", "copyability", 5, "running", 1, 1_800_000_000, 1_800_000_000),
            ],
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at, completed_at
            ) VALUES (?, ?, ?, ?, 5, 0, 'done', NULL, 0, 1, 3, 0, '{}', '{}', '', ?, ?, ?)
            """,
            (
                "wallet_evidence_backfill",
                "0x" + "4" * 40,
                "light_pending",
                "l0_discovered",
                now - 1_800,
                now - 1_800,
                now - 1_800,
            ),
        )
        conn.executemany(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES ('wallet_evidence_backfill', ?, 'medium_pending', 'l1_light',
                      10, 0, 'running', 'stale-worker', 1, 1, 3, 0,
                      '{}', '{}', '', ?, ?)
            """,
            [
                (f"0x{index:040x}", now - index, now - index)
                for index in range(10, 17)
            ],
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES ('copyability_evidence', ?, 'copyability', 'copyability',
                      20, 0, 'failed', NULL, 0, 3, 3, 0,
                      '{}', '{}', 'rate limit exhausted', ?, ?)
            """,
            ("0x" + "f" * 40, now - 20, now - 10),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    status_counts = {(row["job_type"], row["status"]): row["count"] for row in data["ops_health"]["job_status"]}
    progress = {row["job_type"]: row for row in data["ops_health"]["queue_progress"]}

    assert data["ops_health"]["health"] == "attention"
    assert data["ops_health"]["stale_running_count"] == 8
    assert len(data["ops_health"]["stale_running_samples"]) == 6
    assert data["ops_health"]["failed_job_samples"][0]["last_error"] == "rate limit exhausted"
    assert status_counts[("wallet_evidence_backfill", "queued")] == 1
    assert status_counts[("copyability_evidence", "running")] == 1
    assert progress["wallet_evidence_backfill"]["queued_count"] == 1
    assert progress["wallet_evidence_backfill"]["completed_1h"] == 1
    assert progress["wallet_evidence_backfill"]["completed_24h"] == 1
    assert progress["wallet_evidence_backfill"]["eta_label"]

    html = _render_dashboard(settings)
    runtime = _runtime_build_info()
    assert "系统健康" in html
    assert "运行版本" in html
    assert "源码装载" in html
    assert runtime["source_fingerprint"] in html
    assert "生产收敛详情" in html
    assert "Paper 候选" in html
    assert "复核处置分布" in html
    assert "高分阻塞分布" in html
    assert "失败任务样本" in html
    assert "rate limit exhausted" in html
    assert "高分待验证" in html
    assert "主阻塞" in html
    assert "来源质量摘要" in html
    assert "观察/Paper" in html
    assert "队列吞吐" in html
    assert "上游 API 调度" in html
    assert "上游冷却" in html
    assert "过期 running 样本" in html


def test_dashboard_ops_health_reports_runtime_loop_freshness(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        conn.executemany(
            """
            INSERT INTO ingest_runs(
                ingest_type, started_at, finished_at, status,
                wallets_attempted, wallets_succeeded, rows_written, error
            ) VALUES (?, ?, ?, ?, 0, 0, ?, ?)
            """,
            [
                ("wallet_pipeline_worker_0", now - 300, now - 290, "ok", 42, ""),
                ("copyability_evidence_worker_0_test", now - 120, now - 110, "ok", 9, ""),
                ("loop_discovery_leaderboard", now - 180, now - 170, "ok", 5, ""),
                ("loop_research_control", now - 7_200, now - 7_100, "ok", 12, ""),
                ("loop_maintenance", now - 60, now - 50, "failed", 0, "checkpoint failed"),
                (
                    "loop_research_control_step_wallet_pipeline_plan",
                    now - 80,
                    now - 65,
                    "failed",
                    0,
                    "database is locked",
                ),
                (
                    "loop_research_control_step_incremental_score",
                    now - 50,
                    now - 45,
                    "ok",
                    3,
                    "",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    loops = data["ops_health"]["runtime_loops"]
    by_key = {row["loop_key"]: row for row in loops["rows"]}
    phases = data["ops_health"]["research_control_steps"]
    by_step = {row["step_key"]: row for row in phases["rows"]}
    html = _render_dashboard(settings)

    assert data["ops_health"]["health"] == "attention"
    assert loops["state"] == "attention"
    assert loops["attention_count"] == 2
    assert by_key["wallet_pipeline_workers"]["state"] == "ok"
    assert by_key["copyability_workers"]["state"] == "ok"
    assert by_key["discovery_leaderboard"]["state"] == "ok"
    assert by_key["research_control"]["state"] == "stale"
    assert by_key["maintenance"]["state"] == "error"
    assert by_key["discovery_activity"]["state"] == "no_data"
    assert phases["has_data"] is True
    assert phases["attention_count"] == 1
    assert by_step["wallet_pipeline_plan"]["state"] == "error"
    assert by_step["wallet_pipeline_plan"]["duration_label"] == "15 秒"
    assert by_step["wallet_pipeline_plan"]["error"] == "database is locked"
    assert by_step["incremental_score"]["state"] == "ok"
    assert by_step["incremental_score"]["rows_written"] == 3
    assert "常驻循环新鲜度" in html
    assert "研究控制阶段" in html
    assert "钱包队列规划" in html
    assert "增量评分" in html
    assert "摘要/错误" in html
    assert "研究控制循环" in html
    assert "checkpoint failed" in html
    assert "database is locked" in html


def test_dashboard_ops_health_hides_research_control_steps_without_phase_heartbeats(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    data = dashboard_data(settings)
    html = _render_dashboard(settings)

    assert data["ops_health"]["research_control_steps"]["has_data"] is False
    assert "研究控制阶段" not in html


def test_dashboard_ops_health_distinguishes_normal_backlog_from_high_priority_gap(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    now = int(time.time())
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l1_light', 'queued', 120, 0.6, 50,
                      'light_done', 'medium_pending', 0, 120, 4, 80, ?)
            """,
            (wallet, now),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    backlog = data["ops_health"]["pipeline_backlog"]
    readiness = data["production_readiness"]
    html = web_module._evidence_pipeline_panel(data["evidence_pipeline"])

    assert data["ops_health"]["health"] == "ok"
    assert backlog["pending_without_active_job"] == 1
    assert backlog["high_priority_pending_without_active_job"] == 0
    assert backlog["by_action"][0]["next_action"] == "medium_pending"
    assert backlog["by_action"][0]["count"] == 1
    assert backlog["by_action"][0]["high_priority_count"] == 0
    assert readiness["evidence_pending"] == 1
    assert readiness["evidence_active_pending"] == 0
    assert readiness["evidence_state_pending"] == 1
    assert "受控背压" in html
    assert "调度候选" in html

    conn = connect(settings.db_path)
    try:
        conn.execute("UPDATE wallet_processing_state SET priority = 5 WHERE wallet = ?", (wallet,))
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    backlog = data["ops_health"]["pipeline_backlog"]
    html = _render_dashboard(settings)

    assert data["ops_health"]["health"] == "attention"
    assert "高优先级钱包等待补证据" in data["ops_health"]["note"]
    assert backlog["pending_without_active_job"] == 1
    assert backlog["high_priority_pending_without_active_job"] == 1
    assert backlog["high_priority_samples"][0]["wallet"] == wallet
    assert "待派证据状态" in html
    assert "高优先级漏派样本" in html


def test_dashboard_ops_health_ignores_future_scheduled_evidence_state(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    now = 1_800_000_000
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l1_light', 'queued', 120, 0.6, 5,
                      'light_done', 'medium_pending', ?, 120, 4, 80, ?)
            """,
            (wallet, now + 86_400, now),
        )
        conn.commit()
    finally:
        conn.close()

    data = dashboard_data(settings)
    backlog = data["ops_health"]["pipeline_backlog"]
    pipeline = data["evidence_pipeline"]

    assert data["ops_health"]["health"] == "ok"
    assert backlog["pending_without_active_job"] == 0
    assert backlog["high_priority_pending_without_active_job"] == 0
    assert pipeline["pending_state_without_active_job"] == 0
    assert pipeline["high_priority_pending_state_without_active_job"] == 0


def test_wallet_table_filters_by_stage_and_source(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    rows = wallet_table_rows(settings, stage="needs_manual_review", source="polymarket_trades_global")
    thin_rows = wallet_table_rows(settings, signal="review_thin_evidence")

    assert len(rows) == 1
    assert rows[0]["leader_score"] == 55.5
    assert rows[0]["activity_count"] == 1
    assert [row["address"] for row in thin_rows] == ["0xabc0000000000000000000000000000000000001"]


def test_storage_maintenance_summary_marks_large_wal_with_safe_commands(tmp_path):
    settings = RobotSettings(db_path=tmp_path / "pm_robot.sqlite", execution_mode="research")
    settings.db_path.write_bytes(b"x" * 100)
    Path(f"{settings.db_path}-wal").write_bytes(b"w" * 80)
    Path(f"{settings.db_path}-shm").write_bytes(b"s" * 10)

    summary = _storage_maintenance_summary(
        settings,
        wal_warn_bytes=50,
        wal_critical_bytes=70,
        low_free_disk_bytes=1,
    )
    html = _storage_maintenance_panel(summary)

    assert summary["state"] == "wal_critical"
    assert summary["needs_wal_window"] is True
    assert summary["critical_wal"] is True
    assert summary["wal_bytes"] == 80
    assert summary["wal_to_db_ratio"] == 0.8
    assert "./pmrobot-nas.sh wal-truncate-when-idle 7200 900 30" in html
    assert "./pmrobot-nas.sh wal-truncate-window 900" in html
    assert "WAL truncate 会临时停止 research/scoring 服务" in html


def test_storage_maintenance_summary_reports_missing_and_fresh_backups(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "pm_robot.sqlite",
        backup_dir=tmp_path / "backups",
        execution_mode="research",
    )
    settings.db_path.parent.mkdir(parents=True)
    settings.db_path.write_bytes(b"db")

    missing = _storage_maintenance_summary(
        settings,
        now=100_000,
        backup_max_age_seconds=90_000,
        low_free_disk_bytes=1,
        scheduled_backup_enabled=True,
    )
    assert missing["state"] == "backup_missing"
    assert missing["backup_count"] == 0
    assert missing["backup_fresh"] is False

    settings.backup_dir.mkdir(parents=True)
    backup = settings.backup_dir / "pm_robot-19700102-000000.sqlite"
    backup.write_bytes(b"backup")
    backup.touch()
    fresh = _storage_maintenance_summary(
        settings,
        now=int(backup.stat().st_mtime) + 60,
        backup_max_age_seconds=90_000,
        low_free_disk_bytes=1,
        scheduled_backup_enabled=True,
    )
    html = _storage_maintenance_panel(fresh)

    assert fresh["state"] == "ok"
    assert fresh["backup_count"] == 1
    assert fresh["backup_fresh"] is True
    assert fresh["latest_backup_name"] == backup.name
    assert "自动整库备份" in html
    assert "backup-now" in html


def test_storage_maintenance_summary_marks_stale_backup(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "pm_robot.sqlite",
        backup_dir=tmp_path / "backups",
        execution_mode="research",
    )
    settings.db_path.parent.mkdir(parents=True)
    settings.db_path.write_bytes(b"db")
    settings.backup_dir.mkdir(parents=True)
    backup = settings.backup_dir / "pm_robot-19700101-000000.sqlite"
    backup.write_bytes(b"backup")
    backup.touch()

    summary = _storage_maintenance_summary(
        settings,
        now=int(backup.stat().st_mtime) + 90_001,
        backup_max_age_seconds=90_000,
        low_free_disk_bytes=1,
        scheduled_backup_enabled=True,
    )
    assert summary["state"] == "backup_stale"
    assert summary["backup_fresh"] is False
    assert summary["latest_backup_age_seconds"] == 90_001


def test_storage_maintenance_summary_does_not_warn_when_scheduled_backups_are_paused(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "pm_robot.sqlite",
        backup_dir=tmp_path / "backups",
        execution_mode="research",
    )
    settings.db_path.parent.mkdir(parents=True)
    settings.db_path.write_bytes(b"db")

    summary = _storage_maintenance_summary(
        settings,
        now=100_000,
        low_free_disk_bytes=1,
        scheduled_backup_enabled=False,
    )
    html = _storage_maintenance_panel(summary)

    assert summary["state"] == "ok"
    assert summary["scheduled_backup_enabled"] is False
    assert "自动整库备份" in html
    assert "暂停" in html


def test_storage_maintenance_summary_exposes_parquet_archive_state(tmp_path):
    settings = RobotSettings(
        db_path=tmp_path / "data" / "pm_robot.sqlite",
        archive_dir=tmp_path / "data" / "parquet",
        execution_mode="research",
    )
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO evidence_archive_runs(
                run_id, status, archive_path, wallet_count, row_count,
                file_count, byte_size, created_at, pruned_at, updated_at
            ) VALUES ('archive-test', 'pruned', 'evidence/test', 3, 120, 4, 4096, 100, 200, 200)
            """
        )
        conn.commit()
    finally:
        conn.close()

    summary = _storage_maintenance_summary(
        settings,
        low_free_disk_bytes=1,
        scheduled_backup_enabled=False,
    )
    html = _storage_maintenance_panel(summary)

    assert summary["evidence_archive"]["wallet_count"] == 3
    assert summary["evidence_archive"]["row_count"] == 120
    assert summary["evidence_archive"]["byte_size"] == 4096
    assert "Parquet 冷归档" in html
    assert "3 钱包 / 120 行" in html


def test_discovery_data_builds_workbench_metrics(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    data = discovery_data(settings)
    funnel = {row["key"]: row["count"] for row in data["funnel"]}
    wallet = data["wallets"][0]

    assert funnel["discovered"] == 1
    assert funnel["activity_seen"] == 1
    assert data["source_quality"][0]["source"] == "polymarket_trades_global"
    assert wallet["discovery_priority"] > wallet["leader_score"]
    assert wallet["evidence_depth_label"] == "starter"
    assert data["signal_counts"][0]["signal"] == "needs_backfill"


def test_dashboard_groups_secondary_sections_into_workspace_tabs(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    html = _render_dashboard(settings)

    assert 'role="tablist"' in html
    assert 'data-workspace-tab="overview"' in html
    assert 'data-workspace-tab="discovery"' in html
    assert 'data-workspace-tab="paper"' in html
    assert 'data-workspace-tab="operations"' in html
    assert 'data-workspace-panel="overview"' in html
    assert 'data-workspace-panel="discovery" hidden' in html
    assert html.index("研究漏斗") < html.index("候选阶段")


def test_dashboard_starts_with_operator_outcomes_and_pipeline(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    html = _render_dashboard(settings)

    assert "NAS 研究与评分" in html
    assert "今日运行概览" in html
    assert "研究漏斗" in html
    assert "当前处理重点" in html
    assert "正式钱包" in html
    assert "24h 完成任务" in html
    assert html.index("今日运行概览") < html.index("高分待验证")
    assert html.index("研究漏斗") < html.index("生产收敛详情")


def test_dashboard_and_startup_prewarm_use_lightweight_summary(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    _seed(settings)
    original = web_module._dashboard_data_cached
    calls = []

    def capture(_settings, **kwargs):
        calls.append(kwargs)
        return original(_settings, **kwargs)

    monkeypatch.setattr(web_module, "_dashboard_data_cached", capture)

    _render_dashboard(settings)
    web_module._prewarm_dashboard_cache(settings)

    assert calls[0]["include_pair_quality"] is False
    assert calls[0]["include_heavy_audits"] is False
    assert calls[0]["return_none_while_warming"] is True
    assert calls[1]["include_pair_quality"] is False
    assert calls[1]["include_heavy_audits"] is False
    assert calls[1]["force_refresh"] is True


def test_dashboard_returns_immediate_warming_page_during_startup_prewarm(
    tmp_path,
    monkeypatch,
):
    settings = _settings(tmp_path)
    settings.db_path.touch()
    key = web_module._dashboard_cache_key(
        settings,
        include_pair_quality=False,
        include_heavy_audits=False,
    )
    with web_module._DASHBOARD_CACHE_LOCK:
        web_module._DASHBOARD_CACHE.pop(key, None)
        web_module._DASHBOARD_REFRESHING.add(key)
    monkeypatch.setattr(
        web_module,
        "dashboard_data",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not block")),
    )
    try:
        html = _render_dashboard(settings)
    finally:
        with web_module._DASHBOARD_CACHE_LOCK:
            web_module._DASHBOARD_REFRESHING.discard(key)

    assert "正在准备控制台" in html
    assert "后台研究任务仍在运行" in html
    assert "window.location.reload" in html


def test_lightweight_dashboard_defers_heavy_discovery_and_rtds_audits(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    data = web_module.dashboard_data(
        settings,
        include_pair_quality=False,
        include_heavy_audits=False,
    )
    html = _render_dashboard(settings)

    assert data["discovery_freshness"]["summary_mode"] == "fast"
    assert data["rtds_watch_audit"]["deferred"] is True
    assert "重型实时审计已从首屏延后" in html
    assert "打开 RTDS Watch 审计" in html


def test_dashboard_localizes_internal_status_badges(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    html = _render_dashboard(settings)

    assert ">自动复核中<" in html
    assert ">L3 深度证据<" in _localized_cell("l3_deep")
    assert ">证据摘要就绪<" in _localized_cell("summary_ready")
    assert ">Paper 候选<" in _badge("paper_candidate")
    assert _format_cell("active") == "active"


def test_wallet_workbench_places_candidate_queue_before_research_diagnostics(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    html = _render_wallets(settings, stage="", source="", query="", signal="")

    assert "候选队列" in html
    assert "研究诊断" in html
    assert html.index("候选队列") < html.index("研究诊断")
    assert html.index("候选队列") < html.index("证据深度")


def test_wallet_workbench_uses_operator_friendly_filters(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    html = _render_wallets(settings, stage="needs_manual_review", source="", query="", signal="")

    assert '<select name="stage"' in html
    assert '<select name="source"' in html
    assert 'value="needs_manual_review" selected' in html
    assert "自动复核中" in html
    assert "输入地址、标签或备注" in html
    assert 'class="wallet-table"' in html


def test_source_filtered_discovery_includes_focus_diagnostics(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)

    data = discovery_data(settings, source="polymarket_trades_global")
    focus = data["source_focus"]
    html = _render_wallets(settings, stage="", source="polymarket_trades_global", query="", signal="")

    assert focus["source"] == "polymarket_trades_global"
    assert focus["matched_wallets"] == 1
    assert focus["blockers"][0]["blocker"] == "历史证据偏薄"
    assert focus["top_wallets"][0]["address"] == "0xabc0000000000000000000000000000000000001"
    assert focus["top_wallets"][0]["blocker_label"] == "历史证据偏薄"
    assert "当前来源诊断" in html
    assert "来源阻塞分布" in html
    assert "来源高分样本" in html
    assert "/api/discovery?" in html


def test_source_filtered_signal_counts_use_the_same_source_scope(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    conn = connect(settings.db_path)
    other_wallet = "0xdef0000000000000000000000000000000000002"
    try:
        now = 1_800_000_000
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'other_source', 'other', '', '', 'active',
                      'paper_candidate', ?, ?)
            """,
            (other_wallet, now - 100, now),
        )
        conn.execute(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links,
                evidence_json, observed_at, recorded_at
            ) VALUES (?, 'other_source', 'active', 'other', '', '', '{}', ?, ?)
            """,
            (other_wallet, now - 90, now - 80),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 88.0, 'paper_candidate', 'other high score',
                      '{}', '{}', 'test', ?)
            """,
            (other_wallet, now),
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 1,
                      'deep_done', 'score_wallet', 0, 1000, 20, 900, ?)
            """,
            (other_wallet, now),
        )
        conn.execute(
            """
            INSERT INTO evidence_backfill_budget(
                wallet, source, priority, stage, target_depth, current_depth,
                next_attempt_at, evidence_json, created_at, updated_at
            ) VALUES (?, 'test', 1, 'deep_done', 1000, 1000, 0, '{}', ?, ?)
            """,
            (other_wallet, now, now),
        )
        conn.execute(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, 3, 20, 4, 0.6, 10, 2, ?, ?)
            """,
            (other_wallet, now, now),
        )
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready,
                blockers_json, updated_at
            ) VALUES (?, 4, 0, 2, 2, 0, 1.0, 100, 20, 0.2,
                      22, 0.22, 0, '[]', ?)
            """,
            (other_wallet, now),
        )
        conn.commit()
    finally:
        conn.close()

    first_source = discovery_data(settings, source="polymarket_trades_global")
    other_source = discovery_data(settings, source="other_source")
    first_funnel = {row["key"]: row["count"] for row in first_source["funnel"]}
    other_funnel = {row["key"]: row["count"] for row in other_source["funnel"]}
    first_stage_counts = {row["stage"]: row["count"] for row in first_source["stage_counts"]}
    other_stage_counts = {row["stage"]: row["count"] for row in other_source["stage_counts"]}
    first_counts = {row["signal"]: row["count"] for row in first_source["signal_counts"]}
    other_counts = {row["signal"]: row["count"] for row in other_source["signal_counts"]}
    other_rows = wallet_table_rows(settings, source="other_source")

    assert first_source["wallet_count"] == 1
    assert first_source["wallet_total_count"] == 1
    assert first_funnel["discovered"] == 1
    assert first_funnel["evidence_ready"] == 0
    assert first_source["evidence_depth"]["starter"] == 1
    assert first_source["evidence_depth"]["deep"] == 0
    assert first_stage_counts == {"needs_manual_review": 1}
    assert first_counts["high_score"] == 1
    assert first_counts["paper_signal"] == 1
    assert other_source["wallet_count"] == 1
    assert other_source["wallet_total_count"] == 1
    assert other_funnel["discovered"] == 1
    assert other_funnel["evidence_ready"] == 1
    assert other_funnel["copy_signal"] == 1
    assert other_funnel["paper_pool"] == 1
    assert other_source["evidence_depth"]["starter"] == 0
    assert other_source["evidence_depth"]["deep"] == 1
    assert other_stage_counts == {"paper_candidate": 1}
    assert other_counts["high_score"] == 1
    assert other_counts["copy_signal"] == 1
    assert other_counts["paper_signal"] == 1
    assert other_counts["needs_backfill"] == 0
    assert other_counts["thin_evidence"] == 0
    assert other_rows[0]["evidence_tier"] == "l3_deep"
    assert other_rows[0]["evidence_status"] == "summary_ready"


def test_source_filters_do_not_match_source_name_substrings(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    conn = connect(settings.db_path)
    try:
        now = 1_800_000_000
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0xdef0000000000000000000000000000000000002",
                "polymarket_trades_global_invalid_short_address",
                "invalid",
                "source substring collision",
                "",
                "invalid_source_address",
                "rejected",
                now - 100,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO candidate_source_events(
                address, source, status, labels, notes, links,
                evidence_json, observed_at, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "0xdef0000000000000000000000000000000000002",
                "polymarket_trades_global_invalid_short_address",
                "invalid_source_address",
                "invalid",
                "",
                "",
                "{}",
                now - 90,
                now - 80,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    rows = wallet_table_rows(settings, source="polymarket_trades_global")
    data = discovery_data(settings, source="polymarket_trades_global")

    assert [row["address"] for row in rows] == ["0xabc0000000000000000000000000000000000001"]
    assert data["source_focus"]["matched_wallets"] == 1
    assert data["wallet_count"] == 1


def test_wallet_table_signal_filters_copy_candidates(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO copy_leader_stats(
                leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
                containment_pct_median, median_lag_seconds, qualified_follower_count,
                last_copy_event_at, updated_at
            ) VALUES (?, 2, 16, 3, 0.5, 12, 1, 1800000000, 1800000000)
            """,
            ("0xabc0000000000000000000000000000000000001",),
        )
        conn.commit()
    finally:
        conn.close()

    rows = wallet_table_rows(settings, signal="copy_signal")

    assert len(rows) == 1
    assert rows[0]["qualified_follower_count"] == 1


def test_wallet_detail_includes_features_activity_and_quality(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, distinct_markets,
                non_fast_trade_count, updated_at
            ) VALUES (?, 'l2_medium', 'needs_deep', 800, 0.82, 10,
                      'medium_done', 'deep_pending', 0, 800, 12, 220, 1800000000)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO pipeline_jobs(
                job_type, wallet, subject_key, tier, priority, shard, status,
                lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
                input_json, output_json, last_error, created_at, updated_at
            ) VALUES ('wallet_evidence_backfill', ?, 'deep_pending', 'l2_medium',
                      10, 0, 'failed', NULL, 0, 3, 3, 0,
                      '{}', '{}', 'upstream timeout', 1799999900, 1800000001)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 48.0, 'needs_data', 'history_incomplete', '{}', '{}',
                      'test-old', 1799999900)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 55.5, 'needs_manual_review', 'thin but promising', '{}', '{}',
                      'test', 1800000001)
            """,
            (wallet,),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, 'needs_data', 'needs_manual_review', 'evidence_improved', 1800000000)
            """,
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    detail = wallet_detail_data(settings, wallet)
    html = _render_wallet_detail(settings, wallet)

    assert detail["found"] is True
    assert detail["latest_score"]["review_reason"] == "thin but promising"
    assert detail["feature"]["extra"]["materialized"] is True
    assert detail["recent_activity"][0]["market_slug"] == "test-market"
    assert detail["paper_quality"]["orders"] == 3
    assert detail["processing_state"]["discovery_tier"] == "l2_medium"
    assert detail["processing_state"]["next_action"] == "deep_pending"
    assert detail["pipeline_jobs"][0]["last_error"] == "upstream timeout"
    assert detail["processing_diagnostic"]["state"] == "critical"
    assert detail["processing_diagnostic"]["headline"] == "最近一次证据任务失败"
    assert len(detail["score_history"]) == 3
    assert any(row["review_reason"] == "history_incomplete" for row in detail["score_history"])
    assert detail["review_events"][0]["reason"] == "evidence_improved"
    assert detail["history_timeline"][0]["event_type"] == "score"
    assert "证据处理状态" in html
    assert "任务历史" in html
    assert "upstream timeout" in html
    assert "最近一次证据任务失败" in html
    assert "历史证据" in html
    assert "补深度历史" in html
    assert "重试 / 租约" in html
    assert "评分与阶段时间线" in html
    assert "自动复核中" in html


def test_wallet_pipeline_diagnostic_distinguishes_stale_lease_and_retry_wait():
    candidate = {"candidate_stage": "needs_manual_review"}
    processing_state = {
        "evidence_status": "queued",
        "discovery_tier": "l2_medium",
        "next_action": "deep_pending",
        "next_action_at": 90,
    }
    stale = _wallet_pipeline_diagnostic(
        candidate,
        processing_state,
        [
            {
                "job_type": "wallet_evidence_backfill",
                "status": "running",
                "lease_until": 99,
                "attempts": 1,
                "max_attempts": 3,
            }
        ],
        now=100,
    )
    waiting = _wallet_pipeline_diagnostic(
        candidate,
        processing_state,
        [
            {
                "job_type": "wallet_evidence_backfill",
                "status": "queued",
                "next_attempt_at": 3700,
                "attempts": 1,
                "max_attempts": 3,
            }
        ],
        now=100,
    )

    assert stale["state"] == "critical"
    assert stale["headline"] == "运行租约已经过期"
    assert waiting["state"] == "attention"
    assert waiting["headline"] == "任务正在等待重试窗口"
    assert "小时后可再次领取" in waiting["suggested_action"]


def test_wallet_timeline_escapes_malformed_score_values(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    wallet = "0xabc0000000000000000000000000000000000001"
    conn = connect(settings.db_path)
    try:
        conn.execute(
            "UPDATE leader_scores SET leader_score = ? WHERE address = ?",
            ("<script>alert(1)</script>", wallet),
        )
        conn.commit()
    finally:
        conn.close()

    html = _render_wallet_detail(settings, wallet)

    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_wallet_detail_history_queries_have_long_running_indexes(tmp_path):
    settings = _settings(tmp_path)
    _seed(settings)
    conn = connect(settings.db_path)
    try:
        pipeline_indexes = {
            row["name"]
            for row in conn.execute("PRAGMA index_list('pipeline_jobs')").fetchall()
        }
        review_indexes = {
            row["name"]
            for row in conn.execute("PRAGMA index_list('review_events')").fetchall()
        }
        pipeline_plan = " ".join(
            row["detail"]
            for row in conn.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT job_id FROM pipeline_jobs
                WHERE wallet = ?
                ORDER BY updated_at DESC, job_id DESC
                LIMIT 30
                """,
                ("0xabc0000000000000000000000000000000000001",),
            ).fetchall()
        )
        review_plan = " ".join(
            row["detail"]
            for row in conn.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT event_id FROM review_events
                WHERE address = ?
                ORDER BY created_at DESC, event_id DESC
                LIMIT 20
                """,
                ("0xabc0000000000000000000000000000000000001",),
            ).fetchall()
        )
        failed_jobs_plan = " ".join(
            row["detail"]
            for row in conn.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT job_id FROM pipeline_jobs
                WHERE status = 'failed'
                ORDER BY updated_at DESC, priority ASC, job_id DESC
                LIMIT 10
                """
            ).fetchall()
        )
    finally:
        conn.close()

    assert "idx_pipeline_jobs_wallet_updated" in pipeline_indexes
    assert "idx_pipeline_jobs_status_updated" in pipeline_indexes
    assert "idx_review_events_address_created" in review_indexes
    assert "idx_pipeline_jobs_wallet_updated" in pipeline_plan
    assert "idx_pipeline_jobs_status_updated" in failed_jobs_plan
    assert "idx_review_events_address_created" in review_plan
