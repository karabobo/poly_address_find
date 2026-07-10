from pathlib import Path

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress
from pm_robot.research.copy_backtest import backtest_copy_stream
from pm_robot.research.copy_graph import mine_copy_graph
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    get_wallet_features,
    persist_wallet_activity,
    rebuild_wallet_episodes,
    upsert_candidate,
)


def test_mine_copy_graph_promotes_qualified_leader(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "a" * 40
    follower = "0x" + "b" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        leader_events = []
        follower_events = []
        for idx in range(5):
            event = {
                "timestamp": 1_000 + idx * 100,
                "conditionId": f"condition-{idx}",
                "eventSlug": f"event-{idx}",
                "slug": f"market-{idx}",
                "asset": f"asset-{idx}",
                "outcome": "YES",
                "type": "TRADE",
                "side": "BUY",
                "price": 0.55,
                "size": 10,
                "usdcSize": 5.5,
                "transactionHash": f"0xleader{idx}",
            }
            copied = {**event, "timestamp": event["timestamp"] + 5, "transactionHash": f"0xfollower{idx}"}
            closed = {
                **event,
                "timestamp": event["timestamp"] + 50,
                "side": "SELL",
                "price": 0.7,
                "usdcSize": 7,
                "transactionHash": f"0xleaderclose{idx}",
            }
            leader_events.append(event)
            leader_events.append(closed)
            follower_events.append(copied)
        persist_wallet_activity(conn, leader, leader_events, ingested_at=2_000)
        persist_wallet_activity(conn, follower, follower_events, ingested_at=2_000)
        rebuild_wallet_episodes(conn, leader)

        summary = mine_copy_graph(conn, load_policy(Path("config/leader_scoring_policy.json")))
        features = get_wallet_features(conn)

        assert summary.links_written == 5
        assert summary.qualified_pairs == 1
        assert features[leader].leader_in_degree == 1
        assert features[leader].copy_event_count == 5
        assert features[leader].copy_market_count == 5
        assert features[leader].containment_pct_median == 1
        assert follower not in features or features[follower].leader_in_degree is None
    finally:
        conn.close()


def test_mine_copy_graph_allows_multi_strategy_followers(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "e" * 40
    follower = "0x" + "f" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        leader_events = []
        follower_events = []
        for idx in range(5):
            event = {
                "timestamp": 1_000 + idx * 100,
                "conditionId": f"condition-{idx}",
                "eventSlug": f"event-{idx}",
                "slug": f"market-{idx}",
                "asset": f"asset-{idx}",
                "outcome": "YES",
                "type": "TRADE",
                "side": "BUY",
                "price": 0.55,
                "size": 10,
                "usdcSize": 5.5,
                "transactionHash": f"0xleader{idx}",
            }
            leader_events.append(event)
            follower_events.append(
                {
                    **event,
                    "timestamp": event["timestamp"] + 5,
                    "transactionHash": f"0xfollowercopy{idx}",
                }
            )
        for idx in range(20):
            follower_events.append(
                {
                    "timestamp": 1_010 + idx * 20,
                    "conditionId": f"other-condition-{idx}",
                    "eventSlug": f"other-event-{idx}",
                    "slug": f"other-market-{idx}",
                    "asset": f"other-asset-{idx}",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.45,
                    "size": 1,
                    "usdcSize": 0.45,
                    "transactionHash": f"0xfollowerother{idx}",
                }
            )
        persist_wallet_activity(conn, leader, leader_events, ingested_at=2_000)
        persist_wallet_activity(conn, follower, follower_events, ingested_at=2_000)

        summary = mine_copy_graph(conn, load_policy(Path("config/leader_scoring_policy.json")))
        pair = conn.execute(
            "SELECT * FROM copy_pair_stats WHERE leader_wallet = ? AND follower_wallet = ?",
            (leader, follower),
        ).fetchone()

        assert summary.links_written == 5
        assert summary.qualified_pairs == 1
        assert pair["copy_event_count"] == 5
        assert pair["copy_market_count"] == 5
        assert pair["follower_trade_count"] == 25
        assert pair["containment_pct"] == 0.2
        assert pair["qualifies"] == 1
    finally:
        conn.close()


def test_backtest_copy_stream_merges_positive_roi(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "c" * 40
    follower = "0x" + "d" * 40
    policy = load_policy(Path("config/leader_scoring_policy.json"))
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        leader_events = []
        follower_events = []
        for idx in range(5):
            opened = {
                "timestamp": 10_000 + idx * 100,
                "conditionId": f"condition-{idx}",
                "eventSlug": f"event-{idx}",
                "slug": f"market-{idx}",
                "asset": f"asset-{idx}",
                "outcome": "YES",
                "type": "TRADE",
                "side": "BUY",
                "price": 0.5,
                "size": 10,
                "usdcSize": 5,
                "transactionHash": f"0xleaderbuy{idx}",
            }
            closed = {
                **opened,
                "timestamp": opened["timestamp"] + 50,
                "side": "SELL",
                "price": 0.8,
                "usdcSize": 8,
                "transactionHash": f"0xleadersell{idx}",
            }
            copied = {
                **opened,
                "timestamp": opened["timestamp"] + 5,
                "transactionHash": f"0xfollowerbuy{idx}",
            }
            leader_events.extend([opened, closed])
            follower_events.append(copied)
        persist_wallet_activity(conn, leader, leader_events, ingested_at=20_000)
        persist_wallet_activity(conn, follower, follower_events, ingested_at=20_000)
        rebuild_wallet_episodes(conn, leader)
        mine_copy_graph(conn, policy)

        summary = backtest_copy_stream(conn, policy)
        features = get_wallet_features(conn)

        assert summary.trades_written == 5
        assert summary.leader_performance_written == 1
        assert features[leader].copy_stream_roi is not None
        assert features[leader].copy_stream_roi > 0
        assert features[leader].edge_retention_pct is not None
        assert features[leader].edge_retention_pct > 0
        assert features[leader].walk_forward_consistency_pct == 100
        assert features[leader].survival_score == 100
    finally:
        conn.close()


def test_backtest_uses_gamma_settlement_for_open_episode(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "1" * 40
    follower = "0x" + "2" * 40
    policy = load_policy(Path("config/leader_scoring_policy.json"))
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        leader_events = []
        follower_events = []
        for idx in range(5):
            opened = {
                "timestamp": 30_000 + idx * 100,
                "conditionId": f"condition-{idx}",
                "eventSlug": f"event-{idx}",
                "slug": f"settled-market-{idx}",
                "asset": f"winning-asset-{idx}",
                "outcome": "YES",
                "type": "TRADE",
                "side": "BUY",
                "price": 0.8,
                "size": 5,
                "usdcSize": 4,
                "transactionHash": f"0xopenleader{idx}",
            }
            dca = {
                **opened,
                "timestamp": opened["timestamp"] + 1,
                "price": 0.2,
                "size": 5,
                "usdcSize": 1,
                "transactionHash": f"0xdcabuy{idx}",
            }
            leader_events.append(opened)
            leader_events.append(dca)
            follower_events.append(
                {
                    **opened,
                    "timestamp": opened["timestamp"] + 5,
                    "transactionHash": f"0xopenfollower{idx}",
                }
            )
            follower_events.append(
                {
                    **dca,
                    "timestamp": dca["timestamp"] + 5,
                    "transactionHash": f"0xdcafollower{idx}",
                }
            )
            conn.execute(
                """
                INSERT INTO gamma_market_cache(
                    market_slug, condition_id, event_slug, question, title, category,
                    end_date, closed, active, archived, clob_token_ids_json,
                    outcomes_json, outcome_prices_json, raw_json, fetched_at, expires_at
                ) VALUES (?, ?, ?, '', '', '', '', 1, 0, 0, ?, '["YES","NO"]',
                          '[1,0]', '{}', 40000, 4102444800)
                """,
                (
                    opened["slug"],
                    opened["conditionId"],
                    opened["eventSlug"],
                    f'["{opened["asset"]}","losing-asset-{idx}"]',
                ),
            )
        persist_wallet_activity(conn, leader, leader_events, ingested_at=40_000)
        persist_wallet_activity(conn, follower, follower_events, ingested_at=40_000)
        rebuild_wallet_episodes(conn, leader)
        mine_copy_graph(conn, policy)

        summary = backtest_copy_stream(conn, policy)
        features = get_wallet_features(conn)
        trade = conn.execute("SELECT * FROM copy_backtest_trades LIMIT 1").fetchone()

        assert summary.trades_written == 20
        assert summary.leaders_with_positive_net_roi == 1
        assert trade["leader_episode_roi"] == 1
        assert features[leader].copy_stream_roi > 0
    finally:
        conn.close()


def test_containment_uses_pair_overlap_window_not_full_history(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "e" * 40
    follower = "0x" + "f" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        leader_events = []
        follower_events = []
        # Old unrelated follower history must not dilute containment.
        for idx in range(100):
            follower_events.append(
                {
                    "timestamp": 100 + idx,
                    "conditionId": "old-condition",
                    "eventSlug": "old-event",
                    "slug": "old-market",
                    "asset": "old-asset",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 1,
                    "usdcSize": 0.5,
                    "transactionHash": f"0xold{idx}",
                }
            )
        for idx in range(5):
            opened = {
                "timestamp": 10_000 + idx * 100,
                "conditionId": f"condition-{idx}",
                "eventSlug": f"event-{idx}",
                "slug": f"market-{idx}",
                "asset": f"asset-{idx}",
                "outcome": "YES",
                "type": "TRADE",
                "side": "BUY",
                "price": 0.5,
                "size": 10,
                "usdcSize": 5,
                "transactionHash": f"0xleader{idx}",
            }
            leader_events.append(opened)
            follower_events.append(
                {
                    **opened,
                    "timestamp": opened["timestamp"] + 5,
                    "transactionHash": f"0xfollower{idx}",
                }
            )
        persist_wallet_activity(conn, leader, leader_events, ingested_at=20_000)
        persist_wallet_activity(conn, follower, follower_events, ingested_at=20_000)

        summary = mine_copy_graph(conn, load_policy(Path("config/leader_scoring_policy.json")))
        pair = conn.execute(
            "SELECT * FROM copy_pair_stats WHERE leader_wallet = ? AND follower_wallet = ?",
            (leader, follower),
        ).fetchone()

        assert summary.qualified_pairs == 1
        assert pair["follower_trade_count"] == 5
        assert pair["containment_pct"] == 1
    finally:
        conn.close()
