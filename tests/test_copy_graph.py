from pathlib import Path

from pm_robot.config import load_policy
from pm_robot.models import CandidateAddress
from pm_robot.research.copy_backtest import backtest_copy_stream, backtest_copy_stream_for_leaders
from pm_robot.research.copy_graph import (
    mine_copy_graph,
    mine_copy_graph_for_leaders,
    prune_unqualified_copy_links_for_leaders,
)
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    get_wallet_features,
    persist_wallet_activity,
    rebuild_wallet_episodes,
    upsert_candidate,
)


def _mark_deep_evidence_ready(conn, wallet: str) -> None:
    activity_count = conn.execute(
        "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
        (wallet,),
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, current_stage,
            activity_count, updated_at
        ) VALUES (?, 'l3_deep', 'summary_ready', 'deep_done', ?, 1)
        ON CONFLICT(wallet) DO UPDATE SET
            discovery_tier = excluded.discovery_tier,
            evidence_status = excluded.evidence_status,
            current_stage = excluded.current_stage,
            activity_count = excluded.activity_count,
            updated_at = excluded.updated_at
        """,
        (wallet, activity_count),
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
        _mark_deep_evidence_ready(conn, follower)
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
        _mark_deep_evidence_ready(conn, follower)

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
        _mark_deep_evidence_ready(conn, follower)
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


def test_targeted_backtest_preserves_last_good_result_when_no_new_settlement(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "3" * 40
    follower = "0x" + "4" * 40
    policy = load_policy(Path("config/leader_scoring_policy.json"))
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        leader_events = []
        follower_events = []
        for idx in range(5):
            opened = {
                "timestamp": 20_000 + idx * 100,
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
            leader_events.extend(
                [
                    opened,
                    {
                        **opened,
                        "timestamp": opened["timestamp"] + 50,
                        "side": "SELL",
                        "price": 0.8,
                        "usdcSize": 8,
                        "transactionHash": f"0xleadersell{idx}",
                    },
                ]
            )
            follower_events.append(
                {
                    **opened,
                    "timestamp": opened["timestamp"] + 5,
                    "transactionHash": f"0xfollowerbuy{idx}",
                }
            )
        persist_wallet_activity(conn, leader, leader_events, ingested_at=30_000)
        persist_wallet_activity(conn, follower, follower_events, ingested_at=30_000)
        _mark_deep_evidence_ready(conn, follower)
        rebuild_wallet_episodes(conn, leader)
        mine_copy_graph_for_leaders(conn, policy, [leader], now=30_000)
        first = backtest_copy_stream_for_leaders(conn, policy, [leader], now=30_000)
        original_performance = dict(
            conn.execute(
                "SELECT * FROM copy_leader_performance WHERE leader_wallet = ?",
                (leader,),
            ).fetchone()
        )
        original_trade_count = conn.execute(
            "SELECT COUNT(*) FROM copy_backtest_trades WHERE leader_wallet = ?",
            (leader,),
        ).fetchone()[0]

        conn.execute(
            "UPDATE wallet_episodes SET status = 'open' WHERE address = ?",
            (leader,),
        )
        conn.commit()
        refreshed = backtest_copy_stream_for_leaders(
            conn,
            policy,
            [leader],
            now=40_000,
            preserve_existing_on_empty=True,
        )
        preserved_performance = dict(
            conn.execute(
                "SELECT * FROM copy_leader_performance WHERE leader_wallet = ?",
                (leader,),
            ).fetchone()
        )

        assert first.leader_performance_written == 1
        assert refreshed.trades_written == 0
        assert refreshed.leader_performance_written == 0
        assert refreshed.leaders_preserved_on_empty == 1
        assert preserved_performance == original_performance
        assert conn.execute(
            "SELECT COUNT(*) FROM copy_backtest_trades WHERE leader_wallet = ?",
            (leader,),
        ).fetchone()[0] == original_trade_count
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
        _mark_deep_evidence_ready(conn, follower)
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
        _mark_deep_evidence_ready(conn, follower)

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


def test_copy_pair_waits_for_complete_follower_evidence(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "5" * 40
    follower = "0x" + "6" * 40
    policy = load_policy(Path("config/leader_scoring_policy.json"))
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        leader_events = []
        follower_events = []
        for idx in range(5):
            event = {
                "timestamp": 40_000 + idx * 100,
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
            leader_events.append(event)
            follower_events.append(
                {
                    **event,
                    "timestamp": event["timestamp"] + 5,
                    "transactionHash": f"0xfollower{idx}",
                }
            )
        persist_wallet_activity(conn, leader, leader_events, ingested_at=50_000)
        persist_wallet_activity(conn, follower, follower_events, ingested_at=50_000)

        incomplete = mine_copy_graph_for_leaders(conn, policy, [leader], now=50_000)
        incomplete_pair = conn.execute(
            "SELECT qualifies FROM copy_pair_stats WHERE leader_wallet = ? AND follower_wallet = ?",
            (leader, follower),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, current_stage,
                activity_count, distinct_markets, non_fast_trade_count, updated_at
            ) VALUES (?, 'l1_light', 'summary_ready', 'deep_done', 5, 5, 5, 1)
            """,
            (follower,),
        )
        shallow = mine_copy_graph_for_leaders(conn, policy, [leader], now=55_000)
        shallow_pair = conn.execute(
            "SELECT qualifies FROM copy_pair_stats WHERE leader_wallet = ? AND follower_wallet = ?",
            (leader, follower),
        ).fetchone()
        _mark_deep_evidence_ready(conn, follower)
        conn.execute(
            "UPDATE wallet_processing_state SET current_stage = '' WHERE wallet = ?",
            (follower,),
        )
        complete = mine_copy_graph_for_leaders(conn, policy, [leader], now=60_000)
        complete_pair = conn.execute(
            "SELECT qualifies FROM copy_pair_stats WHERE leader_wallet = ? AND follower_wallet = ?",
            (leader, follower),
        ).fetchone()

        assert incomplete.qualified_pairs == 0
        assert incomplete_pair["qualifies"] == 0
        assert shallow.qualified_pairs == 0
        assert shallow_pair["qualifies"] == 0
        assert complete.qualified_pairs == 1
        assert complete_pair["qualifies"] == 1
    finally:
        conn.close()


def test_targeted_copy_graph_prunes_unqualified_raw_links_but_keeps_summary(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "7" * 40
    follower = "0x" + "8" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        event = {
            "timestamp": 50_000,
            "conditionId": "condition-one",
            "eventSlug": "event-one",
            "slug": "market-one",
            "asset": "asset-one",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY",
            "price": 0.5,
            "size": 10,
            "usdcSize": 5,
            "transactionHash": "0xleader-one",
        }
        persist_wallet_activity(conn, leader, [event], ingested_at=60_000)
        persist_wallet_activity(
            conn,
            follower,
            [{**event, "timestamp": 50_005, "transactionHash": "0xfollower-one"}],
            ingested_at=60_000,
        )

        summary = mine_copy_graph_for_leaders(
            conn,
            load_policy(Path("config/leader_scoring_policy.json")),
            [leader],
            now=60_000,
        )
        deleted = prune_unqualified_copy_links_for_leaders(conn, [leader])
        pair = conn.execute(
            "SELECT copy_event_count, qualifies FROM copy_pair_stats WHERE leader_wallet = ?",
            (leader,),
        ).fetchone()
        remaining_links = conn.execute(
            "SELECT COUNT(*) FROM copy_trade_links WHERE leader_wallet = ?",
            (leader,),
        ).fetchone()[0]

        assert summary.links_written == 1
        assert summary.qualified_pairs == 0
        assert deleted == 1
        assert remaining_links == 0
        assert dict(pair) == {"copy_event_count": 1, "qualifies": 0}
    finally:
        conn.close()


def test_targeted_copy_graph_uses_persisted_reverse_pair_summary(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "9" * 40
    follower = "0x" + "a" * 40
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
        event = {
            "timestamp": 70_000,
            "conditionId": "condition-reverse",
            "eventSlug": "event-reverse",
            "slug": "market-reverse",
            "asset": "asset-reverse",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY",
            "price": 0.5,
            "size": 10,
            "usdcSize": 5,
            "transactionHash": "0xleader-reverse",
        }
        persist_wallet_activity(conn, leader, [event], ingested_at=80_000)
        persist_wallet_activity(
            conn,
            follower,
            [{**event, "timestamp": 70_005, "transactionHash": "0xfollower-reverse"}],
            ingested_at=80_000,
        )
        conn.execute(
            """
            INSERT INTO copy_pair_stats(
                leader_wallet, follower_wallet, copy_event_count, copy_market_count,
                follower_trade_count, containment_pct, leader_precedes_pct,
                median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
            ) VALUES (?, ?, 9, 1, 9, 1, 0.9, 5, 60000, 69000, 0, 80000)
            """,
            (follower, leader),
        )
        activity_ids = {
            str(row["address"]): int(row["activity_id"])
            for row in conn.execute(
                "SELECT address, activity_id FROM wallet_activity WHERE address IN (?, ?)",
                (leader, follower),
            )
        }
        conn.execute(
            """
            INSERT INTO copy_trade_links(
                leader_wallet, follower_wallet, leader_activity_id, follower_activity_id,
                condition_id, market_slug, asset_id, outcome, side,
                leader_ts, follower_ts, lag_seconds, created_at
            ) VALUES (?, ?, ?, ?, '', '', '', '', 'BUY', 60000, 60005, 5, 80000)
            """,
            (follower, leader, activity_ids[follower], activity_ids[leader]),
        )
        conn.commit()

        mine_copy_graph_for_leaders(
            conn,
            load_policy(Path("config/leader_scoring_policy.json")),
            [leader],
            now=80_000,
        )
        pair = conn.execute(
            """
            SELECT copy_event_count, leader_precedes_pct
            FROM copy_pair_stats
            WHERE leader_wallet = ? AND follower_wallet = ?
            """,
            (leader, follower),
        ).fetchone()

        assert pair["copy_event_count"] == 1
        assert pair["leader_precedes_pct"] == 0.1
    finally:
        conn.close()
