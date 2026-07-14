import json

import pytest

from pm_robot.models import CandidateAddress
from pm_robot.orchestration.copyability_evidence import plan_copyability_evidence_jobs
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import upsert_candidate


def _seed_copyability_derivatives(
    conn,
    *,
    leader: str,
    follower: str,
    qualifies: int,
) -> None:
    upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
    upsert_candidate(conn, CandidateAddress(address=follower, sources="test"))
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, updated_at
        ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10,
                  'deep_done', '', 0, 1000, 100)
        """,
        (leader,),
    )
    conn.execute(
        "UPDATE candidate_wallets SET candidate_stage = 'blocked_copyability' WHERE address = ?",
        (leader,),
    )
    conn.execute(
        """
        INSERT INTO copy_pair_stats(
            leader_wallet, follower_wallet, copy_event_count, copy_market_count,
            follower_trade_count, containment_pct, leader_precedes_pct,
            median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
        ) VALUES (?, ?, 12, 8, 40, 0.4, 1.0, 2, 100, 200, ?, 300)
        """,
        (leader, follower, qualifies),
    )
    conn.execute(
        """
        INSERT INTO copy_leader_stats(
            leader_wallet, leader_in_degree, copy_event_count, copy_market_count,
            containment_pct_median, median_lag_seconds, qualified_follower_count,
            last_copy_event_at, updated_at
        ) VALUES (?, 1, 12, 8, 0.4, 2, 1, 200, 300)
        """,
        (leader,),
    )
    conn.execute(
        """
        INSERT INTO copy_leader_performance(
            leader_wallet, backtest_trade_count, copied_market_count,
            total_stake_usdc, gross_pnl_usdc, net_pnl_usdc,
            gross_roi, net_roi, win_rate, median_lag_seconds,
            last_backtest_trade_at, updated_at, edge_retention_pct,
            walk_forward_consistency_pct, max_drawdown_pct
        ) VALUES (?, 12, 8, 120, 30, 25, 0.25, 0.2083, 0.75, 2,
                  200, 300, 80, 70, 0.1)
        """,
        (leader,),
    )
    activity_id = conn.execute(
        """
        INSERT INTO wallet_activity(
            address, timestamp, condition_id, market_slug, asset_id, outcome,
            type, side, price, size, usdc_size, transaction_hash, raw_json, ingested_at
        ) VALUES (?, 100, 'condition', 'market', 'asset', 'YES', 'TRADE',
                  'BUY', 0.5, 20, 10, ?, '{}', 300)
        """,
        (leader, f"0x{leader[2:]:0>64}"),
    ).lastrowid
    episode_id = conn.execute(
        """
        INSERT INTO wallet_episodes(
            address, condition_id, market_slug, asset_id, outcome, first_ts,
            last_ts, buy_count, dca_entries, bought_usdc, net_shares,
            avg_entry_price, realized_pnl_est, status, rebuilt_at
        ) VALUES (?, 'condition', 'market', 'asset', 'YES', 100, 200, 1, 1,
                  10, 20, 0.5, 2, 'closed', 300)
        """,
        (leader,),
    ).lastrowid
    conn.execute(
        """
        INSERT INTO copy_backtest_trades(
            leader_wallet, follower_wallet, leader_activity_id, episode_id,
            market_slug, asset_id, outcome, side, leader_ts, copied_ts,
            lag_seconds, entry_price, leader_episode_roi, stake_usdc,
            gross_pnl_usdc, friction_bps, net_pnl_usdc, net_roi, created_at
        ) VALUES (?, ?, ?, ?, 'market', 'asset', 'YES', 'BUY', 100, 102, 2,
                  0.5, 0.2, 10, 2, 100, 1.9, 0.19, 300)
        """,
        (leader, follower, activity_id, episode_id),
    )
    conn.execute(
        """
        INSERT INTO wallet_features(
            address, leader_in_degree, copy_event_count, copy_market_count,
            containment_pct_median, copy_stream_roi, edge_retention_pct,
            walk_forward_consistency_pct, survival_score, extra_json, updated_at
        ) VALUES (?, 1, 12, 8, 0.4, 0.2083, 80, 70, 90, ?, 300)
        """,
        (
            leader,
            json.dumps(
                {
                    "copy_graph_qualified_follower_count": 1,
                    "copy_backtest_trade_count": 12,
                }
            ),
        ),
    )


def test_copyability_planner_reconciles_orphan_derivatives_and_preserves_validated_leaders(
    tmp_path,
):
    conn = connect(tmp_path / "robot.sqlite")
    orphan = "0x" + "1" * 40
    orphan_follower = "0x" + "2" * 40
    valid = "0x" + "3" * 40
    valid_follower = "0x" + "4" * 40
    try:
        run_migrations(conn)
        _seed_copyability_derivatives(
            conn,
            leader=orphan,
            follower=orphan_follower,
            qualifies=0,
        )
        _seed_copyability_derivatives(
            conn,
            leader=valid,
            follower=valid_follower,
            qualifies=1,
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=0,
            max_active_jobs=0,
            shard_count=1,
            now=1_000,
        )

        assert summary.truth_reconciled_leaders == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM copy_pair_stats WHERE leader_wallet = ?",
            (orphan,),
        ).fetchone()[0] == 1
        for table in ("copy_leader_stats", "copy_leader_performance", "copy_backtest_trades"):
            assert conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE leader_wallet = ?",
                (orphan,),
            ).fetchone()[0] == 0
            assert conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE leader_wallet = ?",
                (valid,),
            ).fetchone()[0] == 1
        feature = conn.execute(
            "SELECT * FROM wallet_features WHERE address = ?",
            (orphan,),
        ).fetchone()
        extra = json.loads(feature["extra_json"])
        assert feature["leader_in_degree"] == 0
        assert feature["copy_event_count"] == 0
        assert feature["copy_stream_roi"] == 0
        assert feature["edge_retention_pct"] is None
        assert "copy_graph_qualified_follower_count" not in extra
        assert "copy_backtest_trade_count" not in extra
        state = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (orphan,),
        ).fetchone()
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (orphan,),
        ).fetchone()
        assert state["next_action"] == "score_wallet"
        assert stage["candidate_stage"] == "blocked_copyability"

        second = plan_copyability_evidence_jobs(
            conn,
            limit=0,
            max_active_jobs=0,
            shard_count=1,
            now=1_001,
        )
        unchanged_state = conn.execute(
            "SELECT next_action, updated_at FROM wallet_processing_state WHERE wallet = ?",
            (orphan,),
        ).fetchone()
        assert second.truth_reconciled_leaders == 0
        assert unchanged_state["next_action"] == "score_wallet"
        assert unchanged_state["updated_at"] == 1_000
    finally:
        conn.close()


@pytest.mark.parametrize("pending_action", ["light_pending", "medium_pending", "deep_pending"])
def test_copyability_truth_reconcile_does_not_interrupt_pending_history_work(
    tmp_path,
    pending_action,
):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "5" * 40
    follower = "0x" + "6" * 40
    try:
        run_migrations(conn)
        _seed_copyability_derivatives(
            conn,
            leader=leader,
            follower=follower,
            qualifies=0,
        )
        conn.execute(
            """
            UPDATE wallet_processing_state
            SET current_stage = ?,
                evidence_status = 'queued',
                next_action = ?
            WHERE wallet = ?
            """,
            (pending_action, pending_action, leader),
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=0,
            max_active_jobs=0,
            shard_count=1,
            now=1_000,
        )

        state = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (leader,),
        ).fetchone()
        assert summary.truth_reconciled_leaders == 1
        assert state["next_action"] == pending_action
    finally:
        conn.close()


def test_copyability_truth_reconcile_chunks_large_orphan_sets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = [f"0x{index:040x}" for index in range(1, 1_006)]
    try:
        run_migrations(conn)
        conn.executemany(
            """
            INSERT INTO candidate_wallets(
                address, sources, first_seen_at, updated_at
            ) VALUES (?, 'test', 100, 100)
            """,
            [(wallet,) for wallet in wallets],
        )
        conn.executemany(
            """
            INSERT INTO wallet_processing_state(
                wallet, evidence_status, current_stage, next_action, updated_at
            ) VALUES (?, 'summary_ready', 'deep_done', '', 100)
            """,
            [(wallet,) for wallet in wallets],
        )
        conn.executemany(
            """
            INSERT INTO wallet_features(
                address, leader_in_degree, copy_event_count, copy_market_count,
                extra_json, updated_at
            ) VALUES (?, 1, 5, 5, '{"copy_graph_qualified_follower_count":1}', 100)
            """,
            [(wallet,) for wallet in wallets],
        )
        conn.commit()

        summary = plan_copyability_evidence_jobs(
            conn,
            limit=0,
            max_active_jobs=0,
            shard_count=1,
            now=1_000,
        )

        assert summary.truth_reconciled_leaders == len(wallets)
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_features WHERE leader_in_degree = 0"
        ).fetchone()[0] == len(wallets)
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_processing_state WHERE next_action = 'score_wallet'"
        ).fetchone()[0] == len(wallets)
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("evidence_status", "current_stage"),
    [("queued", "deep_done"), ("summary_ready", "medium_pending")],
)
def test_copyability_truth_reconcile_honors_pending_status_or_stage(
    tmp_path,
    evidence_status,
    current_stage,
):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "7" * 40
    follower = "0x" + "8" * 40
    try:
        run_migrations(conn)
        _seed_copyability_derivatives(
            conn,
            leader=leader,
            follower=follower,
            qualifies=0,
        )
        conn.execute(
            """
            UPDATE wallet_processing_state
            SET evidence_status = ?, current_stage = ?, next_action = ''
            WHERE wallet = ?
            """,
            (evidence_status, current_stage, leader),
        )
        conn.commit()

        plan_copyability_evidence_jobs(
            conn,
            limit=0,
            max_active_jobs=0,
            shard_count=1,
            now=1_000,
        )

        state = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (leader,),
        ).fetchone()
        assert state["next_action"] == ""
    finally:
        conn.close()


def test_copyability_truth_reconcile_does_not_queue_archived_summary_wallets(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    leader = "0x" + "9" * 40
    follower = "0x" + "a" * 40
    try:
        run_migrations(conn)
        _seed_copyability_derivatives(
            conn,
            leader=leader,
            follower=follower,
            qualifies=0,
        )
        conn.execute(
            """
            INSERT INTO wallet_registry(
                address, raw_retention_tier, last_evaluated_at, updated_at
            ) VALUES (?, 'summary_only', 100, 100)
            """,
            (leader,),
        )
        conn.commit()

        first = plan_copyability_evidence_jobs(
            conn,
            limit=0,
            max_active_jobs=0,
            shard_count=1,
            now=1_000,
        )
        state = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (leader,),
        ).fetchone()

        assert first.truth_reconciled_leaders == 1
        assert state["next_action"] == ""

        conn.execute(
            "UPDATE wallet_processing_state SET next_action = 'score_wallet' WHERE wallet = ?",
            (leader,),
        )
        conn.commit()
        second = plan_copyability_evidence_jobs(
            conn,
            limit=0,
            max_active_jobs=0,
            shard_count=1,
            now=1_001,
        )
        state = conn.execute(
            "SELECT next_action FROM wallet_processing_state WHERE wallet = ?",
            (leader,),
        ).fetchone()

        assert second.truth_reconciled_leaders == 0
        assert state["next_action"] == ""
    finally:
        conn.close()
