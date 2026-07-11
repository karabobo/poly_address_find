import sqlite3

from pm_robot.models import CandidateAddress, WalletFeatures
import pm_robot.orchestration.feature_materializer as feature_materializer
from pm_robot.orchestration.feature_materializer import materialize_wallet_features
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    get_wallet_features,
    persist_wallet_activity,
    rebuild_wallet_episodes,
    upsert_candidate,
    upsert_wallet_feature,
)


def _activity(idx: int, *, side: str = "BUY", usdc: float = 10.0) -> dict:
    return {
        "timestamp": 1_000 + idx * 3_600,
        "conditionId": f"condition-{idx % 5}",
        "eventSlug": f"event-{idx % 5}",
        "slug": f"politics-market-{idx % 5}",
        "asset": f"asset-{idx % 5}",
        "outcome": "YES",
        "type": "TRADE",
        "side": side,
        "price": 0.5,
        "size": usdc / 0.5,
        "usdcSize": usdc,
        "transactionHash": f"0x{idx:064x}",
    }


def _seed_wallet(
    conn,
    wallet: str,
    *,
    paper_roi: float = 0.2,
    copy_containment: float = 0.95,
    copy_precedes: float = 0.95,
    qualifies: int = 1,
) -> None:
    follower = "0x" + "f" * 40
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="polymarket_trades_global"))
    upsert_candidate(conn, CandidateAddress(address=follower, sources="test_follower"))
    events = []
    for idx in range(30):
        events.append(_activity(idx, side="BUY", usdc=10))
        if idx % 3 == 0:
            sell = _activity(10_000 + idx, side="SELL", usdc=12)
            sell["conditionId"] = f"condition-{idx % 5}"
            sell["asset"] = f"asset-{idx % 5}"
            events.append(sell)
    persist_wallet_activity(conn, wallet, events, ingested_at=20_000)
    rebuild_wallet_episodes(conn, wallet)
    conn.execute(
        """
        INSERT INTO paper_wallet_quality(
            wallet, orders, open_positions, settled_positions, gamma_marked_positions,
            fallback_marked_positions, mark_coverage, settled_cost_usd, settled_pnl_usd,
            settled_roi, total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
        ) VALUES (?, 25, 0, 8, 8, 0, 1.0, 1000, ?, ?, ?, ?, 0, '[]', 20000)
        """,
        (wallet, paper_roi * 1_000, paper_roi, paper_roi * 1_000, paper_roi),
    )
    conn.execute(
        """
        INSERT INTO copy_pair_stats(
            leader_wallet, follower_wallet, copy_event_count, copy_market_count,
            follower_trade_count, containment_pct, leader_precedes_pct,
            median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
        ) VALUES (?, ?, 12, 3, 20, ?, ?, 4, 1000, 2000, ?, 20000)
        """,
        (wallet, follower, copy_containment, copy_precedes, qualifies),
    )
    conn.commit()


def test_materialize_wallet_features_fills_required_proxy_fields_in_batches(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet_one = "0x" + "1" * 40
    wallet_two = "0x" + "2" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, wallet_one, paper_roi=0.25)
        _seed_wallet(conn, wallet_two, paper_roi=0.10)

        first = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_000)
        features = get_wallet_features(conn)

        assert first.wallets_attempted == 1
        assert first.wallets_updated == 1
        assert features[wallet_one].maker_fraction is None
        assert features[wallet_one].hygiene_status == "screened"
        assert features[wallet_one].leader_in_degree == 1.0
        assert features[wallet_one].copy_event_count == 12.0
        assert features[wallet_one].copy_stream_roi == 0.0
        assert features[wallet_one].net_to_gross_exposure is not None
        assert features[wallet_one].single_market_pnl_share is not None
        assert features[wallet_one].extra["feature_materializer_distinct_markets"] == 5
        assert features[wallet_one].extra["feature_materializer_fast_market_share"] == 0.0
        assert features[wallet_one].extra["paper_roi_after_slippage"] == 0.235
        assert features[wallet_one].extra["copy_candidate_pair_count"] == 1
        assert features[wallet_one].extra["copy_candidate_event_count"] == 12.0
        assert features[wallet_one].extra["copy_validated_pair_count"] == 1
        assert features[wallet_two].copy_event_count is None

        second = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_100)
        features = get_wallet_features(conn)

        assert second.wallets_attempted == 1
        assert second.wallets_updated == 1
        assert features[wallet_two].copy_event_count == 12.0
    finally:
        conn.close()


def test_materialize_wallet_features_keeps_unvalidated_copy_signal_out_of_scoring_fields(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        _seed_wallet(
            conn,
            wallet,
            paper_roi=0.25,
            copy_containment=0.6,
            copy_precedes=0.8,
            qualifies=0,
        )

        summary = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_000)
        features = get_wallet_features(conn)

        assert summary.wallets_attempted == 1
        assert summary.wallets_updated == 1
        assert features[wallet].leader_in_degree == 0.0
        assert features[wallet].copy_event_count == 0.0
        assert features[wallet].copy_market_count == 0.0
        assert features[wallet].copy_stream_roi == 0.0
        assert features[wallet].extra["copy_candidate_pair_count"] == 1
        assert features[wallet].extra["copy_candidate_event_count"] == 12.0
        assert features[wallet].extra["copy_candidate_market_count"] == 3.0
        assert features[wallet].extra["copy_validated_pair_count"] == 0
        assert features[wallet].extra["copy_stream_roi_source"] == "copy_candidate_pair_stats_unvalidated_default_zero"
    finally:
        conn.close()


def test_materialize_wallet_features_prioritizes_score_ready_pipeline_state(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    score_ready = "0x" + "3" * 40
    high_paper = "0x" + "4" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, score_ready, paper_roi=0.05)
        _seed_wallet(conn, high_paper, paper_roi=0.30)
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, updated_at
            ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done', 'score_wallet', 0, 30, 20000)
            """,
            (score_ready,),
        )
        conn.commit()

        summary = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_000)
        features = get_wallet_features(conn)

        assert summary.wallets_attempted == 1
        assert summary.wallets_updated == 1
        assert features[score_ready].copy_event_count == 12.0
        assert features[high_paper].copy_event_count is None
    finally:
        conn.close()


def test_materialize_wallet_features_prioritizes_missing_required_components(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    missing_required = "0x" + "6" * 40
    high_paper = "0x" + "7" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, missing_required, paper_roi=0.02)
        _seed_wallet(conn, high_paper, paper_roi=0.35)
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=missing_required,
                recent_30d_volume_usdc=1_000,
                total_volume_usdc=5_000,
                net_pnl_usdc=100,
                cumulative_win_rate=0.5,
                trade_win_rate=0.5,
                avg_dca_entries=2,
                sell_pct=20,
                hygiene_status="incomplete",
            ),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 0.0, 'needs_data', ?, '{}', '{}', 'test', 20000)
            """,
            (
                missing_required,
                "missing_required_score_components:bot_score,leader_in_degree,copy_event_count,copy_market_count,single_market_pnl_share,net_to_gross_exposure",
            ),
        )
        conn.commit()

        summary = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_000)
        features = get_wallet_features(conn)

        assert summary.wallets_attempted == 1
        assert summary.wallets_updated == 1
        assert features[missing_required].bot_score is not None
        assert features[missing_required].copy_event_count == 12.0
        assert features[missing_required].single_market_pnl_share is not None
        assert features[high_paper].copy_event_count is None
    finally:
        conn.close()


def test_materialize_wallet_features_retries_locked_feature_write(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "5" * 40
    attempts = {"locked": 0}
    original_upsert = feature_materializer.upsert_wallet_feature

    def flaky_upsert(conn_arg, feature):
        if attempts["locked"] == 0:
            attempts["locked"] += 1
            raise sqlite3.OperationalError("database is locked")
        original_upsert(conn_arg, feature)

    try:
        run_migrations(conn)
        _seed_wallet(conn, wallet, paper_roi=0.25)
        monkeypatch.setattr(feature_materializer, "upsert_wallet_feature", flaky_upsert)

        summary = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_000)
        features = get_wallet_features(conn)

        assert attempts["locked"] == 1
        assert summary.wallets_attempted == 1
        assert summary.wallets_updated == 1
        assert features[wallet].copy_event_count == 12.0
    finally:
        conn.close()


def test_materialize_wallet_features_preserves_successful_chunks(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + digit * 40 for digit in ("8", "9")]
    attempts = {"count": 0}
    original_write = feature_materializer._write_materialized_feature_batch

    def fail_first_chunk(conn_arg, materialized):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise sqlite3.OperationalError("database is locked")
        original_write(conn_arg, materialized)

    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_wallet(conn, wallet)
        monkeypatch.setattr(
            feature_materializer,
            "_write_materialized_feature_batch",
            fail_first_chunk,
        )

        summary = materialize_wallet_features(
            conn,
            limit=2,
            min_activity_events=25,
            commit_every=1,
            now=30_000,
        )
        features = get_wallet_features(conn)

        assert attempts["count"] == 2
        assert summary.wallets_attempted == 2
        assert summary.wallets_updated == 1
        assert summary.status == "partial"
        assert sum(
            features[wallet].extra.get("feature_materializer_version")
            == feature_materializer.MATERIALIZER_VERSION
            for wallet in wallets
        ) == 1
    finally:
        conn.close()


def test_materialize_wallet_features_rolls_back_failed_chunk(tmp_path, monkeypatch):
    conn = connect(tmp_path / "robot.sqlite")
    wallets = ["0x" + digit * 40 for digit in ("6", "7")]
    original_upsert = feature_materializer.upsert_wallet_feature
    writes = {"count": 0}

    def fail_after_first_write(conn_arg, feature):
        writes["count"] += 1
        original_upsert(conn_arg, feature)
        if writes["count"] == 2:
            raise RuntimeError("synthetic batch failure")

    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_wallet(conn, wallet)
        monkeypatch.setattr(
            feature_materializer,
            "upsert_wallet_feature",
            fail_after_first_write,
        )

        summary = materialize_wallet_features(
            conn,
            limit=2,
            min_activity_events=25,
            commit_every=2,
            now=30_000,
        )
        features = get_wallet_features(conn)

        assert summary.wallets_updated == 0
        assert summary.status == "partial"
        assert conn.in_transaction is False
        assert all(
            features[wallet].extra.get("feature_materializer_version")
            != feature_materializer.MATERIALIZER_VERSION
            for wallet in wallets
        )
    finally:
        conn.close()


def test_materializer_refreshes_owned_values_but_preserves_external_override(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "a" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, wallet)
        first = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_000)
        assert first.wallets_updated == 1

        persist_wallet_activity(conn, wallet, [_activity(50_000, usdc=25)], ingested_at=30_100)
        conn.execute(
            "UPDATE wallet_episodes SET realized_pnl_est = 25 WHERE address = ?",
            (wallet,),
        )
        conn.commit()
        second = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_200)
        refreshed = get_wallet_features(conn)[wallet]

        assert second.wallets_updated == 1
        assert refreshed.net_pnl_usdc == 125

        conn.execute(
            "UPDATE wallet_features SET net_pnl_usdc = 777 WHERE address = ?",
            (wallet,),
        )
        persist_wallet_activity(conn, wallet, [_activity(60_000, usdc=30)], ingested_at=30_300)
        conn.execute(
            "UPDATE wallet_episodes SET realized_pnl_est = 50 WHERE address = ?",
            (wallet,),
        )
        conn.commit()
        third = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_400)
        preserved = get_wallet_features(conn)[wallet]

        assert third.wallets_updated == 1
        assert preserved.net_pnl_usdc == 777
    finally:
        conn.close()


def test_materializer_refresh_uses_raw_activity_count_when_pipeline_state_is_stale(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "b" * 40
    try:
        run_migrations(conn)
        _seed_wallet(conn, wallet)
        first = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_000)
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, discovery_tier, evidence_status, evidence_depth,
                evidence_confidence, priority, current_stage, next_action,
                next_action_at, activity_count, updated_at
            ) VALUES (?, 'l1_light', 'summary_ready', 40, 0.5, 20,
                      'light_done', 'score_wallet', 0, 40, 30000)
            """,
            (wallet,),
        )
        persist_wallet_activity(
            conn,
            wallet,
            [_activity(70_000, usdc=25)],
            ingested_at=30_100,
        )
        conn.commit()

        second = materialize_wallet_features(conn, limit=1, min_activity_events=25, now=30_200)
        refreshed = get_wallet_features(conn)[wallet]

        assert first.wallets_updated == 1
        assert second.wallets_updated == 1
        assert refreshed.extra["feature_materializer_activity_count"] == 41
    finally:
        conn.close()


def test_materializer_refreshes_rolling_30_day_volume(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "c" * 40
    first_now = 100_000
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        persist_wallet_activity(
            conn,
            wallet,
            [_activity(idx, usdc=10) for idx in range(25)],
            ingested_at=first_now,
        )
        rebuild_wallet_episodes(conn, wallet)
        conn.commit()

        first = materialize_wallet_features(
            conn,
            limit=1,
            min_activity_events=25,
            now=first_now,
        )
        initial = get_wallet_features(conn)[wallet]
        unchanged = materialize_wallet_features(
            conn,
            limit=1,
            min_activity_events=25,
            now=first_now + 100,
        )
        second = materialize_wallet_features(
            conn,
            limit=1,
            min_activity_events=25,
            now=first_now + 31 * 86_400,
        )
        refreshed = get_wallet_features(conn)[wallet]

        assert first.wallets_updated == 1
        assert initial.recent_30d_volume_usdc == 250
        assert unchanged.wallets_attempted == 0
        assert second.wallets_updated == 1
        assert refreshed.recent_30d_volume_usdc == 0
        assert refreshed.last_active_days_ago > 30
    finally:
        conn.close()
