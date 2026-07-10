import json

from pm_robot.models import CandidateAddress, CandidateStage, WalletFeatures
from pm_robot.research.publish import active_published_leaders, publish_leaders, publishable_leader_rows
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import persist_wallet_activity, upsert_candidate, upsert_wallet_feature


def test_publish_leaders_writes_active_rows_and_json(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "7" * 40
        _insert_publishable_wallet(conn, wallet)

        out = tmp_path / "published.json"
        summary = publish_leaders(conn, now=1_000, ttl_seconds=600, output_path=out)
        active = active_published_leaders(conn, now=1_001)
        payload = json.loads(out.read_text(encoding="utf-8"))

        assert summary.active == 1
        assert summary.revoked == 0
        assert summary.expires_at == 1_600
        assert len(active) == 1
        assert active[0]["wallet"] == wallet
        assert active[0]["paper_quality"]["settled_positions"] == 40
        assert active[0]["readiness"]["stable_production_ready"] == 1
        assert active[0]["evidence"]["source_provenance"]["first_source"] == "test"
        assert active[0]["evidence"]["publish_quality"]["grade"] == "warn"
        assert "thin_settled_sample" in active[0]["evidence"]["publish_quality"]["warnings"]
        assert payload["count"] == 1
        assert payload["revoked_count"] == 0
        assert payload["leaders"][0]["wallet"] == wallet
        assert payload["leaders"][0]["evidence"]["source_provenance"]["source_count"] == 1
        assert payload["revoked_leaders"] == []
    finally:
        conn.close()


def test_publish_leaders_skips_unstable_wallet(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "8" * 40
        _insert_publishable_wallet(conn, wallet, observation_times=(100, 200))

        rows = publishable_leader_rows(conn, published_at=1_000, expires_at=1_600)

        assert rows == []
    finally:
        conn.close()


def test_publish_leaders_skips_live_stage_without_l3_evidence(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "6" * 40
        _insert_publishable_wallet(conn, wallet)
        conn.execute("DELETE FROM wallet_processing_state WHERE wallet = ?", (wallet,))
        conn.commit()

        rows = publishable_leader_rows(conn, published_at=1_000, expires_at=1_600)

        assert rows == []
    finally:
        conn.close()


def test_publish_leaders_revokes_no_longer_publishable(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "9" * 40
        _insert_publishable_wallet(conn, wallet)
        first = publish_leaders(conn, now=1_000, ttl_seconds=600)
        assert first.active == 1

        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.BLOCKED_COPYABILITY.value, wallet),
        )
        conn.commit()
        second = publish_leaders(conn, now=1_100, ttl_seconds=600)
        out = tmp_path / "published_after_revoke.json"
        publish_leaders(conn, now=1_200, ttl_seconds=600, output_path=out)
        row = conn.execute("SELECT * FROM leader_publish WHERE wallet = ?", (wallet,)).fetchone()
        payload = json.loads(out.read_text(encoding="utf-8"))

        assert second.active == 0
        assert second.revoked == 1
        assert row["status"] == "revoked"
        assert row["revoke_reason"] == "no_longer_publishable"
        assert payload["count"] == 0
        assert payload["revoked_count"] == 1
        assert payload["revoked_leaders"][0]["wallet"] == wallet
        assert payload["revoked_leaders"][0]["status"] == "revoked"
    finally:
        conn.close()


def test_source_provenance_backfills_existing_candidates(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "a" * 40
        conn.execute(
            """
            INSERT INTO candidate_wallets(
                address, sources, labels, notes, links, status,
                candidate_stage, first_seen_at, updated_at
            ) VALUES (?, 'manual.csv', 'label', 'note', 'https://example.test', 'manual', 'needs_data', 10, 20)
            """,
            (wallet,),
        )
        conn.execute("DELETE FROM schema_migrations WHERE version = 16")
        conn.execute("DROP TABLE candidate_source_events")
        conn.commit()

        applied = run_migrations(conn)
        row = conn.execute(
            "SELECT * FROM candidate_source_events WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert 16 in applied
        assert row["source"] == "manual.csv"
        assert row["observed_at"] == 10
        assert json.loads(row["evidence_json"]) == {"backfill": "candidate_wallets_current_snapshot"}
    finally:
        conn.close()


def _insert_publishable_wallet(
    conn,
    wallet: str,
    *,
    observation_times: tuple[int, ...] = (100, 1900, 3700),
) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            hygiene_status="clean",
            maker_fraction=0.1,
            copy_event_count=20,
            edge_retention_pct=80,
            walk_forward_consistency_pct=100,
            extra={"maker_fraction_source": "verified_test_fixture"},
        ),
    )
    conn.execute(
        "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
        (CandidateStage.LIVE_ELIGIBLE.value, wallet),
    )
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, distinct_markets,
            non_fast_trade_count, updated_at
        ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done',
                  'score_wallet', 0, 1000, 20, 200, 10)
        """,
        (wallet,),
    )
    conn.execute(
        """
        INSERT INTO leader_scores(
            address, leader_score, review_stage, review_reason,
            components_json, penalties_json, policy_version, scored_at
        ) VALUES (?, 88.5, ?, 'paper_quality_production_ready', '{"edge": 1}', '{}', 'test', 10)
        """,
        (wallet, CandidateStage.LIVE_ELIGIBLE.value),
    )
    conn.execute(
        """
        INSERT INTO paper_wallet_quality(
            wallet, orders, open_positions, settled_positions,
            gamma_marked_positions, fallback_marked_positions, mark_coverage,
            settled_cost_usd, settled_pnl_usd, settled_roi,
            total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
        ) VALUES (?, 250, 20, 40, 60, 0, 1, 1000, 100, 0.1, 120, 0.12, 1, '[]', 20)
        """,
        (wallet,),
    )
    persist_wallet_activity(
        conn,
        wallet,
        [
            {
                "timestamp": 1_000 + idx,
                "conditionId": f"condition-{idx % 3}",
                "eventSlug": f"event-{idx % 3}",
                "slug": f"market-{idx % 3}",
                "asset": f"asset-{idx % 3}",
                "outcome": "YES",
                "type": "TRADE",
                "side": "BUY",
                "price": 0.5,
                "size": 10,
                "usdcSize": 5,
                "transactionHash": f"0x{idx:064x}",
            }
            for idx in range(100)
        ],
        ingested_at=2_000,
    )
    for observed_at in observation_times:
        conn.execute(
            """
            INSERT INTO paper_readiness_observations(
                wallet, observed_at, orders, settled_positions, mark_coverage,
                settled_roi, total_roi, production_ready, blockers_json
            ) VALUES (?, ?, 250, 40, 1, 0.1, 0.12, 1, '[]')
            """,
            (wallet, observed_at),
        )
    conn.commit()
