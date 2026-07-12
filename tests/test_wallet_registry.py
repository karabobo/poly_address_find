import json

from pm_robot.config import RobotSettings
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.ops import (
    _wallet_registry_status,
    build_wallet_registry,
    build_winner_library,
    refresh_active_candidate_registry,
)
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import persist_score, persist_wallet_activity, upsert_candidate, upsert_wallet_feature


def _settings(db_path):
    return RobotSettings(db_path=db_path, execution_mode="research")


def _needs_data_retention_status(
    *,
    review_reason: str,
    evidence_stage: str,
    activity_count: int,
    tags: list[str] | None = None,
    existing_registry_status: str = "",
) -> tuple[str, str]:
    return _wallet_registry_status(
        {
            "candidate_stage": CandidateStage.NEEDS_DATA.value,
            "existing_registry_status": existing_registry_status,
            "publish_status": "",
        },
        feature={"extra": {"feature_materializer_version": "test"}},
        score={"leader_score": 0, "review_reason": review_reason},
        evidence={"stage": evidence_stage, "activity_count": activity_count},
        paper={"production_ready": False},
        tags=tags or [],
    )


def _trade_events(count: int) -> list[dict]:
    return [
        {
            "timestamp": 1_000 + idx,
            "conditionId": f"condition-{idx % 5}",
            "eventSlug": f"event-{idx % 5}",
            "slug": f"market-{idx % 5}",
            "asset": f"asset-{idx % 5}",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY",
            "price": 0.5,
            "size": 10,
            "usdcSize": 5,
            "transactionHash": f"0x{idx:064x}",
        }
        for idx in range(count)
    ]


def test_needs_data_retention_keeps_pending_evidence_repairable():
    status = _needs_data_retention_status(
        review_reason="missing_required_score_components:copy_event_count",
        evidence_stage="medium_pending",
        activity_count=10,
    )

    assert status == ("needs_evidence_backfill", "summary_and_recent")


def test_needs_data_retention_reactivates_archived_wallet_when_evidence_is_requeued():
    status = _needs_data_retention_status(
        review_reason="missing_required_score_components:copy_event_count",
        evidence_stage="medium_pending",
        activity_count=10,
        existing_registry_status="archived_raw_pruned",
    )

    assert status == ("needs_evidence_backfill", "summary_and_recent")


def test_needs_data_retention_keeps_actionable_terminal_history_with_enough_rows():
    status = _needs_data_retention_status(
        review_reason="missing_required_score_components:copy_event_count",
        evidence_stage="light_done",
        activity_count=25,
    )

    assert status == ("needs_more_scoring_data", "summary_and_recent")


def test_needs_data_retention_archives_actionable_terminal_history_when_exhausted():
    status = _needs_data_retention_status(
        review_reason="hygiene_evidence_incomplete",
        evidence_stage="light_done",
        activity_count=24,
    )

    assert status == ("archive_low_value", "summary_only")


def test_needs_data_retention_archives_explicit_economic_rejection():
    status = _needs_data_retention_status(
        review_reason="insufficient_net_pnl_usdc:20.00<50.00",
        evidence_stage="deep_done",
        activity_count=1_000,
    )

    assert status == ("archive_low_value", "summary_only")


def _seed_publishable_winner(conn, wallet: str) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source", labels="seed"))
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            total_volume_usdc=50_000,
            recent_30d_volume_usdc=10_000,
            net_pnl_usdc=5_000,
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
    persist_wallet_activity(conn, wallet, _trade_events(100), ingested_at=2_000)
    for observed_at in (100, 1900, 3700):
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


def _seed_manual_review_wallet(conn, wallet: str) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source", labels="seed"))
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            total_volume_usdc=25_000,
            recent_30d_volume_usdc=8_000,
            net_pnl_usdc=2_000,
            hygiene_status="clean",
            copy_event_count=20,
            edge_retention_pct=80,
            walk_forward_consistency_pct=100,
            extra={"maker_fraction_source": "verified_test_fixture"},
        ),
    )
    persist_score(
        conn,
        ScoreBreakdown(
            address=wallet,
            leader_score=70,
            stage=CandidateStage.NEEDS_REVIEW,
            reason="watchlist_score",
            components={"score": 70},
            penalties={},
        ),
        policy_version="test",
    )
    persist_wallet_activity(conn, wallet, _trade_events(100), ingested_at=2_000)
    conn.commit()


def test_wallet_registry_materializes_and_exports_archive_summary(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "a" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source", labels="seed"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                total_volume_usdc=100.0,
                recent_30d_volume_usdc=25.0,
                net_pnl_usdc=2.5,
                hygiene_status="clean",
                extra={"feature_materializer_version": "test"},
            ),
        )
        persist_wallet_activity(
            conn,
            wallet,
            [
                {
                    "timestamp": 1_000,
                    "conditionId": "condition-1",
                    "eventSlug": "event-1",
                    "slug": "market-1",
                    "asset": "asset-1",
                    "outcome": "YES",
                    "type": "TRADE",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "usdcSize": 5,
                    "transactionHash": "0xhash",
                }
            ],
            ingested_at=2_000,
        )
        persist_score(
            conn,
            ScoreBreakdown(
                address=wallet,
                leader_score=0,
                stage=CandidateStage.NEEDS_DATA,
                reason="insufficient_total_volume_usdc:100.00<1000.00",
                components={},
                penalties={},
            ),
            policy_version="test",
        )
        conn.commit()
    finally:
        conn.close()

    csv_out = tmp_path / "wallet_registry.csv"
    json_out = tmp_path / "wallet_registry.json"
    summary = build_wallet_registry(
        _settings(db_path),
        csv_output_path=csv_out,
        json_output_path=json_out,
    )

    conn = connect(db_path)
    try:
        row = conn.execute("SELECT * FROM wallet_registry WHERE address = ?", (wallet,)).fetchone()
    finally:
        conn.close()

    assert summary["wallet_count"] == 1
    assert row["registry_status"] == "archive_low_value"
    assert row["raw_retention_tier"] == "summary_only"
    assert row["activity_count"] == 1
    assert "low_volume" in json.loads(row["tags_json"])
    assert csv_out.read_text(encoding="utf-8").startswith("address,candidate_stage,registry_status")
    assert json.loads(json_out.read_text(encoding="utf-8"))[0]["address"] == wallet


def test_winner_library_exports_only_publishable_eligible_wallets(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    winner = "0x" + "b" * 40
    manual_review = "0x" + "c" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_publishable_winner(conn, winner)
        _seed_manual_review_wallet(conn, manual_review)
    finally:
        conn.close()

    broad_summary = build_wallet_registry(_settings(db_path))
    json_out = tmp_path / "winner_library.json"
    summary = build_winner_library(_settings(db_path), json_output_path=json_out)
    rows = json.loads(json_out.read_text(encoding="utf-8"))

    assert broad_summary["wallet_count"] == 2
    assert summary["winner_library_filtered"] is True
    assert summary["broad_registry_wallet_count"] == 2
    assert summary["wallet_count"] == 1
    assert summary["stage_counts"] == {CandidateStage.LIVE_ELIGIBLE.value: 1}
    assert [row["address"] for row in rows] == [winner]


def test_active_candidate_registry_refresh_materializes_only_stale_active_rows(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "e" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_publishable_winner(conn, wallet)

        first = refresh_active_candidate_registry(conn)
        conn.commit()
        row = conn.execute(
            "SELECT * FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()
        second = refresh_active_candidate_registry(conn)

        assert first == {
            "wallets_refreshed": 1,
            "archived_wallets_skipped": 0,
            "limit": 500,
        }
        assert second["wallets_refreshed"] == 0
        assert row["candidate_stage"] == CandidateStage.LIVE_ELIGIBLE.value
        assert row["registry_status"] == "ready_for_external_validation"
        assert row["raw_retention_tier"] == "keep_full"
        assert row["activity_count"] == 100

        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 95, ?, 'same_stage_rescore', '{}', '{}', 'test-v2', ?)
            """,
            (
                wallet,
                CandidateStage.LIVE_ELIGIBLE.value,
                int(row["updated_at"]) + 1,
            ),
        )
        conn.commit()

        third = refresh_active_candidate_registry(conn)
        rescored = conn.execute(
            "SELECT leader_score, review_reason, policy_version FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert third["wallets_refreshed"] == 1
        assert dict(rescored) == {
            "leader_score": 95.0,
            "review_reason": "same_stage_rescore",
            "policy_version": "test-v2",
        }
    finally:
        conn.close()


def test_active_candidate_registry_refresh_syncs_wallet_downgraded_out_of_paper(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "1" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_publishable_winner(conn, wallet)

        first = refresh_active_candidate_registry(conn)
        conn.commit()
        active_row = conn.execute(
            "SELECT candidate_stage, updated_at FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()

        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.NEEDS_DATA.value, wallet),
        )
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, 12.5, ?, 'downgraded_out_of_paper', '{}', '{}', 'test-downgrade', ?)
            """,
            (
                wallet,
                CandidateStage.NEEDS_DATA.value,
                int(active_row["updated_at"]) + 1,
            ),
        )
        conn.commit()

        second = refresh_active_candidate_registry(conn)
        downgraded = conn.execute(
            """
            SELECT candidate_stage, leader_score, review_stage, review_reason, policy_version
            FROM wallet_registry
            WHERE address = ?
            """,
            (wallet,),
        ).fetchone()

        assert first["wallets_refreshed"] == 1
        assert active_row["candidate_stage"] == CandidateStage.LIVE_ELIGIBLE.value
        assert second["wallets_refreshed"] == 1
        assert dict(downgraded) == {
            "candidate_stage": CandidateStage.NEEDS_DATA.value,
            "leader_score": 12.5,
            "review_stage": CandidateStage.NEEDS_DATA.value,
            "review_reason": "downgraded_out_of_paper",
            "policy_version": "test-downgrade",
        }
    finally:
        conn.close()


def test_active_candidate_registry_refresh_does_not_relabel_pruned_raw_history(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "f" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test_source"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_APPROVED.value, wallet),
        )
        conn.execute(
            """
            INSERT INTO wallet_registry(
                address, candidate_stage, registry_status, raw_retention_tier,
                raw_prune_version, raw_pruned_at, last_evaluated_at, updated_at
            ) VALUES (?, 'needs_data', 'archived_raw_pruned', 'summary_only',
                      'v2_zero_raw', 100, 100, 100)
            """,
            (wallet,),
        )
        conn.commit()

        result = refresh_active_candidate_registry(conn)
        row = conn.execute(
            "SELECT registry_status, raw_retention_tier, raw_pruned_at FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert result["wallets_refreshed"] == 0
        assert result["archived_wallets_skipped"] == 1
        assert dict(row) == {
            "registry_status": "archived_raw_pruned",
            "raw_retention_tier": "summary_only",
            "raw_pruned_at": 100,
        }
    finally:
        conn.close()


def test_winner_library_rejects_live_stage_without_l3_evidence(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "d" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_publishable_winner(conn, wallet)
        conn.execute("DELETE FROM wallet_processing_state WHERE wallet = ?", (wallet,))
        conn.commit()
    finally:
        conn.close()

    summary = build_winner_library(_settings(db_path), json_output_path=tmp_path / "winner_library.json")

    assert summary["wallet_count"] == 0
