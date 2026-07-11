from pm_robot.config import RobotSettings
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.ops import (
    _prune_wallet_evidence_batch,
    build_wallet_registry,
    prune_low_value_evidence,
)
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    enqueue_pipeline_job,
    persist_score,
    persist_wallet_activity,
    upsert_candidate,
    upsert_wallet_feature,
)


def _settings(db_path):
    return RobotSettings(db_path=db_path, execution_mode="research")


def _activity(idx: int) -> dict:
    return {
        "timestamp": 1_000 + idx,
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
        "transactionHash": f"0x{idx:064x}",
    }


def _seed_candidate(conn, wallet: str, *, materialized: bool, roi: float = -0.1) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            hygiene_status="clean",
            extra={"feature_materializer_version": "test"} if materialized else {},
        ),
    )
    persist_wallet_activity(conn, wallet, [_activity(idx) for idx in range(5)], ingested_at=2_000)
    conn.execute(
        """
        INSERT INTO paper_wallet_quality(
            wallet, orders, open_positions, settled_positions, gamma_marked_positions,
            fallback_marked_positions, mark_coverage, settled_cost_usd, settled_pnl_usd,
            settled_roi, total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
        ) VALUES (?, 5, 0, 2, 2, 0, 1.0, 100, ?, ?, ?, ?, 0, '[]', 2000)
        """,
        (wallet, roi * 100, roi, roi * 100, roi),
    )
    persist_score(
        conn,
        ScoreBreakdown(
            address=wallet,
            leader_score=0,
            stage=CandidateStage.NEEDS_DATA,
            reason="missing_required_score_components",
            components={},
            penalties={},
        ),
        policy_version="test",
    )
    conn.commit()


def test_prune_evidence_dry_run_and_execute_low_value_only(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    low = "0x" + "1" * 40
    high = "0x" + "2" * 40
    unmaterialized = "0x" + "3" * 40
    try:
        run_migrations(conn)
        _seed_candidate(conn, low, materialized=True, roi=-0.2)
        _seed_candidate(conn, high, materialized=True, roi=0.5)
        _seed_candidate(conn, unmaterialized, materialized=False, roi=-0.2)
    finally:
        conn.close()

    dry = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        keep_recent_activity=2,
        dry_run=True,
    )
    conn = connect(db_path)
    try:
        assert dry["wallets"] == [low]
        assert dry["deleted"]["wallet_activity"] == 3
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (low,)
        ).fetchone()[0] == 5
    finally:
        conn.close()

    executed = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        keep_recent_activity=2,
        dry_run=False,
    )
    conn = connect(db_path)
    try:
        assert executed["wallets"] == [low]
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (low,)
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT trade_count FROM wallet_activity_watermarks WHERE address = ?",
            (low,),
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (high,)
        ).fetchone()[0] == 5
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (unmaterialized,),
        ).fetchone()[0] == 5
        registry = conn.execute(
            "SELECT registry_status FROM wallet_registry WHERE address = ?",
            (low,),
        ).fetchone()
        assert registry["registry_status"] == "archived_raw_pruned"
    finally:
        conn.close()

    build_wallet_registry(_settings(db_path))
    conn = connect(db_path)
    try:
        rebuilt = conn.execute(
            "SELECT registry_status FROM wallet_registry WHERE address = ?",
            (low,),
        ).fetchone()
        assert rebuilt["registry_status"] == "archived_raw_pruned"
    finally:
        conn.close()


def test_execute_prune_counts_sqlite_changes_without_precount_scan(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    wallet = "0x" + "9" * 40
    statements: list[str] = []
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True)
        conn.set_trace_callback(statements.append)

        deleted = _prune_wallet_evidence_batch(
            conn,
            [wallet],
            keep_recent_activity=0,
            dry_run=False,
        )

        assert deleted["wallet_activity"] == 5
        assert not any(
            "SELECT COUNT(*)" in statement.upper()
            for statement in statements
        )
    finally:
        conn.close()


def test_prune_selection_is_bounded_by_activity_rows(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallets = ["0x" + digit * 40 for digit in ("1", "2", "3")]
    conn = connect(db_path)
    try:
        run_migrations(conn)
        for wallet in wallets:
            _seed_candidate(conn, wallet, materialized=True)
            conn.execute(
                "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
                (wallet,),
            )
            conn.execute(
                "INSERT INTO wallet_processing_state(wallet, activity_count) VALUES (?, 1)",
                (wallet,),
            )
        conn.commit()
    finally:
        conn.close()

    result = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        max_activity_rows=6,
        dry_run=True,
    )

    assert result["wallets"] == [wallets[0]]
    assert result["wallet_count"] == 1
    assert result["selected_activity_rows"] == 5
    assert result["max_activity_rows"] == 6
    assert result["activity_budget_exceeded"] is False


def test_prune_activity_budget_does_not_starve_one_oversized_wallet(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "8" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = 'blocked_hygiene' WHERE address = ?",
            (wallet,),
        )
        conn.commit()
    finally:
        conn.close()

    result = prune_low_value_evidence(
        _settings(db_path),
        limit=10,
        max_activity_rows=3,
        dry_run=True,
    )

    assert result["wallets"] == [wallet]
    assert result["selected_activity_rows"] == 5
    assert result["activity_budget_exceeded"] is True


def test_prune_terminal_wallet_freezes_summary_and_stops_automatic_work(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "4" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_candidate(conn, wallet, materialized=True, roi=0.5)
        leader = "0x" + "5" * 40
        upsert_candidate(conn, CandidateAddress(address=leader, sources="test"))
        conn.execute(
            """
            INSERT INTO copy_pair_stats(
                leader_wallet, follower_wallet, copy_event_count, copy_market_count,
                follower_trade_count, containment_pct, leader_precedes_pct,
                median_lag_seconds, first_copy_ts, last_copy_ts, qualifies, updated_at
            ) VALUES (?, ?, 3, 2, 4, 0.75, 1.0, 10, 1000, 2000, 1, 2000)
            """,
            (leader, wallet),
        )
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.BLOCKED_HYGIENE.value, wallet),
        )
        persist_score(
            conn,
            ScoreBreakdown(
                address=wallet,
                leader_score=42,
                stage=CandidateStage.BLOCKED_HYGIENE,
                reason="hygiene_hard_block",
                components={"profitability": 60},
                penalties={"hygiene": 30},
            ),
            policy_version="test-2",
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, evidence_status, next_action, next_action_at, updated_at
            ) VALUES (?, 'queued', 'deep_pending', 0, 2000)
            """,
            (wallet,),
        )
        enqueue_pipeline_job(
            conn,
            job_type="wallet_evidence_backfill",
            wallet=wallet,
            subject_key="deep_pending",
            tier="l3_deep",
            now=2000,
        )
        enqueue_pipeline_job(
            conn,
            job_type="copyability_evidence",
            wallet=wallet,
            subject_key="copyability_backfill",
            tier="graph_v1",
            now=2000,
        )
        conn.commit()
    finally:
        conn.close()

    result = prune_low_value_evidence(_settings(db_path), limit=10, dry_run=False)
    assert result["wallets"] == [wallet]
    assert result["deleted"]["wallet_activity"] == 5
    assert result["deleted"]["leader_scores"] == 1
    assert result["deleted"]["copy_pair_stats"] == 1

    conn = connect(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM candidate_wallets WHERE address = ?", (wallet,)
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (wallet,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT trade_count FROM wallet_activity_watermarks WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM copy_pair_stats WHERE follower_wallet = ?", (wallet,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM leader_scores WHERE address = ?", (wallet,)
        ).fetchone()[0] == 1
        latest = conn.execute(
            "SELECT review_stage, review_reason FROM leader_latest_scores WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert latest["review_stage"] == CandidateStage.BLOCKED_HYGIENE.value
        assert latest["review_reason"] == "hygiene_hard_block"
        registry = conn.execute(
            """
            SELECT registry_status, raw_prune_version, activity_count, review_reason
            FROM wallet_registry
            WHERE address = ?
            """,
            (wallet,),
        ).fetchone()
        assert registry["registry_status"] == "archived_raw_pruned"
        assert registry["raw_prune_version"] == "v2_zero_raw"
        assert registry["activity_count"] == 5
        assert registry["review_reason"] == "hygiene_hard_block"
        state = conn.execute(
            "SELECT evidence_status, next_action FROM wallet_processing_state WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert state["evidence_status"] == "summary_ready"
        assert state["next_action"] == ""
        jobs = conn.execute(
            "SELECT status, output_json FROM pipeline_jobs WHERE wallet = ? ORDER BY job_type",
            (wallet,),
        ).fetchall()
        assert [row["status"] for row in jobs] == ["done", "done"]
        assert all('"archived":true' in row["output_json"] for row in jobs)
    finally:
        conn.close()

    build_wallet_registry(_settings(db_path))
    conn = connect(db_path)
    try:
        frozen = conn.execute(
            "SELECT activity_count, review_reason FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()
        assert frozen["activity_count"] == 5
        assert frozen["review_reason"] == "hygiene_hard_block"
    finally:
        conn.close()
