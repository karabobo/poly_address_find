import sqlite3

import pytest

import pm_robot.ops as ops
from pm_robot.config import RobotSettings
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.ops import compact_low_value_evidence
from pm_robot.storage.db import connect, database_access_guard, run_migrations
from pm_robot.storage.repository import (
    enqueue_pipeline_job,
    persist_score,
    persist_wallet_activity,
    upsert_candidate,
    upsert_wallet_feature,
)


def _settings(db_path):
    return RobotSettings(
        db_path=db_path,
        backup_dir=db_path.parent / "backups",
        execution_mode="research",
    )


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


def _seed_wallet(conn, wallet: str, *, stage: CandidateStage) -> None:
    upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            hygiene_status="clean",
            extra={"feature_materializer_version": "test"},
        ),
    )
    persist_wallet_activity(
        conn,
        wallet,
        [_activity(index) for index in range(5)],
        ingested_at=2_000,
    )
    persist_score(
        conn,
        ScoreBreakdown(
            address=wallet,
            leader_score=42 if stage != CandidateStage.NEEDS_DATA else 0,
            stage=stage,
            reason="test-stage",
            components={"profitability": 42},
            penalties={},
        ),
        policy_version="test",
    )
    conn.execute(
        "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
        (stage.value, wallet),
    )


def test_compact_evidence_rebuild_preserves_summaries_and_filters_raw_rows(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    low = "0x" + "1" * 40
    high = "0x" + "2" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_wallet(conn, low, stage=CandidateStage.BLOCKED_HYGIENE)
        _seed_wallet(conn, high, stage=CandidateStage.PAPER_CANDIDATE)
        persist_score(
            conn,
            ScoreBreakdown(
                address=low,
                leader_score=45,
                stage=CandidateStage.BLOCKED_HYGIENE,
                reason="latest-hygiene-block",
                components={"profitability": 45},
                penalties={"hygiene": 20},
            ),
            policy_version="test-2",
        )
        conn.execute(
            """
            INSERT INTO wallet_processing_state(
                wallet, evidence_status, next_action, next_action_at, updated_at
            ) VALUES (?, 'queued', 'deep_pending', 0, 2000)
            """,
            (low,),
        )
        enqueue_pipeline_job(
            conn,
            job_type="wallet_evidence_backfill",
            wallet=low,
            subject_key="deep_pending",
            tier="l3_deep",
            now=2_000,
        )
        conn.commit()
        database_id_before = conn.execute(
            "SELECT database_id FROM retention_cycle_state WHERE singleton = 1"
        ).fetchone()[0]
    finally:
        conn.close()

    result = compact_low_value_evidence(_settings(db_path), dry_run=False)

    assert result["ok"] is True
    assert result["database_replaced"] is True
    assert result["wallet_count"] == 1
    assert result["selected_activity_rows"] == 5
    assert result["validation"]["ok"] is True
    assert result["foreign_key_repairs"]["initial_violations"] == 0
    assert not (tmp_path / ".robot.sqlite.compact.partial").exists()
    backup_dir = tmp_path / "backups"
    assert not backup_dir.exists() or not list(backup_dir.iterdir())

    conn = connect(db_path)
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        database_id_after = conn.execute(
            "SELECT database_id FROM retention_cycle_state WHERE singleton = 1"
        ).fetchone()[0]
        assert database_id_after != database_id_before
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (low,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (high,)
        ).fetchone()[0] == 5
        assert conn.execute(
            "SELECT COUNT(*) FROM candidate_wallets WHERE address = ?", (low,)
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM leader_scores WHERE address = ?", (low,)
        ).fetchone()[0] == 1
        latest = conn.execute(
            "SELECT review_stage, review_reason FROM leader_latest_scores WHERE address = ?",
            (low,),
        ).fetchone()
        assert latest["review_stage"] == CandidateStage.BLOCKED_HYGIENE.value
        assert latest["review_reason"] == "latest-hygiene-block"
        registry = conn.execute(
            """
            SELECT registry_status, raw_retention_tier, activity_count
            FROM wallet_registry
            WHERE address = ?
            """,
            (low,),
        ).fetchone()
        assert registry["registry_status"] == "archived_raw_pruned"
        assert registry["raw_retention_tier"] == "summary_only"
        assert registry["activity_count"] == 5
        state = conn.execute(
            "SELECT evidence_status, next_action FROM wallet_processing_state WHERE wallet = ?",
            (low,),
        ).fetchone()
        assert state["evidence_status"] == "summary_ready"
        assert state["next_action"] == ""
        dashboard = conn.execute(
            "SELECT next_action FROM wallet_dashboard_snapshot WHERE address = ?",
            (low,),
        ).fetchone()
        assert dashboard["next_action"] == ""
        job = conn.execute(
            "SELECT status FROM pipeline_jobs WHERE wallet = ?", (low,)
        ).fetchone()
        assert job["status"] == "done"
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_compact_foreign_key_repair_applies_delete_semantics():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(
            """
            PRAGMA foreign_keys = OFF;
            CREATE TABLE parent(parent_id INTEGER PRIMARY KEY);
            CREATE TABLE cascade_child(
                child_id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parent(parent_id) ON DELETE CASCADE
            );
            CREATE TABLE nullable_child(
                child_id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parent(parent_id) ON DELETE SET NULL
            );
            INSERT INTO cascade_child(child_id, parent_id) VALUES (1, 99);
            INSERT INTO nullable_child(child_id, parent_id) VALUES (1, 99);
            """
        )

        result = ops._repair_compact_foreign_key_dependencies(conn)

        assert result["initial_violations"] == 2
        assert result["deleted_rows"] == {"cascade_child": 1}
        assert result["nulled_rows"] == {"nullable_child": 1}
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("SELECT COUNT(*) FROM cascade_child").fetchone()[0] == 0
        assert conn.execute(
            "SELECT parent_id FROM nullable_child WHERE child_id = 1"
        ).fetchone()[0] is None
    finally:
        conn.close()


def test_compact_evidence_repairs_real_schema_orphans_before_replacement(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    low = "0x" + "6" * 40
    high = "0x" + "7" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_wallet(conn, low, stage=CandidateStage.BLOCKED_HYGIENE)
        _seed_wallet(conn, high, stage=CandidateStage.PAPER_CANDIDATE)
        low_activity_id = conn.execute(
            "SELECT activity_id FROM wallet_activity WHERE address = ? LIMIT 1",
            (low,),
        ).fetchone()[0]
        high_activity_id = conn.execute(
            "SELECT activity_id FROM wallet_activity WHERE address = ? LIMIT 1",
            (high,),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO copy_trade_links(
                leader_wallet, follower_wallet,
                leader_activity_id, follower_activity_id,
                condition_id, market_slug, asset_id, outcome, side,
                leader_ts, follower_ts, lag_seconds, created_at
            ) VALUES (?, ?, ?, ?, 'condition-1', 'market-1', 'asset-1',
                      'YES', 'BUY', 1000, 1001, 1, 2000)
            """,
            (high, high, low_activity_id, high_activity_id),
        )
        conn.commit()
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()

    result = compact_low_value_evidence(_settings(db_path), dry_run=False)

    repairs = result["foreign_key_repairs"]
    assert repairs["initial_violations"] == 1
    assert repairs["deleted_rows"] == {"copy_trade_links": 1}
    assert result["validation"]["checks"]["table_row_counts"] is True
    assert result["validation"]["checks"]["schema_objects"] is True

    conn = connect(db_path)
    try:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("SELECT COUNT(*) FROM copy_trade_links").fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type = 'index' AND name = 'idx_copy_trade_links_leader'
            """
        ).fetchone()[0] == 1
        assert conn.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type = 'trigger'
              AND name = 'trg_wallet_dashboard_snapshot_candidate_update'
            """
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_compact_evidence_failure_keeps_source_and_removes_partial(tmp_path, monkeypatch):
    db_path = tmp_path / "robot.sqlite"
    wallet = "0x" + "3" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_wallet(conn, wallet, stage=CandidateStage.BLOCKED_HYGIENE)
        conn.commit()
    finally:
        conn.close()

    def fail_build(**_kwargs):
        partial = tmp_path / ".robot.sqlite.compact.partial"
        sqlite3.connect(partial).close()
        raise RuntimeError("injected compact failure")

    monkeypatch.setattr("pm_robot.ops._build_compact_evidence_database", fail_build)

    with pytest.raises(RuntimeError, match="injected compact failure"):
        compact_low_value_evidence(_settings(db_path), dry_run=False)

    assert not (tmp_path / ".robot.sqlite.compact.partial").exists()
    conn = connect(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (wallet,)
        ).fetchone()[0] == 5
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_registry WHERE address = ?", (wallet,)
        ).fetchone()[0] == 0
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_compact_evidence_rejects_missing_retained_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "robot.sqlite"
    low = "0x" + "4" * 40
    high = "0x" + "5" * 40
    conn = connect(db_path)
    try:
        run_migrations(conn)
        _seed_wallet(conn, low, stage=CandidateStage.BLOCKED_HYGIENE)
        _seed_wallet(conn, high, stage=CandidateStage.PAPER_CANDIDATE)
        conn.commit()
    finally:
        conn.close()

    original_build = ops._build_compact_evidence_database

    def corrupt_retained_rows(**kwargs):
        result = original_build(**kwargs)
        target = sqlite3.connect(kwargs["target_path"])
        try:
            activity_id = target.execute(
                "SELECT activity_id FROM wallet_activity WHERE address = ? LIMIT 1",
                (high,),
            ).fetchone()[0]
            target.execute(
                "DELETE FROM wallet_activity WHERE activity_id = ?",
                (activity_id,),
            )
            target.commit()
        finally:
            target.close()
        return result

    monkeypatch.setattr(
        "pm_robot.ops._build_compact_evidence_database",
        corrupt_retained_rows,
    )

    with pytest.raises(RuntimeError, match="compact database validation failed"):
        compact_low_value_evidence(_settings(db_path), dry_run=False)

    assert not (tmp_path / ".robot.sqlite.compact.partial").exists()
    conn = connect(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (low,)
        ).fetchone()[0] == 5
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?", (high,)
        ).fetchone()[0] == 5
    finally:
        conn.close()


def test_compact_evidence_requires_migrations_without_applying_them(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        latest_version = int(
            conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        )
        conn.execute("DELETE FROM schema_migrations WHERE version = ?", (latest_version,))
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="requires a current schema"):
        compact_low_value_evidence(_settings(db_path), dry_run=True)

    conn = connect(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = ?",
            (latest_version,),
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_database_access_guard_excludes_open_application_connections(tmp_path):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        with pytest.raises(TimeoutError, match="exclusive database access lock"):
            with database_access_guard(
                db_path,
                exclusive=True,
                timeout_seconds=0.05,
            ):
                pass
    finally:
        conn.close()

    with database_access_guard(db_path, exclusive=True, timeout_seconds=0.05):
        with pytest.raises(TimeoutError, match="shared database access lock"):
            with database_access_guard(
                db_path,
                exclusive=False,
                timeout_seconds=0.05,
            ):
                pass
