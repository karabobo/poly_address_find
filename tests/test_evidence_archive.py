import json

import duckdb
import pytest

from pm_robot import ops
from pm_robot.config import RobotSettings
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown, WalletFeatures
from pm_robot.ops import prune_low_value_evidence
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.evidence_archive import archived_wallet_summary, verify_archive_manifest
from pm_robot.storage.repository import (
    persist_score,
    persist_wallet_activity,
    upsert_candidate,
    upsert_wallet_feature,
)


def _settings(tmp_path):
    return RobotSettings(
        db_path=tmp_path / "robot.sqlite",
        archive_dir=tmp_path / "parquet",
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


def _seed_low_value_wallet(settings: RobotSettings, wallet: str) -> None:
    conn = connect(settings.db_path)
    try:
        run_migrations(conn)
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="archive-test"))
        upsert_wallet_feature(
            conn,
            WalletFeatures(
                address=wallet,
                hygiene_status="clean",
                extra={"feature_materializer_version": "archive-test"},
            ),
        )
        persist_wallet_activity(
            conn,
            wallet,
            [_activity(idx) for idx in range(5)],
            ingested_at=2_000,
        )
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions, gamma_marked_positions,
                fallback_marked_positions, mark_coverage, settled_cost_usd, settled_pnl_usd,
                settled_roi, total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 5, 0, 2, 2, 0, 1.0, 100, -20, -0.2, -20, -0.2, 0, '[]', 2000)
            """,
            (wallet,),
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
            policy_version="archive-test",
        )
        conn.commit()
    finally:
        conn.close()


def test_verified_parquet_archive_precedes_sqlite_prune(tmp_path):
    settings = _settings(tmp_path)
    wallet = "0x" + "1" * 40
    _seed_low_value_wallet(settings, wallet)

    result = prune_low_value_evidence(
        settings,
        limit=5,
        dry_run=False,
        archive=True,
    )

    assert result["ok"] is True
    assert result["archive"]["status"] == "pruned"
    assert result["archive"]["row_count"] >= 5
    assert result["deleted"]["wallet_activity"] == 5
    manifest_path = settings.archive_dir / result["archive"]["manifest_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["source_schema_version"] == 45
    assert manifest["prune_version"] == "v3_parquet_archive"
    assert manifest["compression"] == "zstd"
    activity_file = next(item for item in manifest["files"] if item["table_name"] == "wallet_activity")
    assert any(column["name"] == "activity_id" and column["primary_key_order"] == 1 for column in activity_file["columns"])
    parquet_path = settings.archive_dir / activity_file["relative_path"]
    with duckdb.connect(":memory:") as parquet_db:
        archived_rows = parquet_db.execute(
            "SELECT address, COUNT(*) FROM read_parquet(?) GROUP BY address",
            [str(parquet_path)],
        ).fetchall()
    assert archived_rows == [(wallet, 5)]

    conn = connect(settings.db_path)
    try:
        run = dict(conn.execute("SELECT * FROM evidence_archive_runs").fetchone())
        registry = dict(
            conn.execute(
                """
                SELECT registry_status, raw_prune_version, raw_archive_run_id,
                       raw_archived_at, raw_archive_locator
                FROM wallet_registry WHERE address = ?
                """,
                (wallet,),
            ).fetchone()
        )
        remaining = conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert run["status"] == "pruned"
    assert run["manifest_path"] == result["archive"]["manifest_path"]
    assert registry["registry_status"] == "archived_raw_pruned"
    assert registry["raw_prune_version"] == "v3_parquet_archive"
    assert registry["raw_archive_run_id"] == run["run_id"]
    assert registry["raw_archived_at"] is not None
    assert registry["raw_archive_locator"] == f"parquet-wallet://{wallet}"
    assert remaining == 0

    second = prune_low_value_evidence(settings, limit=5, dry_run=False, archive=True)
    assert second["wallet_count"] == 0
    conn = connect(settings.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM evidence_archive_runs").fetchone()[0] == 1
    finally:
        conn.close()


def test_archive_failure_keeps_hot_rows_and_resumes_same_run(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    wallet = "0x" + "2" * 40
    _seed_low_value_wallet(settings, wallet)
    real_export = ops.export_evidence_archive

    def fail_export(*args, **kwargs):
        raise OSError("simulated archive volume failure")

    monkeypatch.setattr(ops, "export_evidence_archive", fail_export)
    failed = prune_low_value_evidence(settings, limit=5, dry_run=False, archive=True)

    assert failed["ok"] is False
    assert failed["archive"]["status"] == "failed"
    conn = connect(settings.db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 5
        run = dict(conn.execute("SELECT run_id, status FROM evidence_archive_runs").fetchone())
        retention = conn.execute(
            "SELECT registry_status, raw_retention_tier FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()
        blocked_write = persist_wallet_activity(
            conn,
            wallet,
            [_activity(99)],
            ingested_at=3_000,
        )
    finally:
        conn.close()
    assert run["status"] == "failed"
    assert tuple(retention) == ("archive_pending", "summary_only")
    assert blocked_write == 0

    monkeypatch.setattr(ops, "export_evidence_archive", real_export)
    recovered = prune_low_value_evidence(settings, limit=5, dry_run=False, archive=True)
    assert recovered["ok"] is True
    assert recovered["archive"]["run_id"] == run["run_id"]
    conn = connect(settings.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM evidence_archive_runs").fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_verified_archive_resume_reuses_original_keep_recent_scope(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    wallet = "0x" + "3" * 40
    _seed_low_value_wallet(settings, wallet)
    real_prune = ops._prune_wallet_evidence_batch

    def interrupt_prune(*args, **kwargs):
        raise RuntimeError("simulated interruption after archive verification")

    monkeypatch.setattr(ops, "_prune_wallet_evidence_batch", interrupt_prune)
    try:
        prune_low_value_evidence(
            settings,
            limit=5,
            keep_recent_activity=2,
            dry_run=False,
            archive=True,
        )
    except RuntimeError as exc:
        assert "simulated interruption" in str(exc)
    else:  # pragma: no cover - protects the failure injection itself
        raise AssertionError("expected prune interruption")

    conn = connect(settings.db_path)
    try:
        run = dict(
            conn.execute(
                "SELECT run_id, status, keep_recent_activity FROM evidence_archive_runs"
            ).fetchone()
        )
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 5
    finally:
        conn.close()
    assert run["status"] == "verified"
    assert run["keep_recent_activity"] == 2

    monkeypatch.setattr(ops, "_prune_wallet_evidence_batch", real_prune)
    recovered = prune_low_value_evidence(
        settings,
        limit=5,
        keep_recent_activity=0,
        dry_run=False,
        archive=True,
    )
    assert recovered["archive"]["run_id"] == run["run_id"]
    assert recovered["deleted"]["wallet_activity"] == 3
    conn = connect(settings.db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_archive_manifest_rejects_paths_outside_archive_root(tmp_path):
    with pytest.raises(ValueError, match="must remain relative"):
        verify_archive_manifest(tmp_path, "../outside/manifest.json")


def test_post_export_write_is_not_deleted_outside_captured_scope(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    wallet = "0x" + "4" * 40
    _seed_low_value_wallet(settings, wallet)
    real_prune = ops._prune_wallet_evidence_batch
    injected = False

    def inject_late_row(conn, wallets, **kwargs):
        nonlocal injected
        if not injected and kwargs.get("archive_run_id"):
            injected = True
            late = _activity(99)
            conn.execute(
                """
                INSERT INTO wallet_activity(
                    address, activity_key, timestamp, condition_id, event_slug, market_slug,
                    asset_id, outcome, type, side, price, size, usdc_size,
                    transaction_hash, raw_json, ingested_at
                ) VALUES (?, 'late-row', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', 3000)
                """,
                (
                    wallet,
                    late["timestamp"],
                    late["conditionId"],
                    late["eventSlug"],
                    late["slug"],
                    late["asset"],
                    late["outcome"],
                    late["type"],
                    late["side"],
                    late["price"],
                    late["size"],
                    late["usdcSize"],
                    late["transactionHash"],
                ),
            )
        return real_prune(conn, wallets, **kwargs)

    monkeypatch.setattr(ops, "_prune_wallet_evidence_batch", inject_late_row)
    first = prune_low_value_evidence(settings, limit=5, dry_run=False, archive=True)

    assert first["archive"]["status"] == "pruned_partial"
    assert first["archive"]["residual"]["wallet_activity"] == 1
    assert first["deleted"]["wallet_activity"] == 5
    conn = connect(settings.db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT registry_status FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == "archive_pending"
    finally:
        conn.close()

    monkeypatch.setattr(ops, "_prune_wallet_evidence_batch", real_prune)
    second = prune_low_value_evidence(settings, limit=5, dry_run=False, archive=True)
    assert second["archive"]["status"] == "pruned"
    assert second["deleted"]["wallet_activity"] == 1
    conn = connect(settings.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM evidence_archive_runs").fetchone()[0] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet,),
        ).fetchone()[0] == 0
        archived = archived_wallet_summary(conn, wallet)
        locator = conn.execute(
            "SELECT raw_archive_locator FROM wallet_registry WHERE address = ?",
            (wallet,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert locator == f"parquet-wallet://{wallet}"
    assert archived["locator"] == locator
    assert archived["run_count"] == 2
    assert [run["status"] for run in archived["runs"]] == ["pruned_partial", "pruned"]
