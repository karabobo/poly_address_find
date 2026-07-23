import os
from pathlib import Path

import duckdb
import pytest

import pm_robot.storage.wallet_history_store as wallet_history_store_module
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.wallet_history_store import (
    audit_wallet_history_artifacts,
    prune_superseded_wallet_history_artifacts,
    persist_wallet_history_artifact,
)
from pm_robot.wallet_levels import HistoryDepth


def _rows(count: int) -> list[dict]:
    return [
        {
            "timestamp": 1_000 + index,
            "conditionId": f"condition-{index % 3}",
            "eventSlug": f"event-{index % 3}",
            "slug": f"market-{index % 3}",
            "asset": f"asset-{index % 3}",
            "outcome": "YES",
            "type": "TRADE",
            "side": "BUY" if index % 2 == 0 else "SELL",
            "price": 0.5,
            "size": 20,
            "usdcSize": 10,
            "transactionHash": f"0x{index:064x}",
        }
        for index in range(count)
    ]


def test_history_artifact_writes_verified_parquet_without_sqlite_raw_rows(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "1" * 40
    try:
        run_migrations(conn)

        artifact = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(25),
            now=2_000,
        )
        conn.commit()

        path = archive_dir / artifact.relative_path
        assert path.is_file()
        assert artifact.row_count == 25
        assert artifact.min_timestamp == 1_000
        assert artifact.max_timestamp == 1_024
        with duckdb.connect(":memory:") as db:
            assert db.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0] == 25
            row = db.execute(
                "SELECT wallet, market_slug, usdc_size FROM read_parquet(?) ORDER BY timestamp LIMIT 1",
                [str(path)],
            ).fetchone()
        assert row == (wallet, "market-0", 10.0)
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'wallet_activity'"
        ).fetchone() is None
        catalog = conn.execute(
            "SELECT history_depth, row_count, status FROM wallet_history_artifacts "
            "WHERE wallet = ?",
            (wallet,),
        ).fetchone()
        assert dict(catalog) == {
            "history_depth": "light",
            "row_count": 25,
            "status": "active",
        }
    finally:
        conn.close()


def test_deep_artifact_supersedes_light_and_light_cannot_replace_deep(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "2" * 40
    try:
        run_migrations(conn)
        light = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(20),
            now=2_000,
        )
        deep = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.DEEP,
            rows=_rows(100),
            now=3_000,
        )
        conn.commit()

        rows = conn.execute(
            "SELECT artifact_id, history_depth, status FROM wallet_history_artifacts "
            "WHERE wallet = ? ORDER BY created_at",
            (wallet,),
        ).fetchall()
        assert [dict(row) for row in rows] == [
            {"artifact_id": light.artifact_id, "history_depth": "light", "status": "superseded"},
            {"artifact_id": deep.artifact_id, "history_depth": "deep", "status": "active"},
        ]

        with pytest.raises(ValueError, match="cannot replace deep history with light"):
            persist_wallet_history_artifact(
                conn,
                archive_dir=archive_dir,
                wallet=wallet,
                history_depth=HistoryDepth.LIGHT,
                rows=_rows(10),
                now=4_000,
            )
    finally:
        conn.close()


def test_light_writer_cannot_race_and_replace_new_deep_artifact(tmp_path, monkeypatch):
    db_path = tmp_path / "robot.sqlite"
    archive_dir = tmp_path / "parquet"
    light_conn = connect(db_path)
    deep_conn = connect(db_path)
    wallet = "0x" + "a" * 40
    original_write = wallet_history_store_module._write_verified_parquet
    interleaved: dict[str, object] = {}

    def write_and_commit_deep(path, rows):
        original_write(path, rows)
        if "depth=light" not in path.as_posix() or interleaved:
            return
        interleaved["artifact"] = persist_wallet_history_artifact(
            deep_conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.DEEP,
            rows=_rows(100),
            now=3_000,
        )
        deep_conn.commit()

    try:
        run_migrations(light_conn)
        monkeypatch.setattr(
            wallet_history_store_module,
            "_write_verified_parquet",
            write_and_commit_deep,
        )

        with pytest.raises(ValueError, match="cannot replace deep history with light"):
            persist_wallet_history_artifact(
                light_conn,
                archive_dir=archive_dir,
                wallet=wallet,
                history_depth=HistoryDepth.LIGHT,
                rows=_rows(20),
                now=2_000,
            )

        active = light_conn.execute(
            """
            SELECT artifact_id, history_depth, status
            FROM wallet_history_artifacts
            WHERE wallet = ?
            """,
            (wallet,),
        ).fetchall()
        deep_artifact = interleaved["artifact"]
        assert [dict(row) for row in active] == [
            {
                "artifact_id": deep_artifact.artifact_id,
                "history_depth": "deep",
                "status": "active",
            }
        ]
        parquet_paths = list(archive_dir.rglob("*.parquet"))
        assert len(parquet_paths) == 1
        assert "depth=deep" in parquet_paths[0].as_posix()
    finally:
        deep_conn.close()
        light_conn.close()


def test_history_artifact_deduplicates_repeated_activity_rows(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "3" * 40
    try:
        run_migrations(conn)
        repeated = _rows(2)
        artifact = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=[repeated[0], repeated[0], repeated[1]],
            now=2_000,
        )

        assert artifact.row_count == 2
        assert Path(archive_dir / artifact.relative_path).is_file()
    finally:
        conn.close()


def test_history_artifact_deduplicates_repeated_rows_without_transaction_hash(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        no_hash = _rows(1)[0]
        no_hash.pop("transactionHash")
        artifact = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=[dict(no_hash), dict(no_hash)],
            now=2_000,
        )

        assert artifact.row_count == 1
        with duckdb.connect(":memory:") as db:
            assert (
                db.execute(
                    "SELECT COUNT(*) FROM read_parquet(?)",
                    [str(archive_dir / artifact.relative_path)],
                ).fetchone()[0]
                == 1
            )
    finally:
        conn.close()


def test_history_gc_keeps_active_and_latest_superseded_snapshot_with_audit_tombstone(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "4" * 40
    try:
        run_migrations(conn)
        oldest = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(10),
            now=1_000,
        )
        retained = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(20),
            now=2_000,
        )
        active = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.DEEP,
            rows=_rows(30),
            now=3_000,
        )
        conn.commit()

        preview = prune_superseded_wallet_history_artifacts(
            conn,
            archive_dir=archive_dir,
            keep_per_wallet=1,
            min_age_seconds=0,
            dry_run=True,
            now=4_000,
        )
        executed = prune_superseded_wallet_history_artifacts(
            conn,
            archive_dir=archive_dir,
            keep_per_wallet=1,
            min_age_seconds=0,
            dry_run=False,
            now=4_000,
        )
        conn.commit()

        assert preview.candidates == 1
        assert preview.files_deleted == 0
        assert executed.files_deleted == 1
        assert executed.catalog_rows_marked == 1
        assert not (archive_dir / oldest.relative_path).exists()
        assert (archive_dir / retained.relative_path).is_file()
        assert (archive_dir / active.relative_path).is_file()
        rows = conn.execute(
            "SELECT artifact_id, status, byte_size, purged_at "
            "FROM wallet_history_artifacts WHERE wallet = ? ORDER BY created_at",
            (wallet,),
        ).fetchall()
        assert [dict(row) for row in rows] == [
            {
                "artifact_id": oldest.artifact_id,
                "status": "superseded",
                "byte_size": 0,
                "purged_at": 4_000,
            },
            {
                "artifact_id": retained.artifact_id,
                "status": "superseded",
                "byte_size": retained.byte_size,
                "purged_at": None,
            },
            {
                "artifact_id": active.artifact_id,
                "status": "active",
                "byte_size": active.byte_size,
                "purged_at": None,
            },
        ]
    finally:
        conn.close()


def test_history_gc_rejects_catalog_paths_outside_archive_root(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"do not delete")
    wallet = "0x" + "5" * 40
    try:
        run_migrations(conn)
        conn.execute(
            """
            INSERT INTO wallet_history_artifacts(
                artifact_id, wallet, history_depth, storage_version,
                relative_path, row_count, byte_size, checksum, status,
                created_at, updated_at
            ) VALUES ('unsafe', ?, 'light', 'test', '../outside.parquet',
                      1, 13, 'checksum', 'superseded', 1000, 1000)
            """,
            (wallet,),
        )
        conn.commit()

        result = prune_superseded_wallet_history_artifacts(
            conn,
            archive_dir=archive_dir,
            keep_per_wallet=0,
            min_age_seconds=0,
            dry_run=False,
            now=2_000,
        )

        assert result.candidates == 1
        assert result.unsafe_paths == 1
        assert result.catalog_rows_marked == 0
        assert outside.read_bytes() == b"do not delete"
        assert conn.execute(
            "SELECT purged_at FROM wallet_history_artifacts WHERE artifact_id = 'unsafe'"
        ).fetchone()[0] is None
    finally:
        conn.close()


def test_history_gc_recovers_after_file_delete_before_final_tombstone(
    tmp_path,
    monkeypatch,
):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "b" * 40
    original_finalize = wallet_history_store_module._finalize_purged_artifact
    try:
        run_migrations(conn)
        oldest = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(10),
            now=1_000,
        )
        persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(20),
            now=2_000,
        )
        persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.DEEP,
            rows=_rows(30),
            now=3_000,
        )
        conn.commit()

        def crash_before_tombstone(*args, **kwargs):
            raise RuntimeError("simulated crash after delete")

        monkeypatch.setattr(
            wallet_history_store_module,
            "_finalize_purged_artifact",
            crash_before_tombstone,
        )
        with pytest.raises(RuntimeError, match="simulated crash after delete"):
            prune_superseded_wallet_history_artifacts(
                conn,
                archive_dir=archive_dir,
                keep_per_wallet=1,
                min_age_seconds=0,
                dry_run=False,
                now=4_000,
            )

        interrupted = conn.execute(
            """
            SELECT byte_size, purge_started_at, purged_at
            FROM wallet_history_artifacts
            WHERE artifact_id = ?
            """,
            (oldest.artifact_id,),
        ).fetchone()
        assert not (archive_dir / oldest.relative_path).exists()
        assert dict(interrupted) == {
            "byte_size": oldest.byte_size,
            "purge_started_at": 4_000,
            "purged_at": None,
        }

        monkeypatch.setattr(
            wallet_history_store_module,
            "_finalize_purged_artifact",
            original_finalize,
        )
        recovered = prune_superseded_wallet_history_artifacts(
            conn,
            archive_dir=archive_dir,
            keep_per_wallet=1,
            min_age_seconds=0,
            dry_run=False,
            now=4_001,
        )
        final = conn.execute(
            """
            SELECT byte_size, purge_started_at, purged_at
            FROM wallet_history_artifacts
            WHERE artifact_id = ?
            """,
            (oldest.artifact_id,),
        ).fetchone()
        assert recovered.files_missing == 1
        assert recovered.catalog_rows_marked == 1
        assert dict(final) == {
            "byte_size": 0,
            "purge_started_at": 4_000,
            "purged_at": 4_001,
        }
    finally:
        conn.close()


def test_history_gc_refuses_to_delete_checksum_mismatch(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "c" * 40
    try:
        run_migrations(conn)
        oldest = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(10),
            now=1_000,
        )
        persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(20),
            now=2_000,
        )
        persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.DEEP,
            rows=_rows(30),
            now=3_000,
        )
        conn.commit()
        path = archive_dir / oldest.relative_path
        content = bytearray(path.read_bytes())
        content[len(content) // 2] ^= 1
        path.write_bytes(content)

        result = prune_superseded_wallet_history_artifacts(
            conn,
            archive_dir=archive_dir,
            keep_per_wallet=1,
            min_age_seconds=0,
            dry_run=False,
            now=4_000,
        )

        state = conn.execute(
            """
            SELECT purge_started_at, purged_at
            FROM wallet_history_artifacts
            WHERE artifact_id = ?
            """,
            (oldest.artifact_id,),
        ).fetchone()
        assert result.checksum_mismatches == 1
        assert result.catalog_rows_marked == 0
        assert result.status == "partial"
        assert path.is_file()
        assert dict(state) == {"purge_started_at": None, "purged_at": None}
    finally:
        conn.close()


def test_history_audit_reports_catalog_drift_with_relative_issue_paths(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    good_wallet = "0x" + "6" * 40
    changed_wallet = "0x" + "7" * 40
    missing_wallet = "0x" + "8" * 40
    try:
        run_migrations(conn)
        good = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=good_wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(10),
            now=1_000,
        )
        changed = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=changed_wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(10),
            now=1_000,
        )
        conn.execute(
            """
            INSERT INTO wallet_history_artifacts(
                artifact_id, wallet, history_depth, storage_version,
                relative_path, row_count, byte_size, checksum, status,
                created_at, updated_at
            ) VALUES ('missing', ?, 'light', 'test',
                      'wallet_history/missing.parquet', 1, 10, 'checksum',
                      'active', 1000, 1000)
            """,
            (missing_wallet,),
        )
        conn.commit()
        changed_path = archive_dir / changed.relative_path
        changed_path.write_bytes(changed_path.read_bytes() + b"changed")

        result = audit_wallet_history_artifacts(
            conn,
            archive_dir=archive_dir,
            now=2_000,
        )

        assert result.catalog_rows == 3
        assert result.expected_files == 3
        assert result.verified_files == 1
        assert result.missing_files == 1
        assert result.size_mismatches == 1
        assert result.checksum_mismatches == 0
        assert result.status == "partial"
        assert f"missing:wallet_history/missing.parquet" in result.issue_paths
        assert f"size:{changed.relative_path}" in result.issue_paths
        assert str(tmp_path) not in "\n".join(result.issue_paths)
        assert (archive_dir / good.relative_path).is_file()
    finally:
        conn.close()


def test_history_audit_checksum_mode_detects_same_size_tampering(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    wallet = "0x" + "9" * 40
    try:
        run_migrations(conn)
        artifact = persist_wallet_history_artifact(
            conn,
            archive_dir=archive_dir,
            wallet=wallet,
            history_depth=HistoryDepth.LIGHT,
            rows=_rows(10),
            now=1_000,
        )
        conn.commit()
        path = archive_dir / artifact.relative_path
        content = bytearray(path.read_bytes())
        content[len(content) // 2] ^= 1
        path.write_bytes(content)

        result = audit_wallet_history_artifacts(
            conn,
            archive_dir=archive_dir,
            verify_checksums=True,
            now=2_000,
        )

        assert result.size_mismatches == 0
        assert result.checksum_mismatches == 1
        assert result.verified_files == 0
        assert result.status == "partial"
    finally:
        conn.close()


def test_history_audit_deletes_only_old_uncatalogued_files(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    archive_dir = tmp_path / "parquet"
    old = archive_dir / "wallet_history" / "orphan-old.parquet"
    young = archive_dir / "wallet_history" / "orphan-young.parquet"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    young.write_bytes(b"young")
    os.utime(old, (1_000, 1_000))
    os.utime(young, (1_900, 1_900))
    try:
        run_migrations(conn)

        result = audit_wallet_history_artifacts(
            conn,
            archive_dir=archive_dir,
            orphan_min_age_seconds=500,
            orphan_limit=10,
            delete_orphans=True,
            now=2_000,
        )

        assert result.orphan_files == 2
        assert result.orphan_candidates == 1
        assert result.orphan_files_deleted == 1
        assert result.orphan_bytes_deleted == 3
        assert result.status == "ok"
        assert not old.exists()
        assert young.exists()
    finally:
        conn.close()
