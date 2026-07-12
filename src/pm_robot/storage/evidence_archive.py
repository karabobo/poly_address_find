"""Crash-safe Parquet archive for evidence leaving the SQLite hot store."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from pm_robot.storage.db import connect


ARCHIVE_VERSION = "evidence_parquet_v1"
RESUMABLE_ARCHIVE_STATUSES = ("pending", "exporting", "failed", "verified")
QUERYABLE_ARCHIVE_STATUSES = ("verified", "pruned_partial", "pruned")
MAX_ARCHIVE_QUERY_LIMIT = 10_000


@dataclass(frozen=True)
class EvidenceTableSpec:
    table: str
    where_sql: str


EVIDENCE_TABLE_SPECS = (
    EvidenceTableSpec(
        "copy_backtest_trades",
        "leader_wallet IN (SELECT wallet FROM temp_prune_wallets) "
        "OR follower_wallet IN (SELECT wallet FROM temp_prune_wallets)",
    ),
    EvidenceTableSpec(
        "copy_trade_links",
        "leader_wallet IN (SELECT wallet FROM temp_prune_wallets) "
        "OR follower_wallet IN (SELECT wallet FROM temp_prune_wallets)",
    ),
    EvidenceTableSpec(
        "copy_pair_stats",
        "leader_wallet IN (SELECT wallet FROM temp_prune_wallets) "
        "OR follower_wallet IN (SELECT wallet FROM temp_prune_wallets)",
    ),
    EvidenceTableSpec("paper_fills", "wallet IN (SELECT wallet FROM temp_prune_wallets)"),
    EvidenceTableSpec("paper_orders", "wallet IN (SELECT wallet FROM temp_prune_wallets)"),
    EvidenceTableSpec("paper_positions", "wallet IN (SELECT wallet FROM temp_prune_wallets)"),
    EvidenceTableSpec("paper_settlements", "wallet IN (SELECT wallet FROM temp_prune_wallets)"),
    EvidenceTableSpec("paper_marks", "wallet IN (SELECT wallet FROM temp_prune_wallets)"),
    EvidenceTableSpec("wallet_episodes", "address IN (SELECT wallet FROM temp_prune_wallets)"),
    EvidenceTableSpec(
        "wallet_activity",
        "address IN (SELECT wallet FROM temp_prune_wallets) "
        "AND activity_id NOT IN (SELECT activity_id FROM temp_prune_keep_activity)",
    ),
    EvidenceTableSpec("wallet_positions", "address IN (SELECT wallet FROM temp_prune_wallets)"),
    EvidenceTableSpec(
        "leader_scores",
        "address IN (SELECT wallet FROM temp_prune_wallets) "
        "AND score_id NOT IN ("
        "SELECT score_id FROM leader_latest_scores "
        "WHERE address IN (SELECT wallet FROM temp_prune_wallets))",
    ),
)


def ensure_archive_backend() -> None:
    """Fail before changing wallet state when the Parquet backend is unavailable."""

    try:
        import duckdb  # noqa: F401
    except ImportError as exc:  # pragma: no cover - deployment packaging guard
        raise RuntimeError("DuckDB is required for Parquet evidence archival") from exc


def prepare_prune_temp_tables(
    conn: sqlite3.Connection,
    wallets: Iterable[str],
    *,
    keep_recent_activity: int,
) -> None:
    """Materialize the exact wallet and retained-activity scope used by archive and prune."""

    conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_prune_wallets(wallet TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM temp_prune_wallets")
    conn.executemany(
        "INSERT OR IGNORE INTO temp_prune_wallets(wallet) VALUES (?)",
        ((wallet.lower(),) for wallet in wallets),
    )
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_prune_keep_activity(activity_id INTEGER PRIMARY KEY)")
    conn.execute("DELETE FROM temp_prune_keep_activity")
    if keep_recent_activity <= 0 or not _table_exists(conn, "wallet_activity"):
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO temp_prune_keep_activity(activity_id)
        SELECT activity_id
        FROM (
            SELECT
                wa.activity_id,
                ROW_NUMBER() OVER (
                    PARTITION BY wa.address
                    ORDER BY wa.timestamp DESC, wa.activity_id DESC
                ) AS rn
            FROM wallet_activity wa
            JOIN temp_prune_wallets pw ON pw.wallet = wa.address
        )
        WHERE rn <= ?
        """,
        (keep_recent_activity,),
    )


def drop_prune_temp_tables(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS temp_prune_keep_activity")
    conn.execute("DROP TABLE IF EXISTS temp_prune_wallets")


def capture_archive_scope(
    conn: sqlite3.Connection,
    run_id: str,
    wallets: list[str],
    *,
    keep_recent_activity: int,
) -> dict[str, int]:
    """Freeze exact SQLite rowids so export and delete share one immutable scope."""

    prepare_prune_temp_tables(
        conn,
        wallets,
        keep_recent_activity=keep_recent_activity,
    )
    conn.execute("DELETE FROM evidence_archive_scope WHERE run_id = ?", (run_id,))
    tables = _table_names(conn)
    captured: dict[str, int] = {}
    for spec in EVIDENCE_TABLE_SPECS:
        if spec.table not in tables:
            captured[spec.table] = 0
            continue
        before = conn.total_changes
        conn.execute(
            f"""
            INSERT OR IGNORE INTO evidence_archive_scope(run_id, table_name, row_id)
            SELECT ?, ?, rowid
            FROM "{spec.table}"
            WHERE {spec.where_sql}
            """,
            (run_id, spec.table),
        )
        captured[spec.table] = conn.total_changes - before
    drop_prune_temp_tables(conn)
    return captured


def create_archive_run(
    conn: sqlite3.Connection,
    wallets: list[str],
    *,
    keep_recent_activity: int,
    now: int | None = None,
) -> dict[str, Any]:
    """Create a durable archive run; this does not export or delete evidence."""

    ts = int(time.time()) if now is None else int(now)
    digest = hashlib.sha256(("|".join(sorted(wallets)) + f"|{ts}|{uuid.uuid4().hex}").encode()).hexdigest()[:12]
    run_id = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(ts))}-{digest}"
    relative_dir = Path("evidence") / f"version={ARCHIVE_VERSION}" / f"archived_date={time.strftime('%Y-%m-%d', time.gmtime(ts))}" / f"run_id={run_id}"
    conn.execute(
        """
        INSERT INTO evidence_archive_runs(
            run_id, archive_version, status, archive_path, wallet_count,
            keep_recent_activity, created_at, updated_at
        ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            ARCHIVE_VERSION,
            relative_dir.as_posix(),
            len(wallets),
            max(0, int(keep_recent_activity)),
            ts,
            ts,
        ),
    )
    conn.executemany(
        "INSERT INTO evidence_archive_wallets(run_id, wallet) VALUES (?, ?)",
        ((run_id, wallet.lower()) for wallet in wallets),
    )
    return {
        "run_id": run_id,
        "status": "pending",
        "archive_path": relative_dir.as_posix(),
        "keep_recent_activity": max(0, int(keep_recent_activity)),
        "wallets": [wallet.lower() for wallet in wallets],
    }


def resumable_archive_run(conn: sqlite3.Connection) -> dict[str, Any] | None:
    placeholders = ",".join("?" for _ in RESUMABLE_ARCHIVE_STATUSES)
    row = conn.execute(
        f"""
        SELECT *
        FROM evidence_archive_runs
        WHERE status IN ({placeholders})
        ORDER BY created_at ASC, run_id ASC
        LIMIT 1
        """,
        RESUMABLE_ARCHIVE_STATUSES,
    ).fetchone()
    if row is None:
        return None
    result = dict(row)
    result["wallets"] = [
        str(wallet_row["wallet"])
        for wallet_row in conn.execute(
            "SELECT wallet FROM evidence_archive_wallets WHERE run_id = ? ORDER BY wallet",
            (result["run_id"],),
        ).fetchall()
    ]
    return result


def set_archive_run_status(
    conn: sqlite3.Connection,
    run_id: str,
    status: str,
    *,
    error: str = "",
    now: int | None = None,
) -> None:
    ts = int(time.time()) if now is None else int(now)
    conn.execute(
        """
        UPDATE evidence_archive_runs
        SET status = ?, error = ?, updated_at = ?,
            verified_at = CASE WHEN ? = 'verified' THEN ? ELSE verified_at END,
            pruned_at = CASE WHEN ? = 'pruned' THEN ? ELSE pruned_at END
        WHERE run_id = ?
        """,
        (status, error[:2000], ts, status, ts, status, ts, run_id),
    )


def export_evidence_archive(
    db_path: Path,
    archive_root: Path,
    *,
    run_id: str,
    archive_path: str,
    wallets: list[str],
    keep_recent_activity: int,
    archived_at: int | None = None,
) -> dict[str, Any]:
    """Write and verify immutable Parquet files without holding a SQLite write lock."""

    ensure_archive_backend()
    archived_at = int(time.time()) if archived_at is None else int(archived_at)
    target_dir = _archive_path(archive_root, archive_path)
    target_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        files: list[dict[str, Any]] = []
        tables = _table_names(conn)
        source_schema_version = int(
            conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0]
        )
        for spec in EVIDENCE_TABLE_SPECS:
            if spec.table not in tables:
                continue
            result = _write_table_parquet(
                conn,
                spec,
                target_dir=target_dir,
                archive_root=archive_root,
                run_id=run_id,
                archived_at=archived_at,
            )
            if result is not None:
                files.append(result)
    finally:
        conn.close()

    manifest = {
        "schema_version": ARCHIVE_VERSION,
        "source_schema_version": source_schema_version,
        "prune_version": "v3_parquet_archive",
        "compression": "zstd",
        "run_id": run_id,
        "archived_at": archived_at,
        "wallets": sorted(wallet.lower() for wallet in wallets),
        "files": files,
        "row_count": sum(int(item["row_count"]) for item in files),
        "file_count": len(files),
        "byte_size": sum(int(item["byte_size"]) for item in files),
        "restore_hint": "Filter archived tables by wallet columns before restoring into a scratch database.",
    }
    manifest_path = target_dir / "manifest.json"
    _atomic_write_json(manifest_path, manifest)
    manifest_relative = manifest_path.relative_to(archive_root).as_posix()
    verify_archive_manifest(archive_root, manifest_relative)
    manifest["manifest_path"] = manifest_relative
    return manifest


def register_archive_manifest(conn: sqlite3.Connection, manifest: dict[str, Any]) -> None:
    run_id = str(manifest["run_id"])
    conn.execute("DELETE FROM evidence_archive_files WHERE run_id = ?", (run_id,))
    conn.executemany(
        """
        INSERT INTO evidence_archive_files(
            run_id, table_name, relative_path, row_count, byte_size,
            checksum, min_ts, max_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                run_id,
                item["table_name"],
                item["relative_path"],
                int(item["row_count"]),
                int(item["byte_size"]),
                item["checksum"],
                item.get("min_ts"),
                item.get("max_ts"),
            )
            for item in manifest["files"]
        ),
    )
    conn.execute(
        """
        UPDATE evidence_archive_runs
        SET manifest_path = ?, row_count = ?, file_count = ?, byte_size = ?,
            status = 'verified', error = '', verified_at = ?, updated_at = ?
        WHERE run_id = ?
        """,
        (
            manifest["manifest_path"],
            int(manifest["row_count"]),
            int(manifest["file_count"]),
            int(manifest["byte_size"]),
            int(manifest["archived_at"]),
            int(manifest["archived_at"]),
            run_id,
        ),
    )


def verify_archive_manifest(archive_root: Path, manifest_path: str) -> dict[str, Any]:
    import duckdb

    path = _archive_path(archive_root, manifest_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for item in manifest.get("files", []):
        parquet_path = _archive_path(archive_root, str(item["relative_path"]))
        if not parquet_path.is_file():
            raise RuntimeError(f"archive file missing: {item['relative_path']}")
        if parquet_path.stat().st_size != int(item["byte_size"]):
            raise RuntimeError(f"archive size mismatch: {item['relative_path']}")
        if _sha256_file(parquet_path) != item["checksum"]:
            raise RuntimeError(f"archive checksum mismatch: {item['relative_path']}")
        with duckdb.connect(":memory:") as parquet_db:
            actual_rows = int(
                parquet_db.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(parquet_path)]).fetchone()[0]
            )
        if actual_rows != int(item["row_count"]):
            raise RuntimeError(f"archive row count mismatch: {item['relative_path']}")
    return manifest


def evidence_archive_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(conn, "evidence_archive_runs"):
        return {
            "run_count": 0,
            "wallet_count": 0,
            "file_count": 0,
            "row_count": 0,
            "byte_size": 0,
            "failed_count": 0,
            "pending_count": 0,
            "latest_pruned_at": 0,
            "recent_runs": [],
        }
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS run_count,
            COALESCE(SUM(wallet_count), 0) AS wallet_count,
            COALESCE(SUM(file_count), 0) AS file_count,
            COALESCE(SUM(row_count), 0) AS row_count,
            COALESCE(SUM(byte_size), 0) AS byte_size,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
            SUM(CASE WHEN status IN ('pending', 'exporting', 'verified', 'pruned_partial') THEN 1 ELSE 0 END) AS pending_count,
            COALESCE(MAX(pruned_at), 0) AS latest_pruned_at
        FROM evidence_archive_runs
        """
    ).fetchone()
    result = dict(row)
    result["recent_runs"] = recent_archive_runs(conn)
    return result


def recent_archive_runs(
    conn: sqlite3.Connection,
    *,
    limit: int = 8,
) -> list[dict[str, Any]]:
    """Return bounded run-level audit details without verifying file contents again."""

    if not _table_exists(conn, "evidence_archive_runs"):
        return []
    rows = conn.execute(
        """
        SELECT
            run_id,
            status,
            manifest_path,
            wallet_count,
            row_count,
            file_count,
            byte_size,
            error,
            created_at,
            verified_at,
            pruned_at,
            updated_at
        FROM evidence_archive_runs
        ORDER BY updated_at DESC, run_id DESC
        LIMIT ?
        """,
        (max(1, min(int(limit), 100)),),
    ).fetchall()
    return [dict(row) for row in rows]


def archive_catalog_coverage(
    conn: sqlite3.Connection,
    archive_root: Path,
    *,
    sample_limit: int = 5,
) -> dict[str, Any]:
    """Compare queryable archive catalog entries with Parquet files on disk."""

    catalog_rows: list[sqlite3.Row] = []
    run_metadata_mismatches: list[str] = []
    if _table_exists(conn, "evidence_archive_files") and _table_exists(
        conn, "evidence_archive_runs"
    ):
        placeholders = ",".join("?" for _ in QUERYABLE_ARCHIVE_STATUSES)
        catalog_rows = conn.execute(
            f"""
            SELECT files.run_id, files.table_name, files.relative_path, files.byte_size
            FROM evidence_archive_files files
            JOIN evidence_archive_runs runs ON runs.run_id = files.run_id
            WHERE runs.status IN ({placeholders})
            ORDER BY files.relative_path, files.run_id, files.table_name
            """,
            QUERYABLE_ARCHIVE_STATUSES,
        ).fetchall()
        run_rows = conn.execute(
            f"""
            SELECT
                runs.run_id,
                runs.file_count AS expected_file_count,
                runs.byte_size AS expected_byte_size,
                COUNT(files.relative_path) AS catalog_file_count,
                COALESCE(SUM(files.byte_size), 0) AS catalog_byte_size
            FROM evidence_archive_runs runs
            LEFT JOIN evidence_archive_files files ON files.run_id = runs.run_id
            WHERE runs.status IN ({placeholders})
            GROUP BY runs.run_id, runs.file_count, runs.byte_size
            ORDER BY runs.run_id
            """,
            QUERYABLE_ARCHIVE_STATUSES,
        ).fetchall()
        run_metadata_mismatches = [
            str(row["run_id"])
            for row in run_rows
            if int(row["expected_file_count"] or 0)
            != int(row["catalog_file_count"] or 0)
            or int(row["expected_byte_size"] or 0)
            != int(row["catalog_byte_size"] or 0)
        ]

    root = archive_root.resolve()
    catalog: dict[str, int] = {}
    duplicate_catalog_paths = 0
    invalid_catalog_paths: list[str] = []
    for row in catalog_rows:
        relative_path = str(row["relative_path"])
        try:
            path = _archive_path(archive_root, relative_path)
        except ValueError:
            invalid_catalog_paths.append(relative_path)
            continue
        normalized = path.relative_to(root).as_posix()
        if normalized in catalog:
            duplicate_catalog_paths += 1
            continue
        catalog[normalized] = int(row["byte_size"] or 0)

    actual: dict[str, int] = {}
    unsafe_file_count = 0
    if archive_root.exists():
        for candidate in archive_root.rglob("*.parquet"):
            if not candidate.is_file():
                continue
            resolved = candidate.resolve()
            if not resolved.is_relative_to(root):
                unsafe_file_count += 1
                continue
            actual[resolved.relative_to(root).as_posix()] = int(
                resolved.stat().st_size
            )

    catalog_paths = set(catalog)
    actual_paths = set(actual)
    matched_paths = catalog_paths & actual_paths
    missing_paths = sorted(catalog_paths - actual_paths)
    untracked_paths = sorted(actual_paths - catalog_paths)
    size_mismatch_paths = sorted(
        path for path in matched_paths if actual[path] != catalog[path]
    )
    parquet_total_bytes = sum(actual.values())
    cataloged_on_disk_bytes = sum(actual[path] for path in matched_paths)
    if (
        missing_paths
        or size_mismatch_paths
        or unsafe_file_count
        or duplicate_catalog_paths
        or run_metadata_mismatches
        or invalid_catalog_paths
    ):
        state = "broken"
    elif untracked_paths:
        state = "partial"
    elif actual_paths or catalog_paths:
        state = "complete"
    else:
        state = "empty"
    bounded_samples = max(0, min(int(sample_limit), 20))
    return {
        "available": True,
        "state": state,
        "healthy": state not in {"broken"},
        "parquet_file_count": len(actual),
        "parquet_total_bytes": parquet_total_bytes,
        "catalog_entry_count": len(catalog_rows),
        "cataloged_file_count": len(catalog),
        "cataloged_expected_bytes": sum(catalog.values()),
        "cataloged_on_disk_file_count": len(matched_paths),
        "cataloged_on_disk_bytes": cataloged_on_disk_bytes,
        "catalog_coverage_ratio": round(
            cataloged_on_disk_bytes / parquet_total_bytes,
            4,
        )
        if parquet_total_bytes > 0
        else 0.0,
        "untracked_file_count": len(untracked_paths),
        "untracked_bytes": sum(actual[path] for path in untracked_paths),
        "missing_file_count": len(missing_paths),
        "missing_expected_bytes": sum(catalog[path] for path in missing_paths),
        "size_mismatch_count": len(size_mismatch_paths),
        "unsafe_file_count": unsafe_file_count,
        "duplicate_catalog_path_count": duplicate_catalog_paths,
        "invalid_catalog_path_count": len(invalid_catalog_paths),
        "run_metadata_mismatch_count": len(run_metadata_mismatches),
        "untracked_examples": untracked_paths[:bounded_samples],
        "missing_examples": missing_paths[:bounded_samples],
        "size_mismatch_examples": size_mismatch_paths[:bounded_samples],
        "invalid_catalog_path_examples": invalid_catalog_paths[:bounded_samples],
        "run_metadata_mismatch_examples": run_metadata_mismatches[:bounded_samples],
    }


def archived_wallet_summary(conn: sqlite3.Connection, wallet: str) -> dict[str, Any]:
    """Resolve the stable wallet locator to every archive run that contains it."""

    normalized = wallet.lower()
    if not _table_exists(conn, "evidence_archive_runs"):
        return {
            "wallet": normalized,
            "locator": f"parquet-wallet://{normalized}",
            "run_count": 0,
            "row_count": 0,
            "byte_size": 0,
            "runs": [],
        }
    rows = conn.execute(
        """
        SELECT
            runs.run_id,
            runs.status,
            runs.manifest_path,
            runs.row_count,
            runs.file_count,
            runs.byte_size,
            runs.verified_at,
            runs.pruned_at
        FROM evidence_archive_wallets wallets
        JOIN evidence_archive_runs runs ON runs.run_id = wallets.run_id
        WHERE wallets.wallet = ?
          AND runs.status IN ('verified', 'pruned_partial', 'pruned')
        ORDER BY runs.rowid ASC
        """,
        (normalized,),
    ).fetchall()
    runs = [dict(row) for row in rows]
    return {
        "wallet": normalized,
        "locator": f"parquet-wallet://{normalized}",
        "run_count": len(runs),
        "row_count": sum(int(row["row_count"] or 0) for row in runs),
        "file_count": sum(int(row["file_count"] or 0) for row in runs),
        "byte_size": sum(int(row["byte_size"] or 0) for row in runs),
        "runs": runs,
    }


def query_archived_wallet_activity(
    conn: sqlite3.Connection,
    archive_root: Path,
    wallet: str,
    *,
    limit: int = 100,
    since: int | None = None,
    until: int | None = None,
) -> dict[str, Any]:
    """Read one wallet's verified cold activity through DuckDB without mutating storage."""

    ensure_archive_backend()
    normalized = wallet.strip().lower()
    if not normalized:
        raise ValueError("wallet must not be empty")
    bounded_limit = max(1, min(int(limit), MAX_ARCHIVE_QUERY_LIMIT))
    if not _table_exists(conn, "evidence_archive_files"):
        return _empty_archived_activity(normalized, bounded_limit, since, until)
    placeholders = ",".join("?" for _ in QUERYABLE_ARCHIVE_STATUSES)
    file_rows = conn.execute(
        f"""
        SELECT files.run_id, files.relative_path
        FROM evidence_archive_wallets wallets
        JOIN evidence_archive_runs runs ON runs.run_id = wallets.run_id
        JOIN evidence_archive_files files ON files.run_id = runs.run_id
        WHERE wallets.wallet = ?
          AND files.table_name = 'wallet_activity'
          AND runs.status IN ({placeholders})
        ORDER BY runs.created_at, files.run_id
        """,
        (normalized, *QUERYABLE_ARCHIVE_STATUSES),
    ).fetchall()
    if not file_rows:
        return _empty_archived_activity(normalized, bounded_limit, since, until)

    parquet_paths: list[str] = []
    run_ids: set[str] = set()
    for row in file_rows:
        path = _archive_path(archive_root, str(row["relative_path"]))
        if not path.is_file():
            raise RuntimeError(f"cataloged archive file missing: {row['relative_path']}")
        parquet_paths.append(str(path))
        run_ids.add(str(row["run_id"]))

    filters = ["lower(address) = ?"]
    parameters: list[Any] = [parquet_paths, normalized]
    if since is not None:
        filters.append("timestamp >= ?")
        parameters.append(int(since))
    if until is not None:
        filters.append("timestamp <= ?")
        parameters.append(int(until))
    parameters.append(bounded_limit)
    query = f"""
        SELECT
            activity_id,
            address,
            timestamp,
            condition_id,
            event_slug,
            market_slug,
            asset_id,
            outcome,
            type,
            side,
            price,
            size,
            usdc_size,
            transaction_hash,
            ingested_at,
            _archive_run_id AS archive_run_id,
            _archived_at AS archived_at,
            COUNT(*) OVER () AS matching_row_count
        FROM read_parquet(?, union_by_name = true)
        WHERE {' AND '.join(filters)}
        ORDER BY timestamp DESC, activity_id DESC
        LIMIT ?
    """
    import duckdb

    with duckdb.connect(":memory:") as archive_db:
        cursor = archive_db.execute(query, parameters)
        columns = [str(item[0]) for item in cursor.description or []]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    matching_row_count = int(rows[0].pop("matching_row_count")) if rows else 0
    for row in rows[1:]:
        row.pop("matching_row_count", None)
    return {
        "ok": True,
        "state": "ready",
        "wallet": normalized,
        "locator": f"parquet-wallet://{normalized}",
        "run_count": len(run_ids),
        "file_count": len(parquet_paths),
        "matching_row_count": matching_row_count,
        "returned_row_count": len(rows),
        "limit": bounded_limit,
        "since": since,
        "until": until,
        "rows": rows,
    }


def _empty_archived_activity(
    wallet: str,
    limit: int,
    since: int | None,
    until: int | None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "state": "not_archived",
        "wallet": wallet,
        "locator": f"parquet-wallet://{wallet}",
        "run_count": 0,
        "file_count": 0,
        "matching_row_count": 0,
        "returned_row_count": 0,
        "limit": limit,
        "since": since,
        "until": until,
        "rows": [],
    }


def _write_table_parquet(
    conn: sqlite3.Connection,
    spec: EvidenceTableSpec,
    *,
    target_dir: Path,
    archive_root: Path,
    run_id: str,
    archived_at: int,
) -> dict[str, Any] | None:
    import duckdb

    cursor = conn.execute(
        f"""
        SELECT archived.*
        FROM "{spec.table}" archived
        JOIN evidence_archive_scope scope
          ON scope.row_id = archived.rowid
        WHERE scope.run_id = ? AND scope.table_name = ?
        ORDER BY archived.rowid
        """,
        (run_id, spec.table),
    )
    columns = [str(item[0]) for item in cursor.description or []]
    first_batch = cursor.fetchmany(2_000)
    if not first_batch:
        return None
    table_info = conn.execute(f'PRAGMA table_info("{spec.table}")').fetchall()
    declared_types = {str(row["name"]): str(row["type"] or "") for row in table_info}
    parquet_path = target_dir / f"{spec.table}.parquet"
    partial_path = target_dir / f".{spec.table}.parquet.partial"
    partial_path.unlink(missing_ok=True)
    column_defs = [
        f'{_quote_identifier(column)} {_duckdb_type(declared_types.get(column, ""))}'
        for column in columns
    ]
    column_defs.extend(["_archive_run_id VARCHAR", "_archived_at BIGINT"])
    placeholders = ", ".join("?" for _ in range(len(columns) + 2))
    row_count = 0
    with duckdb.connect(":memory:") as parquet_db:
        parquet_db.execute(f"CREATE TABLE payload ({', '.join(column_defs)})")
        batch = first_batch
        while batch:
            values = [tuple(row) + (run_id, archived_at) for row in batch]
            parquet_db.executemany(f"INSERT INTO payload VALUES ({placeholders})", values)
            row_count += len(values)
            batch = cursor.fetchmany(2_000)
        escaped_path = str(partial_path).replace("'", "''")
        parquet_db.execute(
            f"COPY payload TO '{escaped_path}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
        )
    _fsync_file(partial_path)
    os.replace(partial_path, parquet_path)
    _fsync_directory(target_dir)
    with duckdb.connect(":memory:") as parquet_db:
        verified_count = int(
            parquet_db.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(parquet_path)]).fetchone()[0]
        )
    if verified_count != row_count:
        raise RuntimeError(f"Parquet verification failed for {spec.table}: {verified_count} != {row_count}")
    min_ts, max_ts = _timestamp_bounds(conn, spec, run_id=run_id)
    return {
        "table_name": spec.table,
        "relative_path": parquet_path.relative_to(archive_root).as_posix(),
        "row_count": row_count,
        "byte_size": parquet_path.stat().st_size,
        "checksum": _sha256_file(parquet_path),
        "min_ts": min_ts,
        "max_ts": max_ts,
        "columns": [
            {
                "name": str(row["name"]),
                "sqlite_type": str(row["type"] or ""),
                "primary_key_order": int(row["pk"] or 0),
            }
            for row in table_info
        ],
    }


def _timestamp_bounds(
    conn: sqlite3.Connection,
    spec: EvidenceTableSpec,
    *,
    run_id: str,
) -> tuple[int | None, int | None]:
    columns = {
        str(row["name"])
        for row in conn.execute(f'PRAGMA table_info("{spec.table}")').fetchall()
    }
    for column in (
        "timestamp",
        "scored_at",
        "captured_at",
        "follower_ts",
        "leader_ts",
        "updated_at",
        "created_at",
    ):
        if column not in columns:
            continue
        row = conn.execute(
            f"""
            SELECT MIN(archived."{column}"), MAX(archived."{column}")
            FROM "{spec.table}" archived
            JOIN evidence_archive_scope scope
              ON scope.row_id = archived.rowid
            WHERE scope.run_id = ? AND scope.table_name = ?
            """,
            (run_id, spec.table),
        ).fetchone()
        return (_optional_int(row[0]), _optional_int(row[1]))
    return (None, None)


def _duckdb_type(declared_type: str) -> str:
    normalized = declared_type.upper()
    if "INT" in normalized:
        return "BIGINT"
    if any(token in normalized for token in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE"
    if "BLOB" in normalized:
        return "BLOB"
    if "BOOL" in normalized:
        return "BOOLEAN"
    return "VARCHAR"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    partial = path.with_name(f".{path.name}.partial")
    partial.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _fsync_file(partial)
    os.replace(partial, path)
    _fsync_directory(path.parent)


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone() is not None


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _archive_path(archive_root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"archive path must remain relative to its root: {relative_path}")
    root = archive_root.resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f"archive path escapes its root: {relative_path}")
    return path
