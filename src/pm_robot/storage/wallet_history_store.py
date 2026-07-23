"""Immutable Parquet storage for wallet history fetched after L1 screening."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from pm_robot.storage.wallet_levels import normalize_wallet
from pm_robot.wallet_levels import HistoryDepth


STORAGE_VERSION = "wallet_history_v1"
_DEPTH_RANK = {HistoryDepth.LIGHT: 1, HistoryDepth.DEEP: 2}
DEFAULT_GC_MIN_AGE_SECONDS = 30 * 86_400
DEFAULT_GC_KEEP_PER_WALLET = 1


@dataclass(frozen=True)
class WalletHistoryArtifact:
    artifact_id: str
    wallet: str
    history_depth: HistoryDepth
    relative_path: str
    row_count: int
    byte_size: int
    checksum: str
    min_timestamp: int | None
    max_timestamp: int | None
    created_at: int


@dataclass(frozen=True)
class WalletHistoryGcSummary:
    candidates: int
    files_deleted: int
    files_missing: int
    bytes_deleted: int
    catalog_rows_marked: int
    unsafe_paths: int
    checksum_mismatches: int
    dry_run: bool
    status: str


@dataclass(frozen=True)
class WalletHistoryAuditSummary:
    catalog_rows: int
    expected_files: int
    verified_files: int
    missing_files: int
    size_mismatches: int
    checksum_mismatches: int
    unsafe_paths: int
    orphan_files: int
    orphan_candidates: int
    orphan_files_deleted: int
    orphan_bytes_deleted: int
    checksums_verified: bool
    issue_paths: tuple[str, ...]
    status: str


def persist_wallet_history_artifact(
    conn: sqlite3.Connection,
    *,
    archive_dir: Path,
    wallet: str,
    history_depth: HistoryDepth,
    rows: list[dict[str, Any]],
    now: int | None = None,
) -> WalletHistoryArtifact:
    """Write, verify, and catalog one complete wallet-history snapshot."""

    normalized_wallet = normalize_wallet(wallet)
    depth = HistoryDepth(history_depth)
    if depth not in _DEPTH_RANK:
        raise ValueError("wallet history artifacts support only light or deep depth")
    active = conn.execute(
        "SELECT history_depth FROM wallet_history_artifacts "
        "WHERE wallet = ? AND status = 'active'",
        (normalized_wallet,),
    ).fetchone()
    if active is not None:
        active_depth = HistoryDepth(str(active["history_depth"]))
        if _DEPTH_RANK[active_depth] > _DEPTH_RANK[depth]:
            raise ValueError("cannot replace deep history with light history")

    ts = int(time.time()) if now is None else int(now)
    artifact_id = uuid.uuid4().hex
    normalized_rows = _normalize_rows(normalized_wallet, rows, captured_at=ts)
    relative_path = _relative_path(
        normalized_wallet,
        depth=depth,
        artifact_id=artifact_id,
        captured_at=ts,
    )
    path = Path(archive_dir) / relative_path
    _write_verified_parquet(path, normalized_rows)
    checksum = _sha256_file(path)
    timestamps = [int(row[1]) for row in normalized_rows if int(row[1]) > 0]
    artifact = WalletHistoryArtifact(
        artifact_id=artifact_id,
        wallet=normalized_wallet,
        history_depth=depth,
        relative_path=relative_path.as_posix(),
        row_count=len(normalized_rows),
        byte_size=path.stat().st_size,
        checksum=checksum,
        min_timestamp=min(timestamps) if timestamps else None,
        max_timestamp=max(timestamps) if timestamps else None,
        created_at=ts,
    )
    started_transaction = not conn.in_transaction
    savepoint = f"wallet_history_catalog_{artifact_id}"
    try:
        if started_transaction:
            conn.execute("BEGIN IMMEDIATE")
        conn.execute(f"SAVEPOINT {savepoint}")
        current = conn.execute(
            "SELECT history_depth FROM wallet_history_artifacts "
            "WHERE wallet = ? AND status = 'active'",
            (normalized_wallet,),
        ).fetchone()
        if current is not None:
            current_depth = HistoryDepth(str(current["history_depth"]))
            if _DEPTH_RANK[current_depth] > _DEPTH_RANK[depth]:
                raise ValueError("cannot replace deep history with light history")
        conn.execute(
            "UPDATE wallet_history_artifacts SET status = 'superseded', updated_at = ? "
            "WHERE wallet = ? AND status = 'active' "
            "AND (? = 'deep' OR history_depth = 'light')",
            (ts, normalized_wallet, depth.value),
        )
        conn.execute(
            """
            INSERT INTO wallet_history_artifacts(
                artifact_id, wallet, history_depth, storage_version, relative_path,
                row_count, byte_size, checksum, min_timestamp, max_timestamp,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                artifact.artifact_id,
                artifact.wallet,
                artifact.history_depth.value,
                STORAGE_VERSION,
                artifact.relative_path,
                artifact.row_count,
                artifact.byte_size,
                artifact.checksum,
                artifact.min_timestamp,
                artifact.max_timestamp,
                ts,
                ts,
            ),
        )
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    except BaseException:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        except sqlite3.Error:
            pass
        if started_transaction:
            conn.rollback()
        try:
            _delete_exact_artifact_file(path, expected_checksum=artifact.checksum)
        except OSError:
            pass
        raise
    return artifact


def discard_uncommitted_wallet_history_artifact(
    conn: sqlite3.Connection,
    *,
    archive_dir: Path,
    artifact: WalletHistoryArtifact,
) -> bool:
    """Delete a newly written file only when SQLite has no catalog row for it."""

    catalogued = conn.execute(
        "SELECT 1 FROM wallet_history_artifacts WHERE artifact_id = ?",
        (artifact.artifact_id,),
    ).fetchone()
    if catalogued is not None:
        return False
    path = _safe_artifact_path(Path(archive_dir), artifact.relative_path)
    if path is None:
        return False
    return _delete_exact_artifact_file(path, expected_checksum=artifact.checksum)


def audit_wallet_history_artifacts(
    conn: sqlite3.Connection,
    *,
    archive_dir: Path,
    verify_checksums: bool = False,
    orphan_min_age_seconds: int = 7 * 86_400,
    orphan_limit: int = 500,
    delete_orphans: bool = False,
    now: int | None = None,
) -> WalletHistoryAuditSummary:
    """Reconcile the SQLite artifact catalog with the wallet-history Parquet tree."""

    ts = int(time.time()) if now is None else int(now)
    root = Path(archive_dir)
    rows = conn.execute(
        """
        SELECT artifact_id, relative_path, byte_size, checksum, purged_at
        FROM wallet_history_artifacts
        ORDER BY artifact_id
        """
    ).fetchall()
    expected_relative_paths: set[str] = set()
    missing_files = 0
    size_mismatches = 0
    checksum_mismatches = 0
    unsafe_paths = 0
    verified_files = 0
    issues: list[str] = []
    expected_files = 0
    for row in rows:
        if row["purged_at"] is not None:
            continue
        expected_files += 1
        relative_path = str(row["relative_path"] or "")
        path = _safe_artifact_path(root, relative_path)
        if path is None:
            unsafe_paths += 1
            _append_issue(issues, f"unsafe:{relative_path}")
            continue
        expected_relative_paths.add(relative_path)
        if not path.exists():
            missing_files += 1
            _append_issue(issues, f"missing:{relative_path}")
            continue
        if not path.is_file() or path.is_symlink():
            unsafe_paths += 1
            _append_issue(issues, f"unsafe:{relative_path}")
            continue
        actual_size = path.stat().st_size
        if actual_size != int(row["byte_size"] or 0):
            size_mismatches += 1
            _append_issue(issues, f"size:{relative_path}")
            continue
        if verify_checksums and _sha256_file(path) != str(row["checksum"] or ""):
            checksum_mismatches += 1
            _append_issue(issues, f"checksum:{relative_path}")
            continue
        verified_files += 1

    orphan_rows: list[tuple[str, Path, int]] = []
    history_root = root / "wallet_history"
    if history_root.exists():
        for path in history_root.rglob("*.parquet"):
            if not path.is_file() or path.is_symlink():
                continue
            safe_path = _safe_artifact_path(root, path.relative_to(root).as_posix())
            if safe_path is None:
                continue
            relative_path = safe_path.relative_to(root.resolve()).as_posix()
            if relative_path in expected_relative_paths:
                continue
            stat = safe_path.stat()
            orphan_rows.append((relative_path, safe_path, int(stat.st_size)))

    orphan_rows.sort(key=lambda item: item[0])
    old_orphans = [
        row
        for row in orphan_rows
        if row[1].stat().st_mtime <= ts - max(0, int(orphan_min_age_seconds))
    ]
    candidates = old_orphans[: max(0, int(orphan_limit))]
    orphan_files_deleted = 0
    orphan_bytes_deleted = 0
    if delete_orphans:
        for relative_path, path, byte_size in candidates:
            if _safe_artifact_path(root, relative_path) != path or path.is_symlink():
                unsafe_paths += 1
                _append_issue(issues, f"unsafe:{relative_path}")
                continue
            path.unlink()
            orphan_files_deleted += 1
            orphan_bytes_deleted += byte_size
    remaining_old_orphans = len(old_orphans) - orphan_files_deleted
    if remaining_old_orphans:
        for relative_path, _path, _size in old_orphans[:10]:
            _append_issue(issues, f"orphan:{relative_path}")

    has_catalog_issue = any(
        (missing_files, size_mismatches, checksum_mismatches, unsafe_paths)
    )
    return WalletHistoryAuditSummary(
        catalog_rows=len(rows),
        expected_files=expected_files,
        verified_files=verified_files,
        missing_files=missing_files,
        size_mismatches=size_mismatches,
        checksum_mismatches=checksum_mismatches,
        unsafe_paths=unsafe_paths,
        orphan_files=len(orphan_rows),
        orphan_candidates=len(candidates),
        orphan_files_deleted=orphan_files_deleted,
        orphan_bytes_deleted=orphan_bytes_deleted,
        checksums_verified=bool(verify_checksums),
        issue_paths=tuple(issues),
        status="partial" if has_catalog_issue or remaining_old_orphans else "ok",
    )


def prune_superseded_wallet_history_artifacts(
    conn: sqlite3.Connection,
    *,
    archive_dir: Path,
    min_age_seconds: int = DEFAULT_GC_MIN_AGE_SECONDS,
    keep_per_wallet: int = DEFAULT_GC_KEEP_PER_WALLET,
    limit: int = 500,
    dry_run: bool = True,
    now: int | None = None,
) -> WalletHistoryGcSummary:
    """Remove bounded superseded files while retaining catalog tombstones."""

    ts = int(time.time()) if now is None else int(now)
    bounded_limit = max(0, int(limit))
    if bounded_limit == 0:
        return WalletHistoryGcSummary(
            candidates=0,
            files_deleted=0,
            files_missing=0,
            bytes_deleted=0,
            catalog_rows_marked=0,
            unsafe_paths=0,
            checksum_mismatches=0,
            dry_run=bool(dry_run),
            status="ok",
        )
    if not dry_run and conn.in_transaction:
        raise RuntimeError("wallet history GC requires a clean SQLite transaction")
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                artifact_id,
                relative_path,
                byte_size,
                checksum,
                purge_started_at,
                updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY wallet
                    ORDER BY created_at DESC, artifact_id DESC
                ) AS superseded_rank
            FROM wallet_history_artifacts
            WHERE status = 'superseded'
              AND purged_at IS NULL
        )
        SELECT artifact_id, relative_path, byte_size, checksum, purge_started_at
        FROM ranked
        WHERE purge_started_at IS NOT NULL
           OR (superseded_rank > ? AND updated_at <= ?)
        ORDER BY
            CASE WHEN purge_started_at IS NOT NULL THEN 0 ELSE 1 END,
            COALESCE(purge_started_at, updated_at) ASC,
            artifact_id ASC
        LIMIT ?
        """,
        (
            max(0, int(keep_per_wallet)),
            ts - max(0, int(min_age_seconds)),
            bounded_limit,
        ),
    ).fetchall()
    files_deleted = 0
    files_missing = 0
    bytes_deleted = 0
    catalog_rows_marked = 0
    unsafe_paths = 0
    checksum_mismatches = 0
    for row in rows:
        path = _safe_artifact_path(Path(archive_dir), str(row["relative_path"] or ""))
        if path is None:
            unsafe_paths += 1
            continue
        if dry_run:
            continue
        path_exists = path.exists()
        actual_size = 0
        if path_exists:
            if not path.is_file() or path.is_symlink():
                unsafe_paths += 1
                continue
            actual_size = path.stat().st_size
            if _sha256_file(path) != str(row["checksum"] or ""):
                checksum_mismatches += 1
                continue
        started = conn.execute(
            """
            UPDATE wallet_history_artifacts
            SET purge_started_at = COALESCE(purge_started_at, ?)
            WHERE artifact_id = ?
              AND status = 'superseded'
              AND purged_at IS NULL
            """,
            (ts, str(row["artifact_id"])),
        )
        conn.commit()
        if int(started.rowcount or 0) <= 0:
            continue
        if path_exists:
            deleted = _delete_exact_artifact_file(
                path,
                expected_checksum=str(row["checksum"] or ""),
            )
            if not deleted:
                if path.exists():
                    checksum_mismatches += 1
                    continue
                files_missing += 1
            else:
                files_deleted += 1
                bytes_deleted += actual_size
        else:
            files_missing += 1
        catalog_rows_marked += _finalize_purged_artifact(
            conn,
            artifact_id=str(row["artifact_id"]),
            now=ts,
        )
    return WalletHistoryGcSummary(
        candidates=len(rows),
        files_deleted=files_deleted,
        files_missing=files_missing,
        bytes_deleted=bytes_deleted,
        catalog_rows_marked=catalog_rows_marked,
        unsafe_paths=unsafe_paths,
        checksum_mismatches=checksum_mismatches,
        dry_run=bool(dry_run),
        status="partial" if unsafe_paths or checksum_mismatches else "ok",
    )


def _finalize_purged_artifact(
    conn: sqlite3.Connection,
    *,
    artifact_id: str,
    now: int,
) -> int:
    """Durably finish a purge after the delete intent is already committed."""

    updated = conn.execute(
        """
        UPDATE wallet_history_artifacts
        SET byte_size = 0, purged_at = ?, updated_at = ?
        WHERE artifact_id = ?
          AND status = 'superseded'
          AND purge_started_at IS NOT NULL
          AND purged_at IS NULL
        """,
        (int(now), int(now), artifact_id),
    )
    conn.commit()
    return max(0, int(updated.rowcount or 0))


def _safe_artifact_path(archive_dir: Path, relative_path: str) -> Path | None:
    relative = Path(relative_path)
    if not relative_path or relative.is_absolute() or ".." in relative.parts:
        return None
    root = archive_dir.resolve()
    candidate = root / relative
    try:
        candidate.resolve(strict=False).relative_to(root)
    except ValueError:
        return None
    return candidate


def _delete_exact_artifact_file(path: Path, *, expected_checksum: str) -> bool:
    if not path.exists() or not path.is_file() or path.is_symlink():
        return False
    if _sha256_file(path) != expected_checksum:
        return False
    path.unlink()
    return True


def _append_issue(issues: list[str], value: str, *, limit: int = 50) -> None:
    if len(issues) < limit:
        issues.append(value)


def _normalize_rows(
    wallet: str,
    rows: list[dict[str, Any]],
    *,
    captured_at: int,
) -> list[tuple[Any, ...]]:
    normalized: dict[str, tuple[Any, ...]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        timestamp = _int(row.get("timestamp"))
        condition_id = _text(row, "conditionId", "condition_id")
        event_slug = _text(row, "eventSlug", "event_slug")
        market_slug = _text(row, "slug", "marketSlug", "market_slug")
        asset_id = _text(row, "asset", "assetId", "asset_id")
        outcome = _text(row, "outcome")
        activity_type = _text(row, "type") or "TRADE"
        side = _text(row, "side").upper()
        price = _float_or_none(row.get("price"))
        size = _float_or_none(row.get("size"))
        usdc_size = _float_or_none(row.get("usdcSize"))
        if usdc_size is None:
            usdc_size = _float_or_none(row.get("usdc_size"))
        if usdc_size is None and price is not None and size is not None:
            usdc_size = price * size
        transaction_hash = _text(row, "transactionHash", "transaction_hash")
        raw_json = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
        if transaction_hash:
            key = "|".join(
                (
                    transaction_hash,
                    str(timestamp),
                    condition_id,
                    event_slug,
                    market_slug,
                    asset_id,
                    outcome,
                    activity_type,
                    side,
                    str(price if price is not None else ""),
                    str(size if size is not None else ""),
                    str(usdc_size if usdc_size is not None else ""),
                )
            )
        else:
            key = "raw:" + hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
        normalized[key] = (
            wallet,
            timestamp,
            condition_id,
            event_slug,
            market_slug,
            asset_id,
            outcome,
            activity_type,
            side,
            price,
            size,
            usdc_size,
            transaction_hash,
            raw_json,
            captured_at,
        )
    return sorted(normalized.values(), key=lambda row: (int(row[1]), str(row[12]), str(row[5])))


def _write_verified_parquet(path: Path, rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial")
    partial.unlink(missing_ok=True)
    schema = """
        wallet VARCHAR,
        timestamp BIGINT,
        condition_id VARCHAR,
        event_slug VARCHAR,
        market_slug VARCHAR,
        asset_id VARCHAR,
        outcome VARCHAR,
        activity_type VARCHAR,
        side VARCHAR,
        price DOUBLE,
        size DOUBLE,
        usdc_size DOUBLE,
        transaction_hash VARCHAR,
        raw_json VARCHAR,
        captured_at BIGINT
    """
    try:
        with duckdb.connect(":memory:") as db:
            db.execute(f"CREATE TABLE history ({schema})")
            if rows:
                placeholders = ", ".join("?" for _ in range(15))
                db.executemany(f"INSERT INTO history VALUES ({placeholders})", rows)
            escaped = str(partial).replace("'", "''")
            db.execute(
                f"COPY history TO '{escaped}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
            )
        _fsync_file(partial)
        os.replace(partial, path)
        _fsync_directory(path.parent)
        with duckdb.connect(":memory:") as db:
            actual = int(
                db.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
            )
        if actual != len(rows):
            raise RuntimeError(
                f"wallet history Parquet verification failed: {actual} != {len(rows)}"
            )
    except BaseException:
        partial.unlink(missing_ok=True)
        path.unlink(missing_ok=True)
        raise


def _relative_path(
    wallet: str,
    *,
    depth: HistoryDepth,
    artifact_id: str,
    captured_at: int,
) -> Path:
    day = time.strftime("%Y-%m-%d", time.gmtime(captured_at))
    return (
        Path("wallet_history")
        / f"version={STORAGE_VERSION}"
        / f"depth={depth.value}"
        / f"captured_date={day}"
        / f"shard={wallet[2:4]}"
        / f"wallet={wallet}"
        / f"{captured_at}-{artifact_id}.parquet"
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _text(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _float_or_none(value: Any) -> float | None:
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0
