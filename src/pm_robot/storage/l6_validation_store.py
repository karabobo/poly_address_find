"""Immutable Parquet snapshots for the small L6 independent-validation cohort."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb

from pm_robot.storage.wallet_levels import normalize_wallet


STORAGE_VERSION = "l6_validation_raw_v1"


@dataclass(frozen=True)
class L6ValidationArtifact:
    artifact_id: str
    wallet: str
    relative_path: str
    row_count: int
    byte_size: int
    checksum: str
    captured_at: int


def persist_l6_validation_artifact(
    *,
    archive_dir: Path,
    wallet: str,
    source_rows: Mapping[str, Sequence[dict[str, Any]]],
    now: int,
) -> L6ValidationArtifact:
    """Write and verify one immutable multi-source validation snapshot."""

    normalized_wallet = normalize_wallet(wallet)
    captured_at = int(now)
    artifact_id = uuid.uuid4().hex
    day = time.strftime("%Y-%m-%d", time.gmtime(captured_at))
    relative_path = (
        Path("l6_validation")
        / f"version={STORAGE_VERSION}"
        / f"captured_date={day}"
        / f"shard={normalized_wallet[2:4]}"
        / f"wallet={normalized_wallet}"
        / f"{captured_at}-{artifact_id}.parquet"
    )
    path = Path(archive_dir) / relative_path
    rows: list[tuple[Any, ...]] = []
    for record_type in sorted(source_rows):
        for index, row in enumerate(source_rows[record_type]):
            if not isinstance(row, dict):
                continue
            rows.append(
                (
                    normalized_wallet,
                    str(record_type),
                    int(index),
                    _timestamp(row),
                    json.dumps(row, ensure_ascii=False, sort_keys=True, default=str),
                    captured_at,
                )
            )
    _write_verified_parquet(path, rows)
    return L6ValidationArtifact(
        artifact_id=artifact_id,
        wallet=normalized_wallet,
        relative_path=relative_path.as_posix(),
        row_count=len(rows),
        byte_size=path.stat().st_size,
        checksum=_sha256_file(path),
        captured_at=captured_at,
    )


def discard_l6_validation_artifact(*, archive_dir: Path, artifact: L6ValidationArtifact) -> bool:
    """Remove an uncommitted validation snapshot after a failed control-plane write."""

    root = Path(archive_dir).resolve()
    path = (root / artifact.relative_path).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        return False
    if not path.is_file() or path.is_symlink():
        return False
    if _sha256_file(path) != artifact.checksum:
        return False
    path.unlink()
    return True


def _write_verified_parquet(path: Path, rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial")
    partial.unlink(missing_ok=True)
    try:
        with duckdb.connect(":memory:") as db:
            db.execute(
                """
                CREATE TABLE validation_raw(
                    wallet VARCHAR,
                    record_type VARCHAR,
                    record_index INTEGER,
                    record_timestamp BIGINT,
                    raw_json VARCHAR,
                    captured_at BIGINT
                )
                """
            )
            if rows:
                db.executemany("INSERT INTO validation_raw VALUES (?, ?, ?, ?, ?, ?)", rows)
            escaped = str(partial).replace("'", "''")
            db.execute(
                f"COPY validation_raw TO '{escaped}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
            )
        _fsync_file(partial)
        os.replace(partial, path)
        _fsync_directory(path.parent)
        with duckdb.connect(":memory:") as db:
            actual = int(db.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
        if actual != len(rows):
            raise RuntimeError(f"L6 validation Parquet verification failed: {actual} != {len(rows)}")
    except BaseException:
        partial.unlink(missing_ok=True)
        path.unlink(missing_ok=True)
        raise


def _timestamp(row: dict[str, Any]) -> int:
    for key in ("timestamp", "closedAt", "closed_at"):
        try:
            return int(float(row.get(key) or 0))
        except (TypeError, ValueError):
            continue
    return 0


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
