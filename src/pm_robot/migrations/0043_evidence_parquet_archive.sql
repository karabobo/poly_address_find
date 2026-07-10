CREATE TABLE IF NOT EXISTS evidence_archive_runs (
    run_id TEXT PRIMARY KEY,
    archive_version TEXT NOT NULL DEFAULT 'evidence_parquet_v1',
    status TEXT NOT NULL DEFAULT 'pending',
    archive_path TEXT NOT NULL DEFAULT '',
    manifest_path TEXT NOT NULL DEFAULT '',
    wallet_count INTEGER NOT NULL DEFAULT 0,
    keep_recent_activity INTEGER NOT NULL DEFAULT 0,
    row_count INTEGER NOT NULL DEFAULT 0,
    file_count INTEGER NOT NULL DEFAULT 0,
    byte_size INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0,
    verified_at INTEGER,
    pruned_at INTEGER,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_evidence_archive_runs_status_updated
    ON evidence_archive_runs(status, updated_at, run_id);

CREATE TABLE IF NOT EXISTS evidence_archive_wallets (
    run_id TEXT NOT NULL REFERENCES evidence_archive_runs(run_id) ON DELETE CASCADE,
    wallet TEXT NOT NULL,
    PRIMARY KEY(run_id, wallet)
);

CREATE INDEX IF NOT EXISTS idx_evidence_archive_wallets_wallet
    ON evidence_archive_wallets(wallet, run_id);

CREATE TABLE IF NOT EXISTS evidence_archive_scope (
    run_id TEXT NOT NULL REFERENCES evidence_archive_runs(run_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    row_id INTEGER NOT NULL,
    PRIMARY KEY(run_id, table_name, row_id)
);

CREATE INDEX IF NOT EXISTS idx_evidence_archive_scope_table_row
    ON evidence_archive_scope(table_name, row_id, run_id);

CREATE TABLE IF NOT EXISTS evidence_archive_files (
    run_id TEXT NOT NULL REFERENCES evidence_archive_runs(run_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    byte_size INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    min_ts INTEGER,
    max_ts INTEGER,
    PRIMARY KEY(run_id, table_name)
);

ALTER TABLE wallet_registry ADD COLUMN raw_archive_run_id TEXT NOT NULL DEFAULT '';
ALTER TABLE wallet_registry ADD COLUMN raw_archived_at INTEGER;
ALTER TABLE wallet_registry ADD COLUMN raw_archive_locator TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_wallet_registry_archive_run
    ON wallet_registry(raw_archive_run_id, registry_status, address);
