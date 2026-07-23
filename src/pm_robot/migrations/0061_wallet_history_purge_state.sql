ALTER TABLE wallet_history_artifacts
    ADD COLUMN purge_started_at INTEGER;

CREATE INDEX IF NOT EXISTS idx_wallet_history_artifacts_purge
    ON wallet_history_artifacts(status, purged_at, purge_started_at, updated_at, wallet);
