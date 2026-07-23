ALTER TABLE wallet_history_artifacts
    ADD COLUMN purged_at INTEGER;

CREATE INDEX IF NOT EXISTS idx_wallet_history_artifacts_gc
    ON wallet_history_artifacts(status, purged_at, updated_at, wallet);
