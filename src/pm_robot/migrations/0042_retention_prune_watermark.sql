ALTER TABLE wallet_registry ADD COLUMN raw_prune_version TEXT NOT NULL DEFAULT '';
ALTER TABLE wallet_registry ADD COLUMN raw_pruned_at INTEGER;

CREATE INDEX IF NOT EXISTS idx_candidate_wallets_stage_updated
    ON candidate_wallets(candidate_stage, updated_at, address);

CREATE INDEX IF NOT EXISTS idx_wallet_registry_prune_version
    ON wallet_registry(raw_prune_version, registry_status, address);
