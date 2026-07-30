-- L0 is a transient observation buffer. These partial indexes keep admission
-- and bounded expiry scans cheap as the real-time stream grows.
CREATE INDEX IF NOT EXISTS idx_observed_wallets_l0_admission
    ON observed_wallets(recent_usdc_total DESC, first_seen_at ASC, wallet ASC)
    WHERE promoted_at IS NULL AND recent_trade_count > 0;

CREATE INDEX IF NOT EXISTS idx_observed_wallets_l0_retention
    ON observed_wallets(updated_at, wallet)
    WHERE promoted_at IS NULL;
