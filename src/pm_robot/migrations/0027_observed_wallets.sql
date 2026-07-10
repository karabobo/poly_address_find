CREATE TABLE IF NOT EXISTS observed_wallets (
    wallet TEXT PRIMARY KEY,
    sources TEXT NOT NULL DEFAULT '',
    labels TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    links TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    observed_trade_count INTEGER NOT NULL DEFAULT 0,
    recent_trade_count INTEGER NOT NULL DEFAULT 0,
    recent_usdc_total REAL NOT NULL DEFAULT 0,
    recent_max_trade_usdc REAL NOT NULL DEFAULT 0,
    recent_trades_json TEXT NOT NULL DEFAULT '[]',
    promoted_at INTEGER,
    promotion_reason TEXT NOT NULL DEFAULT '',
    first_seen_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observed_wallets_promotion
    ON observed_wallets(promoted_at, recent_max_trade_usdc DESC, recent_usdc_total DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_observed_wallets_updated
    ON observed_wallets(updated_at DESC);
