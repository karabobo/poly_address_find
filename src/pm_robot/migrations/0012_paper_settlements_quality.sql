CREATE TABLE IF NOT EXISTS paper_settlements (
    wallet TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    shares REAL NOT NULL,
    cost_usd REAL NOT NULL,
    settlement_price REAL NOT NULL,
    payout_usd REAL NOT NULL,
    realized_pnl_usd REAL NOT NULL,
    settlement_source TEXT NOT NULL,
    settled_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(wallet, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_paper_settlements_wallet
    ON paper_settlements(wallet, updated_at DESC);

CREATE TABLE IF NOT EXISTS paper_wallet_quality (
    wallet TEXT PRIMARY KEY,
    orders INTEGER NOT NULL,
    open_positions INTEGER NOT NULL,
    settled_positions INTEGER NOT NULL,
    gamma_marked_positions INTEGER NOT NULL,
    fallback_marked_positions INTEGER NOT NULL,
    mark_coverage REAL NOT NULL,
    settled_cost_usd REAL NOT NULL,
    settled_pnl_usd REAL NOT NULL,
    settled_roi REAL NOT NULL,
    total_pnl_usd REAL NOT NULL,
    total_roi REAL NOT NULL,
    production_ready INTEGER NOT NULL,
    blockers_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
