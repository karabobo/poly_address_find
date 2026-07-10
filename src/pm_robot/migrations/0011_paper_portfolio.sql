CREATE TABLE IF NOT EXISTS paper_fills (
    fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE REFERENCES paper_orders(order_id) ON DELETE CASCADE,
    wallet TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    fill_price REAL NOT NULL,
    stake_usd REAL NOT NULL,
    shares REAL NOT NULL,
    filled_at INTEGER NOT NULL,
    source_order_created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_fills_wallet_asset
    ON paper_fills(wallet, asset_id);

CREATE INDEX IF NOT EXISTS idx_paper_fills_market
    ON paper_fills(market_slug);

CREATE TABLE IF NOT EXISTS paper_positions (
    wallet TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    shares REAL NOT NULL,
    cost_usd REAL NOT NULL,
    avg_price REAL NOT NULL,
    mark_price REAL NOT NULL,
    mark_value_usd REAL NOT NULL,
    unrealized_pnl_usd REAL NOT NULL,
    realized_pnl_usd REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    opened_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(wallet, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_paper_positions_status
    ON paper_positions(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS paper_marks (
    mark_id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    mark_price REAL NOT NULL,
    mark_source TEXT NOT NULL,
    marked_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_marks_wallet_asset_time
    ON paper_marks(wallet, asset_id, marked_at DESC);

CREATE TABLE IF NOT EXISTS paper_wallet_performance (
    wallet TEXT PRIMARY KEY,
    orders INTEGER NOT NULL,
    open_positions INTEGER NOT NULL,
    total_cost_usd REAL NOT NULL,
    mark_value_usd REAL NOT NULL,
    unrealized_pnl_usd REAL NOT NULL,
    realized_pnl_usd REAL NOT NULL,
    total_pnl_usd REAL NOT NULL,
    roi REAL NOT NULL,
    updated_at INTEGER NOT NULL
);
