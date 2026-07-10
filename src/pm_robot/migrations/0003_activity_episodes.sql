CREATE TABLE IF NOT EXISTS wallet_activity (
    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    timestamp INTEGER NOT NULL,
    condition_id TEXT,
    event_slug TEXT,
    market_slug TEXT,
    asset_id TEXT,
    outcome TEXT,
    type TEXT NOT NULL,
    side TEXT,
    price REAL,
    size REAL,
    usdc_size REAL,
    transaction_hash TEXT,
    raw_json TEXT NOT NULL DEFAULT '{}',
    ingested_at INTEGER NOT NULL,
    UNIQUE(address, transaction_hash, asset_id, timestamp, side, size, usdc_size)
);

CREATE INDEX IF NOT EXISTS idx_wallet_activity_address_time
    ON wallet_activity(address, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_wallet_activity_address_market
    ON wallet_activity(address, condition_id, asset_id);

CREATE TABLE IF NOT EXISTS wallet_episodes (
    episode_id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    condition_id TEXT,
    event_slug TEXT,
    market_slug TEXT,
    asset_id TEXT,
    outcome TEXT,
    first_ts INTEGER,
    last_ts INTEGER,
    buy_count INTEGER NOT NULL DEFAULT 0,
    sell_count INTEGER NOT NULL DEFAULT 0,
    dca_entries INTEGER NOT NULL DEFAULT 0,
    bought_usdc REAL NOT NULL DEFAULT 0,
    sold_usdc REAL NOT NULL DEFAULT 0,
    net_shares REAL NOT NULL DEFAULT 0,
    avg_entry_price REAL,
    realized_pnl_est REAL,
    status TEXT NOT NULL DEFAULT 'open',
    rebuilt_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wallet_episodes_address
    ON wallet_episodes(address, rebuilt_at DESC);
