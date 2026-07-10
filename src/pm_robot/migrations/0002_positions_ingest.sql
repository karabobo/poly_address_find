CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingest_type TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    status TEXT NOT NULL DEFAULT 'running',
    wallets_attempted INTEGER NOT NULL DEFAULT 0,
    wallets_succeeded INTEGER NOT NULL DEFAULT 0,
    rows_written INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT ''
);

ALTER TABLE candidate_wallets ADD COLUMN last_ingested_at INTEGER;

CREATE TABLE IF NOT EXISTS wallet_positions (
    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    asset_id TEXT NOT NULL,
    condition_id TEXT,
    market_slug TEXT,
    event_slug TEXT,
    title TEXT,
    outcome TEXT,
    size REAL,
    avg_price REAL,
    current_price REAL,
    current_value REAL,
    initial_value REAL,
    cash_pnl REAL,
    realized_pnl REAL,
    percent_pnl REAL,
    end_date TEXT,
    neg_risk INTEGER NOT NULL DEFAULT 0,
    captured_at INTEGER NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_wallet_positions_address_time
    ON wallet_positions(address, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_wallet_positions_asset_time
    ON wallet_positions(asset_id, captured_at DESC);
