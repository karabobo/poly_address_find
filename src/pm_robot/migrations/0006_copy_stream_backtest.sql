CREATE TABLE IF NOT EXISTS copy_backtest_trades (
    backtest_trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    leader_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    follower_wallet TEXT REFERENCES candidate_wallets(address) ON DELETE SET NULL,
    link_id INTEGER REFERENCES copy_trade_links(link_id) ON DELETE SET NULL,
    leader_activity_id INTEGER NOT NULL REFERENCES wallet_activity(activity_id) ON DELETE CASCADE,
    episode_id INTEGER NOT NULL REFERENCES wallet_episodes(episode_id) ON DELETE CASCADE,
    market_slug TEXT,
    asset_id TEXT,
    outcome TEXT,
    side TEXT NOT NULL,
    leader_ts INTEGER NOT NULL,
    copied_ts INTEGER NOT NULL,
    lag_seconds INTEGER NOT NULL,
    entry_price REAL,
    leader_episode_roi REAL NOT NULL,
    stake_usdc REAL NOT NULL,
    gross_pnl_usdc REAL NOT NULL,
    friction_bps REAL NOT NULL,
    net_pnl_usdc REAL NOT NULL,
    net_roi REAL NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(leader_activity_id, follower_wallet, copied_ts)
);

CREATE INDEX IF NOT EXISTS idx_copy_backtest_trades_leader
    ON copy_backtest_trades(leader_wallet, copied_ts DESC);

CREATE TABLE IF NOT EXISTS copy_leader_performance (
    leader_wallet TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    backtest_trade_count INTEGER NOT NULL,
    copied_market_count INTEGER NOT NULL,
    total_stake_usdc REAL NOT NULL,
    gross_pnl_usdc REAL NOT NULL,
    net_pnl_usdc REAL NOT NULL,
    gross_roi REAL NOT NULL,
    net_roi REAL NOT NULL,
    win_rate REAL,
    median_lag_seconds REAL,
    last_backtest_trade_at INTEGER,
    updated_at INTEGER NOT NULL
);
