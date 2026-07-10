CREATE TABLE IF NOT EXISTS copy_trade_links (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    leader_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    follower_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    leader_activity_id INTEGER NOT NULL REFERENCES wallet_activity(activity_id) ON DELETE CASCADE,
    follower_activity_id INTEGER NOT NULL REFERENCES wallet_activity(activity_id) ON DELETE CASCADE,
    condition_id TEXT,
    market_slug TEXT,
    asset_id TEXT,
    outcome TEXT,
    side TEXT,
    leader_ts INTEGER NOT NULL,
    follower_ts INTEGER NOT NULL,
    lag_seconds INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(leader_activity_id, follower_activity_id)
);

CREATE INDEX IF NOT EXISTS idx_copy_trade_links_leader
    ON copy_trade_links(leader_wallet, follower_wallet, follower_ts DESC);

CREATE INDEX IF NOT EXISTS idx_copy_trade_links_follower
    ON copy_trade_links(follower_wallet, follower_ts DESC);

CREATE TABLE IF NOT EXISTS copy_pair_stats (
    leader_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    follower_wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    copy_event_count INTEGER NOT NULL,
    copy_market_count INTEGER NOT NULL,
    follower_trade_count INTEGER NOT NULL,
    containment_pct REAL NOT NULL,
    leader_precedes_pct REAL NOT NULL,
    median_lag_seconds REAL,
    first_copy_ts INTEGER,
    last_copy_ts INTEGER,
    qualifies INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(leader_wallet, follower_wallet)
);

CREATE INDEX IF NOT EXISTS idx_copy_pair_stats_leader_qualifies
    ON copy_pair_stats(leader_wallet, qualifies, copy_event_count DESC);

CREATE TABLE IF NOT EXISTS copy_leader_stats (
    leader_wallet TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    leader_in_degree INTEGER NOT NULL,
    copy_event_count INTEGER NOT NULL,
    copy_market_count INTEGER NOT NULL,
    containment_pct_median REAL,
    median_lag_seconds REAL,
    qualified_follower_count INTEGER NOT NULL,
    last_copy_event_at INTEGER,
    updated_at INTEGER NOT NULL
);
