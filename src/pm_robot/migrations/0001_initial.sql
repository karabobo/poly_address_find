CREATE TABLE IF NOT EXISTS candidate_wallets (
    address TEXT PRIMARY KEY,
    sources TEXT NOT NULL DEFAULT '',
    labels TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    links TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    candidate_stage TEXT NOT NULL DEFAULT 'needs_data',
    first_seen_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS wallet_features (
    address TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    cumulative_win_rate REAL,
    recent_30d_volume_usdc REAL,
    net_pnl_usdc REAL,
    total_volume_usdc REAL,
    event_win_rate REAL,
    trade_win_rate REAL,
    avg_dca_entries REAL,
    sell_pct REAL,
    bot_score REAL,
    trades_per_day REAL,
    median_gap_sec REAL,
    maker_fraction REAL,
    leader_in_degree REAL,
    copy_event_count REAL,
    copy_market_count REAL,
    containment_pct_median REAL,
    copy_stream_roi REAL,
    edge_retention_pct REAL,
    walk_forward_consistency_pct REAL,
    survival_score REAL,
    single_market_pnl_share REAL,
    net_to_gross_exposure REAL,
    hygiene_status TEXT NOT NULL DEFAULT '',
    primary_category TEXT NOT NULL DEFAULT '',
    last_active_days_ago REAL,
    extra_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS leader_scores (
    score_id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    leader_score REAL NOT NULL,
    review_stage TEXT NOT NULL,
    review_reason TEXT NOT NULL,
    components_json TEXT NOT NULL DEFAULT '{}',
    penalties_json TEXT NOT NULL DEFAULT '{}',
    policy_version TEXT NOT NULL DEFAULT '',
    scored_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leader_scores_address_time
    ON leader_scores(address, scored_at DESC);

CREATE TABLE IF NOT EXISTS review_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    from_stage TEXT,
    to_stage TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_orders (
    order_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    wallet TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    stake_usd REAL NOT NULL,
    route TEXT NOT NULL,
    accepted INTEGER NOT NULL,
    reason TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
