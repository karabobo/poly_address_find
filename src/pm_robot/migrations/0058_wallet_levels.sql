CREATE TABLE IF NOT EXISTS wallet_levels (
    wallet TEXT PRIMARY KEY,
    level TEXT NOT NULL DEFAULT 'l0'
        CHECK(level IN ('l0', 'l1', 'l2', 'l3', 'l4', 'l5')),
    level_reason TEXT NOT NULL DEFAULT '',
    policy_version TEXT NOT NULL DEFAULT '',
    hard_risk_block INTEGER NOT NULL DEFAULT 0,
    first_seen_at INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL DEFAULT 0,
    level_updated_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_wallet_levels_level_updated
    ON wallet_levels(level, level_updated_at DESC, wallet);

CREATE TABLE IF NOT EXISTS wallet_level_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    from_level TEXT NOT NULL,
    to_level TEXT NOT NULL,
    reason TEXT NOT NULL,
    policy_version TEXT NOT NULL DEFAULT '',
    facts_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wallet_level_events_wallet_time
    ON wallet_level_events(wallet, created_at DESC, event_id DESC);

CREATE TABLE IF NOT EXISTS wallet_screen_summaries (
    wallet TEXT PRIMARY KEY,
    sample_limit INTEGER NOT NULL DEFAULT 10,
    sample_trade_count INTEGER NOT NULL DEFAULT 0,
    sample_volume_usdc REAL NOT NULL DEFAULT 0,
    sample_market_count INTEGER NOT NULL DEFAULT 0,
    latest_trade_at INTEGER,
    screen_complete INTEGER NOT NULL DEFAULT 0,
    screen_qualified INTEGER NOT NULL DEFAULT 0,
    screen_reason TEXT NOT NULL DEFAULT '',
    source_snapshot_json TEXT NOT NULL DEFAULT '{}',
    computed_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_wallet_screen_summaries_qualified
    ON wallet_screen_summaries(screen_complete, screen_qualified, sample_volume_usdc DESC, updated_at);

CREATE TABLE IF NOT EXISTS wallet_pnl_summaries (
    wallet TEXT PRIMARY KEY,
    current_position_value_usdc REAL NOT NULL DEFAULT 0,
    open_estimated_pnl_usdc REAL NOT NULL DEFAULT 0,
    closed_realized_pnl_usdc REAL NOT NULL DEFAULT 0,
    total_estimated_pnl_usdc REAL NOT NULL DEFAULT 0,
    capital_basis_usdc REAL NOT NULL DEFAULT 0,
    cost_roi_estimate REAL,
    open_position_count INTEGER NOT NULL DEFAULT 0,
    closed_position_count INTEGER NOT NULL DEFAULT 0,
    coverage TEXT NOT NULL DEFAULT 'none',
    methodology_version TEXT NOT NULL DEFAULT '',
    captured_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS wallet_history_artifacts (
    artifact_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    history_depth TEXT NOT NULL CHECK(history_depth IN ('light', 'deep')),
    storage_version TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    row_count INTEGER NOT NULL DEFAULT 0,
    byte_size INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL,
    min_timestamp INTEGER,
    max_timestamp INTEGER,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK(status IN ('active', 'superseded')),
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_wallet_history_artifacts_wallet
    ON wallet_history_artifacts(wallet, status, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_history_artifacts_one_active
    ON wallet_history_artifacts(wallet)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS wallet_history_summaries (
    wallet TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    history_depth TEXT NOT NULL CHECK(history_depth IN ('light', 'deep')),
    activity_count INTEGER NOT NULL DEFAULT 0,
    distinct_markets INTEGER NOT NULL DEFAULT 0,
    non_fast_trade_count INTEGER NOT NULL DEFAULT 0,
    fast_market_share REAL NOT NULL DEFAULT 0,
    total_volume_usdc REAL NOT NULL DEFAULT 0,
    buy_count INTEGER NOT NULL DEFAULT 0,
    sell_count INTEGER NOT NULL DEFAULT 0,
    median_gap_sec REAL,
    trades_per_day REAL,
    market_volume_top_share REAL NOT NULL DEFAULT 0,
    oldest_timestamp INTEGER,
    latest_timestamp INTEGER,
    strategy_tags_json TEXT NOT NULL DEFAULT '[]',
    risk_flags_json TEXT NOT NULL DEFAULT '[]',
    research_score REAL NOT NULL DEFAULT 0,
    score_components_json TEXT NOT NULL DEFAULT '{}',
    methodology_version TEXT NOT NULL DEFAULT '',
    computed_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_wallet_history_summaries_rank
    ON wallet_history_summaries(history_depth, research_score DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS wallet_level_selections (
    wallet TEXT NOT NULL,
    target_level TEXT NOT NULL CHECK(target_level IN ('l3', 'l4', 'l5')),
    evidence_artifact_id TEXT NOT NULL DEFAULT '',
    policy_version TEXT NOT NULL,
    selected INTEGER NOT NULL DEFAULT 0,
    rank_in_cohort INTEGER NOT NULL DEFAULT 0,
    cohort_size INTEGER NOT NULL DEFAULT 0,
    source_bucket TEXT NOT NULL DEFAULT '',
    strategy_bucket TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    decided_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(wallet, target_level, evidence_artifact_id, policy_version)
);

INSERT OR IGNORE INTO wallet_levels(
    wallet, level, level_reason, first_seen_at, last_seen_at,
    level_updated_at, updated_at
)
SELECT
    address,
    'l1',
    'legacy_candidate_backfill',
    first_seen_at,
    updated_at,
    updated_at,
    updated_at
FROM candidate_wallets;

INSERT OR IGNORE INTO wallet_levels(
    wallet, level, level_reason, first_seen_at, last_seen_at,
    level_updated_at, updated_at
)
SELECT
    wallet,
    'l0',
    'legacy_sighting_backfill',
    first_seen_at,
    updated_at,
    first_seen_at,
    updated_at
FROM observed_wallets;
