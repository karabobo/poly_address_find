CREATE TABLE IF NOT EXISTS wallet_registry (
    address TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    candidate_stage TEXT NOT NULL DEFAULT '',
    registry_status TEXT NOT NULL DEFAULT '',
    raw_retention_tier TEXT NOT NULL DEFAULT 'keep_full',
    leader_score REAL NOT NULL DEFAULT 0,
    review_stage TEXT NOT NULL DEFAULT '',
    review_reason TEXT NOT NULL DEFAULT '',
    policy_version TEXT NOT NULL DEFAULT '',
    scored_at INTEGER NOT NULL DEFAULT 0,
    total_volume_usdc REAL,
    recent_30d_volume_usdc REAL,
    net_pnl_usdc REAL,
    event_win_rate REAL,
    trade_win_rate REAL,
    copy_stream_roi REAL,
    copy_backtest_net_pnl_usdc REAL,
    edge_retention_pct REAL,
    walk_forward_consistency_pct REAL,
    hygiene_status TEXT NOT NULL DEFAULT '',
    primary_category TEXT NOT NULL DEFAULT '',
    evidence_stage TEXT NOT NULL DEFAULT '',
    activity_count INTEGER NOT NULL DEFAULT 0,
    oldest_activity_ts INTEGER,
    newest_activity_ts INTEGER,
    paper_orders INTEGER NOT NULL DEFAULT 0,
    paper_settled_positions INTEGER NOT NULL DEFAULT 0,
    paper_total_roi REAL,
    paper_settled_roi REAL,
    production_ready INTEGER NOT NULL DEFAULT 0,
    tags_json TEXT NOT NULL DEFAULT '[]',
    blockers_json TEXT NOT NULL DEFAULT '[]',
    source_json TEXT NOT NULL DEFAULT '{}',
    feature_json TEXT NOT NULL DEFAULT '{}',
    score_json TEXT NOT NULL DEFAULT '{}',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    paper_json TEXT NOT NULL DEFAULT '{}',
    summary_json TEXT NOT NULL DEFAULT '{}',
    last_evaluated_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wallet_registry_stage_score
    ON wallet_registry(candidate_stage, leader_score DESC);

CREATE INDEX IF NOT EXISTS idx_wallet_registry_retention
    ON wallet_registry(raw_retention_tier, registry_status, updated_at);
