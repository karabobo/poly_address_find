CREATE TABLE IF NOT EXISTS data_artifacts (
    artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_type TEXT NOT NULL,
    uri TEXT NOT NULL,
    storage_backend TEXT NOT NULL DEFAULT 'sqlite',
    partition_key TEXT NOT NULL DEFAULT '',
    content_format TEXT NOT NULL DEFAULT 'json',
    row_count INTEGER NOT NULL DEFAULT 0,
    byte_size INTEGER NOT NULL DEFAULT 0,
    checksum TEXT NOT NULL DEFAULT '',
    min_ts INTEGER,
    max_ts INTEGER,
    source TEXT NOT NULL DEFAULT '',
    schema_version TEXT NOT NULL DEFAULT 'v1',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    UNIQUE(uri)
);

CREATE INDEX IF NOT EXISTS idx_data_artifacts_type_source
    ON data_artifacts(artifact_type, source, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_data_artifacts_partition
    ON data_artifacts(partition_key, artifact_type, updated_at DESC);

CREATE TABLE IF NOT EXISTS wallet_processing_state (
    wallet TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    discovery_tier TEXT NOT NULL DEFAULT 'l0_discovered',
    evidence_status TEXT NOT NULL DEFAULT 'pending',
    evidence_depth INTEGER NOT NULL DEFAULT 0,
    evidence_confidence REAL NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 100,
    current_stage TEXT NOT NULL DEFAULT '',
    next_action TEXT NOT NULL DEFAULT '',
    next_action_at INTEGER NOT NULL DEFAULT 0,
    newest_activity_ts INTEGER,
    oldest_activity_ts INTEGER,
    newest_activity_key TEXT NOT NULL DEFAULT '',
    activity_count INTEGER NOT NULL DEFAULT 0,
    non_fast_trade_count INTEGER NOT NULL DEFAULT 0,
    distinct_markets INTEGER NOT NULL DEFAULT 0,
    last_light_backfill_at INTEGER,
    last_medium_backfill_at INTEGER,
    last_deep_backfill_at INTEGER,
    raw_artifact_uri TEXT NOT NULL DEFAULT '',
    summary_artifact_uri TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_wallet_processing_state_next
    ON wallet_processing_state(evidence_status, next_action_at, priority, updated_at);

CREATE INDEX IF NOT EXISTS idx_wallet_processing_state_tier
    ON wallet_processing_state(discovery_tier, evidence_confidence DESC, activity_count DESC);

CREATE TABLE IF NOT EXISTS wallet_evidence_summary (
    wallet TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    summary_version TEXT NOT NULL DEFAULT 'v1',
    activity_count INTEGER NOT NULL DEFAULT 0,
    distinct_markets INTEGER NOT NULL DEFAULT 0,
    non_fast_trade_count INTEGER NOT NULL DEFAULT 0,
    non_fast_distinct_markets INTEGER NOT NULL DEFAULT 0,
    fast_market_share REAL NOT NULL DEFAULT 0,
    buy_count INTEGER NOT NULL DEFAULT 0,
    sell_count INTEGER NOT NULL DEFAULT 0,
    total_usdc_volume REAL NOT NULL DEFAULT 0,
    median_gap_sec REAL,
    oldest_ts INTEGER,
    latest_ts INTEGER,
    strategy_tags_json TEXT NOT NULL DEFAULT '[]',
    risk_flags_json TEXT NOT NULL DEFAULT '[]',
    copyability_json TEXT NOT NULL DEFAULT '{}',
    representative_trades_json TEXT NOT NULL DEFAULT '[]',
    source_artifacts_json TEXT NOT NULL DEFAULT '[]',
    computed_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_wallet_evidence_summary_copyable
    ON wallet_evidence_summary(activity_count DESC, distinct_markets DESC, fast_market_share ASC);

CREATE TABLE IF NOT EXISTS pipeline_jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    wallet TEXT NOT NULL DEFAULT '',
    subject_key TEXT NOT NULL DEFAULT '',
    tier TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 100,
    shard INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued',
    lease_owner TEXT,
    lease_until INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    input_json TEXT NOT NULL DEFAULT '{}',
    output_json TEXT NOT NULL DEFAULT '{}',
    last_error TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    completed_at INTEGER,
    UNIQUE(job_type, wallet, tier, subject_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_claim
    ON pipeline_jobs(status, shard, next_attempt_at, priority, updated_at);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_wallet
    ON pipeline_jobs(wallet, job_type, status);
