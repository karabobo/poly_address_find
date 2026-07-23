CREATE TABLE IF NOT EXISTS wallet_validation_summaries (
    wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    cohort TEXT NOT NULL CHECK(cohort IN ('validation', 'exploratory_copyability')),
    total_trials INTEGER NOT NULL DEFAULT 0,
    open_trials INTEGER NOT NULL DEFAULT 0,
    resolved_trials INTEGER NOT NULL DEFAULT 0,
    market_samples INTEGER NOT NULL DEFAULT 0,
    open_markets INTEGER NOT NULL DEFAULT 0,
    resolved_markets INTEGER NOT NULL DEFAULT 0,
    settled_cost_usd REAL NOT NULL DEFAULT 0,
    settled_pnl_usd REAL NOT NULL DEFAULT 0,
    settled_roi_pct REAL NOT NULL DEFAULT 0,
    max_market_cost_share_pct REAL NOT NULL DEFAULT 0,
    execution_eligible INTEGER NOT NULL DEFAULT 0,
    validation_status TEXT NOT NULL DEFAULT 'collecting',
    validation_reason TEXT NOT NULL DEFAULT '',
    policy_version TEXT NOT NULL DEFAULT '',
    source_updated_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(wallet, cohort)
);

CREATE INDEX IF NOT EXISTS idx_wallet_validation_summaries_status
    ON wallet_validation_summaries(cohort, validation_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS candidate_decision_state (
    wallet TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    research_stage TEXT NOT NULL,
    reason TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    policy_version TEXT NOT NULL DEFAULT '',
    score_id INTEGER,
    decided_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_candidate_decision_state_stage
    ON candidate_decision_state(research_stage, updated_at DESC);
