-- L5 remains the relative-ranking result. L6 records a separate independent
-- validation pass and therefore does not alter the L3/L4/L5 selection table.
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

CREATE TABLE wallet_levels_next (
    wallet TEXT PRIMARY KEY,
    level TEXT NOT NULL DEFAULT 'l0'
        CHECK(level IN ('l0', 'l1', 'l2', 'l3', 'l4', 'l5', 'l6')),
    level_reason TEXT NOT NULL DEFAULT '',
    policy_version TEXT NOT NULL DEFAULT '',
    hard_risk_block INTEGER NOT NULL DEFAULT 0,
    first_seen_at INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL DEFAULT 0,
    level_updated_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

INSERT INTO wallet_levels_next(
    wallet, level, level_reason, policy_version, hard_risk_block,
    first_seen_at, last_seen_at, level_updated_at, updated_at
)
SELECT
    wallet, level, level_reason, policy_version, hard_risk_block,
    first_seen_at, last_seen_at, level_updated_at, updated_at
FROM wallet_levels;

DROP TABLE wallet_levels;
ALTER TABLE wallet_levels_next RENAME TO wallet_levels;

CREATE INDEX idx_wallet_levels_level_updated
    ON wallet_levels(level, level_updated_at DESC, wallet);

CREATE TABLE wallet_l6_validations (
    validation_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    evidence_artifact_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    decision TEXT NOT NULL CHECK(decision IN ('pass', 'warning', 'fail')),
    reason TEXT NOT NULL DEFAULT '',
    coverage_start INTEGER NOT NULL DEFAULT 0,
    coverage_end INTEGER NOT NULL DEFAULT 0,
    closed_position_count INTEGER NOT NULL DEFAULT 0,
    timestamped_closed_position_count INTEGER NOT NULL DEFAULT 0,
    activity_count INTEGER NOT NULL DEFAULT 0,
    active_weeks INTEGER NOT NULL DEFAULT 0,
    positive_week_ratio REAL NOT NULL DEFAULT 0,
    realized_pnl_usdc REAL NOT NULL DEFAULT 0,
    recent_realized_pnl_usdc REAL NOT NULL DEFAULT 0,
    open_pnl_usdc REAL NOT NULL DEFAULT 0,
    max_drawdown_usdc REAL NOT NULL DEFAULT 0,
    max_drawdown_ratio REAL NOT NULL DEFAULT 0,
    top_market_profit_share REAL NOT NULL DEFAULT 0,
    top_day_profit_share REAL NOT NULL DEFAULT 0,
    churn_ratio REAL NOT NULL DEFAULT 0,
    unrealized_profit_share REAL NOT NULL DEFAULT 0,
    abnormal_flags_json TEXT NOT NULL DEFAULT '[]',
    evidence_metrics_json TEXT NOT NULL DEFAULT '{}',
    raw_relative_path TEXT NOT NULL DEFAULT '',
    raw_byte_size INTEGER NOT NULL DEFAULT 0,
    raw_checksum TEXT NOT NULL DEFAULT '',
    validated_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    UNIQUE(wallet, evidence_artifact_id, policy_version, validated_at)
);

CREATE INDEX idx_wallet_l6_validations_latest
    ON wallet_l6_validations(wallet, validated_at DESC, validation_id DESC);

CREATE INDEX idx_wallet_l6_validations_decision
    ON wallet_l6_validations(decision, validated_at DESC, wallet);

COMMIT;
PRAGMA foreign_keys = ON;
