CREATE TABLE IF NOT EXISTS evidence_backfill_budget (
    wallet TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    source TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 100,
    stage TEXT NOT NULL DEFAULT 'light_pending',
    target_depth INTEGER NOT NULL DEFAULT 200,
    current_depth INTEGER NOT NULL DEFAULT 0,
    last_attempt_at INTEGER,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    stop_reason TEXT NOT NULL DEFAULT '',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_evidence_backfill_stage_next
    ON evidence_backfill_budget(stage, next_attempt_at, priority, updated_at);

CREATE INDEX IF NOT EXISTS idx_evidence_backfill_source_stage
    ON evidence_backfill_budget(source, stage);

INSERT OR IGNORE INTO evidence_backfill_budget(
    wallet, source, priority, stage, target_depth, current_depth,
    next_attempt_at, evidence_json, created_at, updated_at
)
SELECT
    cw.address,
    'polymarket_trades_global',
    50,
    'light_pending',
    200,
    COUNT(wa.activity_id),
    0,
    '{}',
    strftime('%s','now'),
    strftime('%s','now')
FROM candidate_wallets cw
LEFT JOIN wallet_activity wa
  ON wa.address = cw.address
WHERE cw.sources LIKE '%polymarket_trades_global%'
  AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
GROUP BY cw.address;
