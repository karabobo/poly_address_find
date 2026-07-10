CREATE TABLE IF NOT EXISTS evidence_backfill_jobs (
    job_id INTEGER PRIMARY KEY,
    wallet TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    target_depth INTEGER NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    shard INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued',
    lease_owner TEXT,
    lease_until INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    last_attempt_at INTEGER,
    last_error TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER,
    UNIQUE(wallet, stage, target_depth)
);

CREATE INDEX IF NOT EXISTS idx_evidence_backfill_jobs_claim
    ON evidence_backfill_jobs(status, shard, next_attempt_at, priority, updated_at);

CREATE INDEX IF NOT EXISTS idx_evidence_backfill_jobs_wallet
    ON evidence_backfill_jobs(wallet, status);
