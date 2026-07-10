CREATE TABLE IF NOT EXISTS leader_publish (
    wallet TEXT PRIMARY KEY,
    publish_stage TEXT NOT NULL,
    status TEXT NOT NULL,
    leader_score REAL NOT NULL DEFAULT 0,
    review_reason TEXT NOT NULL DEFAULT '',
    paper_quality_json TEXT NOT NULL DEFAULT '{}',
    readiness_json TEXT NOT NULL DEFAULT '{}',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    blockers_json TEXT NOT NULL DEFAULT '[]',
    published_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    revoked_at INTEGER,
    revoke_reason TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_leader_publish_status_expires
    ON leader_publish(status, expires_at);

CREATE INDEX IF NOT EXISTS idx_leader_publish_score
    ON leader_publish(leader_score DESC);
