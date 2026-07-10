CREATE TABLE IF NOT EXISTS candidate_source_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    source TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    labels TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    links TEXT NOT NULL DEFAULT '',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    observed_at INTEGER NOT NULL,
    recorded_at INTEGER NOT NULL,
    UNIQUE(address, source, status, labels, notes, links)
);

CREATE INDEX IF NOT EXISTS idx_candidate_source_events_address_time
    ON candidate_source_events(address, observed_at ASC, event_id ASC);

CREATE INDEX IF NOT EXISTS idx_candidate_source_events_source
    ON candidate_source_events(source, recorded_at DESC);

INSERT OR IGNORE INTO candidate_source_events(
    address, source, status, labels, notes, links, evidence_json, observed_at, recorded_at
)
SELECT
    address,
    sources,
    status,
    labels,
    notes,
    links,
    '{"backfill":"candidate_wallets_current_snapshot"}',
    first_seen_at,
    updated_at
FROM candidate_wallets;
