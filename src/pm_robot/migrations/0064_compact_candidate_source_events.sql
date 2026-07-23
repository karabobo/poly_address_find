-- Keep one bounded provenance summary per wallet and discovery source.
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

CREATE TABLE candidate_source_events_next (
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
    UNIQUE(address, source)
);

WITH ranked AS (
    SELECT
        event_id,
        address,
        source,
        status,
        labels,
        notes,
        links,
        evidence_json,
        MIN(observed_at) OVER (PARTITION BY address, source) AS first_observed_at,
        MAX(recorded_at) OVER (PARTITION BY address, source) AS last_recorded_at,
        ROW_NUMBER() OVER (
            PARTITION BY address, source
            ORDER BY recorded_at DESC, event_id DESC
        ) AS latest_row
    FROM candidate_source_events
)
INSERT INTO candidate_source_events_next(
    event_id, address, source, status, labels, notes, links, evidence_json,
    observed_at, recorded_at
)
SELECT
    event_id, address, source, status, labels, notes, links, evidence_json,
    first_observed_at, last_recorded_at
FROM ranked
WHERE latest_row = 1;

DROP TABLE candidate_source_events;
ALTER TABLE candidate_source_events_next RENAME TO candidate_source_events;

CREATE INDEX idx_candidate_source_events_address_time
    ON candidate_source_events(address, observed_at ASC, event_id ASC);
CREATE INDEX idx_candidate_source_events_source
    ON candidate_source_events(source, recorded_at DESC);
CREATE INDEX idx_candidate_source_events_source_address_time
    ON candidate_source_events(source, address, recorded_at DESC);

COMMIT;
PRAGMA foreign_keys = ON;
