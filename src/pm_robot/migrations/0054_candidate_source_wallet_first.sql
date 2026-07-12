CREATE TABLE IF NOT EXISTS candidate_source_wallet_first (
    address TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    source TEXT NOT NULL,
    first_event_id INTEGER NOT NULL,
    first_observed_at INTEGER NOT NULL,
    first_recorded_at INTEGER NOT NULL
);

INSERT OR IGNORE INTO candidate_source_wallet_first(
    address, source, first_event_id, first_observed_at, first_recorded_at
)
SELECT
    cse.address,
    cse.source,
    cse.event_id,
    cse.observed_at,
    cse.recorded_at
FROM candidate_source_events cse
JOIN (
    SELECT address, MIN(event_id) AS first_event_id
    FROM candidate_source_events
    WHERE source != ''
    GROUP BY address
) first_event
  ON first_event.first_event_id = cse.event_id;

CREATE INDEX IF NOT EXISTS idx_candidate_source_wallet_first_source_time
    ON candidate_source_wallet_first(source, first_recorded_at DESC);

CREATE TRIGGER IF NOT EXISTS trg_candidate_source_wallet_first_after_insert
AFTER INSERT ON candidate_source_events
WHEN NEW.source != ''
BEGIN
    INSERT OR IGNORE INTO candidate_source_wallet_first(
        address, source, first_event_id, first_observed_at, first_recorded_at
    )
    VALUES (
        NEW.address, NEW.source, NEW.event_id, NEW.observed_at, NEW.recorded_at
    );
END;
