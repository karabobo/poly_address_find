CREATE TABLE IF NOT EXISTS candidate_source_wallet_latest (
    source TEXT NOT NULL,
    address TEXT NOT NULL REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    latest_recorded_at INTEGER NOT NULL,
    latest_observed_at INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    PRIMARY KEY(source, address)
);

INSERT INTO candidate_source_wallet_latest(
    source, address, latest_recorded_at, latest_observed_at, event_count
)
SELECT
    source,
    address,
    MAX(recorded_at) AS latest_recorded_at,
    MAX(observed_at) AS latest_observed_at,
    COUNT(*) AS event_count
FROM candidate_source_events
WHERE source != ''
GROUP BY source, address
ON CONFLICT(source, address) DO UPDATE SET
    latest_recorded_at = excluded.latest_recorded_at,
    latest_observed_at = excluded.latest_observed_at,
    event_count = excluded.event_count;

CREATE INDEX IF NOT EXISTS idx_source_wallet_latest_source_time
    ON candidate_source_wallet_latest(source, latest_recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_source_wallet_latest_address
    ON candidate_source_wallet_latest(address);

CREATE TRIGGER IF NOT EXISTS trg_candidate_source_wallet_latest_after_insert
AFTER INSERT ON candidate_source_events
WHEN NEW.source != ''
BEGIN
    INSERT INTO candidate_source_wallet_latest(
        source, address, latest_recorded_at, latest_observed_at, event_count
    )
    VALUES (NEW.source, NEW.address, NEW.recorded_at, NEW.observed_at, 1)
    ON CONFLICT(source, address) DO UPDATE SET
        latest_recorded_at = MAX(candidate_source_wallet_latest.latest_recorded_at, excluded.latest_recorded_at),
        latest_observed_at = MAX(candidate_source_wallet_latest.latest_observed_at, excluded.latest_observed_at),
        event_count = candidate_source_wallet_latest.event_count + 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_candidate_source_wallet_latest_after_update
AFTER UPDATE ON candidate_source_events
BEGIN
    DELETE FROM candidate_source_wallet_latest
    WHERE source = OLD.source
      AND address = OLD.address;

    INSERT INTO candidate_source_wallet_latest(
        source, address, latest_recorded_at, latest_observed_at, event_count
    )
    SELECT
        source,
        address,
        MAX(recorded_at) AS latest_recorded_at,
        MAX(observed_at) AS latest_observed_at,
        COUNT(*) AS event_count
    FROM candidate_source_events
    WHERE source = OLD.source
      AND address = OLD.address
      AND source != ''
    GROUP BY source, address;

    DELETE FROM candidate_source_wallet_latest
    WHERE source = NEW.source
      AND address = NEW.address;

    INSERT INTO candidate_source_wallet_latest(
        source, address, latest_recorded_at, latest_observed_at, event_count
    )
    SELECT
        source,
        address,
        MAX(recorded_at) AS latest_recorded_at,
        MAX(observed_at) AS latest_observed_at,
        COUNT(*) AS event_count
    FROM candidate_source_events
    WHERE source = NEW.source
      AND address = NEW.address
      AND source != ''
    GROUP BY source, address;
END;

CREATE TRIGGER IF NOT EXISTS trg_candidate_source_wallet_latest_after_delete
AFTER DELETE ON candidate_source_events
BEGIN
    DELETE FROM candidate_source_wallet_latest
    WHERE source = OLD.source
      AND address = OLD.address;

    INSERT INTO candidate_source_wallet_latest(
        source, address, latest_recorded_at, latest_observed_at, event_count
    )
    SELECT
        source,
        address,
        MAX(recorded_at) AS latest_recorded_at,
        MAX(observed_at) AS latest_observed_at,
        COUNT(*) AS event_count
    FROM candidate_source_events
    WHERE source = OLD.source
      AND address = OLD.address
      AND source != ''
    GROUP BY source, address;
END;
