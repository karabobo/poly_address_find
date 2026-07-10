CREATE INDEX IF NOT EXISTS idx_candidate_source_events_source_address_time
    ON candidate_source_events(source, address, recorded_at DESC);
