CREATE INDEX IF NOT EXISTS idx_copy_pair_stats_near_miss
    ON copy_pair_stats(qualifies, leader_wallet, copy_event_count DESC, copy_market_count DESC);

CREATE INDEX IF NOT EXISTS idx_copy_pair_stats_quality_fields
    ON copy_pair_stats(copy_event_count, copy_market_count, containment_pct, leader_precedes_pct);
