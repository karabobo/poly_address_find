CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_type_claim
    ON pipeline_jobs(job_type, status, shard, next_attempt_at, priority, updated_at);

CREATE INDEX IF NOT EXISTS idx_wallet_activity_asset_side_time_address
    ON wallet_activity(asset_id, side, timestamp, address);
