ALTER TABLE pipeline_jobs ADD COLUMN terminal_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE pipeline_jobs ADD COLUMN terminal_at INTEGER;
ALTER TABLE pipeline_jobs ADD COLUMN terminal_policy_version TEXT NOT NULL DEFAULT '';

UPDATE pipeline_jobs
SET status = 'terminal_failed',
    terminal_reason = 'wallet_history_data_quality',
    terminal_at = COALESCE(updated_at, next_attempt_at, 0),
    terminal_policy_version = ''
WHERE job_type = 'wallet_history_collect'
  AND status = 'failed'
  AND attempts >= max_attempts
  AND (
        LOWER(COALESCE(last_error, '')) LIKE '%incompatible history data%'
     OR LOWER(COALESCE(last_error, '')) LIKE '%artifact depth%'
     OR LOWER(COALESCE(last_error, '')) LIKE '%cannot replace deep history with light history%'
  );

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_terminal_action
    ON pipeline_jobs(job_type, wallet, job_scope, job_action, status)
    WHERE status = 'terminal_failed';
