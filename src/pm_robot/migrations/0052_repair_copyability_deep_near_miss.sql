INSERT INTO leader_scores(
    address,
    leader_score,
    review_stage,
    review_reason,
    components_json,
    penalties_json,
    policy_version,
    scored_at
)
SELECT
    latest_score.address,
    latest_score.leader_score,
    latest_score.review_stage,
    'copyability_deep_scan_unvalidated',
    latest_score.components_json,
    latest_score.penalties_json,
    latest_score.policy_version,
    MAX(CAST(strftime('%s', 'now') AS INTEGER), latest_score.scored_at + 1)
FROM leader_scores AS latest_score
JOIN (
    SELECT address, MAX(score_id) AS score_id
    FROM leader_scores
    GROUP BY address
) AS latest
  ON latest.score_id = latest_score.score_id
JOIN candidate_wallets AS candidate
  ON candidate.address = latest_score.address
JOIN wallet_features AS feature
  ON feature.address = latest_score.address
JOIN pipeline_jobs AS copy_job
  ON copy_job.job_id = (
      SELECT MAX(job.job_id)
      FROM pipeline_jobs AS job
      WHERE job.job_type = 'copyability_evidence'
        AND job.wallet = latest_score.address
  )
WHERE candidate.candidate_stage = 'needs_data'
  AND latest_score.review_stage = 'needs_data'
  AND latest_score.review_reason = 'score_below_watchlist_after_evidence'
  AND copy_job.status = 'done'
  AND COALESCE(
          json_extract(copy_job.output_json, '$.graph_scan_mode'),
          json_extract(copy_job.input_json, '$.graph_scan_mode'),
          'default'
      ) IN ('default', 'deep')
  AND COALESCE(json_extract(copy_job.output_json, '$.graph.pair_stats_written'), 0) > 0
  AND COALESCE(json_extract(copy_job.output_json, '$.graph.qualified_pairs'), -1) = 0
  AND (
         COALESCE(json_extract(feature.extra_json, '$.copy_candidate_pair_count'), 0) > 0
      OR COALESCE(json_extract(feature.extra_json, '$.copy_candidate_event_count'), 0) > 0
      OR COALESCE(json_extract(feature.extra_json, '$.copy_candidate_market_count'), 0) > 0
  )
  AND NOT EXISTS (
      SELECT 1
      FROM copy_pair_stats AS pair
      WHERE pair.leader_wallet = latest_score.address
        AND pair.qualifies = 1
  );
