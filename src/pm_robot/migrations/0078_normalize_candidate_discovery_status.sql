-- Keep candidate status categorical; discovery timestamps live in dedicated columns.
UPDATE candidate_wallets
SET status = substr(status, 1, instr(status, ':') - 1)
WHERE (
        status LIKE 'activity_discovered:%'
        OR status LIKE 'rtds_activity_discovered:%'
        OR status LIKE 'leaderboard_discovered:%'
      )
  AND substr(status, instr(status, ':') + 1) NOT GLOB '*[^0-9]*'
  AND substr(status, instr(status, ':') + 1) <> '';

UPDATE candidate_source_events
SET status = substr(status, 1, instr(status, ':') - 1)
WHERE (
        status LIKE 'activity_discovered:%'
        OR status LIKE 'rtds_activity_discovered:%'
        OR status LIKE 'leaderboard_discovered:%'
      )
  AND substr(status, instr(status, ':') + 1) NOT GLOB '*[^0-9]*'
  AND substr(status, instr(status, ':') + 1) <> '';
