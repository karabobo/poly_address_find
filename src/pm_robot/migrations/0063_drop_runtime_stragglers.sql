-- Remove one-off and retired runtime tables found in historical production databases.
DROP TABLE IF EXISTS live_canary_events;
DROP TABLE IF EXISTS pipeline_metadata;
DROP TABLE IF EXISTS repair_score_overwrite_20260703;
DROP TABLE IF EXISTS tmp_write_probe;
