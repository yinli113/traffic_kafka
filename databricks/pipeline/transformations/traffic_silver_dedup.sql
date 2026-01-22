-- Silver (deduplicated) - DLT-native
--
-- NOTE: ROW_NUMBER() window functions are NOT supported on streaming tables in DLT SQL.
-- Use APPLY CHANGES INTO (SCD Type 1) to keep the latest record per natural key.
--
-- This keeps the newest record (by dumped_at) for each (link_id, interval_start).

-- Some DLT SQL environments require an explicit schema for the target table.
-- Keep this aligned with `traffic_silver.sql`.
CREATE OR REFRESH STREAMING TABLE traffic_silver_dedup
(
  link_id INT,
  road_name STRING,
  enabled BOOLEAN,
  draft BOOLEAN,
  interval_start TIMESTAMP,
  travel_time_seconds DOUBLE,
  delay_seconds DOUBLE,
  speed_kmh DOUBLE,
  enough_data BOOLEAN,
  ignored BOOLEAN,
  closed BOOLEAN,
  expected_missing BOOLEAN,
  length_m DOUBLE,
  minimum_tt_seconds DOUBLE,
  travel_time_minutes DOUBLE,
  free_flow_speed_kmh DOUBLE,
  potential_incident BOOLEAN
);

APPLY CHANGES INTO LIVE.traffic_silver_dedup
FROM STREAM(LIVE.traffic_silver)
KEYS (link_id, interval_start)
SEQUENCE BY dumped_at
COLUMNS * EXCEPT (dumped_at)
;

