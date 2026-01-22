-- Silver (deduplicated) - DLT-native
--
-- NOTE: ROW_NUMBER() window functions are NOT supported on streaming tables in DLT SQL.
-- Use APPLY CHANGES INTO (SCD Type 1) to keep the latest record per natural key.
--
-- This keeps the newest record (by dumped_at) for each (link_id, interval_start).

CREATE OR REFRESH STREAMING TABLE traffic_silver_dedup;

APPLY CHANGES INTO LIVE.traffic_silver_dedup
FROM STREAM(LIVE.traffic_silver)
KEYS (link_id, interval_start)
SEQUENCE BY dumped_at
COLUMNS *
;

