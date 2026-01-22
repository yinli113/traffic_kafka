-- Silver streaming table
--
-- This is intentionally "exploration-friendly": it keeps enough_data/flags as columns
-- but does not require enough_data=true (your sample file often has enough_data=false).

CREATE OR REFRESH STREAMING TABLE workspace.default.traffic_silver
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
  dumped_at TIMESTAMP,
  travel_time_minutes DOUBLE,
  free_flow_speed_kmh DOUBLE,
  potential_incident BOOLEAN,
  -- If latest_stats is missing/null, these derived fields become NULL.
  -- Dropping NULL interval_start rows is a simple way to remove those incomplete records.
  CONSTRAINT non_null_interval_start EXPECT (interval_start IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_travel_time EXPECT (travel_time_seconds >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_speed EXPECT (speed_kmh >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_length EXPECT (length_m > 0) ON VIOLATION DROP ROW
)
AS
WITH base AS (
  SELECT
    CAST(value_json.payload.id AS INT)                                AS link_id,
    CAST(value_json.payload.name AS STRING)                           AS road_name,
    CAST(value_json.payload.enabled AS BOOLEAN)                       AS enabled,
    CAST(value_json.payload.draft AS BOOLEAN)                         AS draft,

    CAST(value_json.payload.latest_stats.interval_start AS TIMESTAMP) AS interval_start,
    CAST(value_json.payload.latest_stats.travel_time AS DOUBLE)       AS travel_time_seconds,
    CAST(value_json.payload.latest_stats.delay AS DOUBLE)             AS delay_seconds,
    CAST(value_json.payload.latest_stats.speed AS DOUBLE)             AS speed_kmh,

    CAST(value_json.payload.latest_stats.enough_data AS BOOLEAN)      AS enough_data,
    CAST(value_json.payload.latest_stats.ignored AS BOOLEAN)          AS ignored,
    CAST(value_json.payload.latest_stats.closed AS BOOLEAN)           AS closed,
    CAST(value_json.payload.latest_stats.expected_missing AS BOOLEAN) AS expected_missing,

    CAST(value_json.payload.length AS DOUBLE)                         AS length_m,
    CAST(value_json.payload.minimum_tt AS DOUBLE)                     AS minimum_tt_seconds,

    dumped_at
  FROM STREAM(LIVE.traffic_bronze)
)
SELECT
  *,
  travel_time_seconds / 60.0 AS travel_time_minutes,
  (length_m / NULLIF(minimum_tt_seconds, 0)) * 3.6 AS free_flow_speed_kmh,
  CASE
    WHEN speed_kmh IS NOT NULL 
         AND minimum_tt_seconds > 0 
         AND length_m IS NOT NULL
         AND speed_kmh < 0.7 * free_flow_speed_kmh
         AND speed_kmh < length_m / minimum_tt_seconds
    THEN true 
    ELSE false 
  END AS potential_incident
FROM base
WHERE enabled = true
  AND COALESCE(draft, false) = false
  AND COALESCE(ignored, false) = false
  AND COALESCE(closed, false) = false
  AND COALESCE(expected_missing, false) = false;
