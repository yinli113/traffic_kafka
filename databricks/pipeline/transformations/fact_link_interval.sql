-- Fact table: time-series measurements per link per interval
--
-- Natural primary key: (link_id, interval_start)
-- Source: deduplicated silver stream + link dimension for stable attributes.

CREATE OR REFRESH STREAMING TABLE fact_link_interval
AS
SELECT
  s.link_id,
  s.interval_start,

  -- Measurements
  s.travel_time_seconds,
  s.delay_seconds,
  s.speed_kmh,

  -- Provider metrics (from the raw payload; present when latest_stats is present)
  CAST(b.value_json.payload.latest_stats.density AS DOUBLE) AS density,
  CAST(b.value_json.payload.latest_stats.average_density AS DOUBLE) AS average_density,
  CAST(b.value_json.payload.latest_stats.congestion AS DOUBLE) AS congestion_score,
  CAST(b.value_json.payload.latest_stats.estimated_percent AS DOUBLE) AS estimated_percent,

  -- Quality flags
  s.enough_data,
  s.ignored,
  s.closed,
  s.expected_missing,

  -- Derived fields
  s.travel_time_minutes,
  s.free_flow_speed_kmh,
  s.potential_incident,

  -- Dimensions (join for dashboard convenience)
  d.name AS road_name,
  d.direction,
  d.is_freeway,
  d.length_m,
  d.minimum_tt_seconds,
  d.origin_site_id,
  d.destination_site_id,

  -- Pipeline monitoring
  s.dumped_at,
  -- End-to-end latency proxy (event_time -> ingestion)
  (unix_timestamp(s.dumped_at) - unix_timestamp(s.interval_start)) AS event_to_dumped_latency_seconds

FROM STREAM(LIVE.traffic_silver_dedup) s
LEFT JOIN LIVE.dim_link d
  ON d.link_id = s.link_id
-- Join to bronze for fields we didn't carry into silver (density/congestion/etc)
LEFT JOIN LIVE.traffic_bronze b
  ON CAST(b.value_json.payload.id AS INT) = s.link_id
 AND CAST(b.value_json.payload.latest_stats.interval_start AS TIMESTAMP) = s.interval_start;

