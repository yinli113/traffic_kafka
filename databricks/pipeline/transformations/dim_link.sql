-- Dimension table: link metadata (slow-changing)
--
-- Goal: keep "all the options" about each link in one place for easy joins and dashboards.
-- We take the latest observed metadata per link_id.

CREATE OR REFRESH MATERIALIZED VIEW dim_link
AS
WITH ranked AS (
  SELECT
    CAST(value_json.payload.id AS INT) AS link_id,
    CAST(value_json.payload.href AS STRING) AS href,
    CAST(value_json.payload.name AS STRING) AS name,
    CAST(value_json.payload.public_name AS STRING) AS public_name,
    CAST(value_json.payload.direction AS STRING) AS direction,
    CAST(value_json.payload.enabled AS BOOLEAN) AS enabled,
    CAST(value_json.payload.draft AS BOOLEAN) AS draft,

    CAST(value_json.payload.length AS DOUBLE) AS length_m,
    CAST(value_json.payload.min_number_of_lanes AS INT) AS min_number_of_lanes,
    CAST(value_json.payload.minimum_tt AS DOUBLE) AS minimum_tt_seconds,
    CAST(value_json.payload.is_freeway AS BOOLEAN) AS is_freeway,

    CAST(value_json.payload.origin.id AS BIGINT) AS origin_site_id,
    CAST(value_json.payload.destination.id AS BIGINT) AS destination_site_id,

    -- Keep as-is for mapping/heatmap use cases
    value_json.payload.coordinates AS coordinates,

    dumped_at,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(value_json.payload.id AS INT)
      ORDER BY dumped_at DESC
    ) AS rn
  FROM LIVE.traffic_bronze
  WHERE value_json.payload.id IS NOT NULL
)
SELECT
  link_id,
  href,
  name,
  public_name,
  direction,
  enabled,
  draft,
  length_m,
  min_number_of_lanes,
  minimum_tt_seconds,
  is_freeway,
  origin_site_id,
  destination_site_id,
  coordinates,
  dumped_at AS last_seen_at
FROM ranked
WHERE rn = 1;

