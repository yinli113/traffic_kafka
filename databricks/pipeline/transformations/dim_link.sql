-- Dimension table: link metadata (slow-changing)
--
-- Goal: keep "all the options" about each link in one place for easy joins and dashboards.
-- We take the latest observed metadata per link_id.

CREATE OR REFRESH MATERIALIZED VIEW dim_link
AS
WITH ranked AS (
  SELECT
    CAST(get_json_object(value_str, "$.payload.id") AS INT) AS link_id,
    CAST(get_json_object(value_str, "$.payload.href") AS STRING) AS href,
    CAST(get_json_object(value_str, "$.payload.name") AS STRING) AS name,
    CAST(get_json_object(value_str, "$.payload.public_name") AS STRING) AS public_name,
    CAST(get_json_object(value_str, "$.payload.direction") AS STRING) AS direction,
    CAST(get_json_object(value_str, "$.payload.enabled") AS BOOLEAN) AS enabled,
    CAST(get_json_object(value_str, "$.payload.draft") AS BOOLEAN) AS draft,

    CAST(get_json_object(value_str, "$.payload.length") AS DOUBLE) AS length_m,
    CAST(get_json_object(value_str, "$.payload.min_number_of_lanes") AS INT) AS min_number_of_lanes,
    CAST(get_json_object(value_str, "$.payload.minimum_tt") AS DOUBLE) AS minimum_tt_seconds,
    CAST(get_json_object(value_str, "$.payload.is_freeway") AS BOOLEAN) AS is_freeway,

    CAST(get_json_object(value_str, "$.payload.origin.id") AS BIGINT) AS origin_site_id,
    CAST(get_json_object(value_str, "$.payload.destination.id") AS BIGINT) AS destination_site_id,

    -- Keep coordinates as JSON text (schema can vary and it's large)
    CAST(get_json_object(value_str, "$.payload.coordinates") AS STRING) AS coordinates_json,

    dumped_at,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(get_json_object(value_str, "$.payload.id") AS INT)
      ORDER BY dumped_at DESC
    ) AS rn
  FROM LIVE.traffic_bronze
  WHERE get_json_object(value_str, "$.payload.id") IS NOT NULL
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
  coordinates_json,
  dumped_at AS last_seen_at
FROM ranked
WHERE rn = 1;

