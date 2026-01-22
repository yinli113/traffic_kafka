-- Bronze streaming table
--
-- Source is the table created from your uploaded JSONL dump.
-- Adjust the source table name if your upload created a different one.

-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE workspace.default.traffic_bronze
(
  CONSTRAINT valid_partition EXPECT (partition >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_offset EXPECT (offset >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT non_null_topic EXPECT (topic IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT non_null_value_str EXPECT (value_str IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  CAST(topic AS STRING) AS topic,
  CAST(partition AS INT) AS partition,
  CAST(offset AS BIGINT) AS offset,
  CAST(timestamp AS STRING) AS kafka_timestamp_raw,
  CAST(key AS STRING) AS kafka_key,
  CAST(value AS STRING) AS value_str,
  CAST(dumped_at AS TIMESTAMP) AS dumped_at,
  -- envelope timestamps
  CAST(get_json_object(value_str, "$.ingested_at") AS TIMESTAMP) AS ingested_at,
  CAST(get_json_object(value_str, "$.payload.latest_stats.interval_start") AS TIMESTAMP) AS interval_start
FROM STREAM(workspace.default.traffic_raw_file);
