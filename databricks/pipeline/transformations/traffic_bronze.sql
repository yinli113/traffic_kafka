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
  topic,
  CAST(partition AS INT)   AS partition,
  CAST(offset AS BIGINT)   AS offset,
  CAST(key AS STRING)      AS kafka_key,
  CAST(value AS STRING)    AS value_str,
  value_json,
  dumped_at
FROM STREAM(LIVE.traffic_raw_source);
